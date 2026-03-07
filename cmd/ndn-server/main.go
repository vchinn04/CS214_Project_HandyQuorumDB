package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	kvndn "github.com/nStangl/distributed-kv-store/ndn"
	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/ecs"
	dbLog "github.com/nStangl/distributed-kv-store/server/log"
	"github.com/nStangl/distributed-kv-store/server/memtable"
	"github.com/nStangl/distributed-kv-store/server/replication"
	"github.com/nStangl/distributed-kv-store/server/sstable"
	"github.com/nStangl/distributed-kv-store/server/store"
)

const version = "0.1.0"

var (
	serverID  string
	directory string
	logfile   string
	loglevel  string
	bootstrap string
	ndnPort   uint16

	rootCmd = &cobra.Command{
		Use:     "ndn-server",
		Short:   "NDN-based KV store server",
		Long:    "A KV store server that uses Named Data Networking (NDN) as the network layer.\nEach server starts its own embedded NDN forwarder (YaNFD).",
		Version: version,
		RunE:    run,
	}
)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)

	rootCmd.PersistentFlags().StringVarP(&serverID, "id", "i", "server1", "Server ID used in the NDN namespace (/kv/<id>)")
	rootCmd.PersistentFlags().StringVarP(&directory, "directory", "d", "db-data", "Data directory for persistence")
	rootCmd.PersistentFlags().StringVarP(&logfile, "logfile", "l", "log.db", "Relative path of the DB log file")
	rootCmd.PersistentFlags().StringVarP(&loglevel, "loglevel", "o", "ALL", "Log level: ALL, DEBUG, INFO, WARN, ERROR")
	rootCmd.PersistentFlags().StringVarP(&bootstrap, "bootstrap", "b", "127.0.0.1:9090", "ECS bootstrap address (TCP)")
	rootCmd.PersistentFlags().Uint16VarP(&ndnPort, "ndn-port", "p", 6363, "NDN forwarder TCP/UDP port for inter-server traffic")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.SilenceUsage = true
	setLogLevel(loglevel)

	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// --- Embedded NDN Forwarder ---
	// Each server runs its own YaNFD instance with a unique Unix socket
	// and unique TCP/UDP ports for inter-server connectivity.

	fwCfg := kvndn.DefaultForwarderConfig(serverID, ndnPort)
	fwCfg.LogLevel = "WARN"

	forwarder := kvndn.NewForwarder(fwCfg)
	if err := forwarder.Start(); err != nil {
		return fmt.Errorf("failed to start embedded NDN forwarder: %w", err)
	}
	defer forwarder.Stop()

	log.Infof("embedded NDN forwarder started (socket: %s, port: %d)", forwarder.SocketPath(), ndnPort)

	// --- NDN Engine (connects to the embedded forwarder) ---

	ndnEngine, err := kvndn.NewEngineForForwarder(forwarder)
	if err != nil {
		return fmt.Errorf("failed to start NDN engine: %w", err)
	}
	defer ndnEngine.Stop()

	log.Info("NDN engine connected to embedded forwarder")

	// --- Storage layer (same as TCP server) ---

	lg, err := dbLog.NewSeeking(filepath.Join(directory, logfile))
	if err != nil {
		return fmt.Errorf("failed to initiate db commit log: %w", err)
	}

	ssTableManager, err := sstable.NewManager(directory, 4)
	if err != nil {
		return fmt.Errorf("failed to initiate sstable manager: %w", err)
	}
	if err := ssTableManager.Startup(); err != nil {
		return fmt.Errorf("failed to start sstable manager: %w", err)
	}

	go func(ers <-chan error) {
		for err := range ers {
			log.Printf("sstable manager error: %v", err)
		}
	}(ssTableManager.Process())

	kvStore := store.New(lg, ssTableManager, func() memtable.Table {
		return memtable.NewRedBlackTree()
	})

	// --- Replication manager ---

	r := replication.NewManager(lg)
	rdone := r.Start(ctx)

	// --- NDN KV server ---

	signer := kvndn.NewSha256Signer()
	ndnServer, err := kvndn.NewServer(ndnEngine, signer, kvStore, serverID)
	if err != nil {
		return fmt.Errorf("failed to create NDN server: %w", err)
	}

	if err := ndnServer.Start(); err != nil {
		return fmt.Errorf("failed to start NDN server: %w", err)
	}

	log.Infof("NDN KV server ready on prefix /kv/%s", serverID)

	// --- ECS client (still TCP-based for coordination) ---

	ecsClient, err := ecs.NewClient(bootstrap)
	if err != nil {
		return fmt.Errorf("failed to create ECS client: %w", err)
	}

	log.Info("connected to ECS via TCP at ", bootstrap)

	ecsClient.SetCallbacks(&ndnECSCallbacks{
		server:     ndnServer,
		replicator: r,
	})

	go func(ers <-chan error) {
		for err := range ers {
			log.Printf("ECS client error: %v", err)
		}
	}(ecsClient.Listen())

	go func(ers <-chan error) {
		for err := range ers {
			log.Printf("ECS heartbeat error: %v", err)
		}
	}(ecsClient.SendHeartbeats())

	if err := ecsClient.Transition(ctx, &protocol.ECSMessage{
		Type:           protocol.JoinNetwork,
		Address:        fmt.Sprintf("ndn:/kv/%s", serverID),
		PrivateAddress: fmt.Sprintf("ndn:/kv/%s", serverID),
	}); err != nil {
		return fmt.Errorf("failed to join network via ECS: %w", err)
	}

	log.Info("joined ECS network")

	// --- Wait for shutdown ---

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Info("shutting down...")
		cancel()
		if err := ndnServer.Stop(); err != nil {
			log.Errorf("error stopping NDN server: %v", err)
		}
		ecsClient.Close()
	}()

	<-ctx.Done()
	<-rdone
	log.Info("NDN KV server stopped")

	return nil
}

// ndnECSCallbacks bridges ECS events to the NDN server.
type ndnECSCallbacks struct {
	server     *kvndn.Server
	replicator *replication.Manager
}

func (c *ndnECSCallbacks) OnWriteLockSet() error {
	c.server.SetWriteLock(true)
	log.Info("write lock set")
	return nil
}

func (c *ndnECSCallbacks) OnWriteLockLifted() error {
	c.server.SetWriteLock(false)
	log.Info("write lock lifted")
	return nil
}

func (c *ndnECSCallbacks) OnTransferRequested(meta protocol.Metadata) error {
	log.Warn("NDN-based transfer not yet implemented; falling back to no-op")
	return nil
}

func (c *ndnECSCallbacks) OnMetadataUpdated(meta protocol.Metadata) error {
	c.server.SetMetadata(meta)
	log.Infof("metadata updated: %s", meta)
	return nil
}

func setLogLevel(level string) {
	switch strings.ToLower(level) {
	case "all", "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
	log.SetOutput(os.Stderr)
}
