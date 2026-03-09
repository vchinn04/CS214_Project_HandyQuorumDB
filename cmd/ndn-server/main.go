package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"lukechampine.com/uint128"

	kvndn "github.com/nStangl/distributed-kv-store/ndn"
	"github.com/nStangl/distributed-kv-store/protocol"
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
	ecsAddr   string
	ndnPort   uint16

	rootCmd = &cobra.Command{
		Use:     "ndn-server",
		Short:   "NDN-based KV store server",
		Long:    "A KV store server that uses Named Data Networking (NDN) as the network layer.\nEach server starts its own embedded NDN forwarder (YaNFD) and communicates\nwith the NDN-based ECS coordinator for cluster coordination.",
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
	rootCmd.PersistentFlags().StringVarP(&ecsAddr, "ecs-addr", "e", "localhost:9090", "NDN ECS forwarder TCP address (host:port)")
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

	fwCfg := kvndn.DefaultForwarderConfig(serverID, ndnPort)
	fwCfg.LogLevel = "WARN"

	forwarder := kvndn.NewForwarder(fwCfg)
	if err := forwarder.Start(); err != nil {
		return fmt.Errorf("failed to start embedded NDN forwarder: %w", err)
	}
	defer forwarder.Stop()

	log.Infof("embedded NDN forwarder started (socket: %s, port: %d)", forwarder.SocketPath(), ndnPort)

	// --- NDN Engine for KV server (connects to own embedded forwarder) ---

	ndnEngine, err := kvndn.NewEngineForForwarder(forwarder)
	if err != nil {
		return fmt.Errorf("failed to start NDN engine: %w", err)
	}
	defer ndnEngine.Stop()

	log.Info("NDN engine connected to embedded forwarder")

	// --- Storage layer ---

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

	// --- NDN KV server ---

	signer := kvndn.NewSha256Signer()
	ndnServer, err := kvndn.NewServer(ndnEngine, signer, kvStore, serverID)
	if err != nil {
		return fmt.Errorf("failed to create NDN server: %w", err)
	}

	// --- Replication manager ---

	repSender := &ndnReplicationSender{server: ndnServer}
	r := replication.NewManager(lg, repSender)
	rdone := r.Start(ctx)

	if err := ndnServer.Start(); err != nil {
		return fmt.Errorf("failed to start NDN server: %w", err)
	}

	// --- NDN ECS client ---

	ecsEngine, err := kvndn.NewEngine(kvndn.Config{
		FaceType: "tcp",
		FaceAddr: ecsAddr,
		ServerID: serverID,
	})
	if err != nil {
		return fmt.Errorf("failed to create ECS client: %w", err)
	}
	defer ecsEngine.Stop()

	log.Infof("connected to NDN ECS forwarder at %s", ecsAddr)

	ecsClient := kvndn.NewECSClient(ecsEngine, serverID, fmt.Sprintf("localhost:%d", ndnPort))
	ecsCallbacks := &ndnECSCallbacks{
		server:     ndnServer,
		ecsClient:  ecsClient,
		store:      kvStore,
		replicator: r,
	}
	ecsClient.SetCallbacks(ecsCallbacks)

	if err := ecsClient.Join(); err != nil {
		return fmt.Errorf("failed to join ECS network: %w", err)
	}

	log.Info("joined ECS network via NDN")

	go func(ers <-chan error) {
		for err := range ers {
			log.Printf("ECS poll error: %v", err)
		}
	}(ecsClient.StartPolling())

	go func(ers <-chan error) {
		for err := range ers {
			log.Printf("ECS heartbeat error: %v", err)
		}
	}(ecsClient.StartHeartbeats())

	// if err := ecsClient.Transition(ctx, &protocol.ECSMessage{
	// 	Type:           protocol.JoinNetwork,
	// 	Address:        fmt.Sprintf("ndn:/kv/%s", serverID),
	// 	PrivateAddress: fmt.Sprintf("ndn:/kv/%s", serverID),
	// }); err != nil {
	// 	return fmt.Errorf("failed to join network via ECS: %w", err)
	// }

	// log.Info("joined ECS network")

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

type ndnReplicationSender struct {
	server *kvndn.Server
}

func (s *ndnReplicationSender) Replicate(replica replication.Replica, key, value string) error {
	if replica.NDNServerID == "" {
		return fmt.Errorf("missing NDN server ID for replica")
	}
	if replica.PrivateAddr == nil {
		return fmt.Errorf("missing private address for replica %s", replica.NDNServerID)
	}

	targetPrefix := fmt.Sprintf("/kv/%s", replica.NDNServerID)
	targetURI := "tcp4://" + replica.PrivateAddr.String()

	// Make sure the local embedded forwarder knows how to reach the target replica.
	if err := kvndn.AddUpstreamRoute(s.server.Engine(), targetPrefix, targetURI); err != nil {
		log.Warnf("could not add route to %s at %s: %v (proceeding anyway)",
			replica.NDNServerID, targetURI, err)
	}

	resp, err := s.server.SvReplicate(replica.NDNServerID, key, value)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("nil replication response from %s", replica.NDNServerID)
	}
	if resp.Status != kvndn.StatusSuccess {
		return fmt.Errorf("replication rejected by %s: status=%s error=%s",
			replica.NDNServerID, resp.Status, resp.Error)
	}
	return nil
}

// ndnECSCallbacks bridges NDN ECS events to the NDN KV server.
type ndnECSCallbacks struct {
	server     *kvndn.Server
	ecsClient  *kvndn.ECSClient
	store      store.Store
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

func (c *ndnECSCallbacks) OnTransferRequested(targetAddr, targetServerID, rangeStart, rangeEnd string) error {
	log.Infof("transfer requested to %s (%s) range [%s..%s]", targetServerID, targetAddr, rangeStart, rangeEnd)

	start, err := parseHexUint128(rangeStart)
	if err != nil {
		return fmt.Errorf("invalid rangeStart %q: %w", rangeStart, err)
	}
	end, err := parseHexUint128(rangeEnd)
	if err != nil {
		return fmt.Errorf("invalid rangeEnd %q: %w", rangeEnd, err)
	}

	keyRange := protocol.KeyRange{Start: start, End: end}

	// Ensure the server's embedded forwarder has a face and route to the
	// target server before we start expressing Interests toward it.
	targetURI := "tcp4://" + targetAddr
	targetPrefix := fmt.Sprintf("/kv/%s", targetServerID)
	if err := kvndn.AddUpstreamRoute(c.server.Engine(), targetPrefix, targetURI); err != nil {
		log.Warnf("could not add route to %s at %s: %v (proceeding anyway)", targetServerID, targetURI, err)
	}

	if err := kvndn.TransferRange(c.server, c.store, targetServerID, keyRange); err != nil {
		return fmt.Errorf("data transfer to %s failed: %w", targetServerID, err)
	}

	return c.ecsClient.NotifyTransferDone()
}

// parseHexUint128 decodes a big-endian hex string produced by
// util.Uint128BigEndian into a uint128.Uint128. Odd-length strings are
// left-padded with a zero nibble before decoding.
func parseHexUint128(s string) (uint128.Uint128, error) {
	if len(s)%2 != 0 {
		s = "0" + s
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return uint128.Uint128{}, fmt.Errorf("hex decode failed: %w", err)
	}
	// FromBytesBE requires exactly 16 bytes; pad on the left if shorter.
	if len(b) < 16 {
		padded := make([]byte, 16)
		copy(padded[16-len(b):], b)
		b = padded
	}
	return uint128.FromBytesBE(b), nil
}

func (c *ndnECSCallbacks) OnMetadataUpdated(meta protocol.Metadata) error {
	c.server.SetMetadata(meta)

	if err := c.replicator.Reconcile(replication.FromMetadata(meta, c.ecsClient.ID())); err != nil {
		log.Errorf("failed to reconcile new metadata for replication: %v", err)
	}

	log.Infof("metadata updated: %s", meta)
	return nil
}

func (c *ndnECSCallbacks) OnJoinComplete() error {
	log.Info("join complete — ready to serve requests")
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
