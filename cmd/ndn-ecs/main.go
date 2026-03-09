package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	kvndn "github.com/nStangl/distributed-kv-store/ndn"
)

const version = "0.1.0"

var (
	ecsPort  uint16
	loglevel string

	rootCmd = &cobra.Command{
		Use:     "ndn-ecs",
		Short:   "NDN-based Elastic Configuration Service",
		Long:    "ECS coordinator using Named Data Networking.\nRuns an embedded NDN forwarder and handles server coordination via NDN Interests/Data.",
		Version: version,
		RunE:    run,
	}
)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)

	rootCmd.PersistentFlags().Uint16VarP(&ecsPort, "port", "p", 9090, "NDN forwarder TCP/UDP port")
	rootCmd.PersistentFlags().StringVarP(&loglevel, "loglevel", "o", "ALL", "Log level: ALL, DEBUG, INFO, WARN, ERROR")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	setLogLevel(loglevel)

	// --- Embedded NDN Forwarder ---
	fwCfg := kvndn.DefaultForwarderConfig("ecs", ecsPort)
	fwCfg.LogLevel = "WARN"

	forwarder := kvndn.NewForwarder(fwCfg)
	if err := forwarder.Start(); err != nil {
		return fmt.Errorf("failed to start ECS forwarder: %w", err)
	}
	defer forwarder.Stop()

	log.Infof("ECS forwarder started (socket: %s, port: %d)", forwarder.SocketPath(), ecsPort)

	// --- NDN Engine ---
	engine, err := kvndn.NewEngineForForwarder(forwarder)
	if err != nil {
		return fmt.Errorf("failed to start ECS engine: %w", err)
	}
	defer engine.Stop()

	// --- ECS Coordinator ---
	signer := kvndn.NewSha256Signer()
	ecs, err := kvndn.NewECSCoordinator(engine, signer, true)
	if err != nil {
		return fmt.Errorf("failed to create ECS coordinator: %w", err)
	}

	if err := ecs.Start(); err != nil {
		return fmt.Errorf("failed to start ECS coordinator: %w", err)
	}

	log.Infof("NDN ECS coordinator is ready on port %d", ecsPort)

	// --- Graceful shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Info("shutting down ECS...")
	if err := ecs.Stop(); err != nil {
		log.Errorf("error stopping ECS: %v", err)
	}

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
