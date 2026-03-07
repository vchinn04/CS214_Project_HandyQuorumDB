package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	kvndn "github.com/nStangl/distributed-kv-store/ndn"

	kvclient "github.com/nStangl/distributed-kv-store/client"
)

const version = "0.1.0"

var (
	clientID   string
	clientPort uint16
	serverID   string
	serverAddr string
	loglevel   string

	rootCmd = &cobra.Command{
		Use:   "ndn-client",
		Short: "NDN-based KV store client",
		Long: `Interactive client for the NDN-based KV store.
Connects to a server's embedded NDN forwarder via TCP.`,
		Version: version,
		RunE:    run,
	}
)

func init() {
	log.SetLevel(log.WarnLevel)
	log.SetOutput(os.Stderr)

	rootCmd.PersistentFlags().StringVarP(&serverID, "server", "s", "server1", "Target server ID in the NDN namespace")
	rootCmd.PersistentFlags().StringVarP(&serverAddr, "addr", "a", "localhost:6363", "Server's NDN forwarder TCP address (host:port)")
	rootCmd.PersistentFlags().StringVarP(&loglevel, "loglevel", "l", "WARN", "Log level: DEBUG, INFO, WARN, ERROR")

	rootCmd.PersistentFlags().StringVarP(&clientID, "id", "i", "client1", "Client NDN node ID")
	rootCmd.PersistentFlags().Uint16VarP(&clientPort, "ndn-port", "p", 6364, "Client's own NDN forwarder port")
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

	fwCfg := kvndn.DefaultForwarderConfig(clientID, clientPort)
	fwCfg.LogLevel = "WARN"

	forwarder := kvndn.NewForwarder(fwCfg)
	if err := forwarder.Start(); err != nil {
		return fmt.Errorf("failed to start embedded NDN forwarder: %w", err)
	}
	defer forwarder.Stop()

	ndnEngine, err := kvndn.NewEngineForForwarder(forwarder)
	if err != nil {
		return fmt.Errorf("failed to start NDN engine: %w", err)
	}
	defer ndnEngine.Stop()

	log.Info("NDN engine connected to embedded forwarder")

	// Add a face from the client's forwarder to the server's forwarder and
	// register a /kv route over it. This is what makes Interests for
	// /kv/<server-id>/... leave the client node and reach the server node.
	serverURI := "tcp4://" + serverAddr
	if err := kvndn.AddUpstreamRoute(ndnEngine, "/kv", serverURI); err != nil {
		return fmt.Errorf("failed to add upstream route to server at %s: %w", serverURI, err)
	}

	log.Infof("upstream route /kv → %s established", serverURI)

	client := kvndn.NewClient(ndnEngine, serverID)

	fmt.Printf("NDN KV Client connected (server: /kv/%s via %s)\n", serverID, serverAddr)
	fmt.Println("Commands: put <key> <value> | get <key> | delete <key> | keyrange | quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("ndn-kv> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		command := strings.ToLower(parts[0])

		switch command {
		case "quit", "exit", "q":
			fmt.Println("Goodbye.")
			return nil

		case "get":
			err := kvclient.ValidateInput(parts, 2)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			resp, err := client.Get(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			printResponse("GET", resp)

		case "put":
			err := kvclient.ValidateInput(parts, 3)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			resp, err := client.Put(parts[1], parts[2])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			printResponse("PUT", resp)

		case "delete", "del":
			err := kvclient.ValidateInput(parts, 2)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			resp, err := client.Delete(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			printResponse("DELETE", resp)

		case "keyrange":
			err := kvclient.ValidateInput(parts, 1)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			resp, err := client.Keyrange()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			printResponse("KEYRANGE", resp)

		case "keyrange_read":
			err := kvclient.ValidateInput(parts, 1)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			resp, err := client.KeyrangeRead()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			printResponse("KEYRANGE_READ", resp)

		case "help":
			fmt.Println("Commands:")
			fmt.Println("  put <key> <value>  - Store a key-value pair")
			fmt.Println("  get <key>          - Retrieve a value by key")
			fmt.Println("  delete <key>       - Delete a key")
			fmt.Println("  keyrange           - Show key range metadata")
			fmt.Println("  keyrange_read      - Show readable key ranges")
			fmt.Println("  quit               - Exit the client")

		default:
			fmt.Printf("Unknown command: %s (type 'help' for usage)\n", command)
		}
	}

	return nil
}

func printResponse(op string, resp *kvndn.Response) {
	switch resp.Status {
	case kvndn.StatusSuccess:
		if resp.Value != "" {
			fmt.Printf("%s OK: %s = %s\n", op, resp.Key, resp.Value)
		} else if resp.Key != "" {
			fmt.Printf("%s OK: %s\n", op, resp.Key)
		} else {
			fmt.Printf("%s OK\n", op)
		}
	case kvndn.StatusUpdate:
		fmt.Printf("%s UPDATED: %s\n", op, resp.Key)
	case kvndn.StatusNotFound:
		fmt.Printf("%s NOT FOUND: %s\n", op, resp.Key)
	case kvndn.StatusNotResponsible:
		fmt.Printf("%s SERVER NOT RESPONSIBLE\n", op)
	case kvndn.StatusWriteLocked:
		fmt.Printf("%s WRITE LOCKED\n", op)
	case kvndn.StatusError:
		fmt.Printf("%s ERROR: %s\n", op, resp.Error)
	default:
		fmt.Printf("%s [%s]: key=%s value=%s\n", op, resp.Status, resp.Key, resp.Value)
	}
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
		log.SetLevel(log.WarnLevel)
	}
}
