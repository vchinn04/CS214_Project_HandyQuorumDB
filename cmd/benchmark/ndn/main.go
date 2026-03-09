package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	kvndn "github.com/nStangl/distributed-kv-store/ndn"
	"github.com/nStangl/distributed-kv-store/util"
)

type Pair struct {
	key, value string
}

type Result struct {
	successful bool
	op         string
	serverID   string
	serverAddr string
	dur        time.Duration
	status     string
	errMsg     string
}

type Target struct {
	ServerID string
	Addr     string
}

type BenchEnv struct {
	forwarder *kvndn.Forwarder
	engine    kvndn.EngineCloser
	clients   map[string]*kvndn.Client
}

var (
	wg       sync.WaitGroup
	wgResult sync.WaitGroup
	results  chan Result
)

func main() {
	var (
		mode = flag.String("mode", "distributed", "Benchmark mode: single or distributed")

		op = flag.String("op", "put", "Operation: put, get, delete")

		clients = flag.Int("clients", 3, "Number of concurrent benchmark workers")

		iterations = flag.Int("iterations", 50, "Number of keys from words.txt to use")

		wordsPath = flag.String("words", "cmd/benchmark/words.txt", "Path to words file")

		out = flag.String("out", "cmd/benchmark/ndn/results.csv", "CSV output path")

		serverID = flag.String("server-id", "server1", "Server ID for single mode")

		serverAddr = flag.String("server-addr", "localhost:6363", "Server address for single mode")

		targetsArg = flag.String(
			"targets",
			"server1=localhost:6363,server2=localhost:6365,server3=localhost:6367",
			"Targets for distributed mode: server1=host:port,server2=host:port,...",
		)

		timeoutSec = flag.Int("timeout-sec", 4, "Per-request timeout in seconds")
	)
	flag.Parse()

	if *op != "put" && *op != "get" && *op != "delete" {
		log.Fatalf("unsupported -op %q; must be put, get, or delete", *op)
	}

	if *mode != "single" && *mode != "distributed" {
		log.Fatalf("unsupported -mode %q; must be single or distributed", *mode)
	}

	if *clients <= 0 {
		log.Fatal("-clients must be > 0")
	}

	if *iterations <= 0 {
		log.Fatal("-iterations must be > 0")
	}

	targets, err := buildTargets(*mode, *serverID, *serverAddr, *targetsArg)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(dirOf(*out), os.ModePerm); err != nil {
		log.Fatal(err)
	}

	words, err := util.ReadWords(*wordsPath)
	if err != nil {
		log.Fatal(err)
	}

	limit := *iterations
	if limit > len(words) {
		limit = len(words)
	}

	data := make([]Pair, 0, limit)
	for i := 0; i < limit; i++ {
		w := words[i]
		data = append(data, Pair{
			key:   w,
			value: w,
		})
	}

	timeout := time.Duration(*timeoutSec) * time.Second

	env, err := setupBenchEnv(targets, timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer env.engine.Stop()
	defer env.forwarder.Stop()

	results = make(chan Result, (*clients)*limit*2)

	wgResult.Add(1)
	go collectResults(*out)

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			target := chooseTarget(*mode, workerID, targets)
			runWorker(workerID, env, target, *op, data)
		}(i)

		fmt.Printf("Started NDN benchmark worker %d\n", i)
	}

	wg.Wait()
	close(results)
	wgResult.Wait()

	fmt.Println("NDN benchmark complete.")
	fmt.Println("CSV written to:", *out)
}

func setupBenchEnv(targets []Target, timeout time.Duration) (*BenchEnv, error) {
	fwCfg := kvndn.DefaultForwarderConfig("bench-shared-client", 0)
	fwCfg.LogLevel = "WARN"

	// Benchmark client does not need inbound TCP/UDP listeners.
	fwCfg.TCPPort = 0
	fwCfg.UDPUnicastPort = 0

	// Unique Unix socket for this benchmark run.
	fwCfg.SocketPath = filepath.Join(
		os.TempDir(),
		fmt.Sprintf("ndn-kv-bench-shared-%d.sock", time.Now().UnixNano()),
	)

	_ = os.Remove(fwCfg.SocketPath)

	forwarder := kvndn.NewForwarder(fwCfg)
	if err := forwarder.Start(); err != nil {
		return nil, fmt.Errorf("failed to start shared benchmark forwarder: %w", err)
	}

	engine, err := kvndn.NewEngineForForwarder(forwarder)
	if err != nil {
		forwarder.Stop()
		return nil, fmt.Errorf("failed to start shared benchmark engine: %w", err)
	}

	clients := make(map[string]*kvndn.Client)

	for _, target := range targets {
		serverURI := "tcp4://" + target.Addr

		// IMPORTANT:
		// Route each server-specific prefix to the matching server forwarder.
		// This avoids ambiguous routing like "/kv -> all servers".
		targetPrefix := fmt.Sprintf("/kv/%s", target.ServerID)

		if err := kvndn.AddUpstreamRoute(engine, targetPrefix, serverURI); err != nil {
			engine.Stop()
			forwarder.Stop()
			return nil, fmt.Errorf(
				"failed to add upstream route for %s (%s): %w",
				target.ServerID, target.Addr, err,
			)
		}

		c := kvndn.NewClient(engine, target.ServerID)
		c.SetTimeout(timeout)
		clients[target.ServerID] = c
	}

	// Warm-up one request per target so measured results are not cold-started.
	for _, target := range targets {
		_, _ = clients[target.ServerID].Keyrange()
	}
	time.Sleep(100 * time.Millisecond)

	return &BenchEnv{
		forwarder: forwarder,
		engine:    engine,
		clients:   clients,
	}, nil
}

func collectResults(csvOutputPath string) {
	defer wgResult.Done()

	headers := []string{
		"successful",
		"operation",
		"server_id",
		"server_addr",
		"duration_us",
		"status",
		"error",
	}

	collected := make([][]string, 0)

	for r := range results {
		row := []string{
			fmt.Sprintf("%t", r.successful),
			r.op,
			r.serverID,
			r.serverAddr,
			fmt.Sprintf("%d", r.dur.Microseconds()),
			r.status,
			r.errMsg,
		}
		collected = append(collected, row)
	}

	f, err := os.Create(csvOutputPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(headers); err != nil {
		panic(err)
	}
	if err := w.WriteAll(collected); err != nil {
		panic(err)
	}
}

func buildTargets(mode, singleID, singleAddr, targetsArg string) ([]Target, error) {
	if mode == "single" {
		return []Target{
			{
				ServerID: singleID,
				Addr:     singleAddr,
			},
		}, nil
	}

	parts := strings.Split(targetsArg, ",")
	targets := make([]Target, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid target %q; expected serverID=host:port", p)
		}

		targets = append(targets, Target{
			ServerID: strings.TrimSpace(kv[0]),
			Addr:     strings.TrimSpace(kv[1]),
		})
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("no valid distributed targets provided")
	}

	return targets, nil
}

func chooseTarget(mode string, workerID int, targets []Target) Target {
	if mode == "single" {
		return targets[0]
	}

	// Round-robin workers across targets.
	return targets[workerID%len(targets)]
}

func runWorker(workerID int, env *BenchEnv, target Target, op string, data []Pair) {
	client := env.clients[target.ServerID]
	if client == nil {
		for range data {
			results <- Result{
				successful: false,
				op:         op,
				serverID:   target.ServerID,
				serverAddr: target.Addr,
				errMsg:     "missing client for target server",
			}
		}
		return
	}

	log.Printf("[worker %d] running %s benchmark against %s (%s)", workerID, op, target.ServerID, target.Addr)

	startAll := time.Now()

	for _, pair := range data {
		start := time.Now()

		resp, err := doOperation(client, op, pair)
		d := time.Since(start)

		if err != nil {
			results <- Result{
				successful: false,
				op:         op,
				serverID:   target.ServerID,
				serverAddr: target.Addr,
				dur:        d,
				errMsg:     err.Error(),
			}
			continue
		}

		ok := responseSuccessful(op, resp)

		results <- Result{
			successful: ok,
			op:         op,
			serverID:   target.ServerID,
			serverAddr: target.Addr,
			dur:        d,
			status:     resp.Status,
			errMsg:     resp.Error,
		}
	}

	log.Printf("[worker %d] finished %s benchmark in %s", workerID, op, time.Since(startAll))
}

func doOperation(client *kvndn.Client, op string, pair Pair) (*kvndn.Response, error) {
	switch op {
	case "put":
		return client.Put(pair.key, pair.value)
	case "get":
		return client.Get(pair.key)
	case "delete":
		return client.Delete(pair.key)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", op)
	}
}

func responseSuccessful(op string, resp *kvndn.Response) bool {
	if resp == nil {
		return false
	}

	switch op {
	case "put":
		return resp.Status == kvndn.StatusSuccess || resp.Status == kvndn.StatusUpdate
	case "get":
		return resp.Status == kvndn.StatusSuccess
	case "delete":
		return resp.Status == kvndn.StatusSuccess
	default:
		return false
	}
}

func dirOf(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return "."
	}
	return path[:i]
}