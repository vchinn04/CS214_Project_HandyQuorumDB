package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nStangl/distributed-kv-store/protocol"
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

var (
	wg       sync.WaitGroup
	wgResult sync.WaitGroup
	results  chan Result
)

func main() {
	var (
		mode = flag.String("mode", "distributed", "Benchmark mode: single or distributed")
		op   = flag.String("op", "put", "Operation: put, get, delete")

		clients    = flag.Int("clients", 3, "Number of concurrent benchmark workers")
		iterations = flag.Int("iterations", 50, "Number of keys from words.txt to use")

		wordsPath = flag.String("words", "cmd/benchmark/words.txt", "Path to words file")
		out       = flag.String("out", "cmd/benchmark/server/results.csv", "CSV output path")

		serverID   = flag.String("server-id", "server1", "Server ID for single mode")
		serverAddr = flag.String("server-addr", "localhost:8080", "Server address for single mode")

		targetsArg = flag.String(
			"targets",
			"server1=localhost:8080,server2=localhost:8081,server3=localhost:8082",
			"Targets for distributed mode: server1=host:port,server2=host:port,...",
		)
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
		data = append(data, Pair{key: w, value: w})
	}

	results = make(chan Result, (*clients)*limit*2)

	wgResult.Add(1)
	go collectResults(*out)

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			target := chooseTarget(*mode, workerID, targets)
			runWorker(workerID, target, *op, data)
		}(i)

		fmt.Printf("Started TCP benchmark worker %d\n", i)
	}

	wg.Wait()
	close(results)
	wgResult.Wait()

	fmt.Println("TCP benchmark complete.")
	fmt.Println("CSV written to:", *out)
}

func runWorker(workerID int, target Target, op string, data []Pair) {
	conn, err := protocol.Connect(target.Addr)
	if err != nil {
		for range data {
			results <- Result{
				successful: false,
				op:         op,
				serverID:   target.ServerID,
				serverAddr: target.Addr,
				errMsg:     err.Error(),
			}
		}
		return
	}
	defer conn.Close()

	proto := protocol.ForClient(conn)

	log.Printf("[worker %d] running %s benchmark against %s (%s)", workerID, op, target.ServerID, target.Addr)

	startAll := time.Now()

	for _, pair := range data {
		start := time.Now()

		msg := protocol.ClientMessage{Key: pair.key}

		switch op {
		case "put":
			msg.Type = protocol.Put
			msg.Value = pair.value
		case "get":
			msg.Type = protocol.Get
		case "delete":
			msg.Type = protocol.Delete
		}

		if err := proto.Send(msg); err != nil {
			results <- Result{
				successful: false,
				op:         op,
				serverID:   target.ServerID,
				serverAddr: target.Addr,
				errMsg:     err.Error(),
			}
			continue
		}

		resp, err := proto.Receive()
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

		status := resp.Type.String()
		ok := responseSuccessful(status)

		results <- Result{
			successful: ok,
			op:         op,
			serverID:   target.ServerID,
			serverAddr: target.Addr,
			dur:        d,
			status:     status,
		}
	}

	log.Printf("[worker %d] finished %s benchmark in %s", workerID, op, time.Since(startAll))
}

func responseSuccessful(status string) bool {
	s := strings.ToLower(status)
	return !strings.Contains(s, "error")
}

func collectResults(path string) {
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

	var collected [][]string

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

	f, err := os.Create(path)
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
		return []Target{{ServerID: singleID, Addr: singleAddr}}, nil
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
	return targets[workerID%len(targets)]
}

func dirOf(path string) string {
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return "."
	}
	return path[:i]
}
