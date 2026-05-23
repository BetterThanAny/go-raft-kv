package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go-raft-kv/api"
	"go-raft-kv/internal/raft"
	"go-raft-kv/internal/server"
	"go-raft-kv/internal/storage"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}

	store, err := storage.Open(cfg.dataDir)
	if err != nil {
		log.Fatal(err)
	}

	transport := server.NewGRPCTransport(cfg.peers)
	stateMachine := server.NewKVStateMachine()
	node, err := raft.NewNode(raft.Config{
		ID:                 cfg.id,
		Address:            cfg.advertise,
		Peers:              cfg.peers,
		ClientAddresses:    cfg.clientAddrs,
		ElectionTimeoutMin: cfg.electionMin,
		ElectionTimeoutMax: cfg.electionMax,
		HeartbeatInterval:  cfg.heartbeat,
		SnapshotThreshold:  cfg.snapshotThreshold,
	}, store, stateMachine, transport)
	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatal(err)
	}

	service := server.NewService(node)
	grpcServer := grpc.NewServer()
	api.RegisterKVServiceServer(grpcServer, service)
	api.RegisterRaftPeerServiceServer(grpcServer, service)

	node.Start()
	log.Printf("node=%s listen=%s advertise=%s data=%s", cfg.id, cfg.listen, cfg.advertise, cfg.dataDir)
	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	waitForShutdown()
	log.Printf("shutting down node=%s", cfg.id)
	grpcServer.GracefulStop()
	node.Stop()
}

type config struct {
	id                string
	listen            string
	advertise         string
	dataDir           string
	peers             map[string]string
	clientAddrs       map[string]string
	electionMin       time.Duration
	electionMax       time.Duration
	heartbeat         time.Duration
	snapshotThreshold int
}

func parseFlags() (config, error) {
	var cfg config
	var peers, clientAddrs string
	var electionMin, electionMax, heartbeat string

	flag.StringVar(&cfg.id, "id", getenv("NODE_ID", "node1"), "node id")
	flag.StringVar(&cfg.listen, "listen", getenv("RAFT_LISTEN", ":9001"), "gRPC listen address")
	flag.StringVar(&cfg.advertise, "advertise", getenv("RAFT_ADVERTISE", "127.0.0.1:9001"), "address peers use to reach this node")
	flag.StringVar(&cfg.dataDir, "data-dir", getenv("RAFT_DATA_DIR", "data/node1"), "node data directory")
	flag.StringVar(&peers, "peers", getenv("RAFT_PEERS", "node1=127.0.0.1:9001"), "comma separated id=address peer map")
	flag.StringVar(&clientAddrs, "client-addrs", getenv("RAFT_CLIENT_ADDRS", ""), "comma separated id=client-address redirect map")
	flag.StringVar(&electionMin, "election-min", getenv("RAFT_ELECTION_MIN", "1s"), "minimum election timeout")
	flag.StringVar(&electionMax, "election-max", getenv("RAFT_ELECTION_MAX", "2s"), "maximum election timeout")
	flag.StringVar(&heartbeat, "heartbeat", getenv("RAFT_HEARTBEAT", "150ms"), "leader heartbeat interval")
	flag.IntVar(&cfg.snapshotThreshold, "snapshot-threshold", getenvInt("RAFT_SNAPSHOT_THRESHOLD", 64), "compact WAL after this many live log entries")
	flag.Parse()

	var err error
	cfg.peers, err = parseMap(peers)
	if err != nil {
		return cfg, err
	}
	cfg.clientAddrs, err = parseMap(clientAddrs)
	if err != nil {
		return cfg, err
	}
	if cfg.clientAddrs[cfg.id] == "" {
		cfg.clientAddrs[cfg.id] = cfg.advertise
	}
	cfg.electionMin, err = time.ParseDuration(electionMin)
	if err != nil {
		return cfg, err
	}
	cfg.electionMax, err = time.ParseDuration(electionMax)
	if err != nil {
		return cfg, err
	}
	cfg.heartbeat, err = time.ParseDuration(heartbeat)
	return cfg, err
}

func parseMap(raw string) (map[string]string, error) {
	out := map[string]string{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out, nil
	}
	for _, part := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf("invalid mapping %q, want id=address", part)
		}
		out[key] = value
	}
	return out, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func waitForShutdown() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
