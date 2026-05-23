package server

import (
	"context"
	"errors"
	"sync"

	"go-raft-kv/api"
	"go-raft-kv/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type GRPCTransport struct {
	peers map[string]string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport(peers map[string]string) *GRPCTransport {
	out := make(map[string]string, len(peers))
	for id, addr := range peers {
		out[id] = addr
	}
	return &GRPCTransport{
		peers: out,
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Close releases every pooled gRPC connection. Safe to call multiple times.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.conns {
		_ = conn.Close()
		delete(t.conns, id)
	}
	return nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	conn, err := t.connFor(peerID)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	client := api.NewRaftPeerClient(conn)
	resp, err := client.RequestVote(ctx, &api.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		t.dropConn(peerID, conn)
		return raft.RequestVoteResponse{}, err
	}
	return raft.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	conn, err := t.connFor(peerID)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	client := api.NewRaftPeerClient(conn)

	entries := make([]api.LogEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, api.LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: encodeCommand(entry.Command),
		})
	}
	resp, err := client.AppendEntries(ctx, &api.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		t.dropConn(peerID, conn)
		return raft.AppendEntriesResponse{}, err
	}
	return raft.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		LastLogIndex:  resp.MatchIndex,
	}, nil
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peerID string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	conn, err := t.connFor(peerID)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	client := api.NewRaftPeerClient(conn)

	resp, err := client.InstallSnapshot(ctx, &api.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          req.LeaderID,
		LastIncludedIndex: req.Snapshot.LastIncludedIndex,
		LastIncludedTerm:  req.Snapshot.LastIncludedTerm,
		Data:              req.Snapshot.Data,
	})
	if err != nil {
		t.dropConn(peerID, conn)
		return raft.InstallSnapshotResponse{}, err
	}
	return raft.InstallSnapshotResponse{Term: resp.Term, Success: resp.Success}, nil
}

// connFor returns a cached gRPC client connection to the peer, dialing
// lazily on first use. Connections are reused across RPCs so we don't pay
// a TCP+TLS handshake on every heartbeat.
func (t *GRPCTransport) connFor(peerID string) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[peerID]; ok {
		if conn.GetState() != connectivity.Shutdown {
			return conn, nil
		}
		_ = conn.Close()
		delete(t.conns, peerID)
	}
	addr := t.peers[peerID]
	if addr == "" {
		return nil, errors.New("unknown peer")
	}
	conn, err := grpc.NewClient(addr, api.DialOptions()...)
	if err != nil {
		return nil, err
	}
	t.conns[peerID] = conn
	return conn, nil
}

// dropConn removes a connection from the pool after an RPC error so the next
// call dials fresh. Callers pass the connection they observed the error on, so
// we don't drop a newer reconnection.
func (t *GRPCTransport) dropConn(peerID string, observed *grpc.ClientConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.conns[peerID]; ok && cur == observed {
		_ = cur.Close()
		delete(t.conns, peerID)
	}
}

var _ raft.Transport = (*GRPCTransport)(nil)
