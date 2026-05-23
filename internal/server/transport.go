package server

import (
	"context"
	"errors"
	"time"

	"go-raft-kv/api"
	"go-raft-kv/internal/raft"
	"google.golang.org/grpc"
)

type GRPCTransport struct {
	peers map[string]string
}

func NewGRPCTransport(peers map[string]string) *GRPCTransport {
	out := make(map[string]string, len(peers))
	for id, addr := range peers {
		out[id] = addr
	}
	return &GRPCTransport{peers: out}
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	client, closeConn, err := t.client(ctx, peerID)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	defer closeConn()

	resp, err := client.RequestVote(ctx, &api.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return raft.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	client, closeConn, err := t.client(ctx, peerID)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	defer closeConn()

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
	client, closeConn, err := t.client(ctx, peerID)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	defer closeConn()

	resp, err := client.InstallSnapshot(ctx, &api.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          req.LeaderID,
		LastIncludedIndex: req.Snapshot.LastIncludedIndex,
		LastIncludedTerm:  req.Snapshot.LastIncludedTerm,
		Data:              req.Snapshot.Data,
	})
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return raft.InstallSnapshotResponse{Term: resp.Term, Success: resp.Success}, nil
}

func (t *GRPCTransport) client(ctx context.Context, peerID string) (*api.RaftPeerClient, func(), error) {
	addr := t.peers[peerID]
	if addr == "" {
		return nil, nil, errors.New("unknown peer")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	conn, err := grpc.DialContext(dialCtx, addr, append(api.DialOptions(), grpc.WithBlock())...)
	cancel()
	if err != nil {
		return nil, nil, err
	}
	return api.NewRaftPeerClient(conn), func() { _ = conn.Close() }, nil
}

var _ raft.Transport = (*GRPCTransport)(nil)
