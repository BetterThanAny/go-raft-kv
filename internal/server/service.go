package server

import (
	"context"
	"errors"

	"go-raft-kv/api"
	"go-raft-kv/internal/raft"
	"go-raft-kv/internal/storage"
)

type Service struct {
	node *raft.Node
}

func NewService(node *raft.Node) *Service {
	return &Service{node: node}
}

func (s *Service) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	res, err := s.node.Propose(ctx, storage.Command{Op: storage.OpPut, Key: req.Key, Value: req.Value})
	if err != nil {
		leaderID, leaderAddr := leaderHint(err)
		return &api.PutResponse{Error: err.Error(), LeaderID: leaderID, LeaderAddress: leaderAddr}, nil
	}
	return &api.PutResponse{OK: res.OK, Error: res.Error}, nil
}

func (s *Service) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, found, err := s.node.LinearizableGet(ctx, req.Key)
	if err != nil {
		leaderID, leaderAddr := leaderHint(err)
		return &api.GetResponse{Error: err.Error(), LeaderID: leaderID, LeaderAddress: leaderAddr}, nil
	}
	return &api.GetResponse{OK: true, Found: found, Value: value}, nil
}

func (s *Service) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	res, err := s.node.Propose(ctx, storage.Command{Op: storage.OpDelete, Key: req.Key})
	if err != nil {
		leaderID, leaderAddr := leaderHint(err)
		return &api.DeleteResponse{Error: err.Error(), LeaderID: leaderID, LeaderAddress: leaderAddr}, nil
	}
	return &api.DeleteResponse{OK: res.OK, Found: res.Found, PreviousValue: res.Value, Error: res.Error}, nil
}

func (s *Service) CAS(ctx context.Context, req *api.CASRequest) (*api.CASResponse, error) {
	res, err := s.node.Propose(ctx, storage.Command{Op: storage.OpCAS, Key: req.Key, Expected: req.Expected, Value: req.Value})
	if err != nil {
		leaderID, leaderAddr := leaderHint(err)
		return &api.CASResponse{Error: err.Error(), LeaderID: leaderID, LeaderAddress: leaderAddr}, nil
	}
	return &api.CASResponse{OK: res.OK, Swapped: res.Swapped, Found: res.Found, CurrentValue: res.Value, Error: res.Error}, nil
}

func (s *Service) Status(context.Context, *api.StatusRequest) (*api.StatusResponse, error) {
	status := s.node.Status()
	peers := make(map[string]string, len(status.Peers))
	for _, peer := range status.Peers {
		peers[peer.ID] = peer.Address
	}
	return &api.StatusResponse{
		NodeID:        status.ID,
		State:         string(status.Role),
		Term:          status.Term,
		LeaderID:      status.LeaderID,
		LeaderAddress: status.LeaderAddress,
		CommitIndex:   status.CommitIndex,
		LastApplied:   status.LastApplied,
		LastLogIndex:  status.LastLogIndex,
		SnapshotIndex: status.SnapshotIndex,
		Peers:         peers,
	}, nil
}

func (s *Service) RequestVote(ctx context.Context, req *api.RequestVoteRequest) (*api.RequestVoteResponse, error) {
	resp := s.node.HandleRequestVote(raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &api.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *Service) AppendEntries(ctx context.Context, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	resp := s.node.HandleAppendEntries(raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      apiToLogEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	return &api.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		MatchIndex:    resp.LastLogIndex,
	}, nil
}

func (s *Service) InstallSnapshot(ctx context.Context, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error) {
	resp := s.node.HandleInstallSnapshot(raft.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          req.LeaderID,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	})
	return &api.InstallSnapshotResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		BytesReceived: resp.BytesReceived,
	}, nil
}

func logEntriesToAPI(entries []storage.LogEntry) []api.LogEntry {
	out := make([]api.LogEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, api.LogEntry{Index: entry.Index, Term: entry.Term, Command: encodeCommand(entry.Command)})
	}
	return out
}

func apiToLogEntries(entries []api.LogEntry) []storage.LogEntry {
	out := make([]storage.LogEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, storage.LogEntry{Index: entry.Index, Term: entry.Term, Command: decodeCommand(entry.Command)})
	}
	return out
}

func leaderHint(err error) (string, string) {
	var target raft.NotLeaderError
	if errors.As(err, &target) {
		return target.LeaderID, target.LeaderAddress
	}
	return "", ""
}
