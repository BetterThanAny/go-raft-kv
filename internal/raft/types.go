package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-raft-kv/internal/storage"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

var ErrNotLeader = errors.New("not leader")

type NotLeaderError struct {
	LeaderID      string
	LeaderAddress string
}

func (e NotLeaderError) Error() string {
	if e.LeaderAddress == "" {
		return ErrNotLeader.Error()
	}
	return fmt.Sprintf("%s: leader=%s addr=%s", ErrNotLeader, e.LeaderID, e.LeaderAddress)
}

func IsNotLeader(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotLeader) {
		return true
	}
	var target NotLeaderError
	return errors.As(err, &target)
}

type Config struct {
	ID                 string
	Address            string
	Peers              map[string]string
	ClientAddresses    map[string]string
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	SnapshotThreshold  int
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64             `json:"term"`
	LeaderID     string             `json:"leader_id"`
	PrevLogIndex uint64             `json:"prev_log_index"`
	PrevLogTerm  uint64             `json:"prev_log_term"`
	Entries      []storage.LogEntry `json:"entries"`
	LeaderCommit uint64             `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	LastLogIndex  uint64 `json:"last_log_index"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

type InstallSnapshotRequest struct {
	Term     uint64           `json:"term"`
	LeaderID string           `json:"leader_id"`
	Snapshot storage.Snapshot `json:"snapshot"`
}

type InstallSnapshotResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

type Transport interface {
	RequestVote(ctx context.Context, peerID string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, peerID string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// Store is the persistence interface the raft Node depends on. *storage.Store
// from the storage package implements this directly; tests can wrap it with a
// fault-injecting decorator to exercise error paths.
type Store interface {
	Load() (storage.HardState, []storage.LogEntry, storage.Snapshot, error)
	SaveHardState(storage.HardState) error
	AppendEntries([]storage.LogEntry) error
	ReplaceEntries([]storage.LogEntry) error
	SaveSnapshot(storage.Snapshot) error
}

type PeerStatus struct {
	ID         string `json:"id"`
	Address    string `json:"address"`
	NextIndex  uint64 `json:"next_index,omitempty"`
	MatchIndex uint64 `json:"match_index,omitempty"`
}

type Status struct {
	ID            string       `json:"id"`
	Address       string       `json:"address"`
	Role          Role         `json:"role"`
	Term          uint64       `json:"term"`
	LeaderID      string       `json:"leader_id,omitempty"`
	LeaderAddress string       `json:"leader_address,omitempty"`
	CommitIndex   uint64       `json:"commit_index"`
	LastApplied   uint64       `json:"last_applied"`
	LastLogIndex  uint64       `json:"last_log_index"`
	SnapshotIndex uint64       `json:"snapshot_index"`
	Peers         []PeerStatus `json:"peers"`
}
