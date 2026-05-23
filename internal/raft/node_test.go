package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-raft-kv/internal/storage"
)

// mockStore is a Store implementation that holds state in memory and can be
// told to fail individual operations to exercise error paths.
type mockStore struct {
	mu       sync.Mutex
	hs       storage.HardState
	entries  []storage.LogEntry
	snapshot storage.Snapshot

	failHardState bool
	failAppend    bool
	failReplace   bool
	failSnapshot  bool
}

func (s *mockStore) Load() (storage.HardState, []storage.LogEntry, storage.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, append([]storage.LogEntry(nil), s.entries...), s.snapshot, nil
}

func (s *mockStore) SaveHardState(hs storage.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failHardState {
		return errors.New("injected hardstate failure")
	}
	s.hs = hs
	return nil
}

func (s *mockStore) AppendEntries(entries []storage.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failAppend {
		return errors.New("injected append failure")
	}
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *mockStore) ReplaceEntries(entries []storage.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failReplace {
		return errors.New("injected replace failure")
	}
	s.entries = append([]storage.LogEntry(nil), entries...)
	return nil
}

func (s *mockStore) SaveSnapshot(snap storage.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSnapshot {
		return errors.New("injected snapshot failure")
	}
	s.snapshot = snap
	return nil
}

func (s *mockStore) setFailHardState(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failHardState = v
}

func (s *mockStore) setFailAppend(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAppend = v
}

func (s *mockStore) setFailReplace(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failReplace = v
}

func (s *mockStore) setFailSnapshot(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSnapshot = v
}

type stubTransport struct{}

func (stubTransport) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("no peers")
}

func (stubTransport) AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{}, errors.New("no peers")
}

func (stubTransport) InstallSnapshot(context.Context, string, InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, errors.New("no peers")
}

type stubStateMachine struct {
	mu   sync.Mutex
	data map[string]string
}

func newStubSM() *stubStateMachine {
	return &stubStateMachine{data: make(map[string]string)}
}

func (sm *stubStateMachine) Apply(cmd storage.Command) storage.ApplyResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch cmd.Op {
	case storage.OpPut:
		sm.data[cmd.Key] = cmd.Value
	case storage.OpDelete:
		delete(sm.data, cmd.Key)
	}
	return storage.ApplyResult{OK: true}
}

func (sm *stubStateMachine) Snapshot() map[string]string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make(map[string]string, len(sm.data))
	for k, v := range sm.data {
		out[k] = v
	}
	return out
}

func (sm *stubStateMachine) Restore(snap map[string]string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data = make(map[string]string, len(snap))
	for k, v := range snap {
		sm.data[k] = v
	}
}

func newTestNode(t *testing.T, store Store) *Node {
	t.Helper()
	node, err := NewNode(Config{
		ID:              "n1",
		Address:         "n1",
		Peers:           map[string]string{"n1": "n1", "n2": "n2", "n3": "n3"},
		ClientAddresses: map[string]string{},
	}, store, newStubSM(), stubTransport{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node
}

func TestHandleRequestVoteRejectsWhenHardStateFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)
	store.setFailHardState(true)

	resp := node.HandleRequestVote(RequestVoteRequest{
		Term:         5,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Fatal("VoteGranted must be false when persisting HardState fails — otherwise restart could lead to double voting")
	}

	node.mu.Lock()
	term := node.currentTerm
	votedFor := node.votedFor
	node.mu.Unlock()
	if term != 0 {
		t.Fatalf("currentTerm should have rolled back to 0 after persist failure, got %d", term)
	}
	if votedFor != "" {
		t.Fatalf("votedFor should have rolled back to empty, got %q", votedFor)
	}
}

func TestHandleAppendEntriesRejectsWhenReplaceFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)
	store.setFailReplace(true)

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "1"}},
		},
	})

	if resp.Success {
		t.Fatal("Success must be false when WAL replace fails — otherwise leader thinks follower persisted what it didn't")
	}

	node.mu.Lock()
	logLen := len(node.log)
	node.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("in-memory log should be empty after rollback, got %d entries", logLen)
	}
}

func TestHandleAppendEntriesRejectsWhenCommitPersistFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	// First, deliver an entry successfully so we have something to commit.
	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:     1,
		LeaderID: "n2",
		Entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "1"}},
		},
	})
	if !resp.Success {
		t.Fatalf("initial append must succeed: %+v", resp)
	}

	// Now make HardState persistence fail and try to advance commitIndex.
	store.setFailHardState(true)

	resp = node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		LeaderCommit: 1,
	})

	if resp.Success {
		t.Fatal("Success must be false when persisting bumped commitIndex fails")
	}

	node.mu.Lock()
	commit := node.commitIndex
	node.mu.Unlock()
	if commit != 0 {
		t.Fatalf("commitIndex should have rolled back, got %d", commit)
	}
}

func TestHandleInstallSnapshotRejectsWhenSaveFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)
	store.setFailSnapshot(true)

	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:     1,
		LeaderID: "n2",
		Snapshot: storage.Snapshot{
			LastIncludedIndex: 5,
			LastIncludedTerm:  1,
			Data:              map[string]string{"a": "1"},
		},
	})

	if resp.Success {
		t.Fatal("InstallSnapshot must not report Success when SaveSnapshot fails")
	}

	node.mu.Lock()
	snapIdx := node.snapshot.LastIncludedIndex
	node.mu.Unlock()
	if snapIdx != 0 {
		t.Fatalf("in-memory snapshot must not advance when persist fails, got LastIncludedIndex=%d", snapIdx)
	}
}

func TestProposeFailsWhenWALAppendFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	// Drive the node into leader state by hand: we won't run electionLoop here.
	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.leaderID = node.id
	node.matchIndex[node.id] = 0
	node.nextIndex[node.id] = 1
	for peerID := range node.peerAddrs {
		node.nextIndex[peerID] = 1
		node.matchIndex[peerID] = 0
	}
	node.mu.Unlock()

	store.setFailAppend(true)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := node.Propose(ctx, storage.Command{Op: storage.OpPut, Key: "k", Value: "v"})
	if err == nil {
		t.Fatal("Propose must return error when WAL append fails")
	}

	node.mu.Lock()
	logLen := len(node.log)
	matchSelf := node.matchIndex[node.id]
	node.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("in-memory log must be empty when WAL append fails, got %d entries", logLen)
	}
	if matchSelf != 0 {
		t.Fatalf("matchIndex[self] must not advance when WAL append fails, got %d", matchSelf)
	}
}

func TestBecomeLeaderAppendsNoopEntry(t *testing.T) {
	store := &mockStore{}
	// Pre-seed the node with a previous-term entry that needs sweeping.
	store.entries = []storage.LogEntry{
		{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "old", Value: "data"}},
	}
	store.hs = storage.HardState{CurrentTerm: 1}

	node := newTestNode(t, store)

	// Drive into candidate then leader without running the election loop.
	node.mu.Lock()
	node.role = Candidate
	node.currentTerm = 2
	node.votedFor = node.id
	node.mu.Unlock()

	node.becomeLeader(2)

	node.mu.Lock()
	defer node.mu.Unlock()
	if node.role != Leader {
		t.Fatalf("expected leader, got %s", node.role)
	}
	// Expect log to now contain the original entry plus a no-op at index 2.
	if len(node.log) != 2 {
		t.Fatalf("expected log length 2 after no-op append, got %d", len(node.log))
	}
	noop := node.log[len(node.log)-1]
	if noop.Term != 2 {
		t.Fatalf("no-op must use current term, got term %d", noop.Term)
	}
	if noop.Command.Op != storage.OpNoop {
		t.Fatalf("expected OpNoop, got %q", noop.Command.Op)
	}
	if node.matchIndex[node.id] != noop.Index {
		t.Fatalf("self matchIndex must advance to no-op index %d, got %d", noop.Index, node.matchIndex[node.id])
	}
}
