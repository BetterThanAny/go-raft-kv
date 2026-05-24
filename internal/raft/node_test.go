package raft

import (
	"context"
	"encoding/json"
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

	appendCalls   int
	replaceCalls  int
	snapshotCalls int
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
	s.appendCalls++
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *mockStore) ReplaceEntries(entries []storage.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failReplace {
		return errors.New("injected replace failure")
	}
	s.replaceCalls++
	s.entries = append([]storage.LogEntry(nil), entries...)
	return nil
}

func (s *mockStore) SaveSnapshot(snap storage.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSnapshot {
		return errors.New("injected snapshot failure")
	}
	s.snapshotCalls++
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

func (s *mockStore) callCounts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendCalls, s.replaceCalls, s.snapshotCalls
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

type appendAckTransport struct {
	requests chan AppendEntriesRequest
}

func (t *appendAckTransport) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("no votes")
}

func (t *appendAckTransport) AppendEntries(ctx context.Context, _ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	select {
	case t.requests <- req:
	default:
	}
	select {
	case <-ctx.Done():
		return AppendEntriesResponse{}, ctx.Err()
	default:
		return AppendEntriesResponse{Term: req.Term, Success: true, LastLogIndex: req.PrevLogIndex + uint64(len(req.Entries))}, nil
	}
}

func (t *appendAckTransport) InstallSnapshot(context.Context, string, InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, errors.New("no snapshots")
}

type delayedAppendTransport struct {
	firstSeen    chan AppendEntriesRequest
	secondSeen   chan AppendEntriesRequest
	releaseFirst chan struct{}

	mu    sync.Mutex
	calls int
}

func (t *delayedAppendTransport) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("no votes")
}

func (t *delayedAppendTransport) AppendEntries(ctx context.Context, _ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	t.calls++
	call := t.calls
	t.mu.Unlock()

	switch call {
	case 1:
		t.firstSeen <- req
		select {
		case <-t.releaseFirst:
		case <-ctx.Done():
			return AppendEntriesResponse{}, ctx.Err()
		}
	case 2:
		t.secondSeen <- req
	default:
		return AppendEntriesResponse{}, errors.New("unexpected append call")
	}
	return AppendEntriesResponse{Term: req.Term, Success: true, LastLogIndex: req.PrevLogIndex + uint64(len(req.Entries))}, nil
}

func (t *delayedAppendTransport) InstallSnapshot(context.Context, string, InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, errors.New("no snapshots")
}

type blockingAppendTransport struct {
	started chan string
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (t *blockingAppendTransport) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("no votes")
}

func (t *blockingAppendTransport) AppendEntries(ctx context.Context, peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()
	t.started <- peerID
	select {
	case <-t.release:
	case <-ctx.Done():
		return AppendEntriesResponse{}, ctx.Err()
	}
	return AppendEntriesResponse{Term: req.Term, Success: true, LastLogIndex: req.PrevLogIndex + uint64(len(req.Entries))}, nil
}

func (t *blockingAppendTransport) InstallSnapshot(context.Context, string, InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, errors.New("no snapshots")
}

func (t *blockingAppendTransport) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
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

func (sm *stubStateMachine) Get(key string) (string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	value, ok := sm.data[key]
	return value, ok
}

type blockingApplySM struct {
	mu           sync.Mutex
	data         map[string]string
	applyStarted chan struct{}
	releaseApply chan struct{}
	startOnce    sync.Once
}

func newBlockingApplySM() *blockingApplySM {
	return &blockingApplySM{
		data:         make(map[string]string),
		applyStarted: make(chan struct{}),
		releaseApply: make(chan struct{}),
	}
}

func (sm *blockingApplySM) Apply(cmd storage.Command) storage.ApplyResult {
	if cmd.Op == storage.OpPut && cmd.Key == "k" && cmd.Value == "old" {
		sm.startOnce.Do(func() { close(sm.applyStarted) })
		<-sm.releaseApply
	}
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

func (sm *blockingApplySM) Snapshot() map[string]string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make(map[string]string, len(sm.data))
	for k, v := range sm.data {
		out[k] = v
	}
	return out
}

func (sm *blockingApplySM) Restore(snap map[string]string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data = make(map[string]string, len(snap))
	for k, v := range snap {
		sm.data[k] = v
	}
}

func (sm *blockingApplySM) Get(key string) (string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	value, ok := sm.data[key]
	return value, ok
}

func newTestNode(t *testing.T, store Store) *Node {
	t.Helper()
	return newTestNodeWithDeps(t, store, newStubSM(), stubTransport{})
}

func newTestNodeWithDeps(t *testing.T, store Store, sm StateMachine, transport Transport) *Node {
	t.Helper()
	node, err := NewNode(Config{
		ID:              "n1",
		Address:         "n1",
		Peers:           map[string]string{"n1": "n1", "n2": "n2", "n3": "n3"},
		ClientAddresses: map[string]string{},
	}, store, sm, transport)
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

func TestHandleAppendEntriesRejectsWhenAppendFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)
	store.setFailAppend(true)

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
		t.Fatal("Success must be false when WAL append fails — otherwise leader thinks follower persisted what it didn't")
	}

	node.mu.Lock()
	logLen := len(node.log)
	node.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("in-memory log should be empty after rollback, got %d entries", logLen)
	}
}

func TestHandleAppendEntriesUsesAppendForContiguousSuffix(t *testing.T) {
	store := &mockStore{
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "1"}},
		},
	}
	node := newTestNode(t, store)

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []storage.LogEntry{
			{Index: 2, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "b", Value: "2"}},
			{Index: 3, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "c", Value: "3"}},
		},
	})
	if !resp.Success {
		t.Fatalf("append-only suffix should succeed: %+v", resp)
	}
	appendCalls, replaceCalls, _ := store.callCounts()
	if appendCalls != 1 || replaceCalls != 0 {
		t.Fatalf("expected one append and no replace, got append=%d replace=%d", appendCalls, replaceCalls)
	}
}

func TestHandleAppendEntriesRejectsWhenReplaceFails(t *testing.T) {
	store := &mockStore{
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "old"}},
		},
	}
	node := newTestNode(t, store)
	store.setFailReplace(true)

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []storage.LogEntry{
			{Index: 1, Term: 2, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "new"}},
		},
	})

	if resp.Success {
		t.Fatal("Success must be false when WAL replace fails — otherwise leader thinks follower persisted a conflicting suffix")
	}

	node.mu.Lock()
	logCopy := append([]storage.LogEntry(nil), node.log...)
	node.mu.Unlock()
	if len(logCopy) != 1 || logCopy[0].Term != 1 || logCopy[0].Command.Value != "old" {
		t.Fatalf("in-memory log should roll back to the original entry, got %+v", logCopy)
	}
}

func TestHandleAppendEntriesRejectsNonContiguousEntries(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "1"}},
			{Index: 3, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "c", Value: "3"}},
		},
	})

	if resp.Success {
		t.Fatal("AppendEntries with a gap must be rejected")
	}
	if resp.ConflictIndex != 2 {
		t.Fatalf("expected conflict index 2 for missing entry, got %d", resp.ConflictIndex)
	}

	node.mu.Lock()
	logLen := len(node.log)
	node.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("non-contiguous request must not partially mutate the log, got %d entries", logLen)
	}
	appendCalls, replaceCalls, _ := store.callCounts()
	if appendCalls != 0 || replaceCalls != 0 {
		t.Fatalf("non-contiguous request must not touch storage, got append=%d replace=%d", appendCalls, replaceCalls)
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

	body, err := json.Marshal(map[string]string{"a": "1"})
	if err != nil {
		t.Fatal(err)
	}
	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              body,
		Done:              true,
	})

	if resp.Success {
		t.Fatal("InstallSnapshot must not report Success when SaveSnapshot fails")
	}

	node.mu.Lock()
	snapIdx := node.snapshot.LastIncludedIndex
	pending := node.pendingSnapshot
	fatalErr := node.storageFatal
	node.mu.Unlock()
	if snapIdx != 0 {
		t.Fatalf("in-memory snapshot must not advance when persist fails, got LastIncludedIndex=%d", snapIdx)
	}
	if pending != nil {
		t.Fatal("pending snapshot must be cleared after SaveSnapshot failure")
	}
	if fatalErr == nil {
		t.Fatal("node must record a fatal storage error after SaveSnapshot failure")
	}
}

func TestHandleInstallSnapshotClearsPendingWhenReplaceFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)
	store.setFailReplace(true)

	body, err := json.Marshal(map[string]string{"a": "1"})
	if err != nil {
		t.Fatal(err)
	}
	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              body,
		Done:              true,
	})

	if resp.Success {
		t.Fatal("InstallSnapshot must not report Success when ReplaceEntries fails")
	}

	node.mu.Lock()
	snapIdx := node.snapshot.LastIncludedIndex
	pending := node.pendingSnapshot
	fatalErr := node.storageFatal
	node.mu.Unlock()
	if snapIdx != 0 {
		t.Fatalf("in-memory snapshot must not advance when WAL replace fails, got LastIncludedIndex=%d", snapIdx)
	}
	if pending != nil {
		t.Fatal("pending snapshot must be cleared after ReplaceEntries failure")
	}
	if fatalErr == nil {
		t.Fatal("node must record a fatal storage error after WAL replace failure")
	}
}

func TestHandleInstallSnapshotLeavesMemoryUnchangedWhenHardStateFails(t *testing.T) {
	store := &mockStore{}
	sm := newStubSM()
	node := newTestNodeWithDeps(t, store, sm, stubTransport{})
	store.setFailHardState(true)

	body, err := json.Marshal(map[string]string{"a": "1"})
	if err != nil {
		t.Fatal(err)
	}
	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              0,
		LeaderID:          "n2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  0,
		Offset:            0,
		Data:              body,
		Done:              true,
	})

	if resp.Success {
		t.Fatal("InstallSnapshot must not report Success when HardState persist fails")
	}

	node.mu.Lock()
	snapIdx := node.snapshot.LastIncludedIndex
	commit := node.commitIndex
	applied := node.lastApplied
	pending := node.pendingSnapshot
	fatalErr := node.storageFatal
	node.mu.Unlock()
	if snapIdx != 0 || commit != 0 || applied != 0 {
		t.Fatalf("memory should not install snapshot before HardState persists, got snapshot=%d commit=%d applied=%d", snapIdx, commit, applied)
	}
	if pending != nil {
		t.Fatal("pending snapshot should be cleared after HardState failure")
	}
	if fatalErr == nil {
		t.Fatal("node must record a fatal storage error after HardState failure")
	}
	if got, ok := sm.Get("a"); ok || got != "" {
		t.Fatalf("state machine should not restore from uncommitted in-memory snapshot, got value=%q found=%v", got, ok)
	}
}

func TestHandleInstallSnapshotIgnoresAlreadyAppliedSnapshot(t *testing.T) {
	store := &mockStore{
		hs: storage.HardState{CurrentTerm: 1, CommitIndex: 2},
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "k", Value: "old"}},
			{Index: 2, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "k", Value: "new"}},
		},
	}
	sm := newStubSM()
	node := newTestNodeWithDeps(t, store, sm, stubTransport{})

	body, err := json.Marshal(map[string]string{"k": "stale"})
	if err != nil {
		t.Fatal(err)
	}
	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              body,
		Done:              true,
	})
	if !resp.Success {
		t.Fatalf("stale snapshot should be acked, got %+v", resp)
	}

	node.mu.Lock()
	snapIdx := node.snapshot.LastIncludedIndex
	applied := node.lastApplied
	node.mu.Unlock()
	if snapIdx != 0 || applied != 2 {
		t.Fatalf("stale snapshot should not mutate local state, snapshot=%d applied=%d", snapIdx, applied)
	}
	if got, ok := sm.Get("k"); !ok || got != "new" {
		t.Fatalf("already applied value should remain, got value=%q found=%v", got, ok)
	}
}

func TestHandleInstallSnapshotKeepsSuffixOnlyWhenBoundaryTermMatches(t *testing.T) {
	entries := []storage.LogEntry{
		{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 2, Command: storage.Command{Op: storage.OpPut, Key: "b", Value: "2"}},
	}
	body, err := json.Marshal(map[string]string{"a": "snap"})
	if err != nil {
		t.Fatal(err)
	}

	matchingStore := &mockStore{hs: storage.HardState{CurrentTerm: 2}, entries: append([]storage.LogEntry(nil), entries...)}
	matchingNode := newTestNodeWithDeps(t, matchingStore, newStubSM(), stubTransport{})
	resp := matchingNode.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderID:          "n2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              body,
		Done:              true,
	})
	if !resp.Success {
		t.Fatalf("matching-boundary snapshot should succeed: %+v", resp)
	}
	matchingNode.mu.Lock()
	matchingLog := append([]storage.LogEntry(nil), matchingNode.log...)
	matchingNode.mu.Unlock()
	if len(matchingLog) != 1 || matchingLog[0].Index != 2 {
		t.Fatalf("expected suffix after matching boundary to be retained, got %+v", matchingLog)
	}

	mismatchedStore := &mockStore{hs: storage.HardState{CurrentTerm: 2}, entries: append([]storage.LogEntry(nil), entries...)}
	mismatchedNode := newTestNodeWithDeps(t, mismatchedStore, newStubSM(), stubTransport{})
	resp = mismatchedNode.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderID:          "n2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  9,
		Offset:            0,
		Data:              body,
		Done:              true,
	})
	if !resp.Success {
		t.Fatalf("mismatched-boundary snapshot should still install: %+v", resp)
	}
	mismatchedNode.mu.Lock()
	mismatchedLog := append([]storage.LogEntry(nil), mismatchedNode.log...)
	mismatchedNode.mu.Unlock()
	if len(mismatchedLog) != 0 {
		t.Fatalf("expected suffix after mismatched boundary to be discarded, got %+v", mismatchedLog)
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
	role := node.role
	fatalErr := node.storageFatal
	node.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("in-memory log must be empty when WAL append fails, got %d entries", logLen)
	}
	if matchSelf != 0 {
		t.Fatalf("matchIndex[self] must not advance when WAL append fails, got %d", matchSelf)
	}
	if role != Follower {
		t.Fatalf("node must step down after ambiguous WAL append failure, got %s", role)
	}
	if fatalErr == nil {
		t.Fatal("node must record a fatal storage error after WAL append failure")
	}

	store.setFailAppend(false)
	_, err = node.Propose(ctx, storage.Command{Op: storage.OpPut, Key: "again", Value: "v"})
	if err == nil {
		t.Fatal("node must reject later proposals after a fatal storage error")
	}
}

func TestReplicateToPeerDoesNotRegressMatchIndex(t *testing.T) {
	entries := make([]storage.LogEntry, 0, 20)
	for i := 1; i <= 15; i++ {
		entries = append(entries, storage.LogEntry{
			Index:   uint64(i),
			Term:    1,
			Command: storage.Command{Op: storage.OpPut, Key: "k", Value: "v"},
		})
	}
	store := &mockStore{hs: storage.HardState{CurrentTerm: 1}, entries: entries}
	transport := &delayedAppendTransport{
		firstSeen:    make(chan AppendEntriesRequest, 1),
		secondSeen:   make(chan AppendEntriesRequest, 1),
		releaseFirst: make(chan struct{}),
	}
	node := newTestNodeWithDeps(t, store, newStubSM(), transport)

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.leaderID = node.id
	node.matchIndex[node.id] = 15
	node.nextIndex[node.id] = 16
	node.nextIndex["n2"] = 10
	node.matchIndex["n2"] = 0
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	firstDone := make(chan bool, 1)
	go func() {
		firstDone <- node.replicateToPeer(ctx, "n2")
	}()

	firstReq := <-transport.firstSeen
	if got := firstReq.PrevLogIndex + uint64(len(firstReq.Entries)); got != 15 {
		t.Fatalf("first request should cover through index 15, got %d", got)
	}

	node.mu.Lock()
	for i := 16; i <= 20; i++ {
		entry := storage.LogEntry{
			Index:   uint64(i),
			Term:    1,
			Command: storage.Command{Op: storage.OpPut, Key: "k", Value: "v"},
		}
		node.log = append(node.log, entry)
	}
	node.matchIndex[node.id] = 20
	node.nextIndex[node.id] = 21
	node.nextIndex["n2"] = 10
	node.mu.Unlock()

	if ok := node.replicateToPeer(ctx, "n2"); !ok {
		t.Fatal("second replication should succeed")
	}
	secondReq := <-transport.secondSeen
	if got := secondReq.PrevLogIndex + uint64(len(secondReq.Entries)); got != 20 {
		t.Fatalf("second request should cover through index 20, got %d", got)
	}

	node.mu.Lock()
	matchAfterSecond := node.matchIndex["n2"]
	nextAfterSecond := node.nextIndex["n2"]
	node.mu.Unlock()
	if matchAfterSecond != 20 || nextAfterSecond != 21 {
		t.Fatalf("second response should advance peer to 20/21, got match=%d next=%d", matchAfterSecond, nextAfterSecond)
	}

	close(transport.releaseFirst)
	select {
	case ok := <-firstDone:
		if !ok {
			t.Fatal("first replication should still report a successful ack")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first replication")
	}

	node.mu.Lock()
	match := node.matchIndex["n2"]
	next := node.nextIndex["n2"]
	node.mu.Unlock()
	if match != 20 || next != 21 {
		t.Fatalf("late stale response must not regress peer progress, got match=%d next=%d", match, next)
	}
}

func TestReplicateAllOnceSkipsPeerAlreadyInFlight(t *testing.T) {
	store := &mockStore{hs: storage.HardState{CurrentTerm: 1}}
	transport := &blockingAppendTransport{
		started: make(chan string, 4),
		release: make(chan struct{}),
	}
	node := newTestNodeWithDeps(t, store, newStubSM(), transport)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- node.replicateAllOnce(ctx)
	}()

	for i := 0; i < len(node.peerAddrs); i++ {
		select {
		case <-transport.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial replication")
		}
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- node.replicateAllOnce(ctx)
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second replicateAllOnce returned error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second replicateAllOnce should return immediately when peers are in flight")
	}
	if got := transport.callCount(); got != len(node.peerAddrs) {
		t.Fatalf("expected only the first wave of RPCs, got %d calls", got)
	}

	close(transport.release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first replicateAllOnce returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first replicateAllOnce")
	}
}

func TestLinearizableGetWaitsForLeaderNoopApplied(t *testing.T) {
	store := &mockStore{
		hs: storage.HardState{CurrentTerm: 2, CommitIndex: 1},
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpNoop}},
			{Index: 2, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "kept", Value: "new"}},
			{Index: 3, Term: 2, Command: storage.Command{Op: storage.OpNoop}},
		},
	}
	transport := &appendAckTransport{requests: make(chan AppendEntriesRequest, 4)}
	sm := newStubSM()
	node := newTestNodeWithDeps(t, store, sm, transport)

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 2
	node.leaderID = node.id
	node.commitIndex = 1
	node.lastApplied = 1
	node.leaderNoopIndex = 3
	node.matchIndex[node.id] = 3
	node.nextIndex[node.id] = 4
	for peerID := range node.peerAddrs {
		node.nextIndex[peerID] = 2
		node.matchIndex[peerID] = 0
	}
	node.mu.Unlock()

	type readResult struct {
		value string
		found bool
		err   error
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan readResult, 1)
	go func() {
		value, found, err := node.LinearizableGet(ctx, "kept")
		done <- readResult{value: value, found: found, err: err}
	}()

	select {
	case req := <-transport.requests:
		if len(req.Entries) == 0 {
			t.Fatal("read barrier must replicate the pending no-op, got heartbeat only")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for read barrier AppendEntries")
	}

	deadline := time.Now().Add(time.Second)
	for {
		node.mu.Lock()
		committed := node.commitIndex >= 3
		node.mu.Unlock()
		if committed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for no-op commit")
		}
		time.Sleep(time.Millisecond)
	}

	select {
	case res := <-done:
		t.Fatalf("LinearizableGet returned before applying the current-term barrier: %+v", res)
	case <-time.After(50 * time.Millisecond):
	}

	node.applyReady()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("LinearizableGet failed: %v", res.err)
		}
		if !res.found || res.value != "new" {
			t.Fatalf("expected applied value, got value=%q found=%v", res.value, res.found)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for LinearizableGet after apply")
	}
}

func TestLinearizableGetDefaultReadTimeout(t *testing.T) {
	store := &mockStore{
		hs: storage.HardState{CurrentTerm: 2, CommitIndex: 1},
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpNoop}},
			{Index: 2, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "kept", Value: "new"}},
			{Index: 3, Term: 2, Command: storage.Command{Op: storage.OpNoop}},
		},
	}
	transport := &appendAckTransport{requests: make(chan AppendEntriesRequest, 4)}
	node := newTestNodeWithDeps(t, store, newStubSM(), transport)

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 2
	node.leaderID = node.id
	node.commitIndex = 1
	node.lastApplied = 1
	node.leaderNoopIndex = 3
	node.readTimeout = 30 * time.Millisecond
	node.matchIndex[node.id] = 3
	node.nextIndex[node.id] = 4
	for peerID := range node.peerAddrs {
		node.nextIndex[peerID] = 2
		node.matchIndex[peerID] = 0
	}
	node.mu.Unlock()

	start := time.Now()
	_, _, err := node.LinearizableGet(context.Background(), "kept")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected default read timeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("read timeout took too long: %s", elapsed)
	}
}

func TestInstallSnapshotSerializesWithInFlightApply(t *testing.T) {
	store := &mockStore{
		hs: storage.HardState{CurrentTerm: 1},
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "k", Value: "old"}},
		},
	}
	sm := newBlockingApplySM()
	node := newTestNodeWithDeps(t, store, sm, stubTransport{})

	node.mu.Lock()
	node.commitIndex = 1
	node.mu.Unlock()

	applyDone := make(chan struct{})
	go func() {
		node.applyReady()
		close(applyDone)
	}()

	select {
	case <-sm.applyStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for apply to start")
	}

	body, err := json.Marshal(map[string]string{"k": "new"})
	if err != nil {
		t.Fatal(err)
	}
	snapshotDone := make(chan InstallSnapshotResponse, 1)
	go func() {
		snapshotDone <- node.HandleInstallSnapshot(InstallSnapshotRequest{
			Term:              1,
			LeaderID:          "n2",
			LastIncludedIndex: 2,
			LastIncludedTerm:  1,
			Offset:            0,
			Data:              body,
			Done:              true,
		})
	}()

	select {
	case resp := <-snapshotDone:
		close(sm.releaseApply)
		t.Fatalf("snapshot restore raced ahead of in-flight apply: %+v", resp)
	case <-time.After(50 * time.Millisecond):
	}

	close(sm.releaseApply)

	select {
	case <-applyDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for apply to finish")
	}

	select {
	case resp := <-snapshotDone:
		if !resp.Success {
			t.Fatalf("snapshot install failed: %+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot install")
	}

	if value, found := sm.Get("k"); !found || value != "new" {
		t.Fatalf("snapshot value should win after serialization, got value=%q found=%v", value, found)
	}
}

func TestSlowApplyDoesNotBlockRequestVote(t *testing.T) {
	store := &mockStore{
		hs: storage.HardState{CurrentTerm: 1},
		entries: []storage.LogEntry{
			{Index: 1, Term: 1, Command: storage.Command{Op: storage.OpPut, Key: "k", Value: "old"}},
		},
	}
	sm := newBlockingApplySM()
	node := newTestNodeWithDeps(t, store, sm, stubTransport{})

	node.mu.Lock()
	node.commitIndex = 1
	node.mu.Unlock()

	applyDone := make(chan struct{})
	go func() {
		node.applyReady()
		close(applyDone)
	}()

	select {
	case <-sm.applyStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for apply to start")
	}

	voteDone := make(chan RequestVoteResponse, 1)
	go func() {
		voteDone <- node.HandleRequestVote(RequestVoteRequest{
			Term:         2,
			CandidateID:  "n2",
			LastLogIndex: 1,
			LastLogTerm:  1,
		})
	}()

	select {
	case <-voteDone:
	case <-time.After(100 * time.Millisecond):
		close(sm.releaseApply)
		t.Fatal("RequestVote should not wait for a slow state-machine Apply")
	}

	close(sm.releaseApply)
	select {
	case <-applyDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for apply to finish")
	}
}

func TestWaitUntilAppliedReturnsOnStorageFailure(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	errCh := make(chan error, 1)
	go func() {
		errCh <- node.waitUntilApplied(context.Background(), 1)
	}()

	storageErr := errors.New("disk failed")
	node.mu.Lock()
	node.failStorageLocked(storageErr)
	node.mu.Unlock()

	select {
	case err := <-errCh:
		if !errors.Is(err, storageErr) {
			t.Fatalf("expected storage error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitUntilApplied should wake when storage becomes fatal")
	}
}

func TestHandleInstallSnapshotAssemblesMultipleChunks(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	full, err := json.Marshal(map[string]string{
		"alpha": "111", "bravo": "222", "charlie": "333",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Send the snapshot in 3 chunks.
	mid := len(full) / 3
	send := func(offset int, body []byte, done bool) InstallSnapshotResponse {
		return node.HandleInstallSnapshot(InstallSnapshotRequest{
			Term:              1,
			LeaderID:          "n2",
			LastIncludedIndex: 9,
			LastIncludedTerm:  1,
			Offset:            uint64(offset),
			Data:              body,
			Done:              done,
		})
	}

	resp := send(0, full[:mid], false)
	if !resp.Success || resp.BytesReceived != uint64(mid) {
		t.Fatalf("first chunk: %+v", resp)
	}
	resp = send(mid, full[mid:2*mid], false)
	if !resp.Success || resp.BytesReceived != uint64(2*mid) {
		t.Fatalf("second chunk: %+v", resp)
	}
	resp = send(2*mid, full[2*mid:], true)
	if !resp.Success {
		t.Fatalf("final chunk should succeed, got %+v", resp)
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	if node.snapshot.LastIncludedIndex != 9 {
		t.Fatalf("snapshot not installed: %+v", node.snapshot)
	}
	if got := node.snapshot.Data["alpha"]; got != "111" {
		t.Fatalf("snapshot data missing alpha: %v", node.snapshot.Data)
	}
	if node.pendingSnapshot != nil {
		t.Fatal("pendingSnapshot should be cleared after install")
	}
}

func TestHandleInstallSnapshotReportsOffsetMismatch(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	// Seed the first chunk so we have 100 bytes buffered.
	first := make([]byte, 100)
	for i := range first {
		first[i] = '{'
	}
	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 9,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              first,
		Done:              false,
	})
	if !resp.Success || resp.BytesReceived != 100 {
		t.Fatalf("first chunk: %+v", resp)
	}

	// Leader retries from offset=42 (wrong). Follower should answer with the
	// next expected offset (100) so the leader can resume from the right place.
	resp = node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 9,
		LastIncludedTerm:  1,
		Offset:            42,
		Data:              []byte("garbage"),
		Done:              false,
	})
	if resp.Success {
		t.Fatal("offset-mismatched chunk must not be accepted")
	}
	if resp.BytesReceived != 100 {
		t.Fatalf("expected BytesReceived=100, got %d", resp.BytesReceived)
	}
}

func TestHandleInstallSnapshotDiscardsStalePending(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	// Begin streaming snapshot at index=5.
	_ = node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              []byte("partial"),
		Done:              false,
	})

	// A new snapshot at index=12 arrives at offset=0 — should discard the
	// in-flight one and start fresh.
	body, _ := json.Marshal(map[string]string{"k": "v"})
	resp := node.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "n2",
		LastIncludedIndex: 12,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              body,
		Done:              true,
	})
	if !resp.Success {
		t.Fatalf("new snapshot at offset 0 must succeed: %+v", resp)
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	if node.snapshot.LastIncludedIndex != 12 {
		t.Fatalf("expected snapshot 12 to be installed, got %d", node.snapshot.LastIncludedIndex)
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

func TestBecomeLeaderStepsDownWhenNoopAppendFails(t *testing.T) {
	store := &mockStore{}
	node := newTestNode(t, store)

	node.mu.Lock()
	node.role = Candidate
	node.currentTerm = 2
	node.votedFor = node.id
	node.mu.Unlock()

	store.setFailAppend(true)
	node.becomeLeader(2)

	node.mu.Lock()
	defer node.mu.Unlock()
	if node.role != Follower {
		t.Fatalf("leader must step down when no-op append fails, got %s", node.role)
	}
	if node.leaderNoopIndex != 0 {
		t.Fatalf("leaderNoopIndex must remain unset after failed no-op append, got %d", node.leaderNoopIndex)
	}
	if node.storageFatal == nil {
		t.Fatal("failed no-op append must mark storage as fatal")
	}
	if len(node.log) != 0 {
		t.Fatalf("failed no-op append must not mutate in-memory log, got %+v", node.log)
	}
}
