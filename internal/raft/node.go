package raft

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"

	"go-raft-kv/internal/storage"
)

type applyOutcome struct {
	result storage.ApplyResult
	// err is non-nil when the proposal could not be completed in the current
	// term — e.g. the node stepped down before commit, or shut down. A
	// successful apply delivers err == nil even when result.OK is false (which
	// only signals that the command itself was rejected by the state machine).
	err error
}

// snapshotProgress holds the partial bytes a follower has received while a
// chunked InstallSnapshot is in flight. A snapshot is identified by its
// leader-supplied (lastIncludedIndex, lastIncludedTerm) — a new pair from any
// leader invalidates the current progress.
type snapshotProgress struct {
	leaderID          string
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	buffer            []byte
}

// snapshotChunkBytes is the maximum payload of one InstallSnapshot RPC.
// Configurable per Node via Config.SnapshotChunkBytes; falls back to this
// default if unset.
const defaultSnapshotChunkBytes = 1 << 20 // 1 MiB

type Node struct {
	id          string
	address     string
	peerAddrs   map[string]string
	clientAddrs map[string]string
	transport   Transport
	store       Store
	sm          StateMachine

	mu             sync.Mutex
	role           Role
	currentTerm    uint64
	votedFor       string
	leaderID       string
	log            []storage.LogEntry
	snapshot       storage.Snapshot
	commitIndex    uint64
	lastApplied    uint64
	nextIndex      map[string]uint64
	matchIndex     map[string]uint64
	electionDue    time.Time
	heartbeatEvery time.Duration
	timeoutMin     time.Duration
	timeoutMax     time.Duration
	snapshotEvery      int
	snapshotChunkBytes int
	rng                *rand.Rand
	waiters            map[uint64][]chan applyOutcome
	pendingSnapshot    *snapshotProgress
	lastErr            error

	proposeMu   sync.Mutex
	applyCh     chan struct{}
	replicateCh chan struct{}
	stopCh      chan struct{}
	stopOnce    sync.Once
	loops       sync.WaitGroup
}

func NewNode(cfg Config, store Store, sm StateMachine, transport Transport) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft node id is required")
	}
	if store == nil {
		return nil, errors.New("storage store is required")
	}
	if sm == nil {
		return nil, errors.New("state machine is required")
	}
	if transport == nil {
		return nil, errors.New("transport is required")
	}

	normalizeConfig(&cfg)
	hs, entries, snapshot, err := store.Load()
	if err != nil {
		return nil, err
	}
	if snapshot.Data != nil {
		sm.Restore(snapshot.Data)
	}
	commitIndex := hs.CommitIndex
	if commitIndex < snapshot.LastIncludedIndex {
		commitIndex = snapshot.LastIncludedIndex
	}
	if last := lastIndex(snapshot, entries); commitIndex > last {
		commitIndex = last
	}
	lastApplied := snapshot.LastIncludedIndex
	for _, entry := range entries {
		if entry.Index > commitIndex {
			break
		}
		sm.Apply(entry.Command)
		lastApplied = entry.Index
	}

	peerAddrs := make(map[string]string)
	for id, addr := range cfg.Peers {
		if id != cfg.ID {
			peerAddrs[id] = addr
		}
	}
	clientAddrs := make(map[string]string)
	for id, addr := range cfg.ClientAddresses {
		clientAddrs[id] = addr
	}
	if clientAddrs[cfg.ID] == "" {
		clientAddrs[cfg.ID] = cfg.Address
	}

	n := &Node{
		id:             cfg.ID,
		address:        cfg.Address,
		peerAddrs:      peerAddrs,
		clientAddrs:    clientAddrs,
		transport:      transport,
		store:          store,
		sm:             sm,
		role:           Follower,
		currentTerm:    hs.CurrentTerm,
		votedFor:       hs.VotedFor,
		log:            append([]storage.LogEntry(nil), entries...),
		snapshot:       cloneSnapshot(snapshot),
		commitIndex:    commitIndex,
		lastApplied:    lastApplied,
		nextIndex:      make(map[string]uint64),
		matchIndex:     make(map[string]uint64),
		heartbeatEvery: cfg.HeartbeatInterval,
		timeoutMin:     cfg.ElectionTimeoutMin,
		timeoutMax:     cfg.ElectionTimeoutMax,
		snapshotEvery:      cfg.SnapshotThreshold,
		snapshotChunkBytes: cfg.SnapshotChunkBytes,
		rng:            rand.New(rand.NewSource(time.Now().UnixNano() + int64(hashID(cfg.ID)))),
		waiters:        make(map[uint64][]chan applyOutcome),
		applyCh:        make(chan struct{}, 1),
		replicateCh:    make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
	n.resetElectionLocked()
	if hs.CommitIndex != commitIndex {
		if err := n.persistHardStateLocked(); err != nil {
			return nil, err
		}
	}
	return n, nil
}

func normalizeConfig(cfg *Config) {
	if cfg.Peers == nil {
		cfg.Peers = map[string]string{cfg.ID: cfg.Address}
	}
	if cfg.Address == "" {
		cfg.Address = cfg.Peers[cfg.ID]
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 600 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 75 * time.Millisecond
	}
	if cfg.SnapshotThreshold <= 0 {
		cfg.SnapshotThreshold = 64
	}
	if cfg.SnapshotChunkBytes <= 0 {
		cfg.SnapshotChunkBytes = defaultSnapshotChunkBytes
	}
}

func (n *Node) Start() {
	n.loops.Add(3)
	go func() {
		defer n.loops.Done()
		n.electionLoop()
	}()
	go func() {
		defer n.loops.Done()
		n.heartbeatLoop()
	}()
	go func() {
		defer n.loops.Done()
		n.applyLoop()
	}()
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.mu.Lock()
		for index, waiters := range n.waiters {
			for _, waiter := range waiters {
				waiter <- applyOutcome{err: errors.New("node stopped")}
				close(waiter)
			}
			delete(n.waiters, index)
		}
		n.mu.Unlock()
	})
	// Wait for the goroutines started by Start() to fully exit so callers can
	// safely tear down the data directory without racing a still-running loop.
	n.loops.Wait()
}

func (n *Node) Propose(ctx context.Context, command storage.Command) (storage.ApplyResult, error) {
	waiter, index, err := n.proposeAppend(command)
	if err != nil {
		return storage.ApplyResult{}, err
	}
	// Wake the heartbeat loop so we don't wait for the next tick before
	// replicating the new entry.
	n.kickReplication()

	select {
	case outcome := <-waiter:
		if outcome.err != nil {
			return storage.ApplyResult{}, outcome.err
		}
		if !outcome.result.OK && outcome.result.Error != "" {
			return outcome.result, errors.New(outcome.result.Error)
		}
		return outcome.result, nil
	case <-ctx.Done():
		n.removeWaiter(index, waiter)
		return storage.ApplyResult{}, ctx.Err()
	}
}

// proposeAppend atomically allocates the next log index, persists the entry to
// the WAL, and registers a waiter that will be notified when the entry is
// either applied (success) or invalidated by a step-down / shutdown. The
// proposeMu lock is held only for this fast path so concurrent Propose calls
// from different clients pipeline rather than serialize through the
// replication+apply wait.
func (n *Node) proposeAppend(command storage.Command) (chan applyOutcome, uint64, error) {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()

	n.mu.Lock()
	if n.role != Leader {
		err := n.notLeaderLocked()
		n.mu.Unlock()
		return nil, 0, err
	}
	entry := storage.LogEntry{
		Index:   n.lastIndexLocked() + 1,
		Term:    n.currentTerm,
		Command: command,
	}
	// Persist to WAL before any in-memory mutation. proposeMu ensures index
	// monotonicity across concurrent calls so the WAL is always append-ordered.
	if err := n.store.AppendEntries([]storage.LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return nil, 0, err
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = entry.Index
	n.nextIndex[n.id] = entry.Index + 1
	waiter := make(chan applyOutcome, 1)
	n.waiters[entry.Index] = append(n.waiters[entry.Index], waiter)
	// Single-node clusters need this to commit immediately (no peers will ever
	// ack). For multi-node clusters this is a cheap no-op when the new entry
	// doesn't yet have majority replication.
	n.advanceCommitLocked()
	n.mu.Unlock()
	return waiter, entry.Index, nil
}

func (n *Node) kickReplication() {
	select {
	case n.replicateCh <- struct{}{}:
	default:
	}
}

func (n *Node) LinearizableGet(ctx context.Context, key string) (string, bool, error) {
	n.mu.Lock()
	if n.role != Leader {
		err := n.notLeaderLocked()
		n.mu.Unlock()
		return "", false, err
	}
	applied := n.commitIndex
	n.mu.Unlock()

	if err := n.ensureQuorum(ctx); err != nil {
		return "", false, err
	}
	if err := n.waitUntilApplied(ctx, applied); err != nil {
		return "", false, err
	}
	value, found := readValue(n.sm, key)
	return value, found, nil
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()

	peers := make([]PeerStatus, 0, len(n.peerAddrs))
	for id, addr := range n.peerAddrs {
		peers = append(peers, PeerStatus{
			ID:         id,
			Address:    addr,
			NextIndex:  n.nextIndex[id],
			MatchIndex: n.matchIndex[id],
		})
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })

	return Status{
		ID:            n.id,
		Address:       n.address,
		Role:          n.role,
		Term:          n.currentTerm,
		LeaderID:      n.leaderID,
		LeaderAddress: n.leaderAddressLocked(),
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  n.lastIndexLocked(),
		SnapshotIndex: n.snapshot.LastIncludedIndex,
		Peers:         peers,
	}
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm {
		if err := n.becomeFollowerLocked(req.Term, ""); err != nil {
			// Could not durably record the new term; do not grant a vote, otherwise
			// a crash here would allow voting again in the same term after restart.
			return RequestVoteResponse{Term: n.currentTerm}
		}
	}

	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	upToDate := req.LastLogTerm > n.lastTermLocked() ||
		(req.LastLogTerm == n.lastTermLocked() && req.LastLogIndex >= n.lastIndexLocked())
	if canVote && upToDate {
		prevVotedFor := n.votedFor
		n.votedFor = req.CandidateID
		if err := n.persistHardStateLocked(); err != nil {
			n.votedFor = prevVotedFor
			return RequestVoteResponse{Term: n.currentTerm}
		}
		n.resetElectionLocked()
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}
	}
	return RequestVoteResponse{Term: n.currentTerm}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, LastLogIndex: n.lastIndexLocked()}
	}
	if req.Term > n.currentTerm || n.role != Follower {
		if err := n.becomeFollowerLocked(req.Term, req.LeaderID); err != nil {
			return AppendEntriesResponse{Term: n.currentTerm, LastLogIndex: n.lastIndexLocked()}
		}
	}
	n.leaderID = req.LeaderID
	n.resetElectionLocked()

	if req.PrevLogIndex < n.snapshot.LastIncludedIndex {
		return AppendEntriesResponse{
			Term:          n.currentTerm,
			ConflictIndex: n.snapshot.LastIncludedIndex + 1,
			LastLogIndex:  n.lastIndexLocked(),
		}
	}
	if term, ok := n.termAtLocked(req.PrevLogIndex); !ok || term != req.PrevLogTerm {
		return AppendEntriesResponse{
			Term:          n.currentTerm,
			ConflictIndex: n.conflictIndexLocked(req.PrevLogIndex),
			LastLogIndex:  n.lastIndexLocked(),
		}
	}

	// Snapshot in-memory log before mutating so we can roll back if WAL write fails.
	prevLog := n.log
	changed := n.mergeEntriesLocked(req.Entries)
	if changed {
		if err := n.store.ReplaceEntries(n.log); err != nil {
			n.log = prevLog
			return AppendEntriesResponse{Term: n.currentTerm, LastLogIndex: n.lastIndexLocked()}
		}
	}
	if req.LeaderCommit > n.commitIndex {
		prevCommit := n.commitIndex
		n.commitIndex = min(req.LeaderCommit, n.lastIndexLocked())
		if err := n.persistHardStateLocked(); err != nil {
			n.commitIndex = prevCommit
			return AppendEntriesResponse{Term: n.currentTerm, LastLogIndex: n.lastIndexLocked()}
		}
		n.notifyApplyLocked()
	}
	return AppendEntriesResponse{
		Term:         n.currentTerm,
		Success:      true,
		LastLogIndex: n.lastIndexLocked(),
	}
}

func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm || n.role != Follower {
		if err := n.becomeFollowerLocked(req.Term, req.LeaderID); err != nil {
			return InstallSnapshotResponse{Term: n.currentTerm}
		}
	}
	n.leaderID = req.LeaderID
	n.resetElectionLocked()

	if req.LastIncludedIndex <= n.snapshot.LastIncludedIndex {
		// Already at or beyond this snapshot — discard any pending stream and
		// ack so the leader stops re-sending.
		n.pendingSnapshot = nil
		return InstallSnapshotResponse{Term: n.currentTerm, Success: true}
	}

	progress := n.pendingSnapshot
	if progress == nil ||
		progress.leaderID != req.LeaderID ||
		progress.lastIncludedIndex != req.LastIncludedIndex ||
		progress.lastIncludedTerm != req.LastIncludedTerm {
		// Either no in-progress stream, or this chunk belongs to a different
		// snapshot (newer leader, newer snapshot). Start over only if the
		// chunk itself is at offset 0; otherwise tell the leader we need 0.
		if req.Offset != 0 {
			return InstallSnapshotResponse{Term: n.currentTerm, BytesReceived: 0}
		}
		progress = &snapshotProgress{
			leaderID:          req.LeaderID,
			lastIncludedIndex: req.LastIncludedIndex,
			lastIncludedTerm:  req.LastIncludedTerm,
		}
		n.pendingSnapshot = progress
	}

	if req.Offset != uint64(len(progress.buffer)) {
		// Out-of-order chunk — likely from a retry. Tell the leader the actual
		// progress so it can resume from there.
		return InstallSnapshotResponse{
			Term:          n.currentTerm,
			BytesReceived: uint64(len(progress.buffer)),
		}
	}
	progress.buffer = append(progress.buffer, req.Data...)

	if !req.Done {
		return InstallSnapshotResponse{
			Term:          n.currentTerm,
			Success:       true,
			BytesReceived: uint64(len(progress.buffer)),
		}
	}

	// Final chunk: decode, persist disk-first, then mutate memory, then
	// restore the state machine.
	var data map[string]string
	if len(progress.buffer) > 0 {
		if err := json.Unmarshal(progress.buffer, &data); err != nil {
			n.pendingSnapshot = nil
			return InstallSnapshotResponse{Term: n.currentTerm}
		}
	}
	newSnap := storage.Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              data,
	}
	newLog := make([]storage.LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry.Index > newSnap.LastIncludedIndex {
			newLog = append(newLog, entry)
		}
	}

	if err := n.store.SaveSnapshot(newSnap); err != nil {
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	if err := n.store.ReplaceEntries(newLog); err != nil {
		return InstallSnapshotResponse{Term: n.currentTerm}
	}

	prevSnap := n.snapshot
	prevLog := n.log
	prevCommit := n.commitIndex
	prevApplied := n.lastApplied
	n.snapshot = newSnap
	n.log = newLog
	if n.commitIndex < newSnap.LastIncludedIndex {
		n.commitIndex = newSnap.LastIncludedIndex
	}
	if n.lastApplied < newSnap.LastIncludedIndex {
		n.lastApplied = newSnap.LastIncludedIndex
	}
	if err := n.persistHardStateLocked(); err != nil {
		n.snapshot = prevSnap
		n.log = prevLog
		n.commitIndex = prevCommit
		n.lastApplied = prevApplied
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	n.sm.Restore(newSnap.Data)
	n.pendingSnapshot = nil
	n.notifyApplyLocked()
	return InstallSnapshotResponse{
		Term:          n.currentTerm,
		Success:       true,
		BytesReceived: uint64(len(progress.buffer)),
	}
}

func (n *Node) electionLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			start := n.role != Leader && time.Now().After(n.electionDue)
			n.mu.Unlock()
			if start {
				n.startElection()
			}
		}
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.heartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		case <-n.replicateCh:
		}
		if n.isLeader() {
			ctx, cancel := context.WithTimeout(context.Background(), n.heartbeatEvery)
			_ = n.replicateAllOnce(ctx)
			cancel()
		}
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
			n.applyReady()
		case <-time.After(25 * time.Millisecond):
			n.applyReady()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	prevRole := n.role
	prevTerm := n.currentTerm
	prevVotedFor := n.votedFor
	prevLeader := n.leaderID
	n.role = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionLocked()
	lastIndex := n.lastIndexLocked()
	lastTerm := n.lastTermLocked()
	if err := n.persistHardStateLocked(); err != nil {
		// Roll back: a candidate with an unpersisted term increment could,
		// after a crash and restart, vote a second time in the same term.
		n.role = prevRole
		n.currentTerm = prevTerm
		n.votedFor = prevVotedFor
		n.leaderID = prevLeader
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	if n.quorum() == 1 {
		n.becomeLeader(term)
		return
	}

	type voteResult struct {
		resp RequestVoteResponse
		err  error
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.timeoutMax)
	defer cancel()
	results := make(chan voteResult, len(n.peerAddrs))
	req := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	for peerID := range n.peerAddrs {
		go func(peerID string) {
			resp, err := n.transport.RequestVote(ctx, peerID, req)
			results <- voteResult{resp: resp, err: err}
		}(peerID)
	}

	votes := 1
	for range n.peerAddrs {
		select {
		case <-ctx.Done():
			return
		case result := <-results:
			if result.err != nil {
				continue
			}
			if result.resp.Term > term {
				n.stepDown(result.resp.Term, "")
				return
			}
			if result.resp.VoteGranted {
				votes++
				if votes >= n.quorum() {
					n.becomeLeader(term)
					return
				}
			}
		}
	}
}

func (n *Node) becomeLeader(term uint64) {
	n.mu.Lock()
	if n.role != Candidate || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	n.role = Leader
	n.leaderID = n.id
	last := n.lastIndexLocked()
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	n.matchIndex[n.id] = last
	n.nextIndex[n.id] = last + 1
	for peerID := range n.peerAddrs {
		n.nextIndex[peerID] = last + 1
		n.matchIndex[peerID] = 0
	}
	// Append a no-op entry so that commitIndex can advance past any previous-term
	// entries that this leader inherited but cannot commit directly (Raft §5.4.2
	// / Figure 8). Without this, a linearizable read right after election can
	// return stale data when a previous leader committed an entry that hasn't
	// yet been applied here.
	noop := storage.LogEntry{
		Index:   n.lastIndexLocked() + 1,
		Term:    term,
		Command: storage.Command{Op: storage.OpNoop},
	}
	if err := n.store.AppendEntries([]storage.LogEntry{noop}); err != nil {
		// Without the no-op, prev-term entries stay uncommitted until a future
		// successful Propose. We remain leader but record the error.
		n.lastErr = err
	} else {
		n.log = append(n.log, noop)
		n.matchIndex[n.id] = noop.Index
		n.nextIndex[n.id] = noop.Index + 1
		// Single-node clusters commit the no-op immediately; multi-node
		// clusters defer to peer acks.
		n.advanceCommitLocked()
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.timeoutMin)
	_ = n.replicateAllOnce(ctx)
	cancel()
}

func (n *Node) stepDown(term uint64, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	_ = n.becomeFollowerLocked(term, leaderID)
}

// becomeFollowerLocked transitions to follower. If persistence of the
// (possibly new) term/votedFor fails, the in-memory term/votedFor are rolled
// back so we never advertise a term that isn't durably recorded — preventing
// vote duplication across a crash and restart.
func (n *Node) becomeFollowerLocked(term uint64, leaderID string) error {
	prevTerm := n.currentTerm
	prevVotedFor := n.votedFor
	prevRole := n.role
	prevLeader := n.leaderID
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leaderID = leaderID
	n.resetElectionLocked()
	if err := n.persistHardStateLocked(); err != nil {
		n.currentTerm = prevTerm
		n.votedFor = prevVotedFor
		n.role = prevRole
		n.leaderID = prevLeader
		return err
	}
	if prevRole == Leader {
		// Any in-flight Propose for an entry that hasn't yet committed under
		// our leadership must wake up and see NotLeader so the client can
		// retry against the new leader. Entries already at or below
		// commitIndex stay in n.waiters so the apply loop can still deliver
		// the real result if/when those entries are applied locally.
		n.failUncommittedWaitersLocked()
	}
	return nil
}

// failUncommittedWaitersLocked delivers a NotLeader outcome to every waiter
// whose entry index has not yet been committed. Called when this node leaves
// the leader role.
func (n *Node) failUncommittedWaitersLocked() {
	err := n.notLeaderLocked()
	for index, waiters := range n.waiters {
		if index <= n.commitIndex {
			continue
		}
		for _, w := range waiters {
			w <- applyOutcome{err: err}
			close(w)
		}
		delete(n.waiters, index)
	}
}

func (n *Node) replicateAllOnce(ctx context.Context) error {
	n.mu.Lock()
	if n.role != Leader {
		err := n.notLeaderLocked()
		n.mu.Unlock()
		return err
	}
	peers := make([]string, 0, len(n.peerAddrs))
	for peerID := range n.peerAddrs {
		peers = append(peers, peerID)
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, peerID := range peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			n.replicateToPeer(ctx, peerID)
		}(peerID)
	}
	wg.Wait()
	return nil
}

// replicateToPeer drives one peer's replication state forward. It returns true
// iff at least one AppendEntries or InstallSnapshot RPC returned Success during
// this call AND we were still leader at the same term when the ack arrived.
// This is the per-call "did the peer just acknowledge our leadership" signal
// ensureQuorum uses to confirm a fresh read barrier.
func (n *Node) replicateToPeer(ctx context.Context, peerID string) bool {
	for attempts := 0; attempts < 128; attempts++ {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return false
		}
		term := n.currentTerm
		next := n.nextIndex[peerID]
		if next == 0 {
			next = n.lastIndexLocked() + 1
			n.nextIndex[peerID] = next
		}
		if next <= n.snapshot.LastIncludedIndex {
			snapIdx := n.snapshot.LastIncludedIndex
			snapTerm := n.snapshot.LastIncludedTerm
			chunkSize := n.snapshotChunkBytes
			snapData := cloneMap(n.snapshot.Data)
			n.mu.Unlock()
			if !n.sendSnapshot(ctx, peerID, term, snapIdx, snapTerm, snapData, chunkSize) {
				return false
			}
			n.mu.Lock()
			if n.role == Leader && n.currentTerm == term {
				if n.matchIndex[peerID] < snapIdx {
					n.matchIndex[peerID] = snapIdx
				}
				if n.nextIndex[peerID] < snapIdx+1 {
					n.nextIndex[peerID] = snapIdx + 1
				}
			}
			n.mu.Unlock()
			return true
		}

		prevIndex := next - 1
		prevTerm, ok := n.termAtLocked(prevIndex)
		if !ok {
			n.nextIndex[peerID] = max(1, n.snapshot.LastIncludedIndex+1)
			n.mu.Unlock()
			continue
		}
		entries := n.entriesFromLocked(next)
		req := AppendEntriesRequest{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		resp, err := n.transport.AppendEntries(ctx, peerID, req)
		if err != nil {
			return false
		}

		n.mu.Lock()
		if resp.Term > n.currentTerm {
			_ = n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return false
		}
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return false
		}
		if resp.Success {
			match := prevIndex + uint64(len(entries))
			n.matchIndex[peerID] = match
			n.nextIndex[peerID] = match + 1
			n.advanceCommitLocked()
			n.mu.Unlock()
			return true
		}
		if resp.ConflictIndex > 0 {
			n.nextIndex[peerID] = resp.ConflictIndex
		} else if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		n.mu.Unlock()
	}
	return false
}

// sendSnapshot streams the current snapshot to a peer in chunks of at most
// chunkSize bytes. Returns true only when the follower acks the final chunk.
// Caller must hold no locks; we re-acquire n.mu only for term/role checks.
func (n *Node) sendSnapshot(ctx context.Context, peerID string, term, lastIdx, lastTerm uint64, data map[string]string, chunkSize int) bool {
	body, err := json.Marshal(data)
	if err != nil {
		return false
	}
	if chunkSize <= 0 {
		chunkSize = defaultSnapshotChunkBytes
	}
	offset := uint64(0)
	total := uint64(len(body))
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		end := offset + uint64(chunkSize)
		if end > total {
			end = total
		}
		done := end == total
		req := InstallSnapshotRequest{
			Term:              term,
			LeaderID:          n.id,
			LastIncludedIndex: lastIdx,
			LastIncludedTerm:  lastTerm,
			Offset:            offset,
			Data:              body[offset:end],
			Done:              done,
		}
		resp, err := n.transport.InstallSnapshot(ctx, peerID, req)
		if err != nil {
			return false
		}
		n.mu.Lock()
		if resp.Term > n.currentTerm {
			_ = n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return false
		}
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return false
		}
		n.mu.Unlock()
		if !resp.Success {
			// Follower told us the next offset it wants. Resume from there.
			// If the same offset comes back twice, give up to avoid a loop.
			if resp.BytesReceived == offset && !done {
				return false
			}
			offset = resp.BytesReceived
			if offset > total {
				return false
			}
			continue
		}
		if done {
			return true
		}
		offset = end
	}
}

// ensureQuorum implements the read barrier (Raft §6.4 readIndex): it dispatches
// a fresh round of AppendEntries/InstallSnapshot RPCs and only returns nil once
// it has confirmed acks from a current-term majority. Counting per-call ack
// (not cumulative matchIndex) is critical — otherwise a partitioned leader can
// pass the barrier on stale matchIndex values and serve a stale read.
func (n *Node) ensureQuorum(ctx context.Context) error {
	if n.quorum() == 1 {
		return nil
	}
	n.mu.Lock()
	if n.role != Leader {
		err := n.notLeaderLocked()
		n.mu.Unlock()
		return err
	}
	term := n.currentTerm
	peers := make([]string, 0, len(n.peerAddrs))
	for peerID := range n.peerAddrs {
		peers = append(peers, peerID)
	}
	n.mu.Unlock()

	var (
		mu       sync.Mutex
		acked    = 1 // count self
		failed   = 0
		target   = n.quorum()
		quorumCh = make(chan struct{}, 1)
		doneCh   = make(chan struct{}, 1)
	)

	signal := func() {
		mu.Lock()
		defer mu.Unlock()
		if acked >= target {
			select {
			case quorumCh <- struct{}{}:
			default:
			}
			return
		}
		// All peers responded (success or failure) and we still don't have a quorum.
		if acked+failed >= len(peers)+1 {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		}
	}

	for _, peerID := range peers {
		go func(peerID string) {
			ok := n.replicateToPeer(ctx, peerID)
			if ok {
				n.mu.Lock()
				stillLeader := n.role == Leader && n.currentTerm == term
				n.mu.Unlock()
				if !stillLeader {
					ok = false
				}
			}
			mu.Lock()
			if ok {
				acked++
			} else {
				failed++
			}
			mu.Unlock()
			signal()
		}(peerID)
	}

	select {
	case <-quorumCh:
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.role != Leader || n.currentTerm != term {
			return n.notLeaderLocked()
		}
		return nil
	case <-doneCh:
		return errors.New("leader cannot contact quorum")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) advanceCommitLocked() {
	last := n.lastIndexLocked()
	for index := last; index > n.commitIndex; index-- {
		term, ok := n.termAtLocked(index)
		if !ok || term != n.currentTerm {
			continue
		}
		count := 1
		for peerID := range n.peerAddrs {
			if n.matchIndex[peerID] >= index {
				count++
			}
		}
		if count >= n.quorum() {
			prev := n.commitIndex
			n.commitIndex = index
			if err := n.persistHardStateLocked(); err != nil {
				n.commitIndex = prev
				return
			}
			n.notifyApplyLocked()
			return
		}
	}
}

func (n *Node) applyReady() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		nextIndex := n.lastApplied + 1
		entry, ok := n.entryAtLocked(nextIndex)
		if !ok {
			if nextIndex <= n.snapshot.LastIncludedIndex {
				n.lastApplied = nextIndex
			}
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		result := n.sm.Apply(entry.Command)

		n.mu.Lock()
		if n.lastApplied < entry.Index {
			n.lastApplied = entry.Index
			n.finishWaitersLocked(entry.Index, result)
			if n.snapshotEvery > 0 && len(n.log) > n.snapshotEvery && n.lastApplied > n.snapshot.LastIncludedIndex {
				n.createSnapshotLocked(n.lastApplied)
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) createSnapshotLocked(index uint64) {
	term, ok := n.termAtLocked(index)
	if !ok {
		return
	}
	snapshot := storage.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              cloneMap(n.sm.Snapshot()),
	}
	newLog := make([]storage.LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry.Index > index {
			newLog = append(newLog, entry)
		}
	}
	if err := n.store.SaveSnapshot(snapshot); err != nil {
		n.lastErr = err
		return
	}
	if err := n.store.ReplaceEntries(newLog); err != nil {
		n.lastErr = err
		return
	}
	n.snapshot = snapshot
	n.log = newLog
}

func (n *Node) mergeEntriesLocked(entries []storage.LogEntry) bool {
	changed := false
	for i, entry := range entries {
		if entry.Index <= n.snapshot.LastIncludedIndex {
			continue
		}
		if entry.Index <= n.lastIndexLocked() {
			term, ok := n.termAtLocked(entry.Index)
			if ok && term == entry.Term {
				continue
			}
			n.truncateFromLocked(entry.Index)
			n.log = append(n.log, entries[i:]...)
			return true
		}
		if entry.Index == n.lastIndexLocked()+1 {
			n.log = append(n.log, entry)
			changed = true
		}
	}
	return changed
}

func (n *Node) conflictIndexLocked(index uint64) uint64 {
	if index > n.lastIndexLocked() {
		return n.lastIndexLocked() + 1
	}
	if index <= n.snapshot.LastIncludedIndex {
		return n.snapshot.LastIncludedIndex + 1
	}
	term, ok := n.termAtLocked(index)
	if !ok {
		return n.lastIndexLocked() + 1
	}
	for index > n.snapshot.LastIncludedIndex+1 {
		prevTerm, ok := n.termAtLocked(index - 1)
		if !ok || prevTerm != term {
			break
		}
		index--
	}
	return index
}

func (n *Node) truncateFromLocked(index uint64) {
	if index <= n.snapshot.LastIncludedIndex {
		n.log = nil
		return
	}
	keep := index - n.snapshot.LastIncludedIndex - 1
	if keep < uint64(len(n.log)) {
		n.log = append([]storage.LogEntry(nil), n.log[:keep]...)
	}
}

func (n *Node) dropLogThroughLocked(index uint64) {
	kept := n.log[:0]
	for _, entry := range n.log {
		if entry.Index > index {
			kept = append(kept, entry)
		}
	}
	n.log = append([]storage.LogEntry(nil), kept...)
}

func (n *Node) entriesFromLocked(index uint64) []storage.LogEntry {
	entries := make([]storage.LogEntry, 0)
	for _, entry := range n.log {
		if entry.Index >= index {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (n *Node) entryAtLocked(index uint64) (storage.LogEntry, bool) {
	if index <= n.snapshot.LastIncludedIndex {
		return storage.LogEntry{}, false
	}
	offset := index - n.snapshot.LastIncludedIndex - 1
	if offset >= uint64(len(n.log)) {
		return storage.LogEntry{}, false
	}
	entry := n.log[offset]
	return entry, entry.Index == index
}

func (n *Node) termAtLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index == n.snapshot.LastIncludedIndex {
		return n.snapshot.LastIncludedTerm, true
	}
	entry, ok := n.entryAtLocked(index)
	if !ok {
		return 0, false
	}
	return entry.Term, true
}

func (n *Node) lastIndexLocked() uint64 {
	return lastIndex(n.snapshot, n.log)
}

func (n *Node) lastTermLocked() uint64 {
	if len(n.log) == 0 {
		return n.snapshot.LastIncludedTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) persistHardStateLocked() error {
	err := n.store.SaveHardState(storage.HardState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		CommitIndex: n.commitIndex,
	})
	if err != nil {
		n.lastErr = err
	}
	return err
}

func (n *Node) notifyApplyLocked() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) finishWaitersLocked(index uint64, result storage.ApplyResult) {
	waiters := n.waiters[index]
	delete(n.waiters, index)
	for _, waiter := range waiters {
		waiter <- applyOutcome{result: result}
		close(waiter)
	}
}

func (n *Node) removeWaiter(index uint64, target chan applyOutcome) {
	n.mu.Lock()
	defer n.mu.Unlock()
	waiters := n.waiters[index]
	for i, waiter := range waiters {
		if waiter == target {
			n.waiters[index] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(n.waiters[index]) == 0 {
		delete(n.waiters, index)
	}
}

func (n *Node) waitUntilApplied(ctx context.Context, index uint64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= index {
			n.mu.Unlock()
			return nil
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (n *Node) quorum() int {
	return (len(n.peerAddrs)+1)/2 + 1
}

func (n *Node) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

func (n *Node) resetElectionLocked() {
	span := n.timeoutMax - n.timeoutMin
	delay := n.timeoutMin
	if span > 0 {
		delay += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDue = time.Now().Add(delay)
}

func (n *Node) notLeaderLocked() error {
	return NotLeaderError{LeaderID: n.leaderID, LeaderAddress: n.leaderAddressLocked()}
}

func (n *Node) leaderAddressLocked() string {
	if n.leaderID == "" {
		return ""
	}
	if addr := n.clientAddrs[n.leaderID]; addr != "" {
		return addr
	}
	if n.leaderID == n.id {
		return n.address
	}
	return n.peerAddrs[n.leaderID]
}

func (n *Node) LastError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastErr
}

func lastIndex(snapshot storage.Snapshot, entries []storage.LogEntry) uint64 {
	if len(entries) == 0 {
		return snapshot.LastIncludedIndex
	}
	return entries[len(entries)-1].Index
}

func cloneSnapshot(snapshot storage.Snapshot) storage.Snapshot {
	return storage.Snapshot{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              cloneMap(snapshot.Data),
	}
}

func cloneMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func hashID(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}

type valueReader interface {
	Get(key string) (string, bool)
}

func readValue(sm StateMachine, key string) (string, bool) {
	reader, ok := sm.(valueReader)
	if !ok {
		return "", false
	}
	return reader.Get(key)
}
