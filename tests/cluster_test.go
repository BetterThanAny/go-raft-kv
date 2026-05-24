package tests

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go-raft-kv/internal/raft"
	"go-raft-kv/internal/server"
	"go-raft-kv/internal/storage"
)

type memoryCluster struct {
	t                  testing.TB
	root               string
	peers              map[string]string
	client             map[string]string
	net                *memoryTransport
	mu                 sync.RWMutex
	nodes              map[string]*clusterNode
	alive              map[string]bool
	snapshotChunkBytes int
}

type clusterNode struct {
	id   string
	dir  string
	node *raft.Node
	sm   *server.KVStateMachine
}

type memoryTransport struct {
	cluster *memoryCluster
}

func TestLeaderFailoverAndFollowerCatchUp(t *testing.T) {
	cluster := newMemoryCluster(t, 3, 64)
	defer cluster.stopAll()

	leaderID, leader := cluster.waitLeader(t, "")
	proposePut(t, leader, "user:1", "alice")
	cluster.waitActiveValue(t, "user:1", "alice")

	cluster.stopNode(leaderID)
	newLeaderID, newLeader := cluster.waitLeader(t, leaderID)
	if newLeaderID == leaderID {
		t.Fatalf("expected a different leader after stopping %s", leaderID)
	}
	proposePut(t, newLeader, "user:2", "bob")
	cluster.waitActiveValue(t, "user:2", "bob")

	cluster.restartNode(t, leaderID)
	cluster.waitValue(t, leaderID, "user:1", "alice")
	cluster.waitValue(t, leaderID, "user:2", "bob")
}

func TestOneNodeDownStillAcceptsWrites(t *testing.T) {
	cluster := newMemoryCluster(t, 3, 64)
	defer cluster.stopAll()

	leaderID, leader := cluster.waitLeader(t, "")
	var follower string
	for id := range cluster.nodes {
		if id != leaderID {
			follower = id
			break
		}
	}
	cluster.stopNode(follower)

	proposePut(t, leader, "available", "yes")
	cluster.waitValue(t, leaderID, "available", "yes")
}

func TestSnapshotCompactionAndRestartRecovery(t *testing.T) {
	cluster := newMemoryCluster(t, 1, 3)
	defer cluster.stopAll()

	leaderID, leader := cluster.waitLeader(t, "")
	for i := 0; i < 10; i++ {
		proposePut(t, leader, fmt.Sprintf("k:%02d", i), fmt.Sprintf("v:%02d", i))
	}
	cluster.waitValue(t, leaderID, "k:09", "v:09")

	status := leader.Status()
	if status.SnapshotIndex == 0 {
		t.Fatalf("expected snapshot compaction, status=%+v", status)
	}
	store, err := storage.Open(cluster.nodes[leaderID].dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, snapshot, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LastIncludedIndex == 0 {
		t.Fatalf("snapshot was not persisted")
	}
	if len(entries) >= 10 {
		t.Fatalf("expected WAL entries to be truncated after snapshot, got %d", len(entries))
	}

	cluster.restartNode(t, leaderID)
	cluster.waitLeader(t, "")
	cluster.waitValue(t, leaderID, "k:00", "v:00")
	cluster.waitValue(t, leaderID, "k:09", "v:09")
}

// TestFollowerCatchesUpViaChunkedInstallSnapshot stops a follower, advances
// the leader far enough to trigger snapshot compaction, then restarts the
// follower and verifies it catches up via the chunked InstallSnapshot path.
// SnapshotChunkBytes is set tiny so the snapshot bytes split into many chunks
// — the test fails if the leader can't stream them or the follower can't
// reassemble them.
func TestFollowerCatchesUpViaChunkedInstallSnapshot(t *testing.T) {
	cluster := newMemoryClusterWithChunkBytes(t, 3, 3, 8)
	defer cluster.stopAll()

	leaderID, leader := cluster.waitLeader(t, "")
	var followerID string
	for id := range cluster.nodes {
		if id != leaderID {
			followerID = id
			break
		}
	}
	cluster.stopNode(followerID)

	for i := 0; i < 12; i++ {
		proposePut(t, leader, fmt.Sprintf("k:%02d", i), fmt.Sprintf("v:%02d", i))
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Status().SnapshotIndex > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader.Status().SnapshotIndex == 0 {
		t.Fatal("expected leader snapshot to advance before catching up follower")
	}

	cluster.restartNode(t, followerID)

	for i := 0; i < 12; i++ {
		cluster.waitValue(t, followerID, fmt.Sprintf("k:%02d", i), fmt.Sprintf("v:%02d", i))
	}
}

// TestLinearizableGetAfterLeaderChange verifies a freshly elected leader can
// serve a linearizable read of a value committed by the previous leader without
// requiring a fresh client write. This exercises the no-op-on-becomeLeader path
// that sweeps previous-term entries into the new leader's commitIndex, and the
// readIndex barrier that uses fresh ack counts (not stale matchIndex).
func TestLinearizableGetAfterLeaderChange(t *testing.T) {
	cluster := newMemoryCluster(t, 3, 64)
	defer cluster.stopAll()

	leaderID, leader := cluster.waitLeader(t, "")
	proposePut(t, leader, "kept", "across-leader-change")
	cluster.waitActiveValue(t, "kept", "across-leader-change")

	cluster.stopNode(leaderID)
	_, newLeader := cluster.waitLeader(t, leaderID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	value, found, err := newLeader.LinearizableGet(ctx, "kept")
	if err != nil {
		t.Fatalf("linearizable get on new leader failed: %v", err)
	}
	if !found || value != "across-leader-change" {
		t.Fatalf("expected linearizable get to see prev-term value, got value=%q found=%v", value, found)
	}
}

func TestWALRecoversHardStateAndLog(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveHardState(storage.HardState{CurrentTerm: 7, VotedFor: "node2", CommitIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]storage.LogEntry{
		{Index: 1, Term: 7, Command: storage.Command{Op: storage.OpPut, Key: "x", Value: "1"}},
		{Index: 2, Term: 7, Command: storage.Command{Op: storage.OpPut, Key: "y", Value: "2"}},
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs, entries, _, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.CurrentTerm != 7 || hs.VotedFor != "node2" || hs.CommitIndex != 1 {
		t.Fatalf("hard state mismatch: %+v", hs)
	}
	if len(entries) != 2 || entries[1].Command.Key != "y" {
		t.Fatalf("log mismatch: %+v", entries)
	}
}

func BenchmarkSingleNodePut(b *testing.B) {
	cluster := newMemoryCluster(b, 1, 1024)
	defer cluster.stopAll()
	_, leader := cluster.waitLeader(b, "")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := leader.Propose(ctx, storage.Command{
			Op:    storage.OpPut,
			Key:   fmt.Sprintf("bench:%d", i),
			Value: "value",
		})
		cancel()
		if err != nil {
			b.Fatal(err)
		}
		if !res.OK {
			b.Fatalf("proposal failed: %+v", res)
		}
	}
}

func newMemoryCluster(t testing.TB, size int, snapshotThreshold int) *memoryCluster {
	return newMemoryClusterWithChunkBytes(t, size, snapshotThreshold, 0)
}

func newMemoryClusterWithChunkBytes(t testing.TB, size int, snapshotThreshold, chunkBytes int) *memoryCluster {
	t.Helper()

	cluster := &memoryCluster{
		t:                  t,
		root:               t.TempDir(),
		peers:              make(map[string]string),
		client:             make(map[string]string),
		nodes:              make(map[string]*clusterNode),
		alive:              make(map[string]bool),
		snapshotChunkBytes: chunkBytes,
	}
	cluster.net = &memoryTransport{cluster: cluster}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node%d", i)
		cluster.peers[id] = id
		cluster.client[id] = id
	}
	for id := range cluster.peers {
		cluster.nodes[id] = cluster.newNode(t, id, snapshotThreshold)
		cluster.alive[id] = true
	}
	for _, node := range cluster.nodes {
		node.node.Start()
	}
	return cluster
}

func (c *memoryCluster) newNode(t testing.TB, id string, snapshotThreshold int) *clusterNode {
	t.Helper()
	dir := filepath.Join(c.root, id)
	c.mu.RLock()
	old := c.nodes[id]
	c.mu.RUnlock()
	if old != nil {
		dir = old.dir
	}
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sm := server.NewKVStateMachine()
	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Address:            id,
		Peers:              c.peers,
		ClientAddresses:    c.client,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 160 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		SnapshotThreshold:  snapshotThreshold,
		SnapshotChunkBytes: c.snapshotChunkBytes,
	}, store, sm, c.net)
	if err != nil {
		t.Fatal(err)
	}
	return &clusterNode{id: id, dir: dir, node: node, sm: sm}
}

func (c *memoryCluster) stopNode(id string) {
	c.mu.Lock()
	node := c.nodes[id]
	c.alive[id] = false
	c.mu.Unlock()
	if node != nil {
		node.node.Stop()
	}
}

func (c *memoryCluster) restartNode(t testing.TB, id string) {
	t.Helper()
	c.mu.RLock()
	old := c.nodes[id]
	c.mu.RUnlock()
	if old == nil {
		t.Fatalf("unknown node %s", id)
	}
	old.node.Stop()
	node := c.newNode(t, id, 64)
	c.mu.Lock()
	c.nodes[id] = node
	c.alive[id] = true
	c.mu.Unlock()
	node.node.Start()
}

func (c *memoryCluster) stopAll() {
	c.mu.RLock()
	nodes := make([]*clusterNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	c.mu.RUnlock()
	for _, node := range nodes {
		node.node.Stop()
	}
}

func (c *memoryCluster) waitLeader(t testing.TB, exclude string) (string, *raft.Node) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		nodes := make(map[string]*clusterNode, len(c.nodes))
		alive := make(map[string]bool, len(c.alive))
		for id, node := range c.nodes {
			nodes[id] = node
		}
		for id, up := range c.alive {
			alive[id] = up
		}
		c.mu.RUnlock()
		for id, node := range nodes {
			if id == exclude || !alive[id] {
				continue
			}
			status := node.node.Status()
			if status.Role == raft.Leader {
				return id, node.node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader")
	return "", nil
}

func (c *memoryCluster) waitActiveValue(t *testing.T, key, value string) {
	t.Helper()
	c.mu.RLock()
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		if c.alive[id] {
			ids = append(ids, id)
		}
	}
	c.mu.RUnlock()
	for _, id := range ids {
		c.waitValue(t, id, key, value)
	}
}

func (c *memoryCluster) waitValue(t *testing.T, id, key, value string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		node := c.nodes[id]
		c.mu.RUnlock()
		if node != nil {
			if got, ok := node.sm.Get(key); ok && got == value {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %s did not observe %s=%s", id, key, value)
}

func proposePut(t *testing.T, node *raft.Node, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := node.Propose(ctx, storage.Command{Op: storage.OpPut, Key: key, Value: value})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("proposal failed: %+v", res)
	}
}

func (tpt *memoryTransport) RequestVote(ctx context.Context, peerID string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	node, err := tpt.lookup(peerID)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return node.HandleRequestVote(req), nil
}

func (tpt *memoryTransport) AppendEntries(ctx context.Context, peerID string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	node, err := tpt.lookup(peerID)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return node.HandleAppendEntries(req), nil
}

func (tpt *memoryTransport) InstallSnapshot(ctx context.Context, peerID string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	node, err := tpt.lookup(peerID)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return node.HandleInstallSnapshot(req), nil
}

func (tpt *memoryTransport) lookup(peerID string) (*raft.Node, error) {
	tpt.cluster.mu.RLock()
	defer tpt.cluster.mu.RUnlock()
	node := tpt.cluster.nodes[peerID]
	if node == nil || !tpt.cluster.alive[peerID] {
		return nil, errors.New("peer unavailable")
	}
	return node.node, nil
}
