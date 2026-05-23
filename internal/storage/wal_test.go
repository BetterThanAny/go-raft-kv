package storage

import "testing"

func TestStorePersistsHardStateLogAndSnapshot(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entries := []LogEntry{
		{Index: 1, Term: 1, Command: Command{Op: OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Command: Command{Op: OpPut, Key: "b", Value: "2"}},
	}
	if err := store.SaveHardState(HardState{CurrentTerm: 2, VotedFor: "node2", CommitIndex: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: map[string]string{"a": "1"}}); err != nil {
		t.Fatal(err)
	}

	hs, loaded, snapshot, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.CurrentTerm != 2 || hs.VotedFor != "node2" || hs.CommitIndex != 2 {
		t.Fatalf("bad hard state: %+v", hs)
	}
	if snapshot.LastIncludedIndex != 1 || snapshot.Data["a"] != "1" {
		t.Fatalf("bad snapshot: %+v", snapshot)
	}
	if len(loaded) != 1 || loaded[0].Index != 2 {
		t.Fatalf("expected compacted WAL to keep only index 2, got %+v", loaded)
	}
}
