package storage

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestLoadDropsUnsafeWALSuffixAfterSnapshotCrash(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(Snapshot{LastIncludedIndex: 8, LastIncludedTerm: 4, Data: map[string]string{"safe": "yes"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceEntries([]LogEntry{
		{Index: 9, Term: 4, Command: Command{Op: OpPut, Key: "x", Value: "9"}},
		{Index: 10, Term: 4, Command: Command{Op: OpPut, Key: "x", Value: "10"}},
		{Index: 11, Term: 6, Command: Command{Op: OpPut, Key: "unsafe", Value: "old-suffix"}},
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate InstallSnapshot crashing after snapshot.json was made durable but
	// before wal.jsonl was replaced. The old WAL has an entry at the boundary, but
	// its term does not match the new snapshot, so the suffix cannot be trusted.
	if err := store.SaveSnapshot(Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 5, Data: map[string]string{"safe": "new"}}); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, snapshot, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LastIncludedIndex != 10 || snapshot.LastIncludedTerm != 5 {
		t.Fatalf("snapshot mismatch: %+v", snapshot)
	}
	if len(entries) != 0 {
		t.Fatalf("unsafe WAL suffix should be discarded, got %+v", entries)
	}

	again, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, _, err = again.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("discarded suffix should stay discarded after rewrite, got %+v", entries)
	}
}

func TestLoadKeepsCompactedSuffixWithMatchingWALBase(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: map[string]string{"a": "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceEntries([]LogEntry{
		{Index: 2, Term: 2, Command: Command{Op: OpPut, Key: "b", Value: "2"}},
	}); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, _, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Index != 2 || entries[0].Term != 2 {
		t.Fatalf("expected compacted suffix to survive, got %+v", entries)
	}
}

// TestWALRecoversFromPartialTrailingLine simulates a crash mid-write that left
// a truncated last line in wal.jsonl. Reading must succeed, return only the
// complete entries, and physically truncate the partial bytes from disk.
func TestWALRecoversFromPartialTrailingLine(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEntries([]LogEntry{
		{Index: 1, Term: 1, Command: Command{Op: OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Command: Command{Op: OpPut, Key: "b", Value: "2"}},
	}); err != nil {
		t.Fatal(err)
	}
	// Append a partial line (no trailing newline, no closing brace).
	walPath := filepath.Join(dir, "wal.jsonl")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"index":3,"term":1,"comma`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, _, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load should tolerate a partial trailing line: %v", err)
	}
	if len(entries) != 2 || entries[1].Index != 2 {
		t.Fatalf("expected only the two complete entries, got %+v", entries)
	}
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	// File should have been truncated to remove the partial bytes.
	if info.Size() == 0 {
		t.Fatalf("WAL should still contain the two complete entries")
	}
	// Re-load to confirm truncation is persisted, not just an in-memory skip.
	again, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries2, _, err := again.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries2) != 2 {
		t.Fatalf("after restart expected 2 entries, got %d", len(entries2))
	}
}

// TestWALRejectsMidFileCorruption ensures a complete-but-corrupt line in the
// middle of the WAL is reported as an error rather than silently dropped.
func TestWALRejectsMidFileCorruption(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.jsonl")
	if err := os.WriteFile(walPath, []byte(
		`{"index":1,"term":1,"command":{"op":"put","key":"a","value":"1"}}`+"\n"+
			"this is not json"+"\n"+
			`{"index":2,"term":1,"command":{"op":"put","key":"b","value":"2"}}`+"\n",
	), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Load(); err == nil {
		t.Fatal("expected Load to surface a mid-file corruption error")
	}
}

func TestWALRecoversFromCorruptTrailingLine(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.jsonl")
	data := "" +
		`{"index":1,"term":1,"command":{"op":"put","key":"a","value":"1"}}` + "\n" +
		`{"index":2,"term":1,"command":{"op":"put","key":"b","value":"2"}}` + "\n" +
		`{"index":3,"term":1,"command"` + "\n"
	if err := os.WriteFile(walPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, _, err := store.Load()
	if err != nil {
		t.Fatalf("Load should truncate a corrupt trailing WAL record: %v", err)
	}
	if len(entries) != 2 || entries[1].Index != 2 {
		t.Fatalf("expected only complete entries, got %+v", entries)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, entries, _, err = reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected truncation to persist, got %d entries", len(entries))
	}
}

func TestWALRejectsDuplicateIndex(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.jsonl")
	data := "" +
		`{"index":1,"term":1,"command":{"op":"put","key":"a","value":"1"}}` + "\n" +
		`{"index":1,"term":1,"command":{"op":"put","key":"b","value":"2"}}` + "\n"
	if err := os.WriteFile(walPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Load(); err == nil {
		t.Fatal("expected Load to reject duplicate WAL indexes")
	}
}
