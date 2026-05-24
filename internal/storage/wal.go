package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type Store struct {
	mu        sync.Mutex
	dir       string
	hardState string
	wal       string
	snapshot  string
}

const walBaseRecordType = "base"

type walBaseRecord struct {
	Type              string `json:"type"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

type walData struct {
	hasBase   bool
	baseIndex uint64
	baseTerm  uint64
	entries   []LogEntry
}

type walLine struct {
	Type              string  `json:"type,omitempty"`
	LastIncludedIndex uint64  `json:"last_included_index,omitempty"`
	LastIncludedTerm  uint64  `json:"last_included_term,omitempty"`
	Index             uint64  `json:"index,omitempty"`
	Term              uint64  `json:"term,omitempty"`
	Command           Command `json:"command,omitempty"`
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{
		dir:       dir,
		hardState: filepath.Join(dir, "hard_state.json"),
		wal:       filepath.Join(dir, "wal.jsonl"),
		snapshot:  filepath.Join(dir, "snapshot.json"),
	}, nil
}

func (s *Store) Load() (HardState, []LogEntry, Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var hs HardState
	if err := readJSONFile(s.hardState, &hs); err != nil && !errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil, Snapshot{}, err
	}

	var snap Snapshot
	if err := readJSONFile(s.snapshot, &snap); err != nil && !errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil, Snapshot{}, err
	}

	wal, err := readWAL(s.wal)
	if err != nil {
		return HardState{}, nil, Snapshot{}, err
	}
	entries := wal.entries
	if snap.LastIncludedIndex > 0 {
		var rewrite bool
		entries, rewrite = reconcileWALWithSnapshot(wal, snap)
		if rewrite {
			if err := writeWALAtomic(s.dir, s.wal, snap.LastIncludedIndex, snap.LastIncludedTerm, entries); err != nil {
				return HardState{}, nil, Snapshot{}, err
			}
		}
	} else if wal.hasBase && wal.baseIndex > 0 {
		return HardState{}, nil, Snapshot{}, fmt.Errorf("WAL is based at snapshot index %d but snapshot is missing", wal.baseIndex)
	}
	return hs, entries, snap, nil
}

func (s *Store) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONAtomic(s.hardState, hs)
}

func (s *Store) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Detect first-time creation so we know whether to fsync the parent dir.
	_, statErr := os.Stat(s.wal)
	created := errors.Is(statErr, os.ErrNotExist)
	baseIndex, baseTerm := uint64(0), uint64(0)
	if created {
		var err error
		baseIndex, baseTerm, err = s.snapshotBaseLocked()
		if err != nil {
			return err
		}
	}

	file, err := os.OpenFile(s.wal, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	if created {
		if err := enc.Encode(walBaseRecord{
			Type:              walBaseRecordType,
			LastIncludedIndex: baseIndex,
			LastIncludedTerm:  baseTerm,
		}); err != nil {
			_ = file.Close()
			return err
		}
	}
	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if created {
		if err := syncDir(s.dir); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReplaceEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	baseIndex, baseTerm, err := s.snapshotBaseLocked()
	if err != nil {
		return err
	}
	return writeWALAtomic(s.dir, s.wal, baseIndex, baseTerm, entries)
}

func (s *Store) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONAtomic(s.snapshot, snapshot)
}

// readWAL parses the WAL file, tolerating a single partial trailing line caused
// by a crash mid-write. A partial line at EOF is truncated; a corrupt complete
// line in the middle is reported as an error.
func readWAL(path string) (walData, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return walData{}, nil
	}
	if err != nil {
		return walData{}, err
	}

	var (
		wal    walData
		offset int
	)
	for offset < len(data) {
		nl := bytes.IndexByte(data[offset:], '\n')
		if nl < 0 {
			// Trailing bytes without a newline = partial write from a crash. Truncate.
			if err := truncateFile(path, int64(offset)); err != nil {
				return walData{}, fmt.Errorf("truncate partial WAL tail: %w", err)
			}
			break
		}
		line := bytes.TrimSpace(data[offset : offset+nl])
		offset += nl + 1
		if len(line) == 0 {
			continue
		}
		record, err := decodeWALLine(line)
		if err != nil {
			if len(bytes.TrimSpace(data[offset:])) == 0 {
				if err := truncateFile(path, int64(offset-nl-1)); err != nil {
					return walData{}, fmt.Errorf("truncate corrupt WAL tail: %w", err)
				}
				break
			}
			return walData{}, fmt.Errorf("decode WAL entry at byte offset %d: %w", offset-nl-1, err)
		}
		if record.base != nil {
			if wal.hasBase || len(wal.entries) > 0 {
				return walData{}, fmt.Errorf("decode WAL entry at byte offset %d: base record must be first", offset-nl-1)
			}
			wal.hasBase = true
			wal.baseIndex = record.base.LastIncludedIndex
			wal.baseTerm = record.base.LastIncludedTerm
			continue
		}
		entry := record.entry
		if entry.Index == 0 {
			return walData{}, fmt.Errorf("decode WAL entry at byte offset %d: zero log index", offset-nl-1)
		}
		if wal.hasBase && entry.Index <= wal.baseIndex {
			return walData{}, fmt.Errorf("decode WAL entry at byte offset %d: log index %d is not after base index %d", offset-nl-1, entry.Index, wal.baseIndex)
		}
		if len(wal.entries) == 0 && wal.hasBase && entry.Index != wal.baseIndex+1 {
			return walData{}, fmt.Errorf("decode WAL entry at byte offset %d: non-contiguous first log index %d after base %d", offset-nl-1, entry.Index, wal.baseIndex)
		}
		if len(wal.entries) > 0 {
			prev := wal.entries[len(wal.entries)-1]
			if entry.Index != prev.Index+1 {
				return walData{}, fmt.Errorf("decode WAL entry at byte offset %d: non-contiguous log index %d after %d", offset-nl-1, entry.Index, prev.Index)
			}
		}
		wal.entries = append(wal.entries, entry)
	}
	return wal, nil
}

func writeWALAtomic(dir, path string, baseIndex, baseTerm uint64, entries []LogEntry) error {
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	if err := enc.Encode(walBaseRecord{
		Type:              walBaseRecordType,
		LastIncludedIndex: baseIndex,
		LastIncludedTerm:  baseTerm,
	}); err != nil {
		_ = file.Close()
		return err
	}
	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}

type decodedWALLine struct {
	base  *walBaseRecord
	entry LogEntry
}

func decodeWALLine(line []byte) (decodedWALLine, error) {
	var raw walLine
	if err := json.Unmarshal(line, &raw); err != nil {
		return decodedWALLine{}, err
	}
	switch raw.Type {
	case walBaseRecordType:
		return decodedWALLine{
			base: &walBaseRecord{
				Type:              walBaseRecordType,
				LastIncludedIndex: raw.LastIncludedIndex,
				LastIncludedTerm:  raw.LastIncludedTerm,
			},
		}, nil
	case "":
		return decodedWALLine{
			entry: LogEntry{
				Index:   raw.Index,
				Term:    raw.Term,
				Command: raw.Command,
			},
		}, nil
	default:
		return decodedWALLine{}, fmt.Errorf("unknown WAL record type %q", raw.Type)
	}
}

func reconcileWALWithSnapshot(wal walData, snap Snapshot) ([]LogEntry, bool) {
	if wal.hasBase && wal.baseIndex == snap.LastIncludedIndex && wal.baseTerm == snap.LastIncludedTerm {
		entries, changed := entriesAfter(wal.entries, snap.LastIncludedIndex)
		return entries, changed
	}
	if hasSnapshotBoundaryEntry(wal.entries, snap) {
		entries, _ := entriesAfter(wal.entries, snap.LastIncludedIndex)
		return entries, true
	}
	if len(wal.entries) == 0 && !wal.hasBase {
		return nil, false
	}
	return nil, true
}

func hasSnapshotBoundaryEntry(entries []LogEntry, snap Snapshot) bool {
	for _, entry := range entries {
		if entry.Index == snap.LastIncludedIndex {
			return entry.Term == snap.LastIncludedTerm
		}
	}
	return snap.LastIncludedIndex == 0 && snap.LastIncludedTerm == 0
}

func entriesAfter(entries []LogEntry, index uint64) ([]LogEntry, bool) {
	out := entries[:0]
	for _, entry := range entries {
		if entry.Index > index {
			out = append(out, entry)
		}
	}
	changed := len(out) != len(entries)
	return append([]LogEntry(nil), out...), changed
}

func (s *Store) snapshotBaseLocked() (uint64, uint64, error) {
	var snap Snapshot
	if err := readJSONFile(s.snapshot, &snap); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	return snap.LastIncludedIndex, snap.LastIncludedTerm, nil
}

func readJSONFile(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func truncateFile(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// syncDir fsyncs the directory so a prior rename becomes durable.
// Best-effort on platforms (e.g. Windows) that disallow opening a dir for sync;
// on Unix this is required for atomic-rename durability.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		// Some platforms (e.g. macOS on certain filesystems) may reject fsync on a directory.
		// Treat EINVAL/ENOTSUP as success since we did our best.
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}
	return f.Close()
}
