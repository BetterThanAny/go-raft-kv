package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	mu        sync.Mutex
	dir       string
	hardState string
	wal       string
	snapshot  string
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

	entries, err := readWAL(s.wal)
	if err != nil {
		return HardState{}, nil, Snapshot{}, err
	}
	if snap.LastIncludedIndex > 0 {
		kept := entries[:0]
		for _, entry := range entries {
			if entry.Index > snap.LastIncludedIndex {
				kept = append(kept, entry)
			}
		}
		entries = kept
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

	file, err := os.OpenFile(s.wal, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
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
	return writeWALAtomic(s.dir, s.wal, entries)
}

func (s *Store) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONAtomic(s.snapshot, snapshot)
}

// readWAL parses the WAL file, tolerating a single partial trailing line caused
// by a crash mid-write. A partial line at EOF is truncated; a corrupt complete
// line in the middle is reported as an error.
func readWAL(path string) ([]LogEntry, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var (
		entries []LogEntry
		offset  int
	)
	for offset < len(data) {
		nl := bytes.IndexByte(data[offset:], '\n')
		if nl < 0 {
			// Trailing bytes without a newline = partial write from a crash. Truncate.
			if err := truncateFile(path, int64(offset)); err != nil {
				return nil, fmt.Errorf("truncate partial WAL tail: %w", err)
			}
			break
		}
		line := bytes.TrimSpace(data[offset : offset+nl])
		offset += nl + 1
		if len(line) == 0 {
			continue
		}
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("decode WAL entry at byte offset %d: %w", offset-nl-1, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func writeWALAtomic(dir, path string, entries []LogEntry) error {
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
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

func readJSONFile(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
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
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			if errno, ok := pathErr.Err.(interface{ Error() string }); ok {
				msg := errno.Error()
				if msg == "invalid argument" || msg == "operation not supported" {
					return nil
				}
			}
		}
		return err
	}
	return f.Close()
}
