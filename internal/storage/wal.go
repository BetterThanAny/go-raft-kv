package storage

import (
	"bufio"
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

	file, err := os.OpenFile(s.wal, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return err
		}
	}
	return file.Sync()
}

func (s *Store) ReplaceEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeWALAtomic(s.wal, entries)
}

func (s *Store) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONAtomic(s.snapshot, snapshot)
}

func readWAL(path string) ([]LogEntry, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []LogEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("decode WAL entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func writeWALAtomic(path string, entries []LogEntry) error {
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
	return os.Rename(tmp, path)
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
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	file, err := os.OpenFile(tmp, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
