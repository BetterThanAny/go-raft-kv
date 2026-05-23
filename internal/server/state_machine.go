package server

import (
	"sync"

	"go-raft-kv/internal/storage"
)

type KVStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: make(map[string]string)}
}

func (sm *KVStateMachine) Apply(command storage.Command) storage.ApplyResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch command.Op {
	case storage.OpPut:
		sm.data[command.Key] = command.Value
		return storage.ApplyResult{OK: true, Found: true, Value: command.Value}
	case storage.OpDelete:
		previous, found := sm.data[command.Key]
		delete(sm.data, command.Key)
		return storage.ApplyResult{OK: true, Found: found, Value: previous}
	case storage.OpCAS:
		current, found := sm.data[command.Key]
		if !found || current != command.Expected {
			return storage.ApplyResult{OK: true, Found: found, Value: current, Swapped: false}
		}
		sm.data[command.Key] = command.Value
		return storage.ApplyResult{OK: true, Found: true, Value: command.Value, Swapped: true}
	case storage.OpNoop:
		return storage.ApplyResult{OK: true}
	default:
		return storage.ApplyResult{OK: false, Error: "unknown command"}
	}
}

func (sm *KVStateMachine) Snapshot() map[string]string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make(map[string]string, len(sm.data))
	for key, value := range sm.data {
		out[key] = value
	}
	return out
}

func (sm *KVStateMachine) Restore(snapshot map[string]string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data = make(map[string]string, len(snapshot))
	for key, value := range snapshot {
		sm.data[key] = value
	}
}

func (sm *KVStateMachine) Get(key string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	value, found := sm.data[key]
	return value, found
}
