package raft

import "go-raft-kv/internal/storage"

type StateMachine interface {
	Apply(command storage.Command) storage.ApplyResult
	Snapshot() map[string]string
	Restore(snapshot map[string]string)
}
