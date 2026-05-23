package server

import (
	"testing"

	"go-raft-kv/internal/storage"
)

func TestKVStateMachineAppliesNoop(t *testing.T) {
	sm := NewKVStateMachine()
	sm.Apply(storage.Command{Op: storage.OpPut, Key: "k", Value: "v"})

	res := sm.Apply(storage.Command{Op: storage.OpNoop})
	if !res.OK {
		t.Fatalf("OpNoop should apply cleanly, got %+v", res)
	}
	if got, ok := sm.Get("k"); !ok || got != "v" {
		t.Fatalf("OpNoop must not mutate state: got %q ok=%v", got, ok)
	}
}

func TestKVStateMachineUnknownOpStillRejected(t *testing.T) {
	sm := NewKVStateMachine()
	res := sm.Apply(storage.Command{Op: storage.Operation("garbage")})
	if res.OK {
		t.Fatalf("unknown op must not succeed: %+v", res)
	}
}
