package main

import (
	"context"
	"reflect"
	"testing"

	"go-raft-kv/api"
)

func TestWithLeaderRetryTriesNextEndpointWithoutLeaderHint(t *testing.T) {
	var calls []string
	responses := map[string]*api.PutResponse{
		"follower": {OK: false, Error: "not leader"},
		"leader":   {OK: true},
	}

	resp, err := withLeaderRetryUsing(
		context.Background(),
		[]string{"follower", "leader"},
		func(context.Context, *api.KVClient) (*api.PutResponse, error) { return nil, nil },
		func(_ context.Context, endpoint string, _ func(context.Context, *api.KVClient) (*api.PutResponse, error)) (*api.PutResponse, error) {
			calls = append(calls, endpoint)
			return responses[endpoint], nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("expected leader response, got %+v", resp)
	}
	if want := []string{"follower", "leader"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls mismatch: got %v want %v", calls, want)
	}
}

func TestWithLeaderRetryFollowsLeaderHintOnce(t *testing.T) {
	var calls []string
	responses := map[string]*api.PutResponse{
		"follower": {OK: false, Error: "not leader", LeaderAddress: "leader"},
		"leader":   {OK: true},
	}

	resp, err := withLeaderRetryUsing(
		context.Background(),
		[]string{"follower"},
		func(context.Context, *api.KVClient) (*api.PutResponse, error) { return nil, nil },
		func(_ context.Context, endpoint string, _ func(context.Context, *api.KVClient) (*api.PutResponse, error)) (*api.PutResponse, error) {
			calls = append(calls, endpoint)
			return responses[endpoint], nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("expected leader response, got %+v", resp)
	}
	if want := []string{"follower", "leader"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls mismatch: got %v want %v", calls, want)
	}
}
