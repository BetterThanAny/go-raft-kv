package main

import (
	"context"
	"errors"
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

func TestWithLeaderRetryRetriesLeaderHintAfterTransientFailure(t *testing.T) {
	var calls []string
	leaderCalls := 0

	resp, err := withLeaderRetryUsing(
		context.Background(),
		[]string{"leader", "follower"},
		func(context.Context, *api.KVClient) (*api.PutResponse, error) { return nil, nil },
		func(_ context.Context, endpoint string, _ func(context.Context, *api.KVClient) (*api.PutResponse, error)) (*api.PutResponse, error) {
			calls = append(calls, endpoint)
			switch endpoint {
			case "leader":
				leaderCalls++
				if leaderCalls == 1 {
					return nil, errors.New("temporary dial failure")
				}
				return &api.PutResponse{OK: true}, nil
			case "follower":
				return &api.PutResponse{OK: false, Error: "not leader", LeaderAddress: "leader"}, nil
			default:
				return nil, errors.New("unexpected endpoint")
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("expected leader response, got %+v", resp)
	}
	if want := []string{"leader", "follower", "leader"}; !reflect.DeepEqual(calls, want) {
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

func TestWithLeaderRetryDeduplicatesRepeatedLeaderHints(t *testing.T) {
	var calls []string
	responses := map[string]*api.PutResponse{
		"follower1": {OK: false, Error: "not leader", LeaderAddress: "stale"},
		"follower2": {OK: false, Error: "not leader", LeaderAddress: "stale"},
		"stale":     {OK: false, Error: "not leader"},
	}

	_, err := withLeaderRetryUsing(
		context.Background(),
		[]string{"follower1", "follower2"},
		func(context.Context, *api.KVClient) (*api.PutResponse, error) { return nil, nil },
		func(_ context.Context, endpoint string, _ func(context.Context, *api.KVClient) (*api.PutResponse, error)) (*api.PutResponse, error) {
			calls = append(calls, endpoint)
			return responses[endpoint], nil
		},
	)
	if err == nil {
		t.Fatal("expected no reachable leader")
	}
	if want := []string{"follower1", "follower2", "stale"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls mismatch: got %v want %v", calls, want)
	}
}
