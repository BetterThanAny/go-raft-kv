package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"go-raft-kv/api"
	"google.golang.org/grpc"
)

func main() {
	endpointsFlag := flag.String("endpoints", defaultEndpoints(), "comma separated node addresses")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	endpoints := splitCSV(*endpointsFlag)
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "no endpoints configured")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()

	cmd := args[0]
	var err error
	switch cmd {
	case "put":
		if len(args) != 3 {
			usage()
			os.Exit(2)
		}
		err = put(ctx, endpoints, args[1], args[2])
	case "get":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		err = get(ctx, endpoints, args[1])
	case "delete":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		err = del(ctx, endpoints, args[1])
	case "cas":
		if len(args) != 4 {
			usage()
			os.Exit(2)
		}
		err = cas(ctx, endpoints, args[1], args[2], args[3])
	case "status":
		err = status(ctx, endpoints)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func put(ctx context.Context, endpoints []string, key, value string) error {
	resp, err := withLeaderRetry(ctx, endpoints, func(ctx context.Context, client *api.KVClient) (*api.PutResponse, error) {
		return client.Put(ctx, &api.PutRequest{Key: key, Value: value})
	})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	fmt.Println("OK")
	return nil
}

func get(ctx context.Context, endpoints []string, key string) error {
	resp, err := withLeaderRetry(ctx, endpoints, func(ctx context.Context, client *api.KVClient) (*api.GetResponse, error) {
		return client.Get(ctx, &api.GetRequest{Key: key})
	})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if !resp.Found {
		return errors.New("not found")
	}
	fmt.Println(resp.Value)
	return nil
}

func del(ctx context.Context, endpoints []string, key string) error {
	resp, err := withLeaderRetry(ctx, endpoints, func(ctx context.Context, client *api.KVClient) (*api.DeleteResponse, error) {
		return client.Delete(ctx, &api.DeleteRequest{Key: key})
	})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if resp.Found {
		fmt.Println(resp.PreviousValue)
		return nil
	}
	fmt.Println("OK")
	return nil
}

func cas(ctx context.Context, endpoints []string, key, expected, value string) error {
	resp, err := withLeaderRetry(ctx, endpoints, func(ctx context.Context, client *api.KVClient) (*api.CASResponse, error) {
		return client.CAS(ctx, &api.CASRequest{Key: key, Expected: expected, Value: value})
	})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if !resp.Swapped {
		return fmt.Errorf("not swapped current=%q", resp.CurrentValue)
	}
	fmt.Println("OK")
	return nil
}

func status(ctx context.Context, endpoints []string) error {
	statuses := make([]*api.StatusResponse, 0, len(endpoints))
	for _, endpoint := range endpoints {
		resp, err := call(ctx, endpoint, func(ctx context.Context, client *api.KVClient) (*api.StatusResponse, error) {
			return client.Status(ctx, &api.StatusRequest{})
		})
		if err == nil {
			statuses = append(statuses, resp)
		}
	}
	if len(statuses) == 0 {
		return errors.New("no reachable endpoints")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(statuses)
}

func withLeaderRetry[T any](ctx context.Context, endpoints []string, fn func(context.Context, *api.KVClient) (T, error)) (T, error) {
	return withLeaderRetryUsing(ctx, endpoints, fn, call[T])
}

type endpointCaller[T any] func(context.Context, string, func(context.Context, *api.KVClient) (T, error)) (T, error)

func withLeaderRetryUsing[T any](ctx context.Context, endpoints []string, fn func(context.Context, *api.KVClient) (T, error), caller endpointCaller[T]) (T, error) {
	var zero T
	queue := append([]string(nil), endpoints...)
	var lastErr error
	for attempts := 0; attempts < len(endpoints)+5 && len(queue) > 0; attempts++ {
		endpoint := queue[0]
		queue = queue[1:]
		if endpoint == "" {
			continue
		}
		resp, err := caller(ctx, endpoint, fn)
		if err != nil {
			lastErr = err
			continue
		}
		ok, leaderAddr, err := leaderFrom(resp)
		if err != nil {
			return zero, err
		}
		if !ok {
			if msg := responseError(resp); msg != "" {
				lastErr = errors.New(msg)
			}
			if leaderAddr != "" {
				queue = append(queue, leaderAddr)
			}
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return zero, lastErr
	}
	return zero, errors.New("no reachable leader")
}

func call[T any](ctx context.Context, endpoint string, fn func(context.Context, *api.KVClient) (T, error)) (T, error) {
	var zero T
	callCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, api.DialOptions()...)
	if err != nil {
		return zero, err
	}
	defer conn.Close()
	return fn(callCtx, api.NewKVClient(conn))
}

func leaderFrom(resp any) (bool, string, error) {
	switch v := resp.(type) {
	case *api.PutResponse:
		return v.OK, v.LeaderAddress, nil
	case *api.GetResponse:
		return v.OK, v.LeaderAddress, nil
	case *api.DeleteResponse:
		return v.OK, v.LeaderAddress, nil
	case *api.CASResponse:
		return v.OK, v.LeaderAddress, nil
	default:
		return false, "", fmt.Errorf("response %T does not carry a leader hint", resp)
	}
}

func responseError(resp any) string {
	switch v := resp.(type) {
	case *api.PutResponse:
		return v.Error
	case *api.GetResponse:
		return v.Error
	case *api.DeleteResponse:
		return v.Error
	case *api.CASResponse:
		return v.Error
	default:
		return ""
	}
}

func defaultEndpoints() string {
	if env := os.Getenv("KV_ENDPOINTS"); env != "" {
		return env
	}
	return "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9001"
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  kvctl [--endpoints host:port,...] put <key> <value>
  kvctl [--endpoints host:port,...] get <key>
  kvctl [--endpoints host:port,...] delete <key>
  kvctl [--endpoints host:port,...] cas <key> <expected> <value>
  kvctl [--endpoints host:port,...] status`)
}
