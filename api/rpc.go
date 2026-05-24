package api

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

const JSONCodecName = "json"

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return JSONCodecName
}

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(JSONCodecName)),
	}
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type PutResponse struct {
	OK            bool   `json:"ok"`
	Error         string `json:"error,omitempty"`
	LeaderID      string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

type GetRequest struct {
	Key string `json:"key"`
}

type GetResponse struct {
	OK            bool   `json:"ok"`
	Found         bool   `json:"found"`
	Value         string `json:"value,omitempty"`
	Error         string `json:"error,omitempty"`
	LeaderID      string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

type DeleteResponse struct {
	OK            bool   `json:"ok"`
	Found         bool   `json:"found"`
	PreviousValue string `json:"previous_value,omitempty"`
	Error         string `json:"error,omitempty"`
	LeaderID      string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

type CASRequest struct {
	Key      string `json:"key"`
	Expected string `json:"expected"`
	Value    string `json:"value"`
}

type CASResponse struct {
	OK            bool   `json:"ok"`
	Swapped       bool   `json:"swapped"`
	Found         bool   `json:"found"`
	CurrentValue  string `json:"current_value,omitempty"`
	Error         string `json:"error,omitempty"`
	LeaderID      string `json:"leader_id,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

type StatusRequest struct{}

type StatusResponse struct {
	NodeID        string            `json:"node_id"`
	State         string            `json:"state"`
	Term          uint64            `json:"term"`
	LeaderID      string            `json:"leader_id,omitempty"`
	LeaderAddress string            `json:"leader_address,omitempty"`
	CommitIndex   uint64            `json:"commit_index"`
	LastApplied   uint64            `json:"last_applied"`
	LastLogIndex  uint64            `json:"last_log_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	Peers         map[string]string `json:"peers,omitempty"`
}

type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command"`
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	MatchIndex    uint64 `json:"match_index,omitempty"`
}

type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Offset            uint64 `json:"offset"`
	Data              []byte `json:"data"`
	Done              bool   `json:"done"`
}

type InstallSnapshotResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	BytesReceived uint64 `json:"bytes_received,omitempty"`
}

type KVServiceServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	CAS(context.Context, *CASRequest) (*CASResponse, error)
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
}

func RegisterKVServiceServer(server *grpc.Server, service KVServiceServer) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "raftkv.v1.KV",
		HandlerType: (*KVServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Put", Handler: unaryHandler(service.Put)},
			{MethodName: "Get", Handler: unaryHandler(service.Get)},
			{MethodName: "Delete", Handler: unaryHandler(service.Delete)},
			{MethodName: "CAS", Handler: unaryHandler(service.CAS)},
			{MethodName: "Status", Handler: unaryHandler(service.Status)},
		},
	}, service)
}

type KVClient struct {
	conn grpc.ClientConnInterface
}

func NewKVClient(conn grpc.ClientConnInterface) *KVClient {
	return &KVClient{conn: conn}
}

func (c *KVClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.invoke(ctx, "/raftkv.v1.KV/Put", in, out, opts...)
	return out, err
}

func (c *KVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.invoke(ctx, "/raftkv.v1.KV/Get", in, out, opts...)
	return out, err
}

func (c *KVClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.invoke(ctx, "/raftkv.v1.KV/Delete", in, out, opts...)
	return out, err
}

func (c *KVClient) CAS(ctx context.Context, in *CASRequest, opts ...grpc.CallOption) (*CASResponse, error) {
	out := new(CASResponse)
	err := c.invoke(ctx, "/raftkv.v1.KV/CAS", in, out, opts...)
	return out, err
}

func (c *KVClient) Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error) {
	out := new(StatusResponse)
	err := c.invoke(ctx, "/raftkv.v1.KV/Status", in, out, opts...)
	return out, err
}

func (c *KVClient) invoke(ctx context.Context, method string, in, out any, opts ...grpc.CallOption) error {
	opts = append(opts, grpc.CallContentSubtype(JSONCodecName))
	return c.conn.Invoke(ctx, method, in, out, opts...)
}

type RaftPeerServiceServer interface {
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(context.Context, *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

func RegisterRaftPeerServiceServer(server *grpc.Server, service RaftPeerServiceServer) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "raftkv.v1.RaftPeer",
		HandlerType: (*RaftPeerServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "RequestVote", Handler: unaryHandler(service.RequestVote)},
			{MethodName: "AppendEntries", Handler: unaryHandler(service.AppendEntries)},
			{MethodName: "InstallSnapshot", Handler: unaryHandler(service.InstallSnapshot)},
		},
	}, service)
}

type RaftPeerClient struct {
	conn grpc.ClientConnInterface
}

func NewRaftPeerClient(conn grpc.ClientConnInterface) *RaftPeerClient {
	return &RaftPeerClient{conn: conn}
}

func (c *RaftPeerClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	out := new(RequestVoteResponse)
	err := c.invoke(ctx, "/raftkv.v1.RaftPeer/RequestVote", in, out, opts...)
	return out, err
}

func (c *RaftPeerClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := c.invoke(ctx, "/raftkv.v1.RaftPeer/AppendEntries", in, out, opts...)
	return out, err
}

func (c *RaftPeerClient) InstallSnapshot(ctx context.Context, in *InstallSnapshotRequest, opts ...grpc.CallOption) (*InstallSnapshotResponse, error) {
	out := new(InstallSnapshotResponse)
	err := c.invoke(ctx, "/raftkv.v1.RaftPeer/InstallSnapshot", in, out, opts...)
	return out, err
}

func (c *RaftPeerClient) invoke(ctx context.Context, method string, in, out any, opts ...grpc.CallOption) error {
	opts = append(opts, grpc.CallContentSubtype(JSONCodecName))
	return c.conn.Invoke(ctx, method, in, out, opts...)
}

func unaryHandler[Req any, Resp any](fn func(context.Context, *Req) (*Resp, error)) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := new(Req)
		if err := dec(req); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return fn(ctx, req)
		}
		info := &grpc.UnaryServerInfo{Server: srv}
		handler := func(ctx context.Context, request any) (any, error) {
			return fn(ctx, request.(*Req))
		}
		return interceptor(ctx, req, info, handler)
	}
}
