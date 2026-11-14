package flagstreamv1

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// CodecName is the wire codec name registered with gRPC for FlagStreamService messages.
const CodecName = "flagstream-json"

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return CodecName
}

// EventType enumerates the kinds of real-time events streamed to SDK clients.
type EventType int32

const (
	EventType_EVENT_TYPE_UNSPECIFIED   EventType = 0
	EventType_EVENT_TYPE_FULL_SNAPSHOT EventType = 1
	EventType_EVENT_TYPE_FLAG_UPDATED  EventType = 2
	EventType_EVENT_TYPE_FLAG_DELETED  EventType = 3
	EventType_EVENT_TYPE_HEARTBEAT     EventType = 4
)

// SubscribeRequest initiates a persistent server-streaming subscription for an environment.
type SubscribeRequest struct {
	EnvironmentKey   string `json:"environment_key"`
	SdkKey           string `json:"sdk_key,omitempty"`
	LastKnownVersion int64  `json:"last_known_version,omitempty"`
}

// Clause represents a single predicate within a TargetingRule on the wire.
type Clause struct {
	Attribute string   `json:"attribute"`
	Operator  string   `json:"operator"`
	Values    []string `json:"values"`
}

// TargetingRule represents a prioritized audience segment on the wire.
type TargetingRule struct {
	Id                 string   `json:"id"`
	Name               string   `json:"name"`
	Clauses            []Clause `json:"clauses"`
	RolloutBasisPoints int32    `json:"rollout_basis_points"`
	Salt               string   `json:"salt,omitempty"`
}

// FlagConfiguration carries the full evaluation state of a feature flag.
type FlagConfiguration struct {
	Id                        string          `json:"id"`
	FlagId                    string          `json:"flag_id"`
	FlagKey                   string          `json:"flag_key"`
	EnvironmentId             string          `json:"environment_id"`
	EnvironmentKey            string          `json:"environment_key"`
	Enabled                   bool            `json:"enabled"`
	DefaultRolloutBasisPoints int32           `json:"default_rollout_basis_points"`
	TargetingRules            []TargetingRule `json:"targeting_rules"`
	Salt                      string          `json:"salt,omitempty"`
	Version                   int64           `json:"version"`
	UpdatedAtUnixNano         int64           `json:"updated_at_unix_nano"`
}

// FlagEvent is the streaming envelope pushed from the Data Plane to connected SDKs.
type FlagEvent struct {
	Type                EventType           `json:"type"`
	EnvironmentKey      string              `json:"environment_key"`
	Version             int64               `json:"version"`
	PublishedAtUnixNano int64               `json:"published_at_unix_nano"`
	Flags               []FlagConfiguration `json:"flags"`
}

// FlagStreamServiceClient is the client API for FlagStreamService.
type FlagStreamServiceClient interface {
	Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (FlagStreamService_SubscribeClient, error)
}

type flagStreamServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewFlagStreamServiceClient creates a gRPC client for FlagStreamService.
func NewFlagStreamServiceClient(cc grpc.ClientConnInterface) FlagStreamServiceClient {
	return &flagStreamServiceClient{cc: cc}
}

func (c *flagStreamServiceClient) Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (FlagStreamService_SubscribeClient, error) {
	opts = append(opts, grpc.CallContentSubtype(CodecName))
	stream, err := c.cc.NewStream(ctx, &FlagStreamService_ServiceDesc.Streams[0], "/flagstream.v1.FlagStreamService/Subscribe", opts...)
	if err != nil {
		return nil, err
	}
	x := &flagStreamServiceSubscribeClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// FlagStreamService_SubscribeClient represents a client-side gRPC server stream of FlagEvent messages.
type FlagStreamService_SubscribeClient interface {
	Recv() (*FlagEvent, error)
	grpc.ClientStream
}

type flagStreamServiceSubscribeClient struct {
	grpc.ClientStream
}

func (x *flagStreamServiceSubscribeClient) Recv() (*FlagEvent, error) {
	m := new(FlagEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// FlagStreamServiceServer is the server API for FlagStreamService.
type FlagStreamServiceServer interface {
	Subscribe(*SubscribeRequest, FlagStreamService_SubscribeServer) error
}

// UnimplementedFlagStreamServiceServer can be embedded for forward compatibility.
type UnimplementedFlagStreamServiceServer struct{}

func (UnimplementedFlagStreamServiceServer) Subscribe(*SubscribeRequest, FlagStreamService_SubscribeServer) error {
	return nil
}

// RegisterFlagStreamServiceServer registers the FlagStreamService implementation with a grpc.Server.
func RegisterFlagStreamServiceServer(s grpc.ServiceRegistrar, srv FlagStreamServiceServer) {
	s.RegisterService(&FlagStreamService_ServiceDesc, srv)
}

func _FlagStreamService_Subscribe_Handler(srv any, stream grpc.ServerStream) error {
	m := new(SubscribeRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(FlagStreamServiceServer).Subscribe(m, &flagStreamServiceSubscribeServer{stream})
}

// FlagStreamService_SubscribeServer represents the server-side stream used to push FlagEvent messages.
type FlagStreamService_SubscribeServer interface {
	Send(*FlagEvent) error
	grpc.ServerStream
}

type flagStreamServiceSubscribeServer struct {
	grpc.ServerStream
}

func (x *flagStreamServiceSubscribeServer) Send(m *FlagEvent) error {
	return x.ServerStream.SendMsg(m)
}

// FlagStreamService_ServiceDesc is the grpc.ServiceDesc for FlagStreamService.
var FlagStreamService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "flagstream.v1.FlagStreamService",
	HandlerType: (*FlagStreamServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _FlagStreamService_Subscribe_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "api/proto/v1/flagstream.proto",
}

