package grpc

import (
	"log/slog"
	"strings"
	"time"

	flagstreamv1 "github.com/danil/feature-flag-engine/api/proto/v1"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamServer implements flagstreamv1.FlagStreamServiceServer for the Data Plane.
type StreamServer struct {
	flagstreamv1.UnimplementedFlagStreamServiceServer
	flagRepo          flag.Repository
	hub               *Hub
	logger            *slog.Logger
	heartbeatInterval time.Duration
}

// NewStreamServer creates a new gRPC Data Plane streaming server.
func NewStreamServer(flagRepo flag.Repository, hub *Hub, logger *slog.Logger) *StreamServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &StreamServer{
		flagRepo:          flagRepo,
		hub:               hub,
		logger:            logger,
		heartbeatInterval: 20 * time.Second,
	}
}

// Subscribe handles a persistent SDK stream:
//  1. Registers the subscriber in the Hub BEFORE reading the database snapshot so no
//     in-flight events can be missed between snapshot generation and stream activation.
//  2. Sends the initial FULL_SNAPSHOT event.
//  3. Pushes real-time FLAG_UPDATED / FLAG_DELETED events and periodic heartbeats.
func (s *StreamServer) Subscribe(req *flagstreamv1.SubscribeRequest, stream flagstreamv1.FlagStreamService_SubscribeServer) error {
	envKey := strings.TrimSpace(req.EnvironmentKey)
	if envKey == "" {
		return status.Error(codes.InvalidArgument, "environment_key is required")
	}

	subID, eventCh, unsubscribe := s.hub.Subscribe(envKey, 64)
	defer unsubscribe()

	ctx := stream.Context()
	configs, maxVersion, err := s.flagRepo.GetEnvironmentSnapshot(ctx, envKey)
	if err != nil {
		s.logger.Error("failed to load environment snapshot", "env", envKey, "error", err)
		return status.Errorf(codes.Internal, "failed to load snapshot: %v", err)
	}

	wireFlags := make([]flagstreamv1.FlagConfiguration, 0, len(configs))
	for _, cfg := range configs {
		wireFlags = append(wireFlags, FlagConfigToProto(cfg))
	}

	snapshotEvent := &flagstreamv1.FlagEvent{
		Type:                flagstreamv1.EventType_EVENT_TYPE_FULL_SNAPSHOT,
		EnvironmentKey:      envKey,
		Version:             maxVersion,
		PublishedAtUnixNano: time.Now().UTC().UnixNano(),
		Flags:               wireFlags,
	}

	if err := stream.Send(snapshotEvent); err != nil {
		return err
	}

	s.logger.Info("sdk stream subscribed",
		"sub_id", subID,
		"env", envKey,
		"snapshot_flags", len(wireFlags),
		"snapshot_version", maxVersion,
		"active_streams", s.hub.ActiveSubscribers(),
	)

	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("sdk stream disconnected", "sub_id", subID, "env", envKey)
			return nil
		case ev, ok := <-eventCh:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-ticker.C:
			hb := &flagstreamv1.FlagEvent{
				Type:                flagstreamv1.EventType_EVENT_TYPE_HEARTBEAT,
				EnvironmentKey:      envKey,
				Version:             maxVersion,
				PublishedAtUnixNano: time.Now().UTC().UnixNano(),
			}
			if err := stream.Send(hb); err != nil {
				return err
			}
		}
	}
}

