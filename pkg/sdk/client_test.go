package sdk_test

import (
	"context"
	"net"
	"testing"
	"time"

	flagstreamv1 "github.com/danil/feature-flag-engine/api/proto/v1"
	"github.com/danil/feature-flag-engine/internal/application"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
	internalgrpc "github.com/danil/feature-flag-engine/internal/infrastructure/grpc"
	"github.com/danil/feature-flag-engine/internal/infrastructure/memory"
	"github.com/danil/feature-flag-engine/pkg/sdk"
	"google.golang.org/grpc"
)

func TestEndToEndRealTimeInvalidationOverGRPC(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Setup Control Plane + In-Memory Pub/Sub + Data Plane gRPC Streaming Server
	store := memory.NewStore()
	controlPlane := application.NewControlPlaneService(store, store, store)
	hub := internalgrpc.NewHub()

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	go func() {
		_ = store.Subscribe(subCtx, func(ev flag.DomainEvent) {
			hub.BroadcastDomainEvent(ev)
		})
	}()

	// Seed a flag in Control Plane BEFORE the SDK connects
	_, err := controlPlane.CreateFlag(ctx, application.CreateFlagInput{
		Key:         "realtime-checkout-v2",
		Name:        "Realtime Checkout V2",
		Description: "Tests sub-millisecond gRPC push invalidation",
	})
	if err != nil {
		t.Fatalf("CreateFlag failed: %v", err)
	}

	_, err = controlPlane.UpsertFlagConfig(ctx, application.UpsertFlagConfigInput{
		EnvironmentKey:            "production",
		FlagKey:                   "realtime-checkout-v2",
		Enabled:                   true,
		DefaultRolloutBasisPoints: 10000, // 100% enabled initially
	})
	if err != nil {
		t.Fatalf("UpsertFlagConfig initial failed: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	grpcSrv := grpc.NewServer()
	streamSrv := internalgrpc.NewStreamServer(store, hub, nil)
	flagstreamv1.RegisterFlagStreamServiceServer(grpcSrv, streamSrv)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	defer grpcSrv.Stop()

	// 2. Instantiate SDK Client connected to the gRPC Data Plane
	updateReceived := make(chan time.Duration, 1)
	client, err := sdk.NewClient(ctx, sdk.Config{
		DataPlaneAddress:   lis.Addr().String(),
		EnvironmentKey:     "production",
		SDKKey:             "sdk-prod-default-key",
		WaitForInitialSync: true,
		InitTimeout:        3 * time.Second,
		FlushInterval:      100 * time.Millisecond,
		OnFlagUpdated: func(flagKey string, version int64, propagationLatency time.Duration) {
			if flagKey == "realtime-checkout-v2" && version == 3 {
				select {
				case updateReceived <- propagationLatency:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatalf("sdk.NewClient failed: %v", err)
	}
	defer client.Close()

	userCtx := sdk.EvaluationContext{
		UserID: "user-malta-777",
		Attributes: map[string]string{
			"country": "MT",
		},
	}

	// Verify initial state from FULL_SNAPSHOT
	if !client.IsFeatureEnabled("realtime-checkout-v2", userCtx) {
		t.Fatalf("expected flag to be enabled after initial bootstrap snapshot")
	}

	// 3. Product Manager toggles the flag OFF in the Control Plane
	_, err = controlPlane.UpsertFlagConfig(ctx, application.UpsertFlagConfigInput{
		EnvironmentKey:            "production",
		FlagKey:                   "realtime-checkout-v2",
		Enabled:                   false,
		DefaultRolloutBasisPoints: 10000,
	})
	if err != nil {
		t.Fatalf("UpsertFlagConfig killswitch failed: %v", err)
	}

	// 4. Wait for real-time push over gRPC stream
	select {
	case latency := <-updateReceived:
		t.Logf("Real-time gRPC cache invalidation applied in %v", latency)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for real-time gRPC flag invalidation")
	}

	if client.IsFeatureEnabled("realtime-checkout-v2", userCtx) {
		t.Fatalf("expected flag to evaluate to FALSE immediately after real-time push")
	}
}

func BenchmarkClient_IsFeatureEnabled_Parallel(b *testing.B) {
	client, err := sdk.NewClient(context.Background(), sdk.Config{
		EnvironmentKey:   "production",
		DisableTelemetry: false,
		FlushInterval:    60 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	err = client.SeedFlags([]flag.FlagConfig{
		{
			FlagKey:                   "parallel-bench-flag",
			EnvironmentKey:            "production",
			Enabled:                   true,
			DefaultRolloutBasisPoints: 3000,
			Version:                   1,
			TargetingRules: []flag.TargetingRule{
				{
					ID:   "rule-eu",
					Name: "EU Rollout",
					Clauses: []flag.Clause{
						{
							Attribute: "country",
							Operator:  flag.OperatorIn,
							Values:    []string{"MT", "DE", "NL", "IE", "PT"},
						},
					},
					RolloutBasisPoints: 5000,
				},
			},
		},
	}, 1)
	if err != nil {
		b.Fatal(err)
	}

	userCtx := sdk.EvaluationContext{
		UserID: "user-bench-42",
		Attributes: map[string]string{
			"country": "MT",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = client.IsFeatureEnabled("parallel-bench-flag", userCtx)
		}
	})
}

