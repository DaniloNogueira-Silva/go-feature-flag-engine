package sdk_test

import (
	"context"
	"net"
	"sync"
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

// TestSDKDisconnectAndRecoverMissedVersions validates that if the gRPC connection drops,
// 1. The SDK continues to evaluate flags locally with zero downtime (fail-open local availability).
// 2. Flag rules mutated while disconnected are fully reconciled upon reconnect via the authoritative snapshot.
func TestSDKDisconnectAndRecoverMissedVersions(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	// 1. Create flag with initial v2 config (100% enabled)
	_, err := controlPlane.CreateFlag(ctx, application.CreateFlagInput{
		Key:  "disconnect-recovery-flag",
		Name: "Disconnect Recovery Flag",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = controlPlane.UpsertFlagConfig(ctx, application.UpsertFlagConfigInput{
		EnvironmentKey:            "production",
		FlagKey:                   "disconnect-recovery-flag",
		Enabled:                   true,
		DefaultRolloutBasisPoints: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Dynamic proxy listener to simulate network disconnection
	proxyLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxyLis.Close()

	realLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	streamSrv := internalgrpc.NewStreamServer(store, hub, nil)
	flagstreamv1.RegisterFlagStreamServiceServer(grpcSrv, streamSrv)
	go func() {
		_ = grpcSrv.Serve(realLis)
	}()
	defer grpcSrv.Stop()

	// Simple TCP proxy bridging proxyLis -> realLis
	var activeConnsMu sync.Mutex
	activeConns := make(map[net.Conn]struct{})
	proxyBlocked := false

	go func() {
		for {
			clientConn, err := proxyLis.Accept()
			if err != nil {
				return
			}
			activeConnsMu.Lock()
			if proxyBlocked {
				clientConn.Close()
				activeConnsMu.Unlock()
				continue
			}
			backendConn, err := net.Dial("tcp", realLis.Addr().String())
			if err != nil {
				clientConn.Close()
				activeConnsMu.Unlock()
				continue
			}
			activeConns[clientConn] = struct{}{}
			activeConns[backendConn] = struct{}{}
			activeConnsMu.Unlock()

			go func(c1, c2 net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := c1.Read(buf)
					if err != nil {
						break
					}
					if _, err := c2.Write(buf[:n]); err != nil {
						break
					}
				}
				c1.Close()
				c2.Close()
				activeConnsMu.Lock()
				delete(activeConns, c1)
				delete(activeConns, c2)
				activeConnsMu.Unlock()
			}(clientConn, backendConn)

			go func(c1, c2 net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := c2.Read(buf)
					if err != nil {
						break
					}
					if _, err := c1.Write(buf[:n]); err != nil {
						break
					}
				}
				c1.Close()
				c2.Close()
				activeConnsMu.Lock()
				delete(activeConns, c1)
				delete(activeConns, c2)
				activeConnsMu.Unlock()
			}(clientConn, backendConn)
		}
	}()

	// 2. Start SDK connected via the proxy
	client, err := sdk.NewClient(ctx, sdk.Config{
		DataPlaneAddress:   proxyLis.Addr().String(),
		EnvironmentKey:     "production",
		SDKKey:             "sdk-prod-default-key",
		WaitForInitialSync: true,
		InitTimeout:        3 * time.Second,
		FlushInterval:      100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	userCtx := sdk.EvaluationContext{UserID: "usr-42"}
	if !client.IsFeatureEnabled("disconnect-recovery-flag", userCtx) {
		t.Fatal("expected flag to be true initially")
	}

	// 3. Sever the network connection (simulate network partition / drop)
	activeConnsMu.Lock()
	proxyBlocked = true
	for c := range activeConns {
		_ = c.Close()
		delete(activeConns, c)
	}
	activeConnsMu.Unlock()

	// 4. Verify local evaluations still succeed during the network outage (fail-open)
	for i := 0; i < 5; i++ {
		if !client.IsFeatureEnabled("disconnect-recovery-flag", userCtx) {
			t.Fatal("expected flag to remain locally available and true during disconnect")
		}
	}

	// 5. While disconnected, mutate the flag in the Control Plane (version 3)
	_, err = controlPlane.UpsertFlagConfig(ctx, application.UpsertFlagConfigInput{
		EnvironmentKey:            "production",
		FlagKey:                   "disconnect-recovery-flag",
		Enabled:                   false, // Disabled
		DefaultRolloutBasisPoints: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 6. Restore the network connection
	activeConnsMu.Lock()
	proxyBlocked = false
	activeConnsMu.Unlock()

	// 7. Wait for SDK supervisor to reconnect and reconcile via the snapshot
	recovered := false
	for start := time.Now(); time.Since(start) < 3*time.Second; time.Sleep(50 * time.Millisecond) {
		if !client.IsFeatureEnabled("disconnect-recovery-flag", userCtx) {
			recovered = true
			break
		}
	}

	if !recovered {
		t.Fatal("SDK failed to reconcile to latest flag version after reconnecting")
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
