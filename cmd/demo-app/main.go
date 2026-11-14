package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danil/feature-flag-engine/internal/infrastructure/telemetry"
	"github.com/danil/feature-flag-engine/pkg/sdk"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	controlPlaneURL := envOrDefault("CONTROL_PLANE_URL", "http://localhost:8080")
	dataPlaneAddr := envOrDefault("DATA_PLANE_GRPC_ADDR", "localhost:9090")
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	if otlpEndpoint != "" {
		_, shutdownOTel, err := telemetry.InitMeterProvider(ctx, "feature-flag-demo-app", otlpEndpoint)
		if err != nil {
			logger.Warn("failed to initialize OpenTelemetry meter provider", "error", err)
		} else {
			defer func() { _ = shutdownOTel(context.Background()) }()
		}
	}

	// Ensure default demo flag exists in the Control Plane.
	bootstrapDemoFlag(controlPlaneURL, logger)

	client, err := sdk.NewClient(ctx, sdk.Config{
		DataPlaneAddress:   dataPlaneAddr,
		EnvironmentKey:     "production",
		SDKKey:             "sdk-prod-default-key",
		WaitForInitialSync: true,
		InitTimeout:        10 * time.Second,
		FlushInterval:      10 * time.Second,
		Logger:             logger,
		OnFlagUpdated: func(flagKey string, version int64, propagationLatency time.Duration) {
			logger.Info("instant cache invalidation received via gRPC stream",
				"flag_key", flagKey,
				"new_version", version,
				"propagation_latency_ms", float64(propagationLatency.Nanoseconds())/1e6,
			)
		},
		OnTelemetryFlush: func(snap sdk.TelemetrySnapshot) {
			logger.Info("flushed SDK evaluation telemetry buffer to OpenTelemetry",
				"environment", snap.EnvironmentKey,
				"evaluations_by_flag", snap.Evaluations,
			)
		},
	})
	if err != nil {
		logger.Error("failed to initialize Feature Flag SDK", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	countries := []string{"MT", "BR", "DE", "NL", "IE", "PT", "US"}
	plans := []string{"free", "pro", "enterprise"}

	logger.Info("demo application started — evaluating flags locally in sub-microseconds")
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	seq := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down demo application...")
			return
		case <-ticker.C:
			for i := 0; i < 25; i++ {
				seq++
				userCtx := sdk.EvaluationContext{
					UserID: fmt.Sprintf("user-%d", seq%5000),
					Attributes: map[string]string{
						"country":     countries[seq%len(countries)],
						"plan":        plans[seq%len(plans)],
						"app_version": "2.5.0",
					},
				}
				_ = client.IsFeatureEnabled("new-checkout-flow", userCtx)
			}
		}
	}
}

func bootstrapDemoFlag(baseURL string, logger *slog.Logger) {
	createPayload := []byte(`{
		"key": "new-checkout-flow",
		"name": "New Checkout Flow",
		"description": "Next-gen low-latency checkout experience"
	}`)
	resp, err := http.Post(baseURL+"/api/v1/flags", "application/json", bytes.NewReader(createPayload))
	if err != nil {
		logger.Warn("could not reach Control Plane to seed demo flag", "error", err)
		return
	}
	_ = resp.Body.Close()

	rulePayload := []byte(`{
		"enabled": true,
		"default_rollout_basis_points": 2000,
		"targeting_rules": [
			{
				"id": "rule-eu-vip",
				"name": "100% Rollout for Enterprise in Malta & EU",
				"clauses": [
					{
						"attribute": "country",
						"operator": "IN",
						"values": ["MT", "DE", "NL", "IE", "PT"]
					},
					{
						"attribute": "plan",
						"operator": "EQUALS",
						"values": ["enterprise"]
					}
				],
				"rollout_basis_points": 10000
			}
		]
	}`)
	req, err := http.NewRequest(http.MethodPut, baseURL+"/api/v1/environments/production/flags/new-checkout-flow", bytes.NewReader(rulePayload))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		if r, err := http.DefaultClient.Do(req); err == nil {
			_ = r.Body.Close()
		}
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

