package sdk

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danil/feature-flag-engine/internal/domain/flag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metricBucketKey struct {
	flagKey string
	enabled bool
	reason  flag.EvaluationReason
}

// TelemetrySnapshot represents a point-in-time flush of buffered SDK metrics.
type TelemetrySnapshot struct {
	EnvironmentKey string
	FlushedAt      time.Time
	Evaluations    map[string]int64
}

// TelemetryBuffer accumulates high-frequency flag evaluation counts in memory using
// atomic counters and periodically flushes aggregated metrics to OpenTelemetry every 10s.
// This ensures telemetry never degrades the sub-microsecond evaluation hot path.
type TelemetryBuffer struct {
	envKey        string
	flushInterval time.Duration

	mu       sync.RWMutex
	counters map[metricBucketKey]*atomic.Int64

	evalCounter     metric.Int64Counter
	propagationHist metric.Float64Histogram
	streamReconnect metric.Int64Counter

	onFlush func(TelemetrySnapshot)
}

// NewTelemetryBuffer creates a buffered OpenTelemetry recorder for the SDK.
func NewTelemetryBuffer(envKey string, flushInterval time.Duration, meterProvider metric.MeterProvider, onFlush func(TelemetrySnapshot)) (*TelemetryBuffer, error) {
	if flushInterval <= 0 {
		flushInterval = 10 * time.Second
	}
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}
	meter := meterProvider.Meter("github.com/danil/feature-flag-engine/pkg/sdk")

	evalCounter, err := meter.Int64Counter(
		"featureflag.sdk.evaluations_total",
		metric.WithDescription("Total number of local feature flag evaluations flushed from SDK buffer"),
	)
	if err != nil {
		return nil, err
	}

	propagationHist, err := meter.Float64Histogram(
		"featureflag.sdk.propagation_duration_ms",
		metric.WithDescription("End-to-end latency in milliseconds from Control Plane update to SDK in-memory cache update"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	streamReconnect, err := meter.Int64Counter(
		"featureflag.sdk.stream_reconnects_total",
		metric.WithDescription("Total number of gRPC stream reconnection attempts by the SDK"),
	)
	if err != nil {
		return nil, err
	}

	return &TelemetryBuffer{
		envKey:          envKey,
		flushInterval:   flushInterval,
		counters:        make(map[metricBucketKey]*atomic.Int64, 64),
		evalCounter:     evalCounter,
		propagationHist: propagationHist,
		streamReconnect: streamReconnect,
		onFlush:         onFlush,
	}, nil
}

// RecordEvaluation increments the in-memory atomic counter for a flag evaluation outcome.
// Fast path uses an RLock + atomic.Int64.Add(1) so concurrent evaluations of known flags never contend on map writes.
func (tb *TelemetryBuffer) RecordEvaluation(res flag.EvaluationResult) {
	if tb == nil {
		return
	}
	key := metricBucketKey{
		flagKey: res.FlagKey,
		enabled: res.Enabled,
		reason:  res.Reason,
	}

	tb.mu.RLock()
	counter, ok := tb.counters[key]
	tb.mu.RUnlock()

	if ok {
		counter.Add(1)
		return
	}

	tb.mu.Lock()
	counter, ok = tb.counters[key]
	if !ok {
		counter = &atomic.Int64{}
		tb.counters[key] = counter
	}
	tb.mu.Unlock()
	counter.Add(1)
}

// RecordPropagationLatency records the end-to-end cache invalidation latency in milliseconds.
func (tb *TelemetryBuffer) RecordPropagationLatency(ctx context.Context, flagKey string, d time.Duration) {
	if tb == nil {
		return
	}
	ms := float64(d.Nanoseconds()) / 1e6
	if ms < 0 {
		ms = 0
	}
	tb.propagationHist.Record(ctx, ms, metric.WithAttributes(
		attribute.String("environment", tb.envKey),
		attribute.String("flag_key", flagKey),
	))
}

// RecordReconnect increments the stream reconnection counter.
func (tb *TelemetryBuffer) RecordReconnect(ctx context.Context) {
	if tb == nil {
		return
	}
	tb.streamReconnect.Add(ctx, 1, metric.WithAttributes(
		attribute.String("environment", tb.envKey),
	))
}

// Flush drains all accumulated evaluation counts and emits them to the OpenTelemetry Meter.
func (tb *TelemetryBuffer) Flush(ctx context.Context) {
	if tb == nil {
		return
	}

	tb.mu.RLock()
	keys := make([]metricBucketKey, 0, len(tb.counters))
	ptrs := make([]*atomic.Int64, 0, len(tb.counters))
	for k, ptr := range tb.counters {
		keys = append(keys, k)
		ptrs = append(ptrs, ptr)
	}
	tb.mu.RUnlock()

	snap := TelemetrySnapshot{
		EnvironmentKey: tb.envKey,
		FlushedAt:      time.Now().UTC(),
		Evaluations:    make(map[string]int64),
	}

	for i, k := range keys {
		delta := ptrs[i].Swap(0)
		if delta <= 0 {
			continue
		}
		snap.Evaluations[k.flagKey] += delta
		tb.evalCounter.Add(ctx, delta, metric.WithAttributes(
			attribute.String("environment", tb.envKey),
			attribute.String("flag_key", k.flagKey),
			attribute.Bool("enabled", k.enabled),
			attribute.String("reason", string(k.reason)),
		))
	}

	if tb.onFlush != nil && len(snap.Evaluations) > 0 {
		tb.onFlush(snap)
	}
}

// RunPeriodicFlush flushes accumulated metrics on a ticker until ctx is canceled.
func (tb *TelemetryBuffer) RunPeriodicFlush(ctx context.Context) {
	ticker := time.NewTicker(tb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			tb.Flush(flushCtx)
			cancel()
			return
		case <-ticker.C:
			tb.Flush(ctx)
		}
	}
}

