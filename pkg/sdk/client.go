package sdk

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	flagstreamv1 "github.com/danil/feature-flag-engine/api/proto/v1"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
	 internalgrpc "github.com/danil/feature-flag-engine/internal/infrastructure/grpc"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EvaluationContext holds the user identifier and key-value attributes used for local targeting evaluation.
type EvaluationContext struct {
	UserID     string
	Attributes map[string]string
}

// EvaluationDetail provides the boolean decision and the reason why a flag evaluated that way.
type EvaluationDetail struct {
	FlagKey     string
	Enabled     bool
	Reason      string
	MatchedRule string
	Version     int64
}

// Config configures the Feature Flag Go SDK Client.
type Config struct {
	// DataPlaneAddress is the host:port of the gRPC Streaming Server (e.g., "localhost:9090").
	DataPlaneAddress string
	// EnvironmentKey identifies the environment to subscribe to (e.g., "production").
	EnvironmentKey string
	// SDKKey is the client authentication token for the environment.
	SDKKey string
	// WaitForInitialSync blocks NewClient until the initial FULL_SNAPSHOT is loaded in memory.
	WaitForInitialSync bool
	// InitTimeout bounds how long NewClient waits for the initial snapshot. Defaults to 5s.
	InitTimeout time.Duration
	// FlushInterval configures how often evaluation metrics are flushed to OpenTelemetry. Defaults to 10s.
	FlushInterval time.Duration
	// DisableTelemetry disables the in-memory telemetry buffer when measuring raw evaluation benchmarks.
	DisableTelemetry bool
	// MeterProvider allows passing a custom OpenTelemetry MeterProvider.
	MeterProvider metric.MeterProvider
	// OnFlagUpdated is an optional callback invoked whenever a real-time flag update is applied.
	OnFlagUpdated func(flagKey string, version int64, propagationLatency time.Duration)
	// OnTelemetryFlush is an optional callback invoked whenever buffered metrics are flushed.
	OnTelemetryFlush func(TelemetrySnapshot)
	// Logger is the structured logger used by the SDK.
	Logger *slog.Logger
}

// Client is a thread-safe, zero-network-latency Feature Flag evaluation client.
// It maintains a synchronized in-memory cache via gRPC Server Streaming and evaluates
// targeting rules and MurmurHash3 percentage rollouts locally in nanoseconds.
type Client struct {
	cfg       Config
	cache     *InMemoryCache
	telemetry *TelemetryBuffer
	conn      *grpc.ClientConn
	logger    *slog.Logger

	readyOnce sync.Once
	readyCh   chan struct{}
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewClient instantiates the Feature Flag SDK, opens the persistent gRPC stream to the Data Plane,
// downloads the initial flag snapshot into memory, and starts the background telemetry flusher.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.EnvironmentKey == "" {
		cfg.EnvironmentKey = "production"
	}
	if cfg.InitTimeout <= 0 {
		cfg.InitTimeout = 5 * time.Second
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	var tb *TelemetryBuffer
	if !cfg.DisableTelemetry {
		var err error
		tb, err = NewTelemetryBuffer(cfg.EnvironmentKey, cfg.FlushInterval, cfg.MeterProvider, cfg.OnTelemetryFlush)
		if err != nil {
			return nil, fmt.Errorf("init telemetry buffer: %w", err)
		}
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:       cfg,
		cache:     NewInMemoryCache(),
		telemetry: tb,
		logger:    cfg.Logger,
		readyCh:   make(chan struct{}),
		cancel:    cancel,
	}

	if tb != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			tb.RunPeriodicFlush(bgCtx)
		}()
	}

	if cfg.DataPlaneAddress != "" {
		conn, err := grpc.NewClient(
			cfg.DataPlaneAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("dial data plane gRPC %s: %w", cfg.DataPlaneAddress, err)
		}
		c.conn = conn

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runStreamSupervisor(bgCtx)
		}()

		if cfg.WaitForInitialSync {
			initCtx, initCancel := context.WithTimeout(ctx, cfg.InitTimeout)
			defer initCancel()
			select {
			case <-c.readyCh:
			case <-initCtx.Done():
				_ = c.Close()
				return nil, fmt.Errorf("timed out waiting for initial flag snapshot from %s: %w", cfg.DataPlaneAddress, initCtx.Err())
			}
		}
	} else {
		c.markReady()
	}

	return c, nil
}

// IsFeatureEnabled evaluates whether flagKey is enabled for userCtx using only in-memory state.
// This method performs zero network I/O and completes in sub-microseconds.
func (c *Client) IsFeatureEnabled(flagKey string, userCtx EvaluationContext) bool {
	res := c.cache.Evaluate(flagKey, flag.EvaluationContext{
		UserID:     userCtx.UserID,
		Attributes: userCtx.Attributes,
	})
	if c.telemetry != nil {
		c.telemetry.RecordEvaluation(res)
	}
	return res.Enabled
}

// EvaluateDetail returns the boolean evaluation result along with the diagnostic reason and matched rule ID.
func (c *Client) EvaluateDetail(flagKey string, userCtx EvaluationContext) EvaluationDetail {
	res := c.cache.Evaluate(flagKey, flag.EvaluationContext{
		UserID:     userCtx.UserID,
		Attributes: userCtx.Attributes,
	})
	if c.telemetry != nil {
		c.telemetry.RecordEvaluation(res)
	}
	return EvaluationDetail{
		FlagKey:     res.FlagKey,
		Enabled:     res.Enabled,
		Reason:      string(res.Reason),
		MatchedRule: res.MatchedRule,
		Version:     res.Version,
	}
}

// SeedFlags loads a slice of FlagConfigs directly into the SDK's in-memory cache (useful for offline tests/benchmarks).
func (c *Client) SeedFlags(configs []flag.FlagConfig, version int64) error {
	return c.cache.ApplySnapshot(configs, version)
}

// CachedFlagCount returns the number of flags currently stored in the local in-memory cache.
func (c *Client) CachedFlagCount() int {
	return c.cache.Size()
}

// Close gracefully terminates the background gRPC stream, flushes pending OpenTelemetry metrics,
// and closes the underlying network connection.
func (c *Client) Close() error {
	c.cancel()
	c.wg.Wait()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) markReady() {
	c.readyOnce.Do(func() {
		close(c.readyCh)
	})
}

func (c *Client) runStreamSupervisor(ctx context.Context) {
	client := flagstreamv1.NewFlagStreamServiceClient(c.conn)
	attempt := 0

	for {
		if ctx.Err() != nil {
			return
		}

		err := c.consumeStream(ctx, client)
		if ctx.Err() != nil {
			return
		}

		attempt++
		if c.telemetry != nil {
			c.telemetry.RecordReconnect(ctx)
		}

		backoff := computeBackoffWithJitter(attempt, 100*time.Millisecond, 10*time.Second)
		c.logger.Warn("gRPC flag stream disconnected, reconnecting with backoff",
			"attempt", attempt,
			"backoff", backoff,
			"error", err,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (c *Client) consumeStream(ctx context.Context, client flagstreamv1.FlagStreamServiceClient) error {
	stream, err := client.Subscribe(ctx, &flagstreamv1.SubscribeRequest{
		EnvironmentKey:   c.cfg.EnvironmentKey,
		SdkKey:           c.cfg.SDKKey,
		LastKnownVersion: c.cache.Version(),
	})
	if err != nil {
		return err
	}

	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}

		nowNano := time.Now().UTC().UnixNano()
		propagation := time.Duration(nowNano - ev.PublishedAtUnixNano)

		switch ev.Type {
		case flagstreamv1.EventType_EVENT_TYPE_FULL_SNAPSHOT:
			configs := make([]flag.FlagConfig, 0, len(ev.Flags))
			for _, wf := range ev.Flags {
				configs = append(configs, internalgrpc.ProtoToFlagConfig(wf))
			}
			if err := c.cache.ApplySnapshot(configs, ev.Version); err != nil {
				c.logger.Error("failed to apply initial snapshot", "error", err)
				continue
			}
			c.markReady()

		case flagstreamv1.EventType_EVENT_TYPE_FLAG_UPDATED:
			for _, wf := range ev.Flags {
				cfg := internalgrpc.ProtoToFlagConfig(wf)
				applied, err := c.cache.ApplyUpdate(cfg)
				if err != nil {
					c.logger.Error("failed to compile updated flag", "flag_key", cfg.FlagKey, "error", err)
					continue
				}
				if applied {
					if c.telemetry != nil {
						c.telemetry.RecordPropagationLatency(ctx, cfg.FlagKey, propagation)
					}
					if c.cfg.OnFlagUpdated != nil {
						c.cfg.OnFlagUpdated(cfg.FlagKey, cfg.Version, propagation)
					}
				}
			}

		case flagstreamv1.EventType_EVENT_TYPE_FLAG_DELETED:
			for _, wf := range ev.Flags {
				c.cache.Delete(wf.FlagKey, ev.Version)
			}

		case flagstreamv1.EventType_EVENT_TYPE_HEARTBEAT:
			// Keepalive heartbeat received; nothing to mutate.
		}
	}
}

func computeBackoffWithJitter(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt > 10 {
		attempt = 10
	}
	ceil := base * time.Duration(1<<uint(attempt-1))
	if ceil > maxDelay {
		ceil = maxDelay
	}
	// Full jitter in [ceil/2, ceil]
	half := int64(ceil) / 2
	if half <= 0 {
		return base
	}
	return time.Duration(half + rand.Int64N(half))
}

