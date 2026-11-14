package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	flagstreamv1 "github.com/danil/feature-flag-engine/api/proto/v1"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
	internalgrpc "github.com/danil/feature-flag-engine/internal/infrastructure/grpc"
	"github.com/danil/feature-flag-engine/internal/infrastructure/memory"
	"github.com/danil/feature-flag-engine/internal/infrastructure/postgres"
	ffredis "github.com/danil/feature-flag-engine/internal/infrastructure/redis"
	"github.com/danil/feature-flag-engine/internal/infrastructure/telemetry"
	_ "github.com/jackc/pgx/v5/stdlib"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grpcAddr := envOrDefault("DATA_PLANE_GRPC_ADDR", ":9090")
	postgresDSN := os.Getenv("POSTGRES_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	if otlpEndpoint != "" {
		_, shutdownOTel, err := telemetry.InitMeterProvider(ctx, "feature-flag-data-plane", otlpEndpoint)
		if err != nil {
			logger.Warn("failed to initialize OpenTelemetry meter provider", "error", err)
		} else {
			defer func() { _ = shutdownOTel(context.Background()) }()
		}
	}

	var flagRepo flag.Repository
	var subscriber flag.EventSubscriber

	if postgresDSN != "" {
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			logger.Error("failed to open postgres connection", "error", err)
			os.Exit(1)
		}
		defer db.Close()

		if err := db.PingContext(ctx); err != nil {
			logger.Error("failed to ping postgres", "error", err)
			os.Exit(1)
		}
		flagRepo = postgres.NewStore(db)
		logger.Info("Data Plane connected to PostgreSQL for snapshot hydration")
	} else {
		memStore := memory.NewStore()
		flagRepo = memStore
		subscriber = memStore
		logger.Info("POSTGRES_DSN not set; running Data Plane with in-memory store")
	}

	if redisAddr != "" {
		rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr})
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Error("failed to ping redis", "addr", redisAddr, "error", err)
			os.Exit(1)
		}
		subscriber = ffredis.NewBroker(rdb, logger)
		logger.Info("Data Plane subscribed to Redis Pub/Sub channel", "addr", redisAddr)
	}

	hub := internalgrpc.NewHub()

	if subscriber != nil {
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}
				err := subscriber.Subscribe(ctx, func(ev flag.DomainEvent) {
					delivered := hub.BroadcastDomainEvent(ev)
					logger.Info("broadcasted real-time flag update to gRPC streams",
						"env", ev.EnvironmentKey,
						"version", ev.Version,
						"delivered_streams", delivered,
					)
				})
				if ctx.Err() != nil {
					return
				}
				logger.Warn("redis subscription interrupted, retrying in 1s...", "error", err)
				time.Sleep(time.Second)
			}
		}()
	}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC address", "addr", grpcAddr, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	streamSrv := internalgrpc.NewStreamServer(flagRepo, hub, logger)
	flagstreamv1.RegisterFlagStreamServiceServer(grpcServer, streamSrv)

	go func() {
		logger.Info("Data Plane gRPC Streaming Server listening", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down Data Plane gRPC server...")
	grpcServer.GracefulStop()
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

