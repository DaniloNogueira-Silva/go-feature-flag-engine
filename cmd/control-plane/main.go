package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danil/feature-flag-engine/internal/application"
	"github.com/danil/feature-flag-engine/internal/domain/environment"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
	ffhttp "github.com/danil/feature-flag-engine/internal/infrastructure/http"
	"github.com/danil/feature-flag-engine/internal/infrastructure/memory"
	"github.com/danil/feature-flag-engine/internal/infrastructure/postgres"
	ffredis "github.com/danil/feature-flag-engine/internal/infrastructure/redis"
	"github.com/danil/feature-flag-engine/internal/infrastructure/telemetry"
	_ "github.com/jackc/pgx/v5/stdlib"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpAddr := envOrDefault("CONTROL_PLANE_HTTP_ADDR", ":8080")
	postgresDSN := os.Getenv("POSTGRES_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	if otlpEndpoint != "" {
		_, shutdownOTel, err := telemetry.InitMeterProvider(ctx, "feature-flag-control-plane", otlpEndpoint)
		if err != nil {
			logger.Warn("failed to initialize OpenTelemetry meter provider", "error", err)
		} else {
			defer func() { _ = shutdownOTel(context.Background()) }()
		}
	}

	var (
		envRepo   environment.Repository
		flagRepo  flag.Repository
		publisher flag.EventPublisher
	)

	if postgresDSN != "" {
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			logger.Error("failed to open postgres connection", "error", err)
			os.Exit(1)
		}
		defer db.Close()

		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(30 * time.Minute)

		if err := db.PingContext(ctx); err != nil {
			logger.Error("failed to ping postgres", "error", err)
			os.Exit(1)
		}

		pgStore := postgres.NewStore(db)
		if err := pgStore.RunMigrations(ctx); err != nil {
			logger.Error("failed to run database migrations", "error", err)
			os.Exit(1)
		}
		envRepo = pgStore
		flagRepo = pgStore
		logger.Info("connected to PostgreSQL and applied schema migrations")
	} else {
		memStore := memory.NewStore()
		envRepo = memStore
		flagRepo = memStore
		publisher = memStore
		logger.Info("POSTGRES_DSN not set; running Control Plane with in-memory store")
	}

	if redisAddr != "" {
		rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr})
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Error("failed to ping redis", "addr", redisAddr, "error", err)
			os.Exit(1)
		}
		publisher = ffredis.NewBroker(rdb, logger)
		logger.Info("connected to Redis Pub/Sub publisher", "addr", redisAddr)
	}

	svc := application.NewControlPlaneService(envRepo, flagRepo, publisher)
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           ffhttp.NewRouter(svc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("Control Plane REST API listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down Control Plane HTTP server...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

