package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/danil/feature-flag-engine/pkg/sdk"
)

type Config struct {
	ControlPlaneURL string
	DataPlaneAddr   string
	Workers         int
	Duration        time.Duration
	EnableChaos     bool
}

type UserProfile struct {
	ID         string
	Country    string
	Plan       string
	AppVersion string
	Email      string
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.ControlPlaneURL, "control-plane", "http://localhost:8080", "Control Plane HTTP URL")
	flag.StringVar(&cfg.DataPlaneAddr, "data-plane", "localhost:9090", "Data Plane gRPC host:port")
	flag.IntVar(&cfg.Workers, "workers", 50, "Number of concurrent goroutines")
	flag.DurationVar(&cfg.Duration, "duration", 20*time.Second, "Test run duration (e.g. 10s, 30s, 1m)")
	flag.BoolVar(&cfg.EnableChaos, "chaos", true, "Periodically mutate flag rules to test real-time invalidation under load")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Println("================================================================================")
	fmt.Printf("🚀 STARTING FEATURE FLAG LOAD TEST\n")
	fmt.Printf("Workers: %d | Duration: %v | Chaos Mutator: %v\n", cfg.Workers, cfg.Duration, cfg.EnableChaos)
	fmt.Printf("Control Plane: %s | Data Plane: %s\n", cfg.ControlPlaneURL, cfg.DataPlaneAddr)
	fmt.Println("================================================================================")

	// 1. Seed scenarios and multiple flags in Control Plane
	fmt.Println("📦 Seeding 4 complex feature flags in Control Plane...")
	seedScenarios(cfg.ControlPlaneURL)

	// 2. Initialize SDK connected to Data Plane
	var invalidationsReceived atomic.Int64
	var lastLatencyMs atomic.Uint64

	client, err := sdk.NewClient(ctx, sdk.Config{
		DataPlaneAddress:   cfg.DataPlaneAddr,
		EnvironmentKey:     "production",
		SDKKey:             "sdk-prod-default-key",
		WaitForInitialSync: true,
		InitTimeout:        5 * time.Second,
		DisableTelemetry:   true, // Raw evaluation throughput without exporter overhead
		Logger:             slog.New(slog.DiscardHandler),
		OnFlagUpdated: func(flagKey string, version int64, propagationLatency time.Duration) {
			invalidationsReceived.Add(1)
			lastLatencyMs.Store(uint64(propagationLatency.Nanoseconds()))
		},
	})
	if err != nil {
		logger.Error("Failed to initialize Feature Flag SDK", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Printf("✅ SDK connected. Cached %d flags in local memory.\n\n", client.CachedFlagCount())

	// 3. Prepare User Pool (10,000 diverse simulated users)
	users := generateUsers(10_000)

	// 4. Start Chaos Worker if enabled (mutates ai-recommendations every 3 seconds)
	if cfg.EnableChaos {
		go runChaosActor(ctx, cfg.ControlPlaneURL)
	}

	// 5. Worker execution & latency tracking
	testCtx, testCancel := context.WithTimeout(ctx, cfg.Duration)
	defer testCancel()

	flagKeys := []string{
		"new-checkout-flow",
		"instant-pix-payout",
		"beta-dashboard-v2",
		"ai-recommendations",
	}

	var (
		totalEvals atomic.Int64
		trueEvals  atomic.Int64
		falseEvals atomic.Int64
	)

	// Sample latencies for percentile calculation (reservoir sampling per worker)
	samplesPerWorker := 50_000
	workerLatencies := make([][]int64, cfg.Workers)
	for i := range workerLatencies {
		workerLatencies[i] = make([]int64, 0, samplesPerWorker)
	}

	var wg sync.WaitGroup
	startTime := time.Now()

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(workerID), uint64(time.Now().UnixNano())))
			latencies := workerLatencies[workerID]

			for {
				select {
				case <-testCtx.Done():
					workerLatencies[workerID] = latencies
					return
				default:
				}

				// Measure in batches of 500 to overcome OS timer resolution limits on Windows
				const batchSize = 500
				start := time.Now()
				for b := 0; b < batchSize; b++ {
					u := &users[r.IntN(len(users))]
					fKey := flagKeys[r.IntN(len(flagKeys))]

					evalCtx := sdk.EvaluationContext{
						UserID: u.ID,
						Attributes: map[string]string{
							"country":     u.Country,
							"plan":        u.Plan,
							"app_version": u.AppVersion,
							"email":       u.Email,
						},
					}

					enabled := client.IsFeatureEnabled(fKey, evalCtx)
					if enabled {
						trueEvals.Add(1)
					} else {
						falseEvals.Add(1)
					}
				}
				elapsedPerOp := time.Since(start).Nanoseconds() / batchSize
				totalEvals.Add(batchSize)

				if len(latencies) < samplesPerWorker {
					latencies = append(latencies, elapsedPerOp)
				}
			}
		}(w)
	}

	// Progress monitor
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-testCtx.Done():
				return
			case <-ticker.C:
				evals := totalEvals.Load()
				elapsedSec := time.Since(startTime).Seconds()
				rps := float64(evals) / elapsedSec
				invs := invalidationsReceived.Load()
				fmt.Printf("⏱️  [Elapsed: %4.1fs] Total Evals: %9d | Rate: %10.0f evals/sec | Live gRPC Updates: %d\n",
					elapsedSec, evals, rps, invs)
			}
		}
	}()

	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Consolidate latencies
	var allLatencies []int64
	for _, l := range workerLatencies {
		allLatencies = append(allLatencies, l...)
	}
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	p50, p95, p99 := calculatePercentiles(allLatencies)
	total := totalEvals.Load()
	rps := float64(total) / totalElapsed.Seconds()

	fmt.Println("\n================================================================================")
	fmt.Printf("📊 LOAD TEST RESULTS SUMMARY\n")
	fmt.Println("================================================================================")
	fmt.Printf("Total Duration:             %v\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("Concurrent Workers:         %d goroutines\n", cfg.Workers)
	fmt.Printf("Total Evaluations:          %d\n", total)
	fmt.Printf("Throughput:                 %.2f evaluations/sec (~%.2fM evals/sec)\n", rps, rps/1_000_000)
	fmt.Printf("Decisions:                  True: %d (%.1f%%) | False: %d (%.1f%%)\n",
		trueEvals.Load(), float64(trueEvals.Load())*100/float64(total),
		falseEvals.Load(), float64(falseEvals.Load())*100/float64(total))
	fmt.Printf("Live gRPC Updates Received: %d push events\n", invalidationsReceived.Load())
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Latency (In-Memory Evaluation Hot Path):\n")
	fmt.Printf("  P50 (Median):             %d ns (%.3f µs)\n", p50, float64(p50)/1000)
	fmt.Printf("  P95:                      %d ns (%.3f µs)\n", p95, float64(p95)/1000)
	fmt.Printf("  P99:                      %d ns (%.3f µs)\n", p99, float64(p99)/1000)
	fmt.Println("================================================================================")
}

func calculatePercentiles(latencies []int64) (p50, p95, p99 int64) {
	n := len(latencies)
	if n == 0 {
		return 0, 0, 0
	}
	p50 = latencies[n*50/100]
	p95 = latencies[n*95/100]
	p99 = latencies[n*99/100]
	return
}

func generateUsers(count int) []UserProfile {
	countries := []string{"BR", "MT", "PT", "DE", "NL", "IE", "US", "ES", "IT"}
	plans := []string{"free", "pro", "enterprise"}
	versions := []string{"1.9.0", "2.4.0", "3.0.1", "3.2.0"}

	users := make([]UserProfile, count)
	for i := 0; i < count; i++ {
		emailDomain := "gmail.com"
		if i%5 == 0 {
			emailDomain = "company.io"
		}
		users[i] = UserProfile{
			ID:         fmt.Sprintf("user-%06d", i),
			Country:    countries[i%len(countries)],
			Plan:       plans[i%len(plans)],
			AppVersion: versions[i%len(versions)],
			Email:      fmt.Sprintf("usr%d@%s", i, emailDomain),
		}
	}
	return users
}

func seedScenarios(baseURL string) {
	scenarios := []struct {
		key         string
		name        string
		description string
		body        string
	}{
		{
			key:         "new-checkout-flow",
			name:        "New Checkout Flow",
			description: "30% percentage rollout via MurmurHash3",
			body: `{
				"enabled": true,
				"default_rollout_basis_points": 3000,
				"targeting_rules": []
			}`,
		},
		{
			key:         "instant-pix-payout",
			name:        "Instant Pix & SEPA Payout",
			description: "100% for Enterprise in Malta and Brazil",
			body: `{
				"enabled": true,
				"default_rollout_basis_points": 0,
				"targeting_rules": [
					{
						"id": "rule-vip-markets",
						"name": "100% Enterprise in MT and BR",
						"clauses": [
							{ "attribute": "country", "operator": "IN", "values": ["MT", "BR"] },
							{ "attribute": "plan", "operator": "EQUALS", "values": ["enterprise"] }
						],
						"rollout_basis_points": 10000
					}
				]
			}`,
		},
		{
			key:         "beta-dashboard-v2",
			name:        "Beta Dashboard V2",
			description: "Requires App >= 3.0.0 and company.io internal email",
			body: `{
				"enabled": true,
				"default_rollout_basis_points": 0,
				"targeting_rules": [
					{
						"id": "rule-beta-testers",
						"name": "App Version >= 3.0.0 and internal company email",
						"clauses": [
							{ "attribute": "app_version", "operator": "SEMVER_GTE", "values": ["3.0.0"] },
							{ "attribute": "email", "operator": "REGEX", "values": ["^[a-zA-Z0-9._%+-]+@company\\.io$"] }
						],
						"rollout_basis_points": 10000
					}
				]
			}`,
		},
		{
			key:         "ai-recommendations",
			name:        "AI Recommendations Engine",
			description: "Dynamic flag mutated during test by Chaos Actor",
			body: `{
				"enabled": true,
				"default_rollout_basis_points": 1000,
				"targeting_rules": []
			}`,
		},
	}

	for _, s := range scenarios {
		// 1. Create flag definition
		flagPayload, _ := json.Marshal(map[string]string{
			"key":         s.key,
			"name":        s.name,
			"description": s.description,
		})
		resp, _ := http.Post(baseURL+"/api/v1/flags", "application/json", bytes.NewReader(flagPayload))
		if resp != nil {
			_ = resp.Body.Close()
		}

		// 2. Put rule configuration for production
		req, _ := http.NewRequest(http.MethodPut, baseURL+"/api/v1/environments/production/flags/"+s.key, bytes.NewBufferString(s.body))
		req.Header.Set("Content-Type", "application/json")
		if putResp, err := http.DefaultClient.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, putResp.Body)
			_ = putResp.Body.Close()
		}
	}
}

func runChaosActor(ctx context.Context, baseURL string) {
	rollouts := []int{8000, 2000, 10000, 0, 5000}
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	step := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rollout := rollouts[step%len(rollouts)]
			enabled := rollout > 0
			step++

			payload := fmt.Sprintf(`{
				"enabled": %t,
				"default_rollout_basis_points": %d,
				"targeting_rules": []
			}`, enabled, rollout)

			req, _ := http.NewRequest(http.MethodPut, baseURL+"/api/v1/environments/production/flags/ai-recommendations", bytes.NewBufferString(payload))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
	}
}
