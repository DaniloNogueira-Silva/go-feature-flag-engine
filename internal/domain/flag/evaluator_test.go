package flag_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

func TestMurmurHash3UniformityAndDeterminism(t *testing.T) {
	t.Parallel()

	const totalUsers = 100_000
	const targetBasisPoints = 3000 // 30.00%
	flagKey := "new-checkout-experience"

	enabledCount := 0
	for i := 0; i < totalUsers; i++ {
		userID := fmt.Sprintf("user-%d", i)
		b1 := flag.ComputeRolloutBucket(flagKey, userID, "v1", 0)
		b2 := flag.ComputeRolloutBucket(flagKey, userID, "v1", 0)
		if b1 != b2 {
			t.Fatalf("non-deterministic bucket for %s: %d != %d", userID, b1, b2)
		}
		if b1 < 0 || b1 >= flag.MaxBasisPoints {
			t.Fatalf("bucket out of bounds: %d", b1)
		}
		if b1 < targetBasisPoints {
			enabledCount++
		}
	}

	actualRatio := float64(enabledCount) / float64(totalUsers)
	expectedRatio := float64(targetBasisPoints) / float64(flag.MaxBasisPoints)
	if math.Abs(actualRatio-expectedRatio) > 0.006 {
		t.Fatalf("distribution drift too high: got %.4f, want %.4f (±0.006)", actualRatio, expectedRatio)
	}
}

func TestCompiledFlag_EvaluateRulesAndFallback(t *testing.T) {
	t.Parallel()

	cfg := &flag.FlagConfig{
		FlagKey:                   "instant-pix-payout",
		EnvironmentKey:            "production",
		Enabled:                   true,
		DefaultRolloutBasisPoints: 0,
		Version:                   7,
		TargetingRules: []flag.TargetingRule{
			{
				ID:   "rule-vip-countries",
				Name: "100% for Enterprise users in BR, MT, PT",
				Clauses: []flag.Clause{
					{
						Attribute: "country",
						Operator:  flag.OperatorIn,
						Values:    []string{"BR", "MT", "PT", "DE", "NL", "IE"},
					},
					{
						Attribute: "plan",
						Operator:  flag.OperatorEquals,
						Values:    []string{"enterprise"},
					},
					{
						Attribute: "app_version",
						Operator:  flag.OperatorSemverGte,
						Values:    []string{"2.4.0"},
					},
				},
				RolloutBasisPoints: 10000,
			},
			{
				ID:   "rule-internal-qa",
				Name: "Internal QA email domain",
				Clauses: []flag.Clause{
					{
						Attribute: "email",
						Operator:  flag.OperatorRegex,
						Values:    []string{`^[a-z0-9._%+-]+@company\.io$`},
					},
				},
				RolloutBasisPoints: 10000,
			},
		},
	}

	compiled, err := cfg.Compile()
	if err != nil {
		t.Fatalf("Compile() unexpected error: %v", err)
	}

	tests := []struct {
		name       string
		ctx        flag.EvaluationContext
		wantEnable bool
		wantReason flag.EvaluationReason
		wantRule   string
	}{
		{
			name: "matches enterprise in Malta with valid semver",
			ctx: flag.EvaluationContext{
				UserID: "usr-malta-1",
				Attributes: map[string]string{
					"country":     "MT",
					"plan":        "enterprise",
					"app_version": "2.5.1",
				},
			},
			wantEnable: true,
			wantReason: flag.ReasonTargetMatch,
			wantRule:   "rule-vip-countries",
		},
		{
			name: "older app version fails first rule, matches QA email rule",
			ctx: flag.EvaluationContext{
				UserID: "usr-qa-99",
				Attributes: map[string]string{
					"country":     "MT",
					"plan":        "enterprise",
					"app_version": "2.1.0",
					"email":       "qa.engineer@company.io",
				},
			},
			wantEnable: true,
			wantReason: flag.ReasonTargetMatch,
			wantRule:   "rule-internal-qa",
		},
		{
			name: "free tier user falls back to default 0%",
			ctx: flag.EvaluationContext{
				UserID: "usr-free-1",
				Attributes: map[string]string{
					"country":     "BR",
					"plan":        "free",
					"app_version": "3.0.0",
				},
			},
			wantEnable: false,
			wantReason: flag.ReasonDefaultFallback,
			wantRule:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := compiled.Evaluate(tc.ctx)
			if res.Enabled != tc.wantEnable {
				t.Errorf("Enabled = %v, want %v", res.Enabled, tc.wantEnable)
			}
			if res.Reason != tc.wantReason {
				t.Errorf("Reason = %s, want %s", res.Reason, tc.wantReason)
			}
			if res.MatchedRule != tc.wantRule {
				t.Errorf("MatchedRule = %s, want %s", res.MatchedRule, tc.wantRule)
			}
		})
	}
}

func BenchmarkCompiledFlag_Evaluate(b *testing.B) {
	cfg := &flag.FlagConfig{
		FlagKey:                   "new-checkout-flow",
		EnvironmentKey:            "production",
		Enabled:                   true,
		DefaultRolloutBasisPoints: 2500,
		Version:                   42,
		TargetingRules: []flag.TargetingRule{
			{
				ID:   "rule-country-rollout",
				Name: "30% Rollout in Brazil and Malta",
				Clauses: []flag.Clause{
					{
						Attribute: "country",
						Operator:  flag.OperatorIn,
						Values:    []string{"BR", "MT", "PT"},
					},
					{
						Attribute: "plan",
						Operator:  flag.OperatorEquals,
						Values:    []string{"pro"},
					},
				},
				RolloutBasisPoints: 3000,
			},
		},
	}

	compiled, err := cfg.Compile()
	if err != nil {
		b.Fatal(err)
	}

	evalCtx := flag.EvaluationContext{
		UserID: "user-984123-uuid-benchmark",
		Attributes: map[string]string{
			"country": "BR",
			"plan":    "pro",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = compiled.Evaluate(evalCtx)
	}
}

