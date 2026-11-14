package flag

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	ErrFlagNotFound      = errors.New("feature flag not found")
	ErrVersionConflict   = errors.New("version conflict: flag configuration was modified concurrently")
	ErrInvalidFlagKey    = errors.New("flag key must match ^[a-z0-9][a-z0-9-_]{1,118}[a-z0-9]$")
	ErrInvalidRollout    = errors.New("rollout basis points must be between 0 and 10000")
	flagKeyPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9-_]{1,118}[a-z0-9]$`)
)

// EvaluationReason explains why a feature flag evaluated to a specific boolean value.
type EvaluationReason string

const (
	ReasonFlagNotFound    EvaluationReason = "FLAG_NOT_FOUND"
	ReasonFlagDisabled    EvaluationReason = "FLAG_DISABLED"
	ReasonTargetMatch     EvaluationReason = "TARGET_MATCH"
	ReasonRolloutExcluded EvaluationReason = "ROLLOUT_EXCLUDED"
	ReasonDefaultRollout  EvaluationReason = "DEFAULT_ROLLOUT"
	ReasonDefaultFallback EvaluationReason = "DEFAULT_FALLBACK"
)

// Flag represents the global metadata of a feature flag across all environments.
type Flag struct {
	ID          string    `json:"id"`
	Key         string    `json:"key"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Validate checks domain invariants for a Flag aggregate.
func (f *Flag) Validate() error {
	if !flagKeyPattern.MatchString(f.Key) {
		return ErrInvalidFlagKey
	}
	if len(f.Name) == 0 || len(f.Name) > 200 {
		return fmt.Errorf("flag name must be between 1 and 200 characters")
	}
	return nil
}

// Clause represents a single condition inside a TargetingRule.
// All clauses within a rule are combined using logical AND.
type Clause struct {
	Attribute string   `json:"attribute"`
	Operator  Operator `json:"operator"`
	Values    []string `json:"values"`
}

// TargetingRule represents a prioritized audience segment and its percentage rollout.
type TargetingRule struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	Clauses            []Clause `json:"clauses"`
	RolloutBasisPoints int      `json:"rollout_basis_points"` // 0 to 10000 (10000 = 100.00%)
	Salt               string   `json:"salt,omitempty"`
}

// FlagConfig represents the state and targeting rules of a Flag within a specific Environment.
type FlagConfig struct {
	ID                        string          `json:"id"`
	FlagID                    string          `json:"flag_id"`
	FlagKey                   string          `json:"flag_key"`
	EnvironmentID             string          `json:"environment_id"`
	EnvironmentKey            string          `json:"environment_key"`
	Enabled                   bool            `json:"enabled"`
	DefaultRolloutBasisPoints int             `json:"default_rollout_basis_points"` // 0 to 10000
	TargetingRules            []TargetingRule `json:"targeting_rules"`
	Salt                      string          `json:"salt,omitempty"`
	Version                   int64           `json:"version"`
	UpdatedAt                 time.Time       `json:"updated_at"`
}

// Validate ensures the FlagConfig respects all domain invariants and that all
// targeting rules and operators can be compiled without error.
func (fc *FlagConfig) Validate() error {
	if !flagKeyPattern.MatchString(fc.FlagKey) {
		return ErrInvalidFlagKey
	}
	if fc.DefaultRolloutBasisPoints < 0 || fc.DefaultRolloutBasisPoints > MaxBasisPoints {
		return ErrInvalidRollout
	}
	for i, rule := range fc.TargetingRules {
		if rule.RolloutBasisPoints < 0 || rule.RolloutBasisPoints > MaxBasisPoints {
			return fmt.Errorf("rule[%d] (%s): %w", i, rule.Name, ErrInvalidRollout)
		}
		if len(rule.Clauses) == 0 {
			return fmt.Errorf("rule[%d] (%s) must contain at least one clause", i, rule.Name)
		}
		for _, clause := range rule.Clauses {
			if _, err := compileClause(clause); err != nil {
				return fmt.Errorf("rule[%d] (%s): %w", i, rule.Name, err)
			}
		}
	}
	return nil
}

// EvaluationContext holds user identity and contextual attributes passed during evaluation.
type EvaluationContext struct {
	UserID     string
	Attributes map[string]string
}

// EvaluationResult contains the boolean decision along with diagnostic metadata.
type EvaluationResult struct {
	FlagKey     string           `json:"flag_key"`
	Enabled     bool             `json:"enabled"`
	Reason      EvaluationReason `json:"reason"`
	MatchedRule string           `json:"matched_rule,omitempty"`
	Version     int64            `json:"version"`
}

