package flag

// CompiledFlag is an immutable, pre-compiled representation of a FlagConfig
// optimized for sub-microsecond concurrent reads inside the SDK and Data Plane.
type CompiledFlag struct {
	Key                       string
	EnvironmentKey            string
	Enabled                   bool
	DefaultRolloutBasisPoints int
	Salt                      string
	Version                   int64
	rules                     []compiledRule
}

type compiledRule struct {
	id                 string
	name               string
	rolloutBasisPoints int
	salt               string
	clauses            []compiledClause
}

// Compile transforms a FlagConfig into an immutable CompiledFlag with pre-compiled
// regular expressions, semver structures, and lookup sets.
func (fc *FlagConfig) Compile() (*CompiledFlag, error) {
	if err := fc.Validate(); err != nil {
		return nil, err
	}

	rules := make([]compiledRule, 0, len(fc.TargetingRules))
	for _, r := range fc.TargetingRules {
		clauses := make([]compiledClause, 0, len(r.Clauses))
		for _, c := range r.Clauses {
			cc, err := compileClause(c)
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, cc)
		}
		rules = append(rules, compiledRule{
			id:                 r.ID,
			name:               r.Name,
			rolloutBasisPoints: r.RolloutBasisPoints,
			salt:               r.Salt,
			clauses:            clauses,
		})
	}

	return &CompiledFlag{
		Key:                       fc.FlagKey,
		EnvironmentKey:            fc.EnvironmentKey,
		Enabled:                   fc.Enabled,
		DefaultRolloutBasisPoints: fc.DefaultRolloutBasisPoints,
		Salt:                      fc.Salt,
		Version:                   fc.Version,
		rules:                     rules,
	}, nil
}

// Evaluate executes the deterministic evaluation algorithm against the provided context
// with zero heap allocations on the hot path.
func (cf *CompiledFlag) Evaluate(ctx EvaluationContext) EvaluationResult {
	if cf == nil {
		return EvaluationResult{
			Enabled: false,
			Reason:  ReasonFlagNotFound,
		}
	}

	if !cf.Enabled {
		return EvaluationResult{
			FlagKey: cf.Key,
			Enabled: false,
			Reason:  ReasonFlagDisabled,
			Version: cf.Version,
		}
	}

	// 1. Evaluate targeting rules in priority order (first match wins).
	for i := range cf.rules {
		rule := &cf.rules[i]
		if matchesAllClauses(rule.clauses, ctx) {
			if rule.rolloutBasisPoints >= MaxBasisPoints {
				return EvaluationResult{
					FlagKey:     cf.Key,
					Enabled:     true,
					Reason:      ReasonTargetMatch,
					MatchedRule: rule.id,
					Version:     cf.Version,
				}
			}
			if rule.rolloutBasisPoints <= 0 {
				return EvaluationResult{
					FlagKey:     cf.Key,
					Enabled:     false,
					Reason:      ReasonRolloutExcluded,
					MatchedRule: rule.id,
					Version:     cf.Version,
				}
			}
			seed := rule.salt
			if seed == "" {
				seed = cf.Salt
			}
			bucket := ComputeRolloutBucket(cf.Key, ctx.UserID, seed, 0)
			if bucket < rule.rolloutBasisPoints {
				return EvaluationResult{
					FlagKey:     cf.Key,
					Enabled:     true,
					Reason:      ReasonTargetMatch,
					MatchedRule: rule.id,
					Version:     cf.Version,
				}
			}
			return EvaluationResult{
				FlagKey:     cf.Key,
				Enabled:     false,
				Reason:      ReasonRolloutExcluded,
				MatchedRule: rule.id,
				Version:     cf.Version,
			}
		}
	}

	// 2. Fall back to default rollout percentage.
	if cf.DefaultRolloutBasisPoints >= MaxBasisPoints {
		return EvaluationResult{
			FlagKey: cf.Key,
			Enabled: true,
			Reason:  ReasonDefaultRollout,
			Version: cf.Version,
		}
	}
	if cf.DefaultRolloutBasisPoints <= 0 {
		return EvaluationResult{
			FlagKey: cf.Key,
			Enabled: false,
			Reason:  ReasonDefaultFallback,
			Version: cf.Version,
		}
	}

	bucket := ComputeRolloutBucket(cf.Key, ctx.UserID, cf.Salt, 0)
	if bucket < cf.DefaultRolloutBasisPoints {
		return EvaluationResult{
			FlagKey: cf.Key,
			Enabled: true,
			Reason:  ReasonDefaultRollout,
			Version: cf.Version,
		}
	}

	return EvaluationResult{
		FlagKey: cf.Key,
		Enabled: false,
		Reason:  ReasonDefaultFallback,
		Version: cf.Version,
	}
}

func matchesAllClauses(clauses []compiledClause, ctx EvaluationContext) bool {
	for i := range clauses {
		c := &clauses[i]
		var val string
		var ok bool
		if c.attribute == "user_id" || c.attribute == "userId" {
			val = ctx.UserID
			ok = val != ""
		} else if ctx.Attributes != nil {
			val, ok = ctx.Attributes[c.attribute]
		}
		if !c.matches(val, ok) {
			return false
		}
	}
	return true
}

