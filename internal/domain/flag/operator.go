package flag

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Operator defines a comparison predicate used inside a targeting rule clause.
type Operator string

const (
	OperatorEquals     Operator = "EQUALS"
	OperatorNotEquals  Operator = "NOT_EQUALS"
	OperatorIn         Operator = "IN"
	OperatorNotIn      Operator = "NOT_IN"
	OperatorContains   Operator = "CONTAINS"
	OperatorStartsWith Operator = "STARTS_WITH"
	OperatorEndsWith   Operator = "ENDS_WITH"
	OperatorRegex      Operator = "REGEX"
	OperatorSemverGte  Operator = "SEMVER_GTE"
	OperatorSemverLte  Operator = "SEMVER_LTE"
)

// Validate checks whether the operator is supported.
func (op Operator) Validate() error {
	switch op {
	case OperatorEquals, OperatorNotEquals, OperatorIn, OperatorNotIn,
		OperatorContains, OperatorStartsWith, OperatorEndsWith,
		OperatorRegex, OperatorSemverGte, OperatorSemverLte:
		return nil
	default:
		return fmt.Errorf("unsupported operator %q", op)
	}
}

// compiledClause holds pre-computed structures (e.g., compiled regular expressions)
// so that hot-path evaluation never allocates or re-parses patterns.
type compiledClause struct {
	attribute string
	operator  Operator
	values    []string
	valueSet  map[string]struct{}
	regex     *regexp.Regexp
	semverTarget [3]int
}

func compileClause(c Clause) (compiledClause, error) {
	if strings.TrimSpace(c.Attribute) == "" {
		return compiledClause{}, fmt.Errorf("clause attribute cannot be empty")
	}
	if err := c.Operator.Validate(); err != nil {
		return compiledClause{}, err
	}
	if len(c.Values) == 0 {
		return compiledClause{}, fmt.Errorf("clause for attribute %q must have at least one target value", c.Attribute)
	}

	cc := compiledClause{
		attribute: c.Attribute,
		operator:  c.Operator,
		values:    c.Values,
	}

	switch c.Operator {
	case OperatorIn, OperatorNotIn:
		if len(c.Values) > 4 {
			set := make(map[string]struct{}, len(c.Values))
			for _, v := range c.Values {
				set[v] = struct{}{}
			}
			cc.valueSet = set
		}
	case OperatorRegex:
		re, err := regexp.Compile(c.Values[0])
		if err != nil {
			return compiledClause{}, fmt.Errorf("invalid regex pattern %q: %w", c.Values[0], err)
		}
		cc.regex = re
	case OperatorSemverGte, OperatorSemverLte:
		sv, err := parseSemver(c.Values[0])
		if err != nil {
			return compiledClause{}, fmt.Errorf("invalid semver %q: %w", c.Values[0], err)
		}
		cc.semverTarget = sv
	}

	return cc, nil
}

func (cc *compiledClause) matches(actual string, exists bool) bool {
	if !exists {
		return false
	}

	switch cc.operator {
	case OperatorEquals:
		return actual == cc.values[0]
	case OperatorNotEquals:
		return actual != cc.values[0]
	case OperatorIn:
		if cc.valueSet != nil {
			_, ok := cc.valueSet[actual]
			return ok
		}
		for i := range cc.values {
			if cc.values[i] == actual {
				return true
			}
		}
		return false
	case OperatorNotIn:
		if cc.valueSet != nil {
			_, ok := cc.valueSet[actual]
			return !ok
		}
		for i := range cc.values {
			if cc.values[i] == actual {
				return false
			}
		}
		return true
	case OperatorContains:
		return strings.Contains(actual, cc.values[0])
	case OperatorStartsWith:
		return strings.HasPrefix(actual, cc.values[0])
	case OperatorEndsWith:
		return strings.HasSuffix(actual, cc.values[0])
	case OperatorRegex:
		if cc.regex == nil {
			return false
		}
		return cc.regex.MatchString(actual)
	case OperatorSemverGte:
		actualVer, err := parseSemver(actual)
		if err != nil {
			return false
		}
		return compareSemver(actualVer, cc.semverTarget) >= 0
	case OperatorSemverLte:
		actualVer, err := parseSemver(actual)
		if err != nil {
			return false
		}
		return compareSemver(actualVer, cc.semverTarget) <= 0
	default:
		return false
	}
}

func parseSemver(raw string) ([3]int, error) {
	v := strings.TrimPrefix(strings.TrimSpace(raw), "v")
	if idx := strings.IndexByte(v, '-'); idx != -1 {
		v = v[:idx]
	}
	var out [3]int
	parts := 0
	start := 0
	for i := 0; i <= len(v); i++ {
		if i == len(v) || v[i] == '.' {
			if parts >= 3 {
				return out, fmt.Errorf("too many version segments in %q", raw)
			}
			num, err := strconv.Atoi(v[start:i])
			if err != nil || num < 0 {
				return out, fmt.Errorf("invalid version segment in %q", raw)
			}
			out[parts] = num
			parts++
			start = i + 1
		}
	}
	if parts == 0 {
		return out, fmt.Errorf("empty semver")
	}
	return out, nil
}

func compareSemver(a, b [3]int) int {
	for i := 0; i < 3; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

