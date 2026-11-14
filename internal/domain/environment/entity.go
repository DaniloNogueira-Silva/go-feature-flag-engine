package environment

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	ErrEnvironmentNotFound = errors.New("environment not found")
	ErrInvalidEnvKey       = errors.New("environment key must match ^[a-z0-9][a-z0-9-_]{1,62}[a-z0-9]$")
	envKeyPattern          = regexp.MustCompile(`^[a-z0-9][a-z0-9-_]{1,62}[a-z0-9]$`)
)

// Environment represents an isolated deployment stage (e.g., "production", "staging").
type Environment struct {
	ID            string    `json:"id"`
	Key           string    `json:"key"`
	Name          string    `json:"name"`
	SDKSecretHash string    `json:"-"`
	CreatedAt     time.Time `json:"created_at"`
}

// Validate checks domain invariants for an Environment.
func (e *Environment) Validate() error {
	if !envKeyPattern.MatchString(e.Key) {
		return ErrInvalidEnvKey
	}
	if len(e.Name) == 0 || len(e.Name) > 120 {
		return fmt.Errorf("environment name must be between 1 and 120 characters")
	}
	return nil
}

// Repository defines persistence operations for environments.
type Repository interface {
	Create(ctx context.Context, env *Environment) error
	GetByKey(ctx context.Context, key string) (*Environment, error)
	List(ctx context.Context) ([]Environment, error)
}

