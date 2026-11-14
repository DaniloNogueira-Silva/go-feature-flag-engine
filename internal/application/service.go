package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/danil/feature-flag-engine/internal/domain/environment"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

// ControlPlaneService coordinates use cases for managing environments, feature flags,
// targeting rules, audit logs, and real-time cache invalidation broadcasts.
type ControlPlaneService struct {
	envRepo   environment.Repository
	flagRepo  flag.Repository
	publisher flag.EventPublisher
}

// NewControlPlaneService constructs a new ControlPlaneService.
func NewControlPlaneService(
	envRepo environment.Repository,
	flagRepo flag.Repository,
	publisher flag.EventPublisher,
) *ControlPlaneService {
	return &ControlPlaneService{
		envRepo:   envRepo,
		flagRepo:  flagRepo,
		publisher: publisher,
	}
}

// CreateEnvironmentInput holds the parameters to register a new deployment environment.
type CreateEnvironmentInput struct {
	Key  string `json:"key"`
	Name string `json:"name"`
}

// CreateEnvironment creates and validates a new Environment.
func (s *ControlPlaneService) CreateEnvironment(ctx context.Context, in CreateEnvironmentInput) (*environment.Environment, error) {
	env := &environment.Environment{
		ID:            newUUIDv7(),
		Key:           in.Key,
		Name:          in.Name,
		SDKSecretHash: "sdk-" + in.Key + "-" + randomHex(8),
		CreatedAt:     time.Now().UTC(),
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	if err := s.envRepo.Create(ctx, env); err != nil {
		return nil, err
	}
	return env, nil
}

// ListEnvironments returns all environments.
func (s *ControlPlaneService) ListEnvironments(ctx context.Context) ([]environment.Environment, error) {
	return s.envRepo.List(ctx)
}

// CreateFlagInput holds the parameters to create a new global feature flag.
type CreateFlagInput struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// CreateFlag registers a new global feature flag and initializes its default disabled config
// across all existing environments.
func (s *ControlPlaneService) CreateFlag(ctx context.Context, in CreateFlagInput) (*flag.Flag, error) {
	now := time.Now().UTC()
	f := &flag.Flag{
		ID:          newUUIDv7(),
		Key:         in.Key,
		Name:        in.Name,
		Description: in.Description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	if err := s.flagRepo.CreateFlag(ctx, f); err != nil {
		return nil, err
	}

	// Initialize default disabled configuration in every existing environment.
	envs, err := s.envRepo.List(ctx)
	if err == nil {
		for _, env := range envs {
			cfg := &flag.FlagConfig{
				ID:                        newUUIDv7(),
				FlagID:                    f.ID,
				FlagKey:                   f.Key,
				EnvironmentID:             env.ID,
				EnvironmentKey:            env.Key,
				Enabled:                   false,
				DefaultRolloutBasisPoints: 0,
				TargetingRules:            []flag.TargetingRule{},
				Salt:                      f.Key,
			}
			_, _ = s.flagRepo.UpsertFlagConfig(ctx, cfg, nil)
		}
	}

	return f, nil
}

// ListFlags returns all global feature flag definitions.
func (s *ControlPlaneService) ListFlags(ctx context.Context) ([]flag.Flag, error) {
	return s.flagRepo.ListFlags(ctx)
}

// UpsertFlagConfigInput represents a command to update a flag's state and targeting rules in an environment.
type UpsertFlagConfigInput struct {
	EnvironmentKey            string               `json:"-"`
	FlagKey                   string               `json:"-"`
	Enabled                   bool                 `json:"enabled"`
	DefaultRolloutBasisPoints int                  `json:"default_rollout_basis_points"`
	TargetingRules            []flag.TargetingRule `json:"targeting_rules"`
	Salt                      string               `json:"salt,omitempty"`
	Actor                     string               `json:"actor,omitempty"`
	ExpectedVersion           *int64               `json:"expected_version,omitempty"`
}

// UpsertFlagConfig validates and persists a flag configuration update, records an audit log,
// and broadcasts a real-time FLAG_UPDATED event to the Data Plane via Redis Pub/Sub.
func (s *ControlPlaneService) UpsertFlagConfig(ctx context.Context, in UpsertFlagConfigInput) (*flag.FlagConfig, error) {
	env, err := s.envRepo.GetByKey(ctx, in.EnvironmentKey)
	if err != nil {
		return nil, err
	}
	f, err := s.flagRepo.GetFlagByKey(ctx, in.FlagKey)
	if err != nil {
		return nil, err
	}

	prev, err := s.flagRepo.GetFlagConfig(ctx, in.EnvironmentKey, in.FlagKey)
	if err != nil && !errors.Is(err, flag.ErrFlagNotFound) {
		return nil, err
	}

	rules := in.TargetingRules
	if rules == nil {
		rules = []flag.TargetingRule{}
	}
	salt := in.Salt
	if salt == "" {
		salt = f.Key
	}

	cfg := &flag.FlagConfig{
		ID:                        newUUIDv7(),
		FlagID:                    f.ID,
		FlagKey:                   f.Key,
		EnvironmentID:             env.ID,
		EnvironmentKey:            env.Key,
		Enabled:                   in.Enabled,
		DefaultRolloutBasisPoints: in.DefaultRolloutBasisPoints,
		TargetingRules:            rules,
		Salt:                      salt,
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid flag configuration: %w", err)
	}

	saved, err := s.flagRepo.UpsertFlagConfig(ctx, cfg, in.ExpectedVersion)
	if err != nil {
		return nil, err
	}

	actor := in.Actor
	if actor == "" {
		actor = "system-admin"
	}
	_ = s.flagRepo.RecordAuditLog(ctx, &flag.AuditEntry{
		ID:             newUUIDv7(),
		FlagKey:        saved.FlagKey,
		EnvironmentKey: saved.EnvironmentKey,
		Action:         "FLAG_CONFIG_UPDATED",
		Actor:          actor,
		PreviousState:  prev,
		NewState:       saved,
		CreatedAt:      time.Now().UTC(),
	})

	if s.publisher != nil {
		event := flag.DomainEvent{
			Type:                flag.EventFlagUpdated,
			EnvironmentKey:      saved.EnvironmentKey,
			Version:             saved.Version,
			PublishedAtUnixNano: time.Now().UTC().UnixNano(),
			FlagConfig:          saved,
		}
		if pubErr := s.publisher.PublishFlagEvent(ctx, event); pubErr != nil {
			return saved, fmt.Errorf("flag persisted (version %d) but event publish failed: %w", saved.Version, pubErr)
		}
	}

	return saved, nil
}

// GetFlagConfig returns the current configuration of a flag in an environment.
func (s *ControlPlaneService) GetFlagConfig(ctx context.Context, envKey, flagKey string) (*flag.FlagConfig, error) {
	return s.flagRepo.GetFlagConfig(ctx, envKey, flagKey)
}

// EnvironmentSnapshot represents the full point-in-time configuration state of an environment.
type EnvironmentSnapshot struct {
	EnvironmentKey      string            `json:"environment_key"`
	Version             int64             `json:"version"`
	GeneratedAtUnixNano int64             `json:"generated_at_unix_nano"`
	Flags               []flag.FlagConfig `json:"flags"`
}

// GetEnvironmentSnapshot retrieves all flag configurations for an environment.
func (s *ControlPlaneService) GetEnvironmentSnapshot(ctx context.Context, envKey string) (*EnvironmentSnapshot, error) {
	if _, err := s.envRepo.GetByKey(ctx, envKey); err != nil {
		return nil, err
	}
	configs, maxVersion, err := s.flagRepo.GetEnvironmentSnapshot(ctx, envKey)
	if err != nil {
		return nil, err
	}
	if configs == nil {
		configs = []flag.FlagConfig{}
	}
	return &EnvironmentSnapshot{
		EnvironmentKey:      envKey,
		Version:             maxVersion,
		GeneratedAtUnixNano: time.Now().UTC().UnixNano(),
		Flags:               configs,
	}, nil
}

// newUUIDv7 generates a time-ordered UUIDv7 string using standard library crypto/rand.
func newUUIDv7() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(time.Now().UTC().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC4122
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

