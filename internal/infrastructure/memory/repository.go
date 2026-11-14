package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/danil/feature-flag-engine/internal/domain/environment"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

// Store provides a thread-safe in-memory implementation of environment.Repository,
// flag.Repository, flag.EventPublisher, and flag.EventSubscriber for testing and standalone runs.
type Store struct {
	mu          sync.RWMutex
	envs        map[string]environment.Environment
	flags       map[string]flag.Flag
	configs     map[string]flag.FlagConfig // key: envKey + ":" + flagKey
	audits      []flag.AuditEntry
	subscribers []func(flag.DomainEvent)
}

// NewStore creates a new in-memory Store pre-seeded with "production" and "staging" environments.
func NewStore() *Store {
	now := time.Now().UTC()
	s := &Store{
		envs:    make(map[string]environment.Environment),
		flags:   make(map[string]flag.Flag),
		configs: make(map[string]flag.FlagConfig),
	}
	s.envs["production"] = environment.Environment{
		ID:            "018f0000-0000-7000-8000-000000000001",
		Key:           "production",
		Name:          "Production",
		SDKSecretHash: "sdk-prod-default-key",
		CreatedAt:     now,
	}
	s.envs["staging"] = environment.Environment{
		ID:            "018f0000-0000-7000-8000-000000000002",
		Key:           "staging",
		Name:          "Staging",
		SDKSecretHash: "sdk-stag-default-key",
		CreatedAt:     now,
	}
	return s
}

func (s *Store) Create(_ context.Context, env *environment.Environment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envs[env.Key] = *env
	return nil
}

func (s *Store) GetByKey(_ context.Context, key string) (*environment.Environment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	env, ok := s.envs[key]
	if !ok {
		return nil, environment.ErrEnvironmentNotFound
	}
	cp := env
	return &cp, nil
}

func (s *Store) List(_ context.Context) ([]environment.Environment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]environment.Environment, 0, len(s.envs))
	for _, e := range s.envs {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *Store) CreateFlag(_ context.Context, f *flag.Flag) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flags[f.Key] = *f
	return nil
}

func (s *Store) GetFlagByKey(_ context.Context, key string) (*flag.Flag, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.flags[key]
	if !ok {
		return nil, flag.ErrFlagNotFound
	}
	cp := f
	return &cp, nil
}

func (s *Store) ListFlags(_ context.Context) ([]flag.Flag, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]flag.Flag, 0, len(s.flags))
	for _, f := range s.flags {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *Store) UpsertFlagConfig(_ context.Context, cfg *flag.FlagConfig, expectedVersion *int64) (*flag.FlagConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := cfg.EnvironmentKey + ":" + cfg.FlagKey
	existing, exists := s.configs[k]
	if expectedVersion != nil {
		if !exists || existing.Version != *expectedVersion {
			return nil, flag.ErrVersionConflict
		}
	}

	nextVersion := int64(1)
	if exists {
		nextVersion = existing.Version + 1
	}

	updated := *cfg
	updated.Version = nextVersion
	updated.UpdatedAt = time.Now().UTC()
	s.configs[k] = updated

	cp := updated
	return &cp, nil
}

func (s *Store) GetFlagConfig(_ context.Context, envKey, flagKey string) (*flag.FlagConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := envKey + ":" + flagKey
	cfg, ok := s.configs[k]
	if !ok {
		return nil, flag.ErrFlagNotFound
	}
	cp := cfg
	return &cp, nil
}

func (s *Store) GetEnvironmentSnapshot(_ context.Context, envKey string) ([]flag.FlagConfig, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []flag.FlagConfig
	var maxVer int64
	for _, cfg := range s.configs {
		if cfg.EnvironmentKey == envKey {
			out = append(out, cfg)
			if cfg.Version > maxVer {
				maxVer = cfg.Version
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FlagKey < out[j].FlagKey })
	return out, maxVer, nil
}

func (s *Store) RecordAuditLog(_ context.Context, entry *flag.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, *entry)
	return nil
}

func (s *Store) PublishFlagEvent(_ context.Context, event flag.DomainEvent) error {
	s.mu.RLock()
	subs := append([]func(flag.DomainEvent){}, s.subscribers...)
	s.mu.RUnlock()

	for _, fn := range subs {
		fn(event)
	}
	return nil
}

func (s *Store) Subscribe(ctx context.Context, handler func(flag.DomainEvent)) error {
	s.mu.Lock()
	s.subscribers = append(s.subscribers, handler)
	s.mu.Unlock()
	<-ctx.Done()
	return nil
}

