package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danil/feature-flag-engine/internal/domain/environment"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

//go:embed migrations/000001_init_schema.up.sql
var initialMigrationSQL string

// Store implements both flag.Repository and environment.Repository backed by PostgreSQL.
type Store struct {
	db *sql.DB
}

// NewStore creates a new PostgreSQL-backed store.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// RunMigrations executes the embedded schema creation idempotently.
func (s *Store) RunMigrations(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, initialMigrationSQL)
	return err
}

// Create inserts a new environment into PostgreSQL.
func (s *Store) Create(ctx context.Context, env *environment.Environment) error {
	const q = `
		INSERT INTO environments (id, key, name, sdk_secret_hash, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := s.db.ExecContext(ctx, q, env.ID, env.Key, env.Name, env.SDKSecretHash, env.CreatedAt)
	return err
}

// GetByKey retrieves an environment by its unique key.
func (s *Store) GetByKey(ctx context.Context, key string) (*environment.Environment, error) {
	const q = `
		SELECT id, key, name, sdk_secret_hash, created_at
		FROM environments
		WHERE key = $1
	`
	var env environment.Environment
	err := s.db.QueryRowContext(ctx, q, key).Scan(
		&env.ID, &env.Key, &env.Name, &env.SDKSecretHash, &env.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, environment.ErrEnvironmentNotFound
	}
	if err != nil {
		return nil, err
	}
	return &env, nil
}

// List returns all registered environments ordered by key.
func (s *Store) List(ctx context.Context) ([]environment.Environment, error) {
	const q = `
		SELECT id, key, name, sdk_secret_hash, created_at
		FROM environments
		ORDER BY key ASC
	`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []environment.Environment
	for rows.Next() {
		var env environment.Environment
		if err := rows.Scan(&env.ID, &env.Key, &env.Name, &env.SDKSecretHash, &env.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

// CreateFlag inserts a new feature flag definition.
func (s *Store) CreateFlag(ctx context.Context, f *flag.Flag) error {
	const q = `
		INSERT INTO flags (id, key, name, description, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := s.db.ExecContext(ctx, q, f.ID, f.Key, f.Name, f.Description, f.CreatedAt, f.UpdatedAt)
	return err
}

// GetFlagByKey retrieves a feature flag definition by its unique key.
func (s *Store) GetFlagByKey(ctx context.Context, key string) (*flag.Flag, error) {
	const q = `
		SELECT id, key, name, description, created_at, updated_at
		FROM flags
		WHERE key = $1
	`
	var f flag.Flag
	err := s.db.QueryRowContext(ctx, q, key).Scan(
		&f.ID, &f.Key, &f.Name, &f.Description, &f.CreatedAt, &f.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, flag.ErrFlagNotFound
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// ListFlags returns all feature flag definitions.
func (s *Store) ListFlags(ctx context.Context) ([]flag.Flag, error) {
	const q = `
		SELECT id, key, name, description, created_at, updated_at
		FROM flags
		ORDER BY key ASC
	`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []flag.Flag
	for rows.Next() {
		var f flag.Flag
		if err := rows.Scan(&f.ID, &f.Key, &f.Name, &f.Description, &f.CreatedAt, &f.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// UpsertFlagConfig creates or updates a flag configuration for an environment using optimistic locking.
func (s *Store) UpsertFlagConfig(ctx context.Context, cfg *flag.FlagConfig, expectedVersion *int64) (*flag.FlagConfig, error) {
	rulesJSON, err := json.Marshal(cfg.TargetingRules)
	if err != nil {
		return nil, fmt.Errorf("marshal targeting rules: %w", err)
	}

	now := time.Now().UTC()
	if expectedVersion != nil {
		const updateQ = `
			UPDATE flag_configs
			SET enabled = $1,
			    default_rollout_basis_points = $2,
			    targeting_rules = $3,
			    salt = $4,
			    version = version + 1,
			    updated_at = $5
			WHERE flag_id = $6 AND environment_id = $7 AND version = $8
			RETURNING id, version, updated_at
		`
		err := s.db.QueryRowContext(
			ctx,
			updateQ,
			cfg.Enabled,
			cfg.DefaultRolloutBasisPoints,
			rulesJSON,
			cfg.Salt,
			now,
			cfg.FlagID,
			cfg.EnvironmentID,
			*expectedVersion,
		).Scan(&cfg.ID, &cfg.Version, &cfg.UpdatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, flag.ErrVersionConflict
		}
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}

	const upsertQ = `
		INSERT INTO flag_configs (
			id, flag_id, environment_id, enabled, default_rollout_basis_points,
			targeting_rules, salt, version, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8)
		ON CONFLICT (flag_id, environment_id) DO UPDATE
		SET enabled = EXCLUDED.enabled,
		    default_rollout_basis_points = EXCLUDED.default_rollout_basis_points,
		    targeting_rules = EXCLUDED.targeting_rules,
		    salt = EXCLUDED.salt,
		    version = flag_configs.version + 1,
		    updated_at = EXCLUDED.updated_at
		RETURNING id, version, updated_at
	`
	err = s.db.QueryRowContext(
		ctx,
		upsertQ,
		cfg.ID,
		cfg.FlagID,
		cfg.EnvironmentID,
		cfg.Enabled,
		cfg.DefaultRolloutBasisPoints,
		rulesJSON,
		cfg.Salt,
		now,
	).Scan(&cfg.ID, &cfg.Version, &cfg.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// GetFlagConfig fetches the configuration of a specific flag inside an environment.
func (s *Store) GetFlagConfig(ctx context.Context, envKey, flagKey string) (*flag.FlagConfig, error) {
	const q = `
		SELECT fc.id, fc.flag_id, f.key, fc.environment_id, e.key,
		       fc.enabled, fc.default_rollout_basis_points, fc.targeting_rules,
		       fc.salt, fc.version, fc.updated_at
		FROM flag_configs fc
		JOIN flags f ON f.id = fc.flag_id
		JOIN environments e ON e.id = fc.environment_id
		WHERE e.key = $1 AND f.key = $2
	`
	var cfg flag.FlagConfig
	var rulesRaw []byte
	err := s.db.QueryRowContext(ctx, q, envKey, flagKey).Scan(
		&cfg.ID, &cfg.FlagID, &cfg.FlagKey, &cfg.EnvironmentID, &cfg.EnvironmentKey,
		&cfg.Enabled, &cfg.DefaultRolloutBasisPoints, &rulesRaw,
		&cfg.Salt, &cfg.Version, &cfg.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, flag.ErrFlagNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rulesRaw, &cfg.TargetingRules); err != nil {
		return nil, fmt.Errorf("unmarshal targeting rules: %w", err)
	}
	return &cfg, nil
}

// GetEnvironmentSnapshot returns all flag configurations for an environment along with the highest version number.
func (s *Store) GetEnvironmentSnapshot(ctx context.Context, envKey string) ([]flag.FlagConfig, int64, error) {
	const q = `
		SELECT fc.id, fc.flag_id, f.key, fc.environment_id, e.key,
		       fc.enabled, fc.default_rollout_basis_points, fc.targeting_rules,
		       fc.salt, fc.version, fc.updated_at
		FROM flag_configs fc
		JOIN flags f ON f.id = fc.flag_id
		JOIN environments e ON e.id = fc.environment_id
		WHERE e.key = $1
		ORDER BY f.key ASC
	`
	rows, err := s.db.QueryContext(ctx, q, envKey)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var configs []flag.FlagConfig
	var maxVersion int64
	for rows.Next() {
		var cfg flag.FlagConfig
		var rulesRaw []byte
		if err := rows.Scan(
			&cfg.ID, &cfg.FlagID, &cfg.FlagKey, &cfg.EnvironmentID, &cfg.EnvironmentKey,
			&cfg.Enabled, &cfg.DefaultRolloutBasisPoints, &rulesRaw,
			&cfg.Salt, &cfg.Version, &cfg.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		if err := json.Unmarshal(rulesRaw, &cfg.TargetingRules); err != nil {
			return nil, 0, err
		}
		if cfg.Version > maxVersion {
			maxVersion = cfg.Version
		}
		configs = append(configs, cfg)
	}
	return configs, maxVersion, rows.Err()
}

// RecordAuditLog persists an audit entry into PostgreSQL.
func (s *Store) RecordAuditLog(ctx context.Context, entry *flag.AuditEntry) error {
	var prevJSON, newJSON []byte
	var err error
	if entry.PreviousState != nil {
		prevJSON, err = json.Marshal(entry.PreviousState)
		if err != nil {
			return err
		}
	}
	if entry.NewState != nil {
		newJSON, err = json.Marshal(entry.NewState)
		if err != nil {
			return err
		}
	}
	const q = `
		INSERT INTO audit_logs (id, flag_key, environment_key, action, actor, previous_state, new_state, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = s.db.ExecContext(
		ctx, q,
		entry.ID, entry.FlagKey, entry.EnvironmentKey, entry.Action, entry.Actor,
		nullableJSON(prevJSON), nullableJSON(newJSON), entry.CreatedAt,
	)
	return err
}

func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

