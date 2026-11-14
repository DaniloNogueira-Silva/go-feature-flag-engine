-- 000001_init_schema.up.sql
-- Initial schema for the Enterprise Feature Flag Control Plane

CREATE TABLE IF NOT EXISTS environments (
    id UUID PRIMARY KEY,
    key VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(120) NOT NULL,
    sdk_secret_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flags (
    id UUID PRIMARY KEY,
    key VARCHAR(120) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flag_configs (
    id UUID PRIMARY KEY,
    flag_id UUID NOT NULL REFERENCES flags(id) ON DELETE CASCADE,
    environment_id UUID NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    default_rollout_basis_points INT NOT NULL DEFAULT 0 CHECK (default_rollout_basis_points BETWEEN 0 AND 10000),
    targeting_rules JSONB NOT NULL DEFAULT '[]'::JSONB,
    salt VARCHAR(120) NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(flag_id, environment_id)
);

CREATE INDEX IF NOT EXISTS idx_flag_configs_env_id ON flag_configs(environment_id);

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY,
    flag_key VARCHAR(120) NOT NULL,
    environment_key VARCHAR(64) NOT NULL,
    action VARCHAR(50) NOT NULL,
    actor VARCHAR(120) NOT NULL,
    previous_state JSONB,
    new_state JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_flag_env ON audit_logs(flag_key, environment_key, created_at DESC);

-- Seed default environments (production and staging)
INSERT INTO environments (id, key, name, sdk_secret_hash)
VALUES
    ('018f0000-0000-7000-8000-000000000001', 'production', 'Production', 'sdk-prod-default-key'),
    ('018f0000-0000-7000-8000-000000000002', 'staging', 'Staging', 'sdk-stag-default-key')
ON CONFLICT (key) DO NOTHING;

