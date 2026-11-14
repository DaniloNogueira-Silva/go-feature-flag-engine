package flag

import (
	"context"
	"time"
)

// DomainEventType identifies the type of real-time change published by the Control Plane.
type DomainEventType string

const (
	EventFlagUpdated DomainEventType = "FLAG_UPDATED"
	EventFlagDeleted DomainEventType = "FLAG_DELETED"
)

// DomainEvent represents a change notification emitted whenever a flag configuration changes.
type DomainEvent struct {
	Type                DomainEventType `json:"type"`
	EnvironmentKey      string          `json:"environment_key"`
	Version             int64           `json:"version"`
	PublishedAtUnixNano int64           `json:"published_at_unix_nano"`
	FlagConfig          *FlagConfig     `json:"flag_config,omitempty"`
}

// AuditEntry records an immutable state transition in the Control Plane.
type AuditEntry struct {
	ID             string      `json:"id"`
	FlagKey        string      `json:"flag_key"`
	EnvironmentKey string      `json:"environment_key"`
	Action         string      `json:"action"`
	Actor          string      `json:"actor"`
	PreviousState  *FlagConfig `json:"previous_state,omitempty"`
	NewState       *FlagConfig `json:"new_state,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
}

// Repository defines persistence operations for flags and their per-environment configurations.
type Repository interface {
	CreateFlag(ctx context.Context, flag *Flag) error
	GetFlagByKey(ctx context.Context, key string) (*Flag, error)
	ListFlags(ctx context.Context) ([]Flag, error)

	UpsertFlagConfig(ctx context.Context, config *FlagConfig, expectedVersion *int64) (*FlagConfig, error)
	GetFlagConfig(ctx context.Context, envKey, flagKey string) (*FlagConfig, error)
	GetEnvironmentSnapshot(ctx context.Context, envKey string) ([]FlagConfig, int64, error)

	RecordAuditLog(ctx context.Context, entry *AuditEntry) error
}

// EventPublisher broadcasts domain events to downstream Data Plane subscribers.
type EventPublisher interface {
	PublishFlagEvent(ctx context.Context, event DomainEvent) error
}

// EventSubscriber listens for domain events for one or all environments.
type EventSubscriber interface {
	Subscribe(ctx context.Context, handler func(DomainEvent)) error
}

