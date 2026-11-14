package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/danil/feature-flag-engine/internal/domain/flag"
	goredis "github.com/redis/go-redis/v9"
)

const flagEventsChannel = "featureflags:events:v1"

// Broker implements flag.EventPublisher and flag.EventSubscriber using Redis Pub/Sub.
type Broker struct {
	client *goredis.Client
	logger *slog.Logger
}

// NewBroker constructs a new Redis Pub/Sub broker.
func NewBroker(client *goredis.Client, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Broker{
		client: client,
		logger: logger,
	}
}

// PublishFlagEvent serializes and publishes a DomainEvent to the Redis Pub/Sub channel.
func (b *Broker) PublishFlagEvent(ctx context.Context, event flag.DomainEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal domain event: %w", err)
	}
	if err := b.client.Publish(ctx, flagEventsChannel, payload).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

// Subscribe listens to the Redis Pub/Sub channel and invokes handler for every received DomainEvent
// until ctx is canceled.
func (b *Broker) Subscribe(ctx context.Context, handler func(flag.DomainEvent)) error {
	pubsub := b.client.Subscribe(ctx, flagEventsChannel)
	defer pubsub.Close()

	// Wait for subscription confirmation before consuming messages.
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("redis subscribe confirmation: %w", err)
	}

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("redis pubsub channel closed")
			}
			var ev flag.DomainEvent
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				b.logger.Error("failed to unmarshal redis flag event", "error", err)
				continue
			}
			handler(ev)
		}
	}
}

