package grpc

import (
	"sync"
	"sync/atomic"

	flagstreamv1 "github.com/danil/feature-flag-engine/api/proto/v1"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

// Hub manages active gRPC server-streaming subscriptions partitioned by environment key.
type Hub struct {
	mu          sync.RWMutex
	nextSubID   atomic.Uint64
	activeCount atomic.Int64
	subsByEnv   map[string]map[uint64]chan *flagstreamv1.FlagEvent
}

// NewHub creates a new thread-safe streaming connection hub.
func NewHub() *Hub {
	return &Hub{
		subsByEnv: make(map[string]map[uint64]chan *flagstreamv1.FlagEvent),
	}
}

// Subscribe registers a buffered channel to receive real-time FlagEvents for envKey.
// It returns a subscription ID, the event channel, and an unsubscribe cleanup function.
func (h *Hub) Subscribe(envKey string, bufferSize int) (uint64, <-chan *flagstreamv1.FlagEvent, func()) {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	id := h.nextSubID.Add(1)
	ch := make(chan *flagstreamv1.FlagEvent, bufferSize)

	h.mu.Lock()
	if h.subsByEnv[envKey] == nil {
		h.subsByEnv[envKey] = make(map[uint64]chan *flagstreamv1.FlagEvent)
	}
	h.subsByEnv[envKey][id] = ch
	h.activeCount.Add(1)
	h.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			h.mu.Lock()
			if envSubs, ok := h.subsByEnv[envKey]; ok {
				if c, exists := envSubs[id]; exists {
					delete(envSubs, id)
					close(c)
					h.activeCount.Add(-1)
				}
				if len(envSubs) == 0 {
					delete(h.subsByEnv, envKey)
				}
			}
			h.mu.Unlock()
		})
	}

	return id, ch, unsubscribe
}

// BroadcastDomainEvent converts a domain event into a wire FlagEvent and fans it out
// to all connected SDK streams for the target environment without blocking slow consumers.
func (h *Hub) BroadcastDomainEvent(ev flag.DomainEvent) int {
	wireEv := DomainEventToProto(ev)

	h.mu.RLock()
	defer h.mu.RUnlock()

	envSubs := h.subsByEnv[ev.EnvironmentKey]
	delivered := 0
	for _, ch := range envSubs {
		select {
		case ch <- wireEv:
			delivered++
		default:
			// Drop oldest queued item and push latest so slow consumer always converges to latest version.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- wireEv:
				delivered++
			default:
			}
		}
	}
	return delivered
}

// ActiveSubscribers returns the current number of open gRPC stream subscriptions.
func (h *Hub) ActiveSubscribers() int64 {
	return h.activeCount.Load()
}

// DomainEventToProto converts a domain.DomainEvent to its protobuf wire representation.
func DomainEventToProto(ev flag.DomainEvent) *flagstreamv1.FlagEvent {
	eventType := flagstreamv1.EventType_EVENT_TYPE_FLAG_UPDATED
	if ev.Type == flag.EventFlagDeleted {
		eventType = flagstreamv1.EventType_EVENT_TYPE_FLAG_DELETED
	}

	var flags []flagstreamv1.FlagConfiguration
	if ev.FlagConfig != nil {
		flags = []flagstreamv1.FlagConfiguration{FlagConfigToProto(*ev.FlagConfig)}
	}

	return &flagstreamv1.FlagEvent{
		Type:                eventType,
		EnvironmentKey:      ev.EnvironmentKey,
		Version:             ev.Version,
		PublishedAtUnixNano: ev.PublishedAtUnixNano,
		Flags:               flags,
	}
}

// FlagConfigToProto converts a domain.FlagConfig into a wire FlagConfiguration.
func FlagConfigToProto(cfg flag.FlagConfig) flagstreamv1.FlagConfiguration {
	rules := make([]flagstreamv1.TargetingRule, 0, len(cfg.TargetingRules))
	for _, r := range cfg.TargetingRules {
		clauses := make([]flagstreamv1.Clause, 0, len(r.Clauses))
		for _, c := range r.Clauses {
			clauses = append(clauses, flagstreamv1.Clause{
				Attribute: c.Attribute,
				Operator:  string(c.Operator),
				Values:    c.Values,
			})
		}
		rules = append(rules, flagstreamv1.TargetingRule{
			Id:                 r.ID,
			Name:               r.Name,
			Clauses:            clauses,
			RolloutBasisPoints: int32(r.RolloutBasisPoints),
			Salt:               r.Salt,
		})
	}
	return flagstreamv1.FlagConfiguration{
		Id:                        cfg.ID,
		FlagId:                    cfg.FlagID,
		FlagKey:                   cfg.FlagKey,
		EnvironmentId:             cfg.EnvironmentID,
		EnvironmentKey:            cfg.EnvironmentKey,
		Enabled:                   cfg.Enabled,
		DefaultRolloutBasisPoints: int32(cfg.DefaultRolloutBasisPoints),
		TargetingRules:            rules,
		Salt:                      cfg.Salt,
		Version:                   cfg.Version,
		UpdatedAtUnixNano:         cfg.UpdatedAt.UnixNano(),
	}
}

// ProtoToFlagConfig converts a wire FlagConfiguration back into a domain.FlagConfig.
func ProtoToFlagConfig(p flagstreamv1.FlagConfiguration) flag.FlagConfig {
	rules := make([]flag.TargetingRule, 0, len(p.TargetingRules))
	for _, r := range p.TargetingRules {
		clauses := make([]flag.Clause, 0, len(r.Clauses))
		for _, c := range r.Clauses {
			clauses = append(clauses, flag.Clause{
				Attribute: c.Attribute,
				Operator:  flag.Operator(c.Operator),
				Values:    c.Values,
			})
		}
		rules = append(rules, flag.TargetingRule{
			ID:                 r.Id,
			Name:               r.Name,
			Clauses:            clauses,
			RolloutBasisPoints: int(r.RolloutBasisPoints),
			Salt:               r.Salt,
		})
	}
	return flag.FlagConfig{
		ID:                        p.Id,
		FlagID:                    p.FlagId,
		FlagKey:                   p.FlagKey,
		EnvironmentID:             p.EnvironmentId,
		EnvironmentKey:            p.EnvironmentKey,
		Enabled:                   p.Enabled,
		DefaultRolloutBasisPoints: int(p.DefaultRolloutBasisPoints),
		TargetingRules:            rules,
		Salt:                      p.Salt,
		Version:                   p.Version,
	}
}

