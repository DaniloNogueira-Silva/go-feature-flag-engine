package sdk

import (
	"sync"

	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

// InMemoryCache stores pre-compiled feature flags protected by a read-write mutex
// for sub-microsecond concurrent reads across thousands of goroutines.
type InMemoryCache struct {
	mu      sync.RWMutex
	flags   map[string]*flag.CompiledFlag
	version int64
}

// NewInMemoryCache initializes an empty thread-safe flag cache.
func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		flags: make(map[string]*flag.CompiledFlag, 64),
	}
}

// ApplySnapshot atomically replaces or merges a full snapshot of flags into memory,
// respecting monotonic version numbers per flag so newer real-time events are never overwritten.
func (c *InMemoryCache) ApplySnapshot(configs []flag.FlagConfig, snapshotVersion int64) error {
	compiledMap := make(map[string]*flag.CompiledFlag, len(configs))
	for i := range configs {
		compiled, err := configs[i].Compile()
		if err != nil {
			return err
		}
		compiledMap[compiled.Key] = compiled
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, incoming := range compiledMap {
		if existing, ok := c.flags[k]; ok && existing.Version > incoming.Version {
			continue
		}
		c.flags[k] = incoming
	}
	if snapshotVersion > c.version {
		c.version = snapshotVersion
	}
	return nil
}

// ApplyUpdate compiles and updates a single flag in memory if and only if its version
// is strictly newer than the currently cached version (Monotonic Version Guard).
func (c *InMemoryCache) ApplyUpdate(cfg flag.FlagConfig) (applied bool, err error) {
	compiled, err := cfg.Compile()
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.flags[compiled.Key]; ok && existing.Version >= compiled.Version {
		return false, nil
	}
	c.flags[compiled.Key] = compiled
	if compiled.Version > c.version {
		c.version = compiled.Version
	}
	return true, nil
}

// Delete removes a flag from the in-memory store.
func (c *InMemoryCache) Delete(flagKey string, version int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.flags, flagKey)
	if version > c.version {
		c.version = version
	}
}

// Evaluate reads the compiled flag under a read lock (sync.RWMutex.RLock)
// and executes the deterministic evaluation algorithm without network I/O.
func (c *InMemoryCache) Evaluate(flagKey string, evalCtx flag.EvaluationContext) flag.EvaluationResult {
	c.mu.RLock()
	compiled := c.flags[flagKey]
	c.mu.RUnlock()

	if compiled == nil {
		return flag.EvaluationResult{
			FlagKey: flagKey,
			Enabled: false,
			Reason:  flag.ReasonFlagNotFound,
		}
	}
	return compiled.Evaluate(evalCtx)
}

// Version returns the highest configuration version observed by this cache.
func (c *InMemoryCache) Version() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// Size returns the number of flags currently cached in memory.
func (c *InMemoryCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.flags)
}

