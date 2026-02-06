package main

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/vmyroslav/kafka-resilience/resilience"
)

// DemoHandler processes messages with configurable failure behavior.
type DemoHandler struct {
	tracker *resilience.ErrorTracker

	mu          sync.RWMutex
	failKeys    map[string]bool   // Keys that should fail on next attempt
	lockedKeys  map[string]bool   // Keys currently in retry chain (for display)
	processedAt map[string]string // Last processed value per key

	// Stats
	processed atomic.Int64
	retried   atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64
}

// NewDemoHandler creates a handler with fail control.
func NewDemoHandler(tracker *resilience.ErrorTracker) *DemoHandler {
	return &DemoHandler{
		tracker:     tracker,
		failKeys:    make(map[string]bool),
		lockedKeys:  make(map[string]bool),
		processedAt: make(map[string]string),
	}
}

// Handle processes a message, failing if the key is in the fail list.
func (h *DemoHandler) Handle(ctx context.Context, msg resilience.Message) error {
	key := string(msg.Key())
	value := string(msg.Value())

	// Check retry attempt
	attempt := 0
	if v, ok := msg.Headers().Get(resilience.HeaderRetryAttempt); ok {
		attempt, _ = strconv.Atoi(string(v))
	}

	isRetry := attempt > 0
	h.processed.Add(1)

	slog.Info("Processing message",
		"key", key,
		"value", value,
		"attempt", attempt,
		"topic", msg.Topic(),
	)

	// Track locked keys for /locks endpoint
	if h.tracker.IsInRetryChain(ctx, msg) {
		h.setLocked(key, true)
	}

	// Check if this key should fail
	shouldFail := h.checkAndClearFail(key)

	if shouldFail {
		h.failed.Add(1)
		if isRetry {
			h.retried.Add(1)
		}
		slog.Warn("Simulated failure", "key", key, "attempt", attempt)
		return errors.New("simulated failure")
	}

	// Success
	h.succeeded.Add(1)
	h.setLocked(key, false)
	h.setProcessed(key, value)

	slog.Info("Successfully processed", "key", key, "value", value)
	return nil
}

// SetFail marks a key to fail on next processing attempt.
func (h *DemoHandler) SetFail(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failKeys[key] = true
}

// ClearFail removes a key from the fail list.
func (h *DemoHandler) ClearFail(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.failKeys, key)
}

// checkAndClearFail checks if key should fail and clears the flag.
func (h *DemoHandler) checkAndClearFail(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failKeys[key] {
		delete(h.failKeys, key)
		return true
	}
	return false
}

// GetPendingFails returns keys that will fail on next attempt.
func (h *DemoHandler) GetPendingFails() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	keys := make([]string, 0, len(h.failKeys))
	for k := range h.failKeys {
		keys = append(keys, k)
	}
	return keys
}

// setLocked updates the locked status for display.
func (h *DemoHandler) setLocked(key string, locked bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if locked {
		h.lockedKeys[key] = true
	} else {
		delete(h.lockedKeys, key)
	}
}

// GetLockedKeys returns keys currently in retry chain.
func (h *DemoHandler) GetLockedKeys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	keys := make([]string, 0, len(h.lockedKeys))
	for k := range h.lockedKeys {
		keys = append(keys, k)
	}
	return keys
}

// setProcessed records the last processed value for a key.
func (h *DemoHandler) setProcessed(key, value string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processedAt[key] = value
}

// GetStats returns processing statistics.
func (h *DemoHandler) GetStats() map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()

	processed := make(map[string]string, len(h.processedAt))
	for k, v := range h.processedAt {
		processed[k] = v
	}

	return map[string]any{
		"total_processed": h.processed.Load(),
		"succeeded":       h.succeeded.Load(),
		"failed":          h.failed.Load(),
		"retried":         h.retried.Load(),
		"last_values":     processed,
	}
}
