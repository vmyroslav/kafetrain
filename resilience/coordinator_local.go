package resilience

import (
	"context"
	"sync"
)

// LocalStateCoordinator implements StateCoordinator for local in-memory locking.
// It manages reference counting for keys to ensure strict ordering within a single process.
type LocalStateCoordinator struct {
	lm lockMap
	mu sync.RWMutex
}

// NewLocalStateCoordinator creates a new LocalStateCoordinator.
func NewLocalStateCoordinator() *LocalStateCoordinator {
	return &LocalStateCoordinator{
		lm: make(lockMap),
	}
}

// Start is a no-op for LocalStateCoordinator as it requires no background processes.
func (l *LocalStateCoordinator) Start(_ context.Context, _ string) error {
	return nil
}

// Acquire locks the key in local memory.
func (l *LocalStateCoordinator) Acquire(_ context.Context, msg *InternalMessage, originalTopic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	key := string(msg.KeyData)
	l.lm.incrementRef(originalTopic, key)

	return nil
}

// Release unlocks the key in local memory.
func (l *LocalStateCoordinator) Release(_ context.Context, msg *InternalMessage) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	topic, ok := GetHeaderValue[string](&msg.HeaderData, HeaderTopic)
	if !ok {
		topic = msg.topic
	}

	key := string(msg.KeyData)

	newCount, exists := l.lm.decrementRef(topic, key)
	if !exists {
		return nil
	}

	if newCount <= 0 {
		l.lm.removeKey(topic, key)
	}

	if l.lm.isTopicEmpty(topic) {
		l.lm.removeTopic(topic)
	}

	return nil
}

// IsLocked checks if the key is currently locked in local memory.
func (l *LocalStateCoordinator) IsLocked(_ context.Context, msg *InternalMessage) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	count, exists := l.lm.getRefCount(msg.topic, string(msg.KeyData))

	return exists && count > 0
}

// topic map ['topic-name' => ['topic-key' => refCount]].
// Reference counting ensures keys are unlocked only when all messages with that key complete.
type lockMap map[string]map[string]int

// getRefCount returns the reference count for a topic/key pair.
// Returns (count, exists) - exists is false if topic or key doesn't exist.
func (lm lockMap) getRefCount(topic, key string) (int, bool) {
	if lm[topic] == nil {
		return 0, false
	}

	count, ok := lm[topic][key]

	return count, ok
}

// incrementRef increments the reference count for a topic/key pair
func (lm lockMap) incrementRef(topic, key string) int {
	if lm[topic] == nil {
		lm[topic] = make(map[string]int)
	}

	lm[topic][key]++

	return lm[topic][key]
}

// decrementRef decrements the reference count for a topic/key pair.
// Returns the new reference count and whether the key existed.
func (lm lockMap) decrementRef(topic, key string) (int, bool) {
	if lm[topic] == nil {
		return 0, false
	}

	if _, ok := lm[topic][key]; !ok {
		return 0, false
	}

	lm[topic][key]--

	return lm[topic][key], true
}

// removeKey removes a key from a topic
func (lm lockMap) removeKey(topic, key string) {
	if lm[topic] == nil {
		return
	}

	delete(lm[topic], key)
}

// removeTopic removes an entire topic and all its keys.
func (lm lockMap) removeTopic(topic string) {
	delete(lm, topic)
}

// isTopicEmpty returns true if topic has no keys or doesn't exist.
func (lm lockMap) isTopicEmpty(topic string) bool {
	return len(lm[topic]) == 0
}

// hasKey returns true if the topic/key pair exists (regardless of count value).
func (lm lockMap) hasKey(topic, key string) bool {
	if lm[topic] == nil {
		return false
	}

	_, ok := lm[topic][key]

	return ok
}
