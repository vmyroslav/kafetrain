package retryold

import (
	"context"
	"sync"
)

type MessageChainTracker interface {
	IsRelated(ctx context.Context, msg *Message) bool             // IsRelated returns true if message is related to error chain
	AddMessage(ctx context.Context, msg *Message) (string, error) // AddMessage adds message to error chain
	ReleaseMessage(ctx context.Context, msg *Message) error       // ReleaseMessage removes message from error chain
}

type KeyTracker struct {
	lm lockMap

	mu sync.RWMutex
}

func NewKeyTracker() *KeyTracker {
	return &KeyTracker{lm: make(lockMap)}
}

func (kt *KeyTracker) IsRelated(_ context.Context, msg *Message) bool {
	kt.mu.RLock()
	defer kt.mu.RUnlock()

	// if key for this topic has a reference count > 0, then message is related.
	count, exists := kt.lm.getRefCount(msg.topic, string(msg.Key))
	return exists && count > 0
}

func (kt *KeyTracker) AddMessage(_ context.Context, msg *Message) (string, error) {
	kt.mu.Lock()
	defer kt.mu.Unlock()

	key := string(msg.Key)

	// increment reference count for this key
	kt.lm.incrementRef(msg.topic, key)

	return key, nil
}

func (kt *KeyTracker) ReleaseMessage(_ context.Context, msg *Message) error {
	topic := msg.topic
	key := string(msg.Key)

	kt.mu.Lock()
	defer kt.mu.Unlock()

	// decrement reference count
	newCount := kt.lm.decrementRef(topic, key)

	// remove key when count reaches 0
	if newCount <= 0 {
		kt.lm.removeKey(topic, key)
	}

	// clean up topic map if empty
	if kt.lm.isTopicEmpty(topic) {
		kt.lm.removeTopic(topic)
	}

	return nil
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

// incrementRef increments the reference count for a topic/key pair.
// Initializes topic map if needed. Returns the new reference count.
func (lm lockMap) incrementRef(topic, key string) int {
	if lm[topic] == nil {
		lm[topic] = make(map[string]int)
	}
	lm[topic][key]++

	return lm[topic][key]
}

// decrementRef decrements the reference count for a topic/key pair.
// Returns the new reference count (can be negative if key doesn't exist).
func (lm lockMap) decrementRef(topic, key string) int {
	if lm[topic] == nil {
		return -1
	}
	lm[topic][key]--

	return lm[topic][key]
}

// removeKey removes a key from a topic.
// No-op if topic or key doesn't exist.
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
