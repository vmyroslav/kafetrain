package resilience

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

	// If key for this topic has a reference count > 0, then message is related.
	count, ok := kt.lm[msg.topic][string(msg.Key)]
	return ok && count > 0
}

// topic map ['topic-name' => ['topic-key' => refCount]].
// Reference counting ensures keys are unlocked only when all messages with that key complete.
type lockMap map[string]map[string]int

func (kt *KeyTracker) AddMessage(_ context.Context, msg *Message) (string, error) {
	kt.mu.Lock()
	defer kt.mu.Unlock()

	key := string(msg.Key)

	if kt.lm[msg.topic] == nil {
		kt.lm[msg.topic] = make(map[string]int)
	}

	// Increment reference count for this key
	kt.lm[msg.topic][key]++

	return key, nil
}

func (kt *KeyTracker) ReleaseMessage(_ context.Context, msg *Message) error {
	topic := msg.topic
	key := string(msg.Key)

	kt.mu.Lock()
	defer kt.mu.Unlock()

	// Check if topic exists in map
	if kt.lm[topic] == nil {
		return nil
	}

	// Decrement reference count
	kt.lm[topic][key]--

	// Remove key when count reaches 0
	if kt.lm[topic][key] <= 0 {
		delete(kt.lm[topic], key)
	}

	// Clean up topic map if empty
	if len(kt.lm[topic]) == 0 {
		delete(kt.lm, topic)
	}

	return nil
}
