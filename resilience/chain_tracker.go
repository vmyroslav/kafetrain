package resilience

import (
	"context"
	"sync"
)

type MessageChainTracker interface {
	IsRelated(ctx context.Context, msg Message) bool             // IsRelated returns true if message is related to error chain
	AddMessage(ctx context.Context, msg Message) (string, error) // AddMessage adds message to error chain
	ReleaseMessage(ctx context.Context, msg Message) error       // ReleaseMessage removes message from error chain
}

type KeyTracker struct {
	lm lockMap

	sync.RWMutex
}

func NewKeyTracker() *KeyTracker {
	return &KeyTracker{lm: make(lockMap)}
}

func (kt *KeyTracker) IsRelated(_ context.Context, msg Message) bool {
	kt.RLock()
	defer kt.RUnlock()

	// If key for this topic located in map, then message is related.
	if _, ok := kt.lm[msg.topic][string(msg.Key)]; ok {
		return true
	}

	return false
}

// topic map ['topic-name' => ['topic-key' => ['id1', 'id2', 'id3'], 'topic-key-2' => ['id4', 'id5', 'id6']]].
type lockMap map[string]map[string][]string

func (kt *KeyTracker) AddMessage(_ context.Context, msg Message) (string, error) {
	kt.Lock()
	key := string(msg.Key)

	_, ok := kt.lm[msg.topic]
	if !ok {
		kt.lm[msg.topic] = make(map[string][]string)
	}

	_, ok = kt.lm[msg.topic][key]
	if !ok {
		kt.lm[msg.topic][key] = make([]string, 0)
	}

	kt.lm[msg.topic][key] = append(kt.lm[msg.topic][key], key)
	kt.Unlock()

	return key, nil
}

func (kt *KeyTracker) ReleaseMessage(_ context.Context, msg Message) error {
	topic := msg.topic
	key := string(msg.Key)

	kt.Lock()
	mm := remove(kt.lm[topic][key], key)

	if len(mm) == 0 {
		delete(kt.lm[topic], key)
	} else {
		kt.lm[topic][key] = mm
	}

	if kt.lm[topic] == nil || len(kt.lm[topic]) == 0 {
		delete(kt.lm, topic)
	}

	kt.Unlock()

	return nil
}

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}

	return s
}

func (lockMap) remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}

	return s
}
