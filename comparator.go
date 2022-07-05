package kafetrain

import (
	"context"
	"sync"
)

type MessageChainTracker interface {
	IsRelated(ctx context.Context, msg Message) bool   // IsRelated returns true if message is related to error chain
	AddMessage(ctx context.Context, msg Message) error // AddMessage adds message to error chain
}

type KeyComparator struct {
	lm lockMap

	sync.RWMutex
}

func NewKeyComparator() *KeyComparator {
	return &KeyComparator{lm: make(lockMap)}
}

func (c *KeyComparator) IsRelated(_ context.Context, msg Message) bool {
	c.RLock()
	defer c.RUnlock()

	// If key for this topic located in map, then message is related.
	if _, ok := c.lm[msg.topic][string(msg.Key)]; ok {
		return true
	}

	return false
}

// topic map ['topic-name' => ['topic-key' => ['id1', 'id2', 'id3'], 'topic-key-2' => ['id4', 'id5', 'id6']]].
type lockMap map[string]map[string][]string

func (c *KeyComparator) AddMessage(_ context.Context, msg Message) error {
	c.Lock()
	key := string(msg.Key)

	_, ok := c.lm[msg.topic]
	if !ok {
		c.lm[msg.topic] = make(map[string][]string)
	}

	_, ok = c.lm[msg.topic][key]
	if !ok {
		c.lm[msg.topic][key] = make([]string, 0)
	}

	c.lm[msg.topic][key] = append(c.lm[msg.topic][key], key)
	c.Unlock()

	return nil
}
