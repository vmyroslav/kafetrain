package kafetrain

import (
	"context"
	"sync"
)

//TODO: rename
type Comparator interface {
	IsRelated(ctx context.Context, msg Message) bool
	AddMessage(ctx context.Context, msg Message) error
}

type KeyComparator struct {
	lm lockMap

	sync.RWMutex
}

func NewKeyComparator() *KeyComparator {
	return &KeyComparator{lm: make(lockMap)}
}

func (c *KeyComparator) IsRelated(msg Message) bool {
	c.RLock()
	defer c.Unlock()
	if _, ok := c.lm[msg.topic][string(msg.Key)]; ok {
		return true
	}

	return false
}

// topic map ['topic-name' => ['topic-key' => ['id1', 'id2', 'id3'], 'topic-key-2' => ['id4', 'id5', 'id6']]].
type lockMap map[string]map[string][]string

func (c *KeyComparator) LockMessage(_ context.Context, msg Message) error {
	c.Lock()
	c.lm[msg.topic][string(msg.Key)] = append(c.lm[msg.topic][string(msg.Key)], "id")
	c.Unlock()

	return nil
}
