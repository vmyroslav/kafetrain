package resilience

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalStateCoordinator_Acquire(t *testing.T) {
	t.Parallel()

	coordinator := NewLocalStateCoordinator()
	ctx := t.Context()

	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-123"),
	}

	err := coordinator.Acquire(ctx, msg, "orders")
	assert.NoError(t, err)

	assert.True(t, coordinator.IsLocked(ctx, msg))
}

func TestLocalStateCoordinator_Release(t *testing.T) {
	t.Parallel()

	coordinator := NewLocalStateCoordinator()
	ctx := t.Context()

	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-123"),
	}

	// lock first
	_ = coordinator.Acquire(ctx, msg, "orders")
	assert.True(t, coordinator.IsLocked(ctx, msg))

	// release
	err := coordinator.Release(ctx, msg)
	assert.NoError(t, err)

	assert.False(t, coordinator.IsLocked(ctx, msg))
}

func TestLocalStateCoordinator_ReferenceCounting(t *testing.T) {
	t.Parallel()

	coordinator := NewLocalStateCoordinator()
	ctx := t.Context()

	topic := "orders"
	key := "ref-key"
	msg := &InternalMessage{topic: topic, KeyData: []byte(key)}

	// first Lock
	err := coordinator.Acquire(ctx, msg, topic)
	assert.NoError(t, err)

	assert.True(t, coordinator.IsLocked(ctx, msg))
	count, _ := coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 1, count)

	// second lock
	err = coordinator.Acquire(ctx, msg, topic)
	assert.NoError(t, err)

	assert.True(t, coordinator.IsLocked(ctx, msg))
	count, _ = coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 2, count)

	// first release
	err = coordinator.Release(ctx, msg)
	assert.NoError(t, err)

	// should STILL be locked (Ref count 2 -> 1)
	assert.True(t, coordinator.IsLocked(ctx, msg))
	count, _ = coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 1, count)

	// second release
	err = coordinator.Release(ctx, msg)
	assert.NoError(t, err)

	// should be UNLOCKED (Ref count 1 -> 0)
	assert.False(t, coordinator.IsLocked(ctx, msg))
	count, exists := coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 0, count)
	assert.False(t, exists)
}

func TestLocalStateCoordinator_Isolation(t *testing.T) {
	t.Parallel()

	coordinator := NewLocalStateCoordinator()
	ctx := t.Context()

	// lock key A
	msgA := &InternalMessage{topic: "topic1", KeyData: []byte("keyA")}
	_ = coordinator.Acquire(ctx, msgA, "topic1")

	// check key A is locked
	assert.True(t, coordinator.IsLocked(ctx, msgA))

	// check key B is NOT locked
	msgB := &InternalMessage{topic: "topic1", KeyData: []byte("keyB")}
	assert.False(t, coordinator.IsLocked(ctx, msgB))

	// check key A on different topic is NOT locked
	msgOtherTopic := &InternalMessage{topic: "topic2", KeyData: []byte("keyA")}
	assert.False(t, coordinator.IsLocked(ctx, msgOtherTopic))
}

func TestLocalStateCoordinator_Release_WithHeader(t *testing.T) {
	t.Parallel()

	coordinator := NewLocalStateCoordinator()
	ctx := t.Context()

	// acquire on "orders"
	msg := &InternalMessage{topic: "orders", KeyData: []byte("key1")}
	_ = coordinator.Acquire(ctx, msg, "orders")

	// release using a message with explicit HeaderTopic (simulating redirect message)
	releaseMsg := &InternalMessage{
		topic:   "redirect_orders", // different topic
		KeyData: []byte("key1"),
		HeaderData: HeaderList{
			{Key: []byte(HeaderTopic), Value: []byte("orders")},
		},
	}

	err := coordinator.Release(ctx, releaseMsg)
	assert.NoError(t, err)

	assert.False(t, coordinator.IsLocked(ctx, msg))
}

func TestLocalStateCoordinator_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	var (
		ctx         = t.Context()
		coordinator = NewLocalStateCoordinator()
		topic       = "concurrent-topic"
		key         = "concurrent-key"
		msg         = &InternalMessage{topic: topic, KeyData: []byte(key)}
		concurrency = 100
		iterations  = 100

		wg sync.WaitGroup
	)

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				_ = coordinator.Acquire(ctx, msg, topic)

				if !coordinator.IsLocked(ctx, msg) {
					t.Error("should be locked")
				}

				_ = coordinator.Release(ctx, msg)
			}
		}()
	}

	wg.Wait()

	// after all goroutines finish, ref count should be 0
	assert.False(t, coordinator.IsLocked(ctx, msg))
	count, exists := coordinator.lm.getRefCount(topic, key)
	assert.Equal(t, 0, count)
	assert.False(t, exists)
}
