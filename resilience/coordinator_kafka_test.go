package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaStateCoordinator_Acquire(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockAdmin := &AdminMock{}
	mockFactory := &ConsumerFactoryMock{}
	mockLogger := &LoggerMock{
		DebugFunc: func(_ string, _ ...any) {},
		ErrorFunc: func(_ string, _ ...any) {},
	}

	cfg := &Config{
		RedirectTopicPrefix: "redirect",
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		mockLogger,
		mockProducer,
		mockFactory,
		mockAdmin,
		make(chan error, 1),
	)

	ctx := t.Context()
	msg := &InternalMessage{
		topic:      "orders",
		KeyData:    []byte("order-123"),
		HeaderData: &HeaderList{},
	}
	msg.HeaderData.Set("custom", []byte("val"))

	err := coordinator.Acquire(ctx, "orders", msg)
	require.NoError(t, err)

	// optimistic locking: should be locked immediately
	assert.True(t, coordinator.IsLocked(ctx, msg))

	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	assert.Equal(t, "redirect_orders", call.Topic)

	headers := call.Msg.Headers().All()
	assert.Contains(t, headers, "key")
	assert.Equal(t, []byte("order-123"), headers["key"])
	assert.Contains(t, headers, HeaderTopic)
	assert.Equal(t, []byte("orders"), headers[HeaderTopic])
	assert.Contains(t, headers, HeaderCoordinatorID)
	assert.Equal(t, []byte(coordinator.instanceID), headers[HeaderCoordinatorID])
}

func TestKafkaStateCoordinator_Release(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}
	mockLogger := &LoggerMock{
		DebugFunc: func(_ string, _ ...any) {},
	}

	cfg := &Config{
		RedirectTopicPrefix: "redirect",
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		mockLogger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		make(chan error, 1),
	)

	ctx := t.Context()
	msg := &InternalMessage{
		topic:      "orders",
		KeyData:    []byte("order-123"),
		HeaderData: &HeaderList{},
	}

	// Acquire first (sets up required headers: HeaderID, HeaderTopic)
	err := coordinator.Acquire(ctx, "orders", msg)
	require.NoError(t, err)
	assert.True(t, coordinator.IsLocked(ctx, msg))

	err = coordinator.Release(ctx, msg)
	require.NoError(t, err)

	// optimistic release: should be unlocked immediately
	assert.False(t, coordinator.IsLocked(ctx, msg))

	// 2 produce calls: 1 for acquire, 1 for release (tombstone)
	assert.Len(t, mockProducer.ProduceCalls(), 2)
	releaseCall := mockProducer.ProduceCalls()[1]

	headers := releaseCall.Msg.Headers().All()
	assert.Contains(t, headers, HeaderCoordinatorID)
	assert.Equal(t, []byte(coordinator.instanceID), headers[HeaderCoordinatorID])
	// Verify tombstone (nil payload)
	assert.Nil(t, releaseCall.Msg.Value())
}

func TestKafkaStateCoordinator_Start_RestoresState(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10},
				},
			}, nil
		},
		CreateTopicFunc: func(_ context.Context, _ string, _ int32, _ int16, _ map[string]string) error {
			return nil
		},
		DeleteConsumerGroupFunc: func(_ context.Context, _ string) error {
			return nil
		},
	}

	restoreConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, handler ConsumerHandler) error {
			// Simulate reading a lock message from redirect topic
			// NO CoordinatorID header -> simulates legacy message or other instance
			msg := &InternalMessage{
				topic:      "redirect_orders",
				KeyData:    []byte("order-locked"),
				Payload:    []byte("order-locked"),
				HeaderData: &HeaderList{},
			}
			msg.HeaderData.Set("topic", []byte("orders"))
			msg.HeaderData.Set("key", []byte("order-locked"))

			msg.SetPartition(0)
			msg.SetOffset(9) // Last offset (target is 10)
			_ = handler.Handle(ctx, msg)

			return nil
		},
		CloseFunc: func() error { return nil },
	}

	liveConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, _ ConsumerHandler) error {
			<-ctx.Done()
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	mockFactory := &ConsumerFactoryMock{}
	callCount := 0
	mockFactory.NewConsumerFunc = func(_ string) (Consumer, error) {
		callCount++
		if callCount == 1 {
			return restoreConsumer, nil
		}

		return liveConsumer, nil
	}

	cfg := &Config{
		RedirectTopicPrefix:   "redirect",
		StateRestoreTimeoutMs: 100,
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		&LoggerMock{DebugFunc: func(_ string, _ ...any) {}, InfoFunc: func(_ string, _ ...any) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		make(chan error, 1),
	)

	err := coordinator.Start(t.Context(), "orders")
	require.NoError(t, err)

	// verify state was restored
	msg := &InternalMessage{
		topic:      "orders",
		KeyData:    []byte("order-locked"),
		HeaderData: &HeaderList{},
	}
	assert.True(t, coordinator.IsLocked(t.Context(), msg))
}

func TestKafkaStateCoordinator_Acquire_ProducerError(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka error")
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			WarnFunc: func(_ string, _ ...any) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		make(chan error),
	)

	err := coordinator.Acquire(t.Context(), "t", &InternalMessage{topic: "t", KeyData: []byte("k"), HeaderData: &HeaderList{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka error")

	// Verify Rollback: Should NOT be locked
	msg := &InternalMessage{topic: "t", KeyData: []byte("k"), HeaderData: &HeaderList{}}
	assert.False(t, coordinator.IsLocked(t.Context(), msg))
}

func TestKafkaStateCoordinator_ProcessRedirect_Filter(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// 1. Simulate "Echo" message (Same ID)
	echoMsg := &InternalMessage{
		topic:      "redirect_orders",
		KeyData:    []byte("k1"),
		Payload:    []byte("k1"),
		HeaderData: &HeaderList{},
	}
	echoMsg.HeaderData.Set(HeaderCoordinatorID, []byte(coordinator.instanceID))
	echoMsg.HeaderData.Set(HeaderTopic, []byte("orders"))
	echoMsg.HeaderData.Set("key", []byte("k1"))

	// We call Acquire first to set local ref count to 1 (simulating the source of the echo)
	_ = coordinator.local.Acquire(t.Context(), "orders", &InternalMessage{topic: "orders", KeyData: []byte("k1"), HeaderData: &HeaderList{}})

	// Process the echo message
	err := coordinator.processRedirectMessage(t.Context(), echoMsg)
	require.NoError(t, err)

	// Ref count should still be 1 (Not incremented to 2)
	count, _ := coordinator.local.lm.getRefCount("orders", "k1")
	assert.Equal(t, 1, count)

	// 2. Simulate "Foreign" message (Different ID)
	foreignMsg := &InternalMessage{
		topic:      "redirect_orders",
		KeyData:    []byte("k1"),
		Payload:    []byte("k1"),
		HeaderData: &HeaderList{},
	}
	foreignMsg.HeaderData.Set(HeaderCoordinatorID, []byte("other-uuid"))
	foreignMsg.HeaderData.Set(HeaderTopic, []byte("orders"))
	foreignMsg.HeaderData.Set("key", []byte("k1"))

	err = coordinator.processRedirectMessage(t.Context(), foreignMsg)
	require.NoError(t, err)

	// Ref count should now be 2
	count, _ = coordinator.local.lm.getRefCount("orders", "k1")
	assert.Equal(t, 2, count)
}

func TestKafkaStateCoordinator_ForeignTombstone(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// Simulate we have a lock locally (maybe restored or acquired)
	_ = coordinator.local.Acquire(t.Context(), "orders", &InternalMessage{topic: "orders", KeyData: []byte("key1"), HeaderData: &HeaderList{}})
	msg := &InternalMessage{topic: "orders", KeyData: []byte("key1"), HeaderData: &HeaderList{}}
	assert.True(t, coordinator.IsLocked(t.Context(), msg))

	// Receive a Tombstone from a different coordinator (Failover scenario)
	tombstone := createMockRedirectMsg("orders", "key1", "other-instance", false)

	err := coordinator.processRedirectMessage(t.Context(), tombstone)
	require.NoError(t, err)

	// Should be unlocked
	assert.False(t, coordinator.IsLocked(t.Context(), msg))
}

func TestKafkaStateCoordinator_Rebalance_Simulation(t *testing.T) {
	// Scenario:
	// 1. Instance A locks "order-1" (persisted to Redirect Topic).
	// 2. Instance A crashes (or rebalance happens).
	// 3. Instance B starts up, assigned the same partition.
	// 4. Instance B consumes the Redirect Topic and must restore the lock locally.
	topic := testTopicOrders
	key := "order-1"

	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 5}, // HWM is 5
				},
			}, nil
		},
		CreateTopicFunc: func(_ context.Context, _ string, _ int32, _ int16, _ map[string]string) error {
			return nil
		},
		DeleteConsumerGroupFunc: func(_ context.Context, _ string) error {
			return nil
		},
	}

	// this consumer will be used by restoreState
	restoreConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, handler ConsumerHandler) error {
			// Simulate the existing lock message on the topic
			msg := &InternalMessage{
				topic:      "redirect_orders",
				KeyData:    []byte(key),
				Payload:    []byte(key), // Payload exists = Locked
				HeaderData: &HeaderList{},
			}
			msg.HeaderData.Set(HeaderTopic, []byte(topic))
			msg.HeaderData.Set(HeaderKey, []byte(key))
			// Note: Different coordinator ID, simulating Instance A
			msg.HeaderData.Set(HeaderCoordinatorID, []byte("instance-A"))

			msg.SetPartition(0)
			msg.SetOffset(4) // < HWM (5)

			if err := handler.Handle(ctx, msg); err != nil {
				return err
			}

			return nil
		},
		CloseFunc: func() error { return nil },
	}

	// Live consumer (post-restore)
	liveConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, _ ConsumerHandler) error {
			<-ctx.Done()
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	mockFactory := &ConsumerFactoryMock{
		NewConsumerFunc: func(_ string) (Consumer, error) {
			// First call is for restore
			return restoreConsumer, nil
		},
	}
	// We need to patch the factory for the second call (live consumer) inside the test flow or use a counter
	callCount := 0
	mockFactory.NewConsumerFunc = func(_ string) (Consumer, error) {
		callCount++
		if callCount == 1 {
			return restoreConsumer, nil
		}

		return liveConsumer, nil
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect", StateRestoreTimeoutMs: 100},
		&LoggerMock{DebugFunc: func(_ string, _ ...any) {}, InfoFunc: func(_ string, _ ...any) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		make(chan error, 1),
	)

	// Start Instance B
	err := coordinator.Start(t.Context(), topic)
	require.NoError(t, err)

	// Verify Instance B has the lock
	checkMsg := &InternalMessage{topic: topic, KeyData: []byte(key), HeaderData: &HeaderList{}}
	assert.True(t, coordinator.IsLocked(t.Context(), checkMsg), "Instance B should have restored the lock")
}

const testTopicOrders = "orders"

func TestKafkaStateCoordinator_Synchronize_TopicNotStarted(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// should fail because Start() wasn't called (topic is empty)
	err := coordinator.Synchronize(t.Context())
	require.ErrorContains(t, err, "coordinator not started")
}

func TestKafkaStateCoordinator_Synchronize_EmptyRedirectTopic(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			// return metadata indicating empty topic
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{}, // no offsets
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	// manually set topic as if Start() was called
	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	coordinator.mu.Unlock()

	// should return immediately
	err := coordinator.Synchronize(t.Context())
	assert.NoError(t, err)
}

func TestKafkaStateCoordinator_Synchronize_AlreadyCaughtUp(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10}, // HWM = 10
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	// simulate that we have already consumed up to offset 9 (HWM 10 means next is 10, so 9 is the last one)
	coordinator.consumedOffsets[0] = 9
	coordinator.mu.Unlock()

	err := coordinator.Synchronize(t.Context())
	assert.NoError(t, err)
}

func TestKafkaStateCoordinator_Synchronize_BlocksUntilCaughtUp(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10}, // HWM = 10
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			DebugFunc: func(_ string, _ ...any) {
				// no-op
			},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	coordinator.consumedOffsets[0] = 5 // lagging behind (5 < 9)
	coordinator.mu.Unlock()

	// use a channel to signal when Synchronize returns
	done := make(chan error)

	go func() {
		done <- coordinator.Synchronize(t.Context())
	}()

	// ensure it blocks initially
	select {
	case <-done:
		t.Fatal("Synchronize should have blocked")
	case <-time.After(100 * time.Millisecond):
		// expected behavior
	}

	// simulate background consumer updating the offset
	coordinator.mu.Lock()
	coordinator.consumedOffsets[0] = 9  // catch up
	coordinator.offsetsCond.Broadcast() // signal that offsets updated
	coordinator.mu.Unlock()

	// now it should complete
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Synchronize failed to return after catching up")
	}
}

func TestKafkaStateCoordinator_Synchronize_ContextCancellation(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(_ context.Context, _ []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 100}, // HWM = 100
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			DebugFunc: func(_ string, _ ...any) {
				// no-op
			},
		},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = testTopicOrders
	coordinator.consumedOffsets[0] = 5 // lagging
	coordinator.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := coordinator.Synchronize(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	// let it block for a bit
	time.Sleep(50 * time.Millisecond)
	cancel()

	wg.Wait()
}

// Helper to create mock messages
func createMockRedirectMsg(topic, key, coordinatorID string, isLock bool) *InternalMessage {
	var value []byte
	if isLock {
		value = []byte(key)
	} else {
		value = nil
	}

	hl := &HeaderList{}
	hl.Set(HeaderCoordinatorID, []byte(coordinatorID))
	hl.Set(HeaderTopic, []byte(topic))
	hl.Set("key", []byte(key))

	return &InternalMessage{
		topic:      "redirect_" + topic,
		KeyData:    []byte(key),
		Payload:    value,
		HeaderData: hl,
	}
}

type mockTopicMetadata struct {
	name       string
	partitions int32
	offsets    map[int32]int64
}

func (m *mockTopicMetadata) Name() string                      { return m.name }
func (m *mockTopicMetadata) Partitions() int32                 { return m.partitions }
func (m *mockTopicMetadata) PartitionOffsets() map[int32]int64 { return m.offsets }
