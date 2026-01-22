package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create mock messages
func createMockRedirectMsg(topic, key, coordinatorID string, isLock bool) *InternalMessage {
	var value []byte
	if isLock {
		value = []byte(key)
	} else {
		value = nil
	}

	hl := HeaderList{}
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

func TestKafkaStateCoordinator_Acquire(t *testing.T) {
	// Setup Mocks
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockAdmin := &AdminMock{}
	mockFactory := &ConsumerFactoryMock{}
	mockLogger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
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

	ctx := context.Background()
	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-123"),
	}
	msg.HeaderData.Set("custom", []byte("val"))

	// Test Acquire
	err := coordinator.Acquire(ctx, msg, "orders")
	require.NoError(t, err)

	// Optimistic Locking: Should be locked immediately
	assert.True(t, coordinator.IsLocked(ctx, msg))

	// Verify Producer was called correctly
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	// Verify Topic
	assert.Equal(t, "redirect_orders", call.Topic)

	// Verify Headers
	headers := call.Msg.Headers().All()
	assert.Contains(t, headers, "key")
	assert.Equal(t, []byte("order-123"), headers["key"])
	assert.Contains(t, headers, HeaderTopic)
	assert.Equal(t, []byte("orders"), headers[HeaderTopic])

	// Verify CoordinatorID Header
	assert.Contains(t, headers, HeaderCoordinatorID)
	assert.Equal(t, []byte(coordinator.instanceID), headers[HeaderCoordinatorID])
}

func TestKafkaStateCoordinator_Release(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}
	mockLogger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
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

	ctx := context.Background()
	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-123"),
	}

	// Manually lock it first to test release
	_ = coordinator.local.Acquire(ctx, msg, "orders")
	assert.True(t, coordinator.IsLocked(ctx, msg))

	// Test Release
	err := coordinator.Release(ctx, msg)
	require.NoError(t, err)

	// Optimistic Release: Should be unlocked immediately
	assert.False(t, coordinator.IsLocked(ctx, msg))

	// Verify Producer was called
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	// Verify CoordinatorID Header on Tombstone
	headers := call.Msg.Headers().All()
	assert.Contains(t, headers, HeaderCoordinatorID)
	assert.Equal(t, []byte(coordinator.instanceID), headers[HeaderCoordinatorID])
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

	// Mock Consumer for Restoration
	restoreConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, _ []string, handler ConsumerHandler) error {
			// Simulate reading a lock message from redirect topic
			// NO CoordinatorID header -> simulates legacy message or other instance
			msg := &InternalMessage{
				topic:   "redirect_orders",
				KeyData: []byte("order-locked"),
				Payload: []byte("order-locked"),
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
		&LoggerMock{DebugFunc: func(_ string, _ ...interface{}) {}, InfoFunc: func(_ string, _ ...interface{}) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		make(chan error, 1),
	)

	// Test Start
	err := coordinator.Start(context.Background(), "orders")
	require.NoError(t, err)

	// Verify State was restored
	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-locked"),
	}
	assert.True(t, coordinator.IsLocked(context.Background(), msg))
}

func TestKafkaStateCoordinator_Acquire_ProducerError(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka error")
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			WarnFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		make(chan error),
	)

	err := coordinator.Acquire(context.Background(), &InternalMessage{topic: "t", KeyData: []byte("k")}, "t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka error")

	// Verify Rollback: Should NOT be locked
	msg := &InternalMessage{topic: "t", KeyData: []byte("k")}
	assert.False(t, coordinator.IsLocked(context.Background(), msg))
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
		topic:   "redirect_orders",
		KeyData: []byte("k1"),
		Payload: []byte("k1"),
	}
	echoMsg.HeaderData.Set(HeaderCoordinatorID, []byte(coordinator.instanceID))
	echoMsg.HeaderData.Set(HeaderTopic, []byte("orders"))
	echoMsg.HeaderData.Set("key", []byte("k1"))

	// We call Acquire first to set local ref count to 1 (simulating the source of the echo)
	_ = coordinator.local.Acquire(context.Background(), &InternalMessage{topic: "orders", KeyData: []byte("k1")}, "orders")

	// Process the echo message
	err := coordinator.processRedirectMessage(context.Background(), echoMsg)
	require.NoError(t, err)

	// Ref count should still be 1 (Not incremented to 2)
	count, _ := coordinator.local.lm.getRefCount("orders", "k1")
	assert.Equal(t, 1, count)

	// 2. Simulate "Foreign" message (Different ID)
	foreignMsg := &InternalMessage{
		topic:   "redirect_orders",
		KeyData: []byte("k1"),
		Payload: []byte("k1"),
	}
	foreignMsg.HeaderData.Set(HeaderCoordinatorID, []byte("other-uuid"))
	foreignMsg.HeaderData.Set(HeaderTopic, []byte("orders"))
	foreignMsg.HeaderData.Set("key", []byte("k1"))

	err = coordinator.processRedirectMessage(context.Background(), foreignMsg)
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
	_ = coordinator.local.Acquire(context.Background(), &InternalMessage{topic: "orders", KeyData: []byte("key1")}, "orders")
	msg := &InternalMessage{topic: "orders", KeyData: []byte("key1")}
	assert.True(t, coordinator.IsLocked(context.Background(), msg))

	// Receive a Tombstone from a different coordinator (Failover scenario)
	tombstone := createMockRedirectMsg("orders", "key1", "other-instance", false)

	err := coordinator.processRedirectMessage(context.Background(), tombstone)
	require.NoError(t, err)

	// Should be unlocked
	assert.False(t, coordinator.IsLocked(context.Background(), msg))
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
				topic:   "redirect_orders",
				KeyData: []byte(key),
				Payload: []byte(key), // Payload exists = Locked
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
		&LoggerMock{DebugFunc: func(_ string, _ ...interface{}) {}, InfoFunc: func(_ string, _ ...interface{}) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		make(chan error, 1),
	)

	// Start Instance B
	err := coordinator.Start(context.Background(), topic)
	require.NoError(t, err)

	// Verify Instance B has the lock
	checkMsg := &InternalMessage{topic: topic, KeyData: []byte(key)}
	assert.True(t, coordinator.IsLocked(context.Background(), checkMsg), "Instance B should have restored the lock")
}
