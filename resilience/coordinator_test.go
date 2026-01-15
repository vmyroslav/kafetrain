package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafkaStateCoordinator_Acquire(t *testing.T) {
	// Setup Mocks
	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return nil
		},
	}

	mockAdmin := &AdminMock{}
	mockFactory := &ConsumerFactoryMock{}
	mockLogger := &LoggerMock{
		DebugFunc: func(msg string, fields ...interface{}) {},
		ErrorFunc: func(msg string, fields ...interface{}) {},
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
		topic: "orders",
		Key:   []byte("order-123"),
		Headers: HeaderList{
			{Key: []byte("custom"), Value: []byte("val")},
		},
	}

	// Test Acquire
	err := coordinator.Acquire(ctx, msg, "orders")
	assert.NoError(t, err)

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
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return nil
		},
	}
	mockLogger := &LoggerMock{
		DebugFunc: func(msg string, fields ...interface{}) {},
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
		topic: "orders",
		Key:   []byte("order-123"),
	}

	// Manually lock it first to test release
	_ = coordinator.local.Acquire(ctx, msg, "orders")
	assert.True(t, coordinator.IsLocked(ctx, msg))

	// Test Release
	err := coordinator.Release(ctx, msg)
	assert.NoError(t, err)

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
		DescribeTopicsFunc: func(ctx context.Context, topics []string) ([]TopicMetadata, error) {
			return []TopicMetadata{}, nil
		},
		CreateTopicFunc: func(ctx context.Context, name string, partitions int32, replicationFactor int16, config map[string]string) error {
			return nil
		},
		DeleteConsumerGroupFunc: func(ctx context.Context, groupID string) error {
			return nil
		},
	}

	// Mock Consumer for Restoration
	restoreConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, topics []string, handler ConsumerHandler) error {
			// Simulate reading a lock message from redirect topic
			// NO CoordinatorID header -> simulates legacy message or other instance
			msg := &messageWrapper{
				topic: "redirect_orders",
				key:   []byte("order-locked"),
				value: []byte("order-locked"),
				headers: &headerListWrapper{
					headers: HeaderList{
						{Key: []byte("topic"), Value: []byte("orders")},
						{Key: []byte("key"), Value: []byte("order-locked")},
					},
				},
			}
			handler.Handle(ctx, msg)
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	liveConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, topics []string, handler ConsumerHandler) error {
			<-ctx.Done()
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	mockFactory := &ConsumerFactoryMock{}
	callCount := 0
	mockFactory.NewConsumerFunc = func(groupID string) (Consumer, error) {
		callCount++
		if callCount == 1 {
			return restoreConsumer, nil
		}
		return liveConsumer, nil
	}

	cfg := &Config{
		RedirectTopicPrefix:       "redirect",
		StateRestoreTimeoutMs:     100,
		StateRestoreIdleTimeoutMs: 50,
	}

	coordinator := NewKafkaStateCoordinator(
		cfg,
		&LoggerMock{DebugFunc: func(s string, i ...interface{}) {}, InfoFunc: func(s string, i ...interface{}) {}},
		&ProducerMock{},
		mockFactory,
		mockAdmin,
		make(chan error, 1),
	)

	// Test Start
	err := coordinator.Start(context.Background(), "orders")
	assert.NoError(t, err)

	// Verify State was restored
	msg := &InternalMessage{
		topic: "orders",
		Key:   []byte("order-locked"),
	}
	assert.True(t, coordinator.IsLocked(context.Background(), msg))
}

func TestKafkaStateCoordinator_Acquire_ProducerError(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return errors.New("kafka error")
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{
			WarnFunc: func(msg string, fields ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		make(chan error),
	)

	err := coordinator.Acquire(context.Background(), &InternalMessage{topic: "t", Key: []byte("k")}, "t")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka error")

	// Verify Rollback: Should NOT be locked
	msg := &InternalMessage{topic: "t", Key: []byte("k")}
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
	echoMsg := &messageWrapper{
		topic: "redirect_orders",
		key:   []byte("k1"),
		value: []byte("k1"),
		headers: &headerListWrapper{
			headers: HeaderList{
				{Key: []byte(HeaderCoordinatorID), Value: []byte(coordinator.instanceID)},
				{Key: []byte(HeaderTopic), Value: []byte("orders")},
				{Key: []byte("key"), Value: []byte("k1")},
			},
		},
	}

	// We call Acquire first to set local ref count to 1 (simulating the source of the echo)
	_ = coordinator.local.Acquire(context.Background(), &InternalMessage{topic: "orders", Key: []byte("k1")}, "orders")

	// Process the echo message
	err := coordinator.processRedirectMessage(context.Background(), echoMsg)
	assert.NoError(t, err)

	// Ref count should still be 1 (Not incremented to 2)
	count, _ := coordinator.local.lm.getRefCount("orders", "k1")
	assert.Equal(t, 1, count)

	// 2. Simulate "Foreign" message (Different ID)
	foreignMsg := &messageWrapper{
		topic: "redirect_orders",
		key:   []byte("k1"),
		value: []byte("k1"),
		headers: &headerListWrapper{
			headers: HeaderList{
				{Key: []byte(HeaderCoordinatorID), Value: []byte("other-uuid")},
				{Key: []byte(HeaderTopic), Value: []byte("orders")},
				{Key: []byte("key"), Value: []byte("k1")},
			},
		},
	}

	err = coordinator.processRedirectMessage(context.Background(), foreignMsg)
	assert.NoError(t, err)

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
	_ = coordinator.local.Acquire(context.Background(), &InternalMessage{topic: "orders", Key: []byte("key1")}, "orders")
	msg := &InternalMessage{topic: "orders", Key: []byte("key1")}
	assert.True(t, coordinator.IsLocked(context.Background(), msg))

	// Receive a Tombstone from a different coordinator (Failover scenario)
	tombstone := createMockRedirectMsg("orders", "key1", "other-instance", false)

	err := coordinator.processRedirectMessage(context.Background(), tombstone)
	assert.NoError(t, err)

	// Should be unlocked
	assert.False(t, coordinator.IsLocked(context.Background(), msg))
}

// Helper to create mock messages
func createMockRedirectMsg(topic, key, coordinatorID string, isLock bool) *messageWrapper {
	var value []byte
	if isLock {
		value = []byte(key)
	} else {
		value = nil
	}

	return &messageWrapper{
		topic: "redirect_" + topic,
		key:   []byte(key),
		value: value,
		headers: &headerListWrapper{
			headers: HeaderList{
				{Key: []byte(HeaderCoordinatorID), Value: []byte(coordinatorID)},
				{Key: []byte(HeaderTopic), Value: []byte(topic)},
				{Key: []byte("key"), Value: []byte(key)},
			},
		},
	}
}
