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

	// We need a dummy Admin and Factory because NewKafkaStateCoordinator requires them
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

	// Verify Producer was called correctly
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	// Verify Topic
	assert.Equal(t, "redirect_orders", call.Topic)

	// Verify Message content
	producedMsg := call.Msg
	assert.Equal(t, []byte("order-123"), producedMsg.Key())
	assert.Equal(t, []byte("order-123"), producedMsg.Value()) // Value is ID (default to Key)

	// Verify Headers
	headers := producedMsg.Headers().All()
	assert.Contains(t, headers, "key")
	assert.Equal(t, []byte("order-123"), headers["key"])
	assert.Contains(t, headers, "topic")
	assert.Equal(t, []byte("orders"), headers["topic"])
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

	// Test Release
	err := coordinator.Release(ctx, msg)
	assert.NoError(t, err)

	// Verify Producer was called
	// 1 for Release
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	// Verify Topic
	assert.Equal(t, "redirect_orders", call.Topic)

	// Verify Tombstone (Value should be nil)
	assert.Nil(t, call.Msg.Value())
	assert.Equal(t, []byte("order-123"), call.Msg.Key())
}

func TestKafkaStateCoordinator_Start_RestoresState(t *testing.T) {
	// Setup complex interaction for Start()

	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(ctx context.Context, topics []string) ([]TopicMetadata, error) {
			return []TopicMetadata{}, nil // Default fallback
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

	// Mock Consumer for Live Tracking
	liveConsumer := &ConsumerMock{
		ConsumeFunc: func(ctx context.Context, topics []string, handler ConsumerHandler) error {
			<-ctx.Done() // Block until context cancel
			return nil
		},
		CloseFunc: func() error { return nil },
	}

	mockFactory := &ConsumerFactoryMock{
		NewConsumerFunc: func(groupID string) (Consumer, error) {
			return nil, nil // placeholder, overwritten below
		},
	}

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

	// Verify State was restored by checking if key is locked
	msg := &InternalMessage{
		topic: "orders",
		Key:   []byte("order-locked"),
	}
	assert.True(t, coordinator.IsLocked(context.Background(), msg))

	// Verify topics were ensured
	assert.NotEmpty(t, mockAdmin.CreateTopicCalls())
	assert.Equal(t, "redirect_orders", mockAdmin.CreateTopicCalls()[0].Name)
}

func TestKafkaStateCoordinator_Acquire_ProducerError(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return errors.New("kafka error")
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		make(chan error),
	)

	err := coordinator.Acquire(context.Background(), &InternalMessage{topic: "t", Key: []byte("k")}, "t")
	assert.Error(t, err)
	assert.Equal(t, "kafka error", err.Error())
}
