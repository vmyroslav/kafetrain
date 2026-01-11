package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafkaStateCoordinator_Acquire(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return nil
		},
	}
	mockTracker := &MessageChainTrackerMock{
		IsRelatedFunc: func(ctx context.Context, msg *InternalMessage) bool {
			return false
		},
	}

	mockAdmin := &AdminMock{}
	mockFactory := &ConsumerFactoryMock{}
	mockLogger := &LoggerMock{
		DebugFunc: func(msg string, fields ...any) {},
		ErrorFunc: func(msg string, fields ...any) {},
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
		mockTracker,
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

	err := coordinator.Acquire(ctx, msg, "orders")
	assert.NoError(t, err)

	// verify Producer was called correctly
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	// verify Topic
	assert.Equal(t, "redirect_orders", call.Topic)

	// verify Message content
	producedMsg := call.Msg
	assert.Equal(t, []byte("order-123"), producedMsg.Key())
	assert.Equal(t, []byte("order-123"), producedMsg.Value()) // Value is ID (default to Key)

	// verify Headers
	headers := producedMsg.Headers().All()
	assert.Contains(t, headers, "key")
	assert.Equal(t, []byte("order-123"), headers["key"])
	assert.Contains(t, headers, "topic")
	assert.Equal(t, []byte("orders"), headers["topic"])
}

func TestKafkaStateCoordinator_Release(t *testing.T) {
	t.Parallel()

	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return nil
		},
	}
	mockLogger := &LoggerMock{
		DebugFunc: func(msg string, fields ...any) {},
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
		&MessageChainTrackerMock{},
		make(chan error, 1),
	)

	ctx := context.Background()
	msg := &InternalMessage{
		topic: "orders",
		Key:   []byte("order-123"),
	}

	err := coordinator.Release(ctx, msg)
	assert.NoError(t, err)

	// verify Producer was called
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	call := mockProducer.ProduceCalls()[0]

	// verify Topic
	assert.Equal(t, "redirect_orders", call.Topic)

	// verify Tombstone (Value should be nil)
	assert.Nil(t, call.Msg.Value())
	assert.Equal(t, []byte("order-123"), call.Msg.Key())
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
		&MessageChainTrackerMock{},
		make(chan error),
	)

	err := coordinator.Acquire(context.Background(), &InternalMessage{topic: "t", Key: []byte("k")}, "t")
	assert.Error(t, err)
	assert.Equal(t, "kafka error", err.Error())
}
