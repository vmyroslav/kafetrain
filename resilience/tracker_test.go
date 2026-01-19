package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockBackoff struct{}

func (m *mockBackoff) NextDelay(attempt int) time.Duration {
	return time.Millisecond
}

func TestErrorTracker_Redirect_Rollback(t *testing.T) {
	// 1. Setup
	// Producer fails on Produce (simulating write failure to Retry Topic)
	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return errors.New("kafka produce error")
		},
	}

	// Coordinator mocks: Acquire succeeds, Release MUST be called
	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(ctx context.Context, msg *InternalMessage, originalTopic string) error {
			return nil
		},
		ReleaseFunc: func(ctx context.Context, msg *InternalMessage) error {
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(msg string, fields ...interface{}) {},
		InfoFunc:  func(msg string, fields ...interface{}) {},
		ErrorFunc: func(msg string, fields ...interface{}) {},
	}

	cfg := &Config{
		MaxRetries:       5,
		RetryTopicPrefix: "retry",
	}

	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
	)
	assert.NoError(t, err)

	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-1"),
	}

	// 2. Execute Redirect
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	// 3. Verify Result
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka produce error")

	// 4. Verify Rollback (Release called)
	assert.Len(t, mockCoordinator.ReleaseCalls(), 1, "Release should be called to rollback lock")
	assert.Len(t, mockCoordinator.AcquireCalls(), 1, "Acquire should have been called first")
}

func TestErrorTracker_Redirect_HappyPath(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(ctx context.Context, topic string, msg Message) error {
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(ctx context.Context, msg *InternalMessage, originalTopic string) error {
			return nil
		},
	}

	tracker, _ := NewErrorTracker(
		&Config{MaxRetries: 5, RetryTopicPrefix: "retry"},
		&LoggerMock{
			DebugFunc: func(s string, i ...interface{}) {},
		},
		mockProducer,
		nil,
		nil,
		mockCoordinator,
		&mockBackoff{},
	)

	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-1"),
	}

	err := tracker.Redirect(context.Background(), msg, errors.New("fail"))
	assert.NoError(t, err)

	assert.Len(t, mockCoordinator.AcquireCalls(), 1)
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	assert.Equal(t, "retry_orders", mockProducer.ProduceCalls()[0].Topic)
}
