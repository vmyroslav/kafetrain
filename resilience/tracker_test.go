package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBackoff struct{}

func (m *mockBackoff) NextDelay(_ int) time.Duration {
	return time.Millisecond
}

// newTestConfig creates a valid config for tests
func newTestConfig() *Config {
	cfg := NewDefaultConfig()
	cfg.GroupID = testGroupID

	return cfg
}

func TestErrorTracker_Redirect_Rollback(t *testing.T) {
	// 1. Setup
	// Producer fails on Produce (simulating write failure to Retry Topic)
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	// Coordinator mocks: Acquire succeeds, Release MUST be called
	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ *InternalMessage, _ string) error {
			return nil
		},
		ReleaseFunc: func(_ context.Context, _ *InternalMessage) error {
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
	)
	require.NoError(t, err)

	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-1"),
	}

	// 2. Execute Redirect
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	// 3. Verify Result
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka produce error")

	// 4. Verify Rollback (Release called)
	assert.Len(t, mockCoordinator.ReleaseCalls(), 1, "Release should be called to rollback lock")
	assert.Len(t, mockCoordinator.AcquireCalls(), 1, "Acquire should have been called first")
}

func TestErrorTracker_Redirect_HappyPath(t *testing.T) {
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ *InternalMessage, _ string) error {
			return nil
		},
	}

	cfg := newTestConfig()
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		&LoggerMock{
			DebugFunc: func(_ string, _ ...interface{}) {},
		},
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		&mockBackoff{},
	)
	require.NoError(t, err)

	msg := &InternalMessage{
		topic:   "orders",
		KeyData: []byte("order-1"),
	}

	err = tracker.Redirect(context.Background(), msg, errors.New("fail"))
	require.NoError(t, err)

	assert.Len(t, mockCoordinator.AcquireCalls(), 1)
	assert.Len(t, mockProducer.ProduceCalls(), 1)
	assert.Equal(t, "retry_orders", mockProducer.ProduceCalls()[0].Topic)
}
