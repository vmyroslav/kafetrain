package resilience

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProducerFailure_Rollback verifies that if the producer fails to publish
// the retry message, the lock acquired in the coordinator is rolled back.
func TestProducerFailure_Rollback(t *testing.T) {
	// producer fails on Produce
	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	// Acquire succeeds, Release MUST be called for rollback
	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ *InternalMessage, _ string) error {
			return nil
		},
		ReleaseFunc: func(_ context.Context, _ *InternalMessage) error {
			return nil
		},
		StartFunc: func(_ context.Context, _ string) error {
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
	}

	// mockBackoff to avoid waiting
	backoff := &mockBackoff{}

	cfg := NewDefaultConfig()
	cfg.GroupID = testGroupID
	cfg.MaxRetries = 5

	tracker, err := NewErrorTracker(
		cfg,
		logger,
		mockProducer,
		&ConsumerFactoryMock{},
		&AdminMock{},
		mockCoordinator,
		backoff,
	)
	require.NoError(t, err)

	msg := &InternalMessage{
		topic:      "orders",
		KeyData:    []byte("order-1"),
		HeaderData: &HeaderList{},
	}

	// simulating a failure
	// a. Acquire Lock (Success)
	// b. Produce to Retry Topic (Fail)
	// c. Release Lock (Rollback)
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka produce error")

	assert.Len(t, mockCoordinator.AcquireCalls(), 1, "Acquire should have been called once")
	assert.Len(t, mockCoordinator.ReleaseCalls(), 1, "Release should have been called once to rollback")

	// ensure Release was called for the correct key
	releaseCall := mockCoordinator.ReleaseCalls()[0]
	assert.Equal(t, "orders", releaseCall.Msg.topic)
	assert.Equal(t, []byte("order-1"), releaseCall.Msg.KeyData)
}
