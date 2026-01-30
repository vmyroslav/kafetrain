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

func TestErrorTracker_Redirect_RollbackFailure_LogsCriticalError(t *testing.T) {
	// Test zombie key scenario: producer fails AND rollback fails
	// This is a critical error that should be logged
	var (
		criticalErrorLogged bool
		loggedMessage       string
	)

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	// Coordinator: Acquire succeeds, but Release ALSO fails (worst case)
	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ *InternalMessage, _ string) error {
			return nil
		},
		ReleaseFunc: func(_ context.Context, _ *InternalMessage) error {
			return errors.New("release failed - kafka unavailable")
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
		WarnFunc:  func(_ string, _ ...interface{}) {},
		ErrorFunc: func(msg string, _ ...interface{}) {
			if msg == "CRITICAL: failed to rollback lock after retry publish failure. Key may be permanently locked!" {
				criticalErrorLogged = true
				loggedMessage = msg
			}
		},
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
		KeyData: []byte("zombie-key"),
	}

	// Execute Redirect - both produce and rollback will fail
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))

	// Should return the original produce error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka produce error")

	// Critical error should be logged about potential zombie key
	assert.True(t, criticalErrorLogged, "Critical error should be logged when rollback fails. Got: %s", loggedMessage)

	// Verify both Acquire and Release were attempted
	assert.Len(t, mockCoordinator.AcquireCalls(), 1)
	assert.GreaterOrEqual(t, len(mockCoordinator.ReleaseCalls()), 1, "Release should be attempted for rollback")
}

func TestErrorTracker_Redirect_RollbackSuccess_KeyNotLocked(t *testing.T) {
	// After successful rollback, the key should NOT be locked
	// This verifies the compensating transaction works correctly
	lockState := make(map[string]bool) // track lock state

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, _ string, _ Message) error {
			return errors.New("kafka produce error")
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, msg *InternalMessage, _ string) error {
			lockState[string(msg.KeyData)] = true
			return nil
		},
		ReleaseFunc: func(_ context.Context, msg *InternalMessage) error {
			lockState[string(msg.KeyData)] = false
			return nil
		},
		IsLockedFunc: func(_ context.Context, msg *InternalMessage) bool {
			return lockState[string(msg.KeyData)]
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		ErrorFunc: func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
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
		KeyData: []byte("order-123"),
	}

	// Before redirect: key is not locked
	assert.False(t, tracker.IsInRetryChain(context.Background(), msg))

	// Redirect fails (producer error)
	err = tracker.Redirect(context.Background(), msg, errors.New("business error"))
	require.Error(t, err)

	// After failed redirect with successful rollback: key should NOT be locked
	assert.False(t, tracker.IsInRetryChain(context.Background(), msg),
		"Key should not be locked after successful rollback")
}

func TestErrorTracker_NotRetriableError_GoesDirectlyToDLQ(t *testing.T) {
	// NotRetriableError should bypass retry topic and go directly to DLQ
	// It should NOT acquire a lock
	var producedTopics []string

	mockProducer := &ProducerMock{
		ProduceFunc: func(_ context.Context, topic string, _ Message) error {
			producedTopics = append(producedTopics, topic)
			return nil
		},
	}

	mockCoordinator := &StateCoordinatorMock{
		AcquireFunc: func(_ context.Context, _ *InternalMessage, _ string) error {
			t.Error("Acquire should NOT be called for NotRetriableError")
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		WarnFunc:  func(_ string, _ ...interface{}) {},
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
		KeyData: []byte("invalid-order"),
	}

	// Redirect with NotRetriableError
	notRetriableErr := NewNotRetriableError(errors.New("invalid order format - permanent failure"))
	err = tracker.Redirect(context.Background(), msg, notRetriableErr)

	require.NoError(t, err)

	// Should go to DLQ, not retry topic
	require.Len(t, producedTopics, 1)
	assert.Equal(t, "dlq_orders", producedTopics[0], "NotRetriableError should go directly to DLQ")

	// Acquire should NOT have been called
	assert.Empty(t, mockCoordinator.AcquireCalls(), "Lock should not be acquired for NotRetriableError")
}

func TestErrorTracker_NotRetriableError_PreservesOriginalError(t *testing.T) {
	// Verify that NotRetriableError properly wraps and preserves the original error
	originalErr := errors.New("validation failed: order ID must be positive")
	wrappedErr := NewNotRetriableError(originalErr)

	// Error() should return original message
	assert.Equal(t, originalErr.Error(), wrappedErr.Error())

	// Unwrap() should return original error
	assert.Equal(t, originalErr, wrappedErr.Unwrap())

	// errors.Is should work through the wrapper
	require.ErrorIs(t, wrappedErr, originalErr)

	// errors.As should find NotRetriableError
	var notRetriable *NotRetriableError
	require.ErrorAs(t, wrappedErr, &notRetriable)
	assert.Equal(t, originalErr, notRetriable.Origin)
}

func TestErrorTracker_Close_WithTimeout(t *testing.T) {
	// Test that Close respects context timeout
	mockProducer := &ProducerMock{}
	mockCoordinator := &StateCoordinatorMock{
		CloseFunc: func(_ context.Context) error {
			return nil
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
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

	// Close with sufficient timeout should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = tracker.Close(ctx)
	require.NoError(t, err)
}

func TestErrorTracker_Close_TimeoutReturnsError(t *testing.T) {
	// Test that Close returns error when timeout expires
	// Simulates a scenario where workers don't exit in time
	mockProducer := &ProducerMock{}

	// Coordinator that blocks forever on Close (simulating stuck worker)
	mockCoordinator := &StateCoordinatorMock{
		CloseFunc: func(ctx context.Context) error {
			<-ctx.Done() // Block until context canceled
			return ctx.Err()
		},
	}

	logger := &LoggerMock{
		DebugFunc: func(_ string, _ ...interface{}) {},
		InfoFunc:  func(_ string, _ ...interface{}) {},
	}

	cfg := newTestConfig()
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

	// Close with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = tracker.Close(ctx)

	// Should return timeout error (either from workers or coordinator)
	require.Error(t, err)
	assert.True(t,
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
		"Expected context timeout error, got: %v", err)
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
