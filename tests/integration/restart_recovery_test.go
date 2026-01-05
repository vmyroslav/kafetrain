//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmyroslav/kafetrain/resilience"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// TestIntegration_RestartRecovery tests that ErrorTracker recovers state after restart:
// 1. Message fails and goes to retry/redirect topics
// 2. ErrorTracker is stopped
// 3. New ErrorTracker is started
// 4. State is recovered from redirect topic
// 5. Retry continues and succeeds
func TestIntegration_RestartRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))
	topic := "test-restart-recovery"
	cfg := createTestConfig(broker, "test-restart-recovery-group")

	// Create handler that fails on first attempt, succeeds on second
	var attemptCount atomic.Int32
	var processedOnce sync.Once
	processedCh := make(chan struct{})

	handler := retryold.MessageHandleFunc(func(_ context.Context, msg *retryold.Message) error {
		attempt := attemptCount.Add(1)
		logger.Info("handler invoked",
			zap.Int32("attempt", attempt),
		)

		if attempt == 1 {
			return retryold.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("failure before restart"),
			}
		}

		processedOnce.Do(func() {
			close(processedCh)
		})
		return nil
	})

	// === PHASE 1: Initial tracker setup and failure ===
	tracker1, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err, "failed to create first tracker")

	err = tracker1.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start first tracking")

	retryMgr1 := retryold.NewRetryManager(tracker1, handler)
	err = retryMgr1.StartRetryConsumer(ctx, topic)
	require.NoError(t, err, "failed to start first retry consumer")

	consumer1, err := retryold.NewKafkaConsumer(&cfg, logger)
	require.NoError(t, err, "failed to create consumer")

	consumer1.WithMiddlewares(
		retryold.NewLoggingMiddleware(logger),
		retryold.NewErrorHandlingMiddleware(tracker1),
	)

	consumer1Ctx, consumer1Cancel := context.WithCancel(ctx)
	var wg1 sync.WaitGroup
	wg1.Add(1)

	go func() {
		defer wg1.Done()
		_ = consumer1.Consume(consumer1Ctx, topic, handler)
	}()

	// Wait for topics to be ready
	time.Sleep(2 * time.Second)

	// Produce test message (will fail and go to retry topic)
	produceTestMessage(t, broker, topic, "test-key-restart", "test message for restart recovery")

	// Wait for initial failure and redirect
	time.Sleep(5 * time.Second)

	// Verify initial failure occurred
	initialAttempts := attemptCount.Load()
	assert.GreaterOrEqual(t, initialAttempts, int32(1), "handler should be called at least once (initial failure)")
	t.Logf("Handler was called %d times before restart", initialAttempts)

	// === PHASE 2: Simulate restart - stop first tracker ===
	logger.Info("simulating restart - stopping first tracker")
	consumer1Cancel()
	wg1.Wait()
	require.NoError(t, consumer1.Close(), "failed to close first consumer")

	// === PHASE 3: Start new tracker (should recover state from redirect topic) ===
	logger.Info("starting second tracker - should recover state from redirect topic")

	tracker2, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err, "failed to create second tracker")

	err = tracker2.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start second tracking")

	retryMgr2 := retryold.NewRetryManager(tracker2, handler)
	err = retryMgr2.StartRetryConsumer(ctx, topic)
	require.NoError(t, err, "failed to start second retry consumer")

	consumer2, err := retryold.NewKafkaConsumer(&cfg, logger)
	require.NoError(t, err, "failed to create second consumer")

	consumer2.WithMiddlewares(
		retryold.NewLoggingMiddleware(logger),
		retryold.NewErrorHandlingMiddleware(tracker2),
	)

	consumer2Ctx, consumer2Cancel := context.WithCancel(ctx)
	defer consumer2Cancel()

	var wg2 sync.WaitGroup
	wg2.Add(1)

	go func() {
		defer wg2.Done()
		_ = consumer2.Consume(consumer2Ctx, topic, handler)
	}()

	// Wait for retry to be processed (retry consumer should pick up from retry topic)
	select {
	case <-processedCh:
		logger.Info("message processed successfully after restart and retry")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for message to be processed after restart")
	}

	// Allow time for async processing to complete
	// The channel closes when handler succeeds, but the retry consumer may still be processing
	time.Sleep(2 * time.Second)

	// Verify handler was called at least twice total (before restart + after restart)
	// Note: May be more due to Kafka consumer behavior and message reprocessing
	finalAttempts := attemptCount.Load()
	assert.GreaterOrEqual(t, finalAttempts, int32(2), "handler should be called at least twice (before and after restart)")
	assert.GreaterOrEqual(t, finalAttempts, initialAttempts+1, "handler should be called at least once more after restart")
	t.Logf("Handler was called %d times total (%d before restart, at least 1 after restart)", finalAttempts, initialAttempts)

	// Cleanup - cancel contexts first to stop all consumers
	cancel() // Cancel main context to stop ErrorTracker's background consumers
	consumer2Cancel()
	wg2.Wait()
	require.NoError(t, consumer2.Close(), "failed to close second consumer")

	// Allow background tracker goroutines to finish (retry and redirect consumers)
	// Need extra time for error tracker's background consumers to shut down cleanly
	// The ErrorTracker starts background consumers that need time to detect context cancellation
	time.Sleep(15 * time.Second)
}
