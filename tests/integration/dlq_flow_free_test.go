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
	"github.com/vmyroslav/kafetrain/retryold"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// TestIntegration_DLQFlowWithFree tests DLQ flow with FreeOnDLQ=true:
// 1. First message fails and exceeds max retries
// 2. First message is sent to DLQ
// 3. Tombstone IS published to redirect topic (message freed)
// 4. Second message with same key CAN be processed successfully
func TestIntegration_DLQFlowWithFree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-dlq-free-%d", time.Now().UnixNano())
	cfg := createTestConfig(broker, fmt.Sprintf("test-dlq-free-group-%d", time.Now().UnixNano()))
	cfg.MaxRetries = 2
	cfg.FreeOnDLQ = true // KEY DIFFERENCE: allow processing after DLQ

	// Track messages
	var firstMessageAttempts atomic.Int32
	var secondMessageProcessed atomic.Bool

	handler := retryold.MessageHandleFunc(func(_ context.Context, msg *retryold.Message) error {
		payload := string(msg.Payload)

		if payload == "first-message" {
			attempt := firstMessageAttempts.Add(1)

			logger.Info("handler invoked for first-message (will fail)",
				zap.Int32("attempt", attempt),
				zap.String("payload", payload),
			)

			return retryold.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure for first message"),
			}
		}

		if payload == "second-message" {
			secondMessageProcessed.Store(true)
			logger.Info("second message processed successfully")
			return nil
		}

		return nil
	})

	// Setup tracker and consumer
	tracker, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err, "failed to create tracker")

	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryMgr := retryold.NewRetryManager(tracker, handler)
	err = retryMgr.StartRetryConsumer(ctx, topic)
	require.NoError(t, err, "failed to start retry consumer")

	consumer, err := retryold.NewKafkaConsumer(&cfg, logger)
	require.NoError(t, err, "failed to create consumer")

	consumer.WithMiddlewares(
		retryold.NewLoggingMiddleware(logger),
		retryold.NewErrorHandlingMiddleware(tracker),
	)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Consume(consumerCtx, topic, handler)
	}()

	// Wait for consumer to be ready
	time.Sleep(2 * time.Second)

	// Produce two messages with SAME KEY
	produceTestMessage(t, broker, topic, "same-key", "first-message")
	time.Sleep(1 * time.Second) // Ensure ordering
	produceTestMessage(t, broker, topic, "same-key", "second-message")

	// Wait for processing (retries + DLQ + second message processing)
	// With maxRetries=2: attempt 0 (1s delay) + attempt 1 (2s delay) + attempt 2 (fails, goes to DLQ)
	// Then second message should be processed
	time.Sleep(15 * time.Second)

	// Verify first message failed and went to DLQ
	attempts := firstMessageAttempts.Load()
	t.Logf("First message was attempted %d times before DLQ", attempts)
	assert.GreaterOrEqual(t, attempts, int32(3), "first message should fail at least 3 times (initial + 2 retries)")

	// Verify second message WAS processed (not blocked because FreeOnDLQ=true)
	assert.True(t, secondMessageProcessed.Load(),
		"second message should be processed (FreeOnDLQ=true allows this)")

	// Cleanup
	cancel() // Cancel main context to stop ErrorTracker's background consumers
	consumerCancel()
	wg.Wait()
	require.NoError(t, consumer.Close(), "failed to close consumer")

	// Allow background tracker goroutines to finish
	time.Sleep(5 * time.Second)
}
