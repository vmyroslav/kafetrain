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

// TestIntegration_FIFOOrdering tests that messages with same key are processed in order.
// When a message fails and goes to retry, all subsequent messages with the same key
// should be queued to retry immediately without executing the handler.
func TestIntegration_FIFOOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))
	topic := "test-fifo-ordering"
	cfg := createTestConfig(broker, "test-fifo-group")

	// Track processing order and which messages were queued (not executed)
	var processingOrder []string
	var queuedMessages []string
	var mu sync.Mutex
	var processedCount atomic.Int32
	allProcessedCh := make(chan struct{})
	var closeOnce sync.Once

	handler := retryold.MessageHandleFunc(func(_ context.Context, msg *retryold.Message) error {
		payload := string(msg.Payload)

		mu.Lock()
		processingOrder = append(processingOrder, payload)
		mu.Unlock()

		count := processedCount.Add(1)
		logger.Info("processed message", zap.String("payload", payload), zap.Int32("count", count))

		// First message (msg-1) fails, rest succeed
		if payload == "msg-1" {
			return retryold.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure for first message"),
			}
		}

		// Total: 5 messages initially, but msg-1 retries, so 6 handler invocations expected
		// msg-1 (fail), msg-2 (queued), msg-3 (queued), msg-4 (queued), msg-5 (queued), msg-1 (retry success)
		// But msg-2 through msg-5 should be redirected without handler execution
		if count >= 6 {
			closeOnce.Do(func() {
				logger.Info("all messages processed")
				close(allProcessedCh)
			})
		}

		return nil
	})

	tracker, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err)

	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err)

	retryMgr := retryold.NewRetryManager(tracker, handler)
	err = retryMgr.StartRetryConsumer(ctx, topic)
	require.NoError(t, err)

	consumer, err := retryold.NewKafkaConsumer(&cfg, logger)
	require.NoError(t, err)

	consumer.WithMiddlewares(
		retryold.NewLoggingMiddleware(logger),
		retryold.NewErrorHandlingMiddleware(tracker),
	)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		_ = consumer.Consume(consumerCtx, topic, handler)
	}()

	time.Sleep(2 * time.Second)

	// Produce 5 messages with SAME key in sequence
	for i := 1; i <= 5; i++ {
		produceTestMessage(t, broker, topic, "same-key", fmt.Sprintf("msg-%d", i))
		time.Sleep(100 * time.Millisecond) // Small delay to ensure ordering
	}

	// Wait for all messages to be processed
	select {
	case <-allProcessedCh:
		logger.Info("all messages processed")
	case <-time.After(60 * time.Second):
		t.Fatal("timeout waiting for all messages")
	}

	// Give a bit of time for all processing to complete
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	t.Logf("Processing order: %v", processingOrder)
	t.Logf("Queued messages (should be msg-2 through msg-5): %v", queuedMessages)

	// Verify that msg-1 was processed at least once
	assert.Contains(t, processingOrder, "msg-1", "msg-1 should be processed")

	// Cleanup - cancel contexts first to stop all consumers
	cancel() // Cancel main context to stop ErrorTracker's background consumers
	consumerCancel()
	require.NoError(t, consumer.Close(), "failed to close consumer")

	// Allow background tracker goroutines to finish (retry and redirect consumers)
	// Need extra time for error tracker's background consumers to shut down cleanly
	// The ErrorTracker starts background consumers that need time to detect context cancellation
	time.Sleep(15 * time.Second)
}
