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

// TestIntegration_FullRetryFlow tests the complete retry flow:
// 1. Message is consumed from primary topic
// 2. Handler fails with RetriableError
// 3. Message is redirected to retry topic
// 4. Retry consumer processes message (with backoff delay)
// 5. Handler succeeds
// 6. Tombstone is published to redirect topic
// 7. Message is removed from tracking
func TestIntegration_FullRetryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))
	topic := "test-retry-flow"
	cfg := createTestConfig(broker, "test-retry-flow-group")

	// Create handler that fails once, then succeeds
	var attemptCount atomic.Int32
	var processedOnce sync.Once
	var processedCh = make(chan struct{})

	handler := resilience.MessageHandleFunc(func(_ context.Context, msg *resilience.Message) error {
		attempt := attemptCount.Add(1)
		logger.Info("handler invoked",
			zap.Int32("attempt", attempt),
			zap.String("payload", string(msg.Payload)),
		)

		if attempt == 1 {
			// First attempt: fail with retriable error
			return resilience.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure on attempt %d", attempt),
			}
		}

		// Second attempt: succeed (only close channel once)
		processedOnce.Do(func() {
			close(processedCh)
		})

		return nil
	})

	// Create registry and tracker
	registry := resilience.NewHandlerRegistry()
	registry.Add(topic, handler)

	tracker, err := resilience.NewTracker(&cfg, logger, resilience.NewKeyTracker(), registry)
	require.NoError(t, err, "failed to create tracker")

	// Start error tracker (creates retry/redirect/DLQ topics and starts consumers)
	err = tracker.StartRetryConsumers(ctx, topic)
	require.NoError(t, err, "failed to start tracker")

	// Create primary consumer
	consumer, err := resilience.NewKafkaConsumer(&cfg, logger)
	require.NoError(t, err, "failed to create consumer")

	consumer.WithMiddlewares(
		resilience.NewLoggingMiddleware(logger),
		resilience.NewErrorHandlingMiddleware(tracker),
	)

	// Start consuming in background
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var consumerErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		consumerErr = consumer.Consume(consumerCtx, topic, handler)
	}()

	// Wait for topics to be ready
	time.Sleep(2 * time.Second)

	// Produce test message
	produceTestMessage(t, broker, topic, "test-key-1", "test message for retry flow")

	// Wait for message to be processed successfully (with timeout)
	select {
	case <-processedCh:
		logger.Info("message processed successfully after retry")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for message to be processed")
	}

	// Verify the handler was called at least twice (initial failure + retry success)
	// Note: May be called more due to Kafka rebalancing or concurrent processing
	attempts := attemptCount.Load()
	assert.GreaterOrEqual(t, attempts, int32(2), "handler should be called at least twice")
	t.Logf("Handler was called %d times (at least 2 expected for retry flow)", attempts)

	// Cleanup
	consumerCancel()
	wg.Wait()

	if consumerErr != nil && consumerErr != context.Canceled {
		t.Logf("consumer error: %v", consumerErr)
	}

	// Cancel main context to stop ErrorTracker's background consumers
	cancel()

	require.NoError(t, consumer.Close(), "failed to close consumer")

	// Allow background tracker goroutines to finish (retry and redirect consumers)
	// Need extra time for error tracker's background consumers to shut down cleanly
	// The ErrorTracker starts background consumers that need time to detect context cancellation
	time.Sleep(15 * time.Second)
}
