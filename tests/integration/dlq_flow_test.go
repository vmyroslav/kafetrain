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

// TestIntegration_DLQFlow tests the Dead Letter Queue flow:
// 1. Message fails multiple times
// 2. After MaxRetries, message is sent to DLQ
// 3. With FreeOnDLQ=false (default), message is kept in tracking
// 4. Subsequent messages with same key would be blocked
func TestIntegration_DLQFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	// Use unique topic name to avoid old messages from previous test runs
	topic := fmt.Sprintf("test-dlq-flow-%d", time.Now().UnixNano())
	cfg := createTestConfig(broker, fmt.Sprintf("test-dlq-flow-group-%d", time.Now().UnixNano()))
	cfg.MaxRetries = 2 // Fail after 2 retries

	// Create handler that always fails
	var attemptCount atomic.Int32

	handler := retryold.MessageHandleFunc(func(_ context.Context, msg *retryold.Message) error {
		attempt := attemptCount.Add(1)

		// Get retry attempt from headers to understand which consumer is calling this
		headerAttempt, hasHeader := retryold.GetHeaderValue[int](&msg.Headers, retryold.HeaderRetryAttempt)

		logger.Info("handler invoked (will fail)",
			zap.Int32("handler_call_count", attempt),
			zap.Int("header_retry_attempt", headerAttempt),
			zap.Bool("has_retry_header", hasHeader),
			zap.String("topic", msg.Topic()),
			zap.String("payload", string(msg.Payload)),
		)

		// Always fail with retriable error
		return retryold.RetriableError{
			Retry:  true,
			Origin: fmt.Errorf("persistent failure on handler call %d (header attempt: %d)", attempt, headerAttempt),
		}
	})

	// Create registry and tracker
	tracker, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err, "failed to create tracker")

	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryMgr := retryold.NewRetryManager(tracker, handler)
	err = retryMgr.StartRetryConsumer(ctx, topic)
	require.NoError(t, err, "failed to start retry consumer")

	// Create primary consumer
	consumer, err := retryold.NewKafkaConsumer(&cfg, logger)
	require.NoError(t, err, "failed to create consumer")

	consumer.WithMiddlewares(
		retryold.NewLoggingMiddleware(logger),
		retryold.NewErrorHandlingMiddleware(tracker),
	)

	// Start consuming in background
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		_ = consumer.Consume(consumerCtx, topic, handler)
	}()

	// Wait for topics to be ready
	time.Sleep(2 * time.Second)

	// Setup DLQ consumer BEFORE producing message to avoid rebalancing issues
	// Use separate consumer group to avoid conflicts with primary consumer
	dlqTopic := cfg.DLQTopicPrefix + "_" + topic
	dlqCfg := cfg
	dlqCfg.GroupID = cfg.GroupID + "-dlq"
	dlqConsumer, err := retryold.NewKafkaConsumer(&dlqCfg, logger)
	require.NoError(t, err, "failed to create DLQ consumer")

	var dlqMessageReceived atomic.Bool
	dlqHandler := retryold.MessageHandleFunc(func(_ context.Context, msg *retryold.Message) error {
		logger.Info("received message from DLQ",
			zap.String("payload", string(msg.Payload)),
			zap.Any("headers", msg.Headers),
		)

		// Verify DLQ headers are present
		reason, ok := retryold.GetHeaderValue[string](&msg.Headers, "x-dlq-reason")
		assert.True(t, ok, "DLQ message should have x-dlq-reason header")
		assert.Contains(t, reason, "persistent failure", "DLQ reason should contain error message")

		sourceTopicVal, ok := retryold.GetHeaderValue[string](&msg.Headers, "x-dlq-source-topic")
		assert.True(t, ok, "DLQ message should have x-dlq-source-topic header")
		assert.Equal(t, topic, sourceTopicVal, "source topic should match original topic")

		dlqMessageReceived.Store(true)
		return nil
	})

	dlqCtx, dlqCancel := context.WithCancel(ctx)
	defer dlqCancel()

	go func() {
		_ = dlqConsumer.Consume(dlqCtx, dlqTopic, dlqHandler)
	}()

	// Wait for DLQ consumer to be ready
	time.Sleep(2 * time.Second)

	// Produce test message
	produceTestMessage(t, broker, topic, "test-key-dlq", "test message for DLQ")

	// Wait for DLQ processing (initial attempt + retries + eventual DLQ)
	// This may take longer due to backoff delays
	// With maxRetries=2: attempt 0 (1s delay) + attempt 1 (2s delay) + attempt 2 (fails, goes to DLQ)
	time.Sleep(10 * time.Second)

	// Verify the handler was called multiple times
	// Note: Due to Kafka consumer group behavior and topic compaction timing,
	// the exact count may vary, but should be at least initial + maxRetries
	attempts := attemptCount.Load()
	t.Logf("Handler was called %d times before DLQ", attempts)
	assert.GreaterOrEqual(t, attempts, int32(3), "handler should be called at least 3 times (initial + 2 retries)")

	// Verify message ended up in DLQ
	assert.True(t, dlqMessageReceived.Load(), "message should be in DLQ")

	// Cleanup - cancel contexts first to stop all consumers
	cancel() // Cancel main context to stop ErrorTracker's background consumers
	consumerCancel()
	dlqCancel()
	wg.Wait()

	require.NoError(t, consumer.Close(), "failed to close consumer")
	require.NoError(t, dlqConsumer.Close(), "failed to close DLQ consumer")

	// Allow background tracker goroutines to finish (retry and redirect consumers)
	// Need extra time for error tracker's background consumers to shut down cleanly
	// The ErrorTracker starts background consumers that need time to detect context cancellation
	time.Sleep(15 * time.Second)
}
