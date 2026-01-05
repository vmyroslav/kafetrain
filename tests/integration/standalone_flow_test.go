//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmyroslav/kafetrain/resilience"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// testMessageProcessor encapsulates test business logic
// This function is used by BOTH main consumer and retry consumer (via adapter)
type testMessageProcessor struct {
	logger         *zap.Logger
	firstAttempts  *atomic.Int32
	secondAttempts *atomic.Int32
	thirdProcessed *atomic.Bool
	mu             *sync.Mutex
	processedMsgs  *[]string
}

func (p *testMessageProcessor) process(ctx context.Context, msg *sarama.ConsumerMessage) error {
	payload := string(msg.Value)

	switch payload {
	case "first-will-fail-twice":
		attempt := p.firstAttempts.Add(1)
		p.logger.Info("processing first message",
			zap.Int32("attempt", attempt),
			zap.String("source", msg.Topic),
		)

		if attempt <= 2 {
			return retryold.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure for first message (attempt %d)", attempt),
			}
		}

		// Success path
		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	case "second-will-fail-once":
		attempt := p.secondAttempts.Add(1)
		p.logger.Info("processing second message",
			zap.Int32("attempt", attempt),
			zap.String("source", msg.Topic),
		)

		if attempt == 1 {
			return retryold.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure for second message (attempt %d)", attempt),
			}
		}

		// Success path
		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	case "third-will-succeed":
		p.logger.Info("processing third message (will succeed)", zap.String("source", msg.Topic))
		p.thirdProcessed.Store(true)

		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	default:
		return nil
	}
}

// TestIntegration_StandaloneFlow tests the new standalone ErrorTracker API:
// 1. Uses raw Sarama ConsumerGroup (no kafetrain Consumer wrapper)
// 2. Tests public Sarama-based API: Redirect(), Free(), IsInRetryChain()
// 3. Verifies ordered retry flow works with standalone integration
// 4. Message fails, gets retried, succeeds, and is freed from tracking
// 5. Demonstrates realistic DRY pattern: single business logic used by both main and retry consumers
func TestIntegration_StandaloneFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-standalone-%d", time.Now().UnixNano())
	cfg := createTestConfig(broker, fmt.Sprintf("test-standalone-group-%d", time.Now().UnixNano()))
	cfg.MaxRetries = 3

	// Track processing
	var (
		firstAttempts  atomic.Int32
		secondAttempts atomic.Int32
		thirdProcessed atomic.Bool
		mu             sync.Mutex
		processedMsgs  []string
	)

	// 1. Create standalone ErrorTracker
	tracker, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err, "failed to create tracker")

	// Start tracking (only redirect consumer)
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	// Create the business logic processor
	// This processor is used by BOTH main and retry consumers (DRY!)
	processor := &testMessageProcessor{
		logger:         logger,
		firstAttempts:  &firstAttempts,
		secondAttempts: &secondAttempts,
		thirdProcessed: &thirdProcessed,
		mu:             &mu,
		processedMsgs:  &processedMsgs,
	}

	// 2. Create Sarama config (for both consumers)
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 3. Create MAIN consumer for primary topic
	mainConsumer, err := sarama.NewConsumerGroup([]string{broker}, cfg.GroupID, saramaConfig)
	require.NoError(t, err, "failed to create main consumer")
	defer mainConsumer.Close()

	// 4. Create RETRY consumer for retry topic (Layer 1: YOU control both!)
	retryConsumer, err := sarama.NewConsumerGroup([]string{broker}, cfg.GroupID+"-retry", saramaConfig)
	require.NoError(t, err, "failed to create retry consumer")
	defer retryConsumer.Close()

	// 5. Create handler - SAME handler for BOTH consumers (DRY!)
	handler := &standaloneTestHandler{
		tracker:   tracker,
		logger:    logger,
		processor: processor,
	}

	// Start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup

	// Start MAIN consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := mainConsumer.Consume(consumerCtx, []string{topic}, handler); err != nil {
				logger.Error("main consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Start RETRY consumer (Layer 1: user manages it!)
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryTopic := tracker.GetRetryTopic(topic)
		for {
			if err := retryConsumer.Consume(consumerCtx, []string{retryTopic}, handler); err != nil {
				logger.Error("retry consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumer to be ready
	time.Sleep(2 * time.Second)

	// Produce test messages with same key
	produceTestMessage(t, broker, topic, "order-key", "first-will-fail-twice")
	time.Sleep(1 * time.Second)
	produceTestMessage(t, broker, topic, "order-key", "second-will-fail-once")
	time.Sleep(1 * time.Second)
	produceTestMessage(t, broker, topic, "order-key", "third-will-succeed")

	// Wait for retry flow to complete
	// First message: fails on main consumer, retried 2 times by retry consumer, succeeds on 3rd
	// Second message: blocked until first completes, fails on main consumer, retried 1 time, succeeds on 2nd
	// Third message: blocked until second completes, succeeds immediately on main consumer
	time.Sleep(20 * time.Second)

	// Verify first message was attempted 3 times (1 from main consumer + 2 from retry consumer)
	attempts1 := firstAttempts.Load()
	t.Logf("First message attempts: %d", attempts1)
	assert.GreaterOrEqual(t, attempts1, int32(3), "first message should be attempted at least 3 times")

	// Verify second message was attempted 2 times (1 from main consumer + 1 from retry consumer)
	attempts2 := secondAttempts.Load()
	t.Logf("Second message attempts: %d", attempts2)
	assert.GreaterOrEqual(t, attempts2, int32(2), "second message should be attempted at least 2 times")

	// Verify third message was processed
	assert.True(t, thirdProcessed.Load(), "third message should be processed successfully")

	// Verify all messages eventually succeeded
	mu.Lock()
	msgs := make([]string, len(processedMsgs))
	copy(msgs, processedMsgs)
	mu.Unlock()

	t.Logf("Successfully processed messages: %v", msgs)
	assert.Contains(t, msgs, "first-will-fail-twice", "first message should eventually succeed")
	assert.Contains(t, msgs, "second-will-fail-once", "second message should eventually succeed")
	assert.Contains(t, msgs, "third-will-succeed", "third message should succeed")

	// Cleanup
	cancel() // Cancel main context to stop ErrorTracker's background consumers
	consumerCancel()
	wg.Wait()

	// Allow background tracker goroutines to finish (retry and redirect consumers)
	// Need extra time for error tracker's background consumers to shut down cleanly
	// This prevents "Log in goroutine after test has completed" panic
	time.Sleep(15 * time.Second)
}

// standaloneTestHandler implements sarama.ConsumerGroupHandler
// This demonstrates how users would integrate ErrorTracker with their own Sarama consumer
type standaloneTestHandler struct {
	tracker   *retryold.ErrorTracker
	logger    *zap.Logger
	processor *testMessageProcessor // Shared business logic processor
}

func (h *standaloneTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *standaloneTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *standaloneTestHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		// Skip if already in retry chain (NEW PUBLIC API)
		if h.tracker.IsInRetryChain(msg) {
			h.logger.Debug("skipping message in retry chain",
				zap.String("topic", msg.Topic),
				zap.Int64("offset", msg.Offset),
				zap.String("value", string(msg.Value)),
			)
			continue
		}

		h.logger.Info("processing message",
			zap.String("payload", string(msg.Value)),
			zap.Int64("offset", msg.Offset),
			zap.Bool("is_retry", retryold.IsRetryMessage(msg)),
			zap.Int("retry_attempt", retryold.GetRetryAttempt(msg)),
		)

		// Process message using the SAME processor as retry consumer
		err := h.processor.process(session.Context(), msg)
		if err != nil {
			// Check if retriable (using helper)
			if retryold.IsRetriable(err) {
				h.logger.Warn("retriable error, redirecting",
					zap.String("payload", string(msg.Value)),
					zap.Error(err),
				)

				// Redirect to retry topic (NEW PUBLIC API)
				if redirectErr := h.tracker.Redirect(session.Context(), msg, err); redirectErr != nil {
					h.logger.Error("failed to redirect", zap.Error(redirectErr))
					return redirectErr
				}

				session.MarkMessage(msg, "")
				continue
			}

			// Non-retriable error
			h.logger.Error("non-retriable error", zap.Error(err))
			return err
		}

		// Success! Free from tracking if it was a retry message
		if retryold.IsRetryMessage(msg) {
			h.logger.Info("freeing retry message from tracking",
				zap.String("payload", string(msg.Value)),
				zap.Int("attempts", retryold.GetRetryAttempt(msg)),
			)

			// Free from tracking (NEW PUBLIC API)
			if freeErr := h.tracker.Free(session.Context(), msg); freeErr != nil {
				h.logger.Error("failed to free message", zap.Error(freeErr))
				return freeErr
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// TestIntegration_StandaloneFlow_NonRetriableError tests that non-retriable errors
// are not redirected to retry topic when using standalone API
func TestIntegration_StandaloneFlow_NonRetriableError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-standalone-nonretry-%d", time.Now().UnixNano())
	cfg := createTestConfig(broker, fmt.Sprintf("test-standalone-nonretry-group-%d", time.Now().UnixNano()))
	cfg.MaxRetries = 3

	var (
		processAttempts atomic.Int32
		errorReturned   atomic.Bool
	)

	// Create tracker
	tracker, err := retryold.NewErrorTracker(&cfg, logger, retryold.NewKeyTracker())
	require.NoError(t, err)

	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err)

	dummyHandler := retryold.MessageHandleFunc(func(_ context.Context, msg *retryold.Message) error {
		return nil
	})

	retryMgr := retryold.NewRetryManager(tracker, dummyHandler)
	err = retryMgr.StartRetryConsumer(ctx, topic)
	require.NoError(t, err)

	// Create Sarama consumer
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{broker}, cfg.GroupID, saramaConfig)
	require.NoError(t, err)
	defer consumerGroup.Close()

	// Handler that returns non-retriable error
	handler := &nonRetriableTestHandler{
		tracker:         tracker,
		logger:          logger,
		processAttempts: &processAttempts,
		errorReturned:   &errorReturned,
	}

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(consumerCtx, []string{topic}, handler); err != nil {
				logger.Info("consumer error (expected for non-retriable)", zap.Error(err))
				errorReturned.Store(true)
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	time.Sleep(2 * time.Second)

	// Produce message that will cause non-retriable error
	produceTestMessage(t, broker, topic, "key", "bad-message")

	time.Sleep(5 * time.Second)

	// Verify message was only attempted once (no retries for non-retriable errors)
	attempts := processAttempts.Load()
	t.Logf("Message attempts: %d", attempts)
	assert.Equal(t, int32(1), attempts, "non-retriable error should only be attempted once")

	// Verify error was logged (errorReturned flag set)
	assert.True(t, errorReturned.Load(), "non-retriable error should be logged")

	cancel()
	consumerCancel()
	wg.Wait()

	// Allow background tracker goroutines to finish (retry and redirect consumers)
	// Need extra time for error tracker's background consumers to shut down cleanly
	// This prevents "Log in goroutine after test has completed" panic
	time.Sleep(15 * time.Second)
}

type nonRetriableTestHandler struct {
	tracker         *retryold.ErrorTracker
	logger          *zap.Logger
	processAttempts *atomic.Int32
	errorReturned   *atomic.Bool
}

func (h *nonRetriableTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *nonRetriableTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *nonRetriableTestHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		if h.tracker.IsInRetryChain(msg) {
			continue
		}

		h.processAttempts.Add(1)
		payload := string(msg.Value)

		h.logger.Info("processing message", zap.String("payload", payload))

		// Return non-retriable error (plain error, not RetriableError)
		err := fmt.Errorf("invalid message format: %s", payload)

		// Check if retriable (should be false)
		if retryold.IsRetriable(err) {
			h.tracker.Redirect(session.Context(), msg, err)
			session.MarkMessage(msg, "")
			continue
		}

		// Non-retriable error - log it and mark message as processed
		// In real scenarios, you might send to a separate error topic, log to monitoring, etc.
		h.logger.Error("non-retriable error", zap.Error(err))
		h.errorReturned.Store(true)

		// Mark message as processed to avoid Sarama retrying it
		session.MarkMessage(msg, "")
	}

	return nil
}
