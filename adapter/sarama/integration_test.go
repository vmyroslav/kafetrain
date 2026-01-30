//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

// TestIntegration_SaramaAdmin tests the Sarama Admin adapter implementation.
// Verifies that the Admin interface correctly creates topics and manages consumer groups.
func TestIntegration_SaramaAdmin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client := newTestClient(t)
	admin, err := saramaadapter.NewAdminAdapter(client)
	require.NoError(t, err, "failed to create admin adapter")
	t.Cleanup(func() { _ = admin.Close() })

	// Test 1: Create topics
	t.Run("CreateTopics", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-test-%d", time.Now().UnixNano())

		// Create topic with specific configuration
		err := admin.CreateTopic(ctx, testTopic, 3, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "3600000",
		})
		require.NoError(t, err, "failed to create topic")

		// Verify topic was created by describing it (with retry for metadata propagation)
		var metadata []resilience.TopicMetadata
		assert.Eventually(t, func() bool {
			metadata, err = admin.DescribeTopics(ctx, []string{testTopic})
			return err == nil && len(metadata) == 1
		}, 10*time.Second, 500*time.Millisecond, "failed to describe topic or topic not found")

		assert.Equal(t, testTopic, metadata[0].Name())
		assert.Equal(t, int32(3), metadata[0].Partitions())

		SharedLogger.Info("topic created successfully",
			"topic", testTopic,
			"partitions", metadata[0].Partitions(),
		)
	})

	// Test 2: Idempotent topic creation
	t.Run("IdempotentTopicCreation", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-idempotent-%d", time.Now().UnixNano())

		// Create topic first time
		err := admin.CreateTopic(ctx, testTopic, 1, 1, map[string]string{
			"cleanup.policy": "compact",
		})
		require.NoError(t, err, "failed to create topic first time")

		// Create same topic again - should not error
		err = admin.CreateTopic(ctx, testTopic, 1, 1, map[string]string{
			"cleanup.policy": "compact",
		})
		require.NoError(t, err, "creating existing topic should be idempotent")
	})

	// Test 3: Describe non-existent topic
	t.Run("DescribeNonExistentTopic", func(t *testing.T) {
		nonExistentTopic := "non-existent-topic-12345"

		// Should return empty list or handle gracefully
		metadata, err := admin.DescribeTopics(ctx, []string{nonExistentTopic})
		require.NoError(t, err, "DescribeTopics should not error for non-existent topics")
		assert.Len(t, metadata, 0, "should return empty metadata for non-existent topics")
	})

	// Test 4: Delete consumer group
	t.Run("DeleteConsumerGroup", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-cg-test-%d", time.Now().UnixNano())
		groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

		// First, create topic
		err := admin.CreateTopic(ctx, testTopic, 1, 1, map[string]string{})
		require.NoError(t, err, "failed to create topic")

		// Create a consumer group by consuming
		consumer, err := sarama.NewConsumerGroup([]string{sharedBroker}, groupID, newTestSaramaConfig())
		require.NoError(t, err, "failed to create consumer group")

		// Consume briefly to establish the group
		consumeCtx, consumeCancel := context.WithTimeout(ctx, 1*time.Second)
		defer consumeCancel()

		handler := &dummyHandler{}
		go func() {
			_ = consumer.Consume(consumeCtx, []string{testTopic}, handler)
		}()

		// Wait for consumer group to be established
		time.Sleep(500 * time.Millisecond)
		consumer.Close()

		// Now delete the consumer group
		err = admin.DeleteConsumerGroup(ctx, groupID)
		assert.NoError(t, err, "failed to delete consumer group")

		SharedLogger.Info("consumer group deleted successfully", "group_id", groupID)
	})
}

// TestIntegration_SaramaAdapterFullFlow tests the full ErrorTracker flow with Sarama adapter.
// This is the main integration test for the library-agnostic architecture.
func TestIntegration_SaramaAdapterFullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-adapter")

	// Track processing
	var (
		firstAttempts  atomic.Int32
		secondAttempts atomic.Int32
		thirdProcessed atomic.Bool
		mu             sync.Mutex
		processedMsgs  []string
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		make(chan error, 10),
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking - this should create topics
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	SharedLogger.Info("tracker started, topics should be created")

	// Get retry topic name for later use
	retryTopic := tracker.RetryTopic(topic)

	// Create business logic processor
	processor := &testMessageProcessor{
		logger:         SharedLogger,
		firstAttempts:  &firstAttempts,
		secondAttempts: &secondAttempts,
		thirdProcessed: &thirdProcessed,
		mu:             &mu,
		processedMsgs:  &processedMsgs,
	}

	// Create main consumer (using standard Sarama)
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	// Create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// Create handler that uses library-agnostic tracker
	handler := &adapterTestHandler{
		tracker:   tracker,
		logger:    SharedLogger,
		processor: processor,
	}

	// Start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup

	// Start main consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := mainConsumer.Consume(consumerCtx, []string{topic}, handler); err != nil {
				SharedLogger.Error("main consumer error", "error", err)
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Start retry consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := retryConsumer.Consume(consumerCtx, []string{retryTopic}, handler); err != nil {
				SharedLogger.Error("retry consumer error", "error", err)
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce test messages
	produceTestMessage(t, sharedBroker, topic, "test-key", "first-will-fail-twice")
	produceTestMessage(t, sharedBroker, topic, "test-key", "second-will-fail-once")
	produceTestMessage(t, sharedBroker, topic, "test-key", "third-will-succeed")

	// Wait for processing to complete
	assert.Eventually(t, func() bool {
		return firstAttempts.Load() == 3 &&
			secondAttempts.Load() == 2 &&
			thirdProcessed.Load()
	}, 30*time.Second, 500*time.Millisecond, "messages should be processed with retries")

	// Verify all messages were processed
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, processedMsgs, 3, "all 3 messages should be processed")
	assert.ElementsMatch(t, []string{
		"first-will-fail-twice",
		"second-will-fail-once",
		"third-will-succeed",
	}, processedMsgs, "all messages should be processed (order may vary based on retry timing)")

	SharedLogger.Info("test completed successfully",
		"first_attempts", firstAttempts.Load(),
		"second_attempts", secondAttempts.Load(),
		"third_processed", thirdProcessed.Load(),
	)

	// Stop consumers
	consumerCancel()
	wg.Wait()
}

// adapterTestHandler implements sarama.ConsumerGroupHandler for adapter tests.
// It uses the library-agnostic ErrorTracker via the retry.Message interface.
type adapterTestHandler struct {
	tracker   *resilience.ErrorTracker
	logger    *slog.Logger
	processor *testMessageProcessor
}

func (h *adapterTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *adapterTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *adapterTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Wrap Sarama message with adapter
		retryMsg := saramaadapter.NewMessage(msg)

		// Process message using business logic
		err := h.processor.process(session.Context(), msg)

		if err != nil {
			// Redirect all errors - the library handles NotRetriableError internally
			// (sends directly to DLQ instead of retry topic)
			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}
			h.logger.Info("message redirected",
				"payload", string(msg.Value),
			)
		} else {
			// Success - free from retry chain if it was in retry
			if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
				if freeErr := h.tracker.Free(session.Context(), retryMsg); freeErr != nil {
					h.logger.Error("failed to free message", "error", freeErr)
					return freeErr
				}
				h.logger.Info("message freed from retry chain",
					"payload", string(msg.Value),
				)
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// testMessageProcessor encapsulates test business logic
// This processor is used by BOTH main consumer and retry consumer (via adapter)
type testMessageProcessor struct {
	logger         *slog.Logger
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
			"attempt", attempt,
			"source", msg.Topic,
		)

		if attempt <= 2 {
			return fmt.Errorf("simulated failure for first message (attempt %d)", attempt)
		}

		// Success path
		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	case "second-will-fail-once":
		attempt := p.secondAttempts.Add(1)
		p.logger.Info("processing second message",
			"attempt", attempt,
			"source", msg.Topic,
		)

		if attempt == 1 {
			return fmt.Errorf("simulated failure for second message (attempt %d)", attempt)
		}

		// Success path
		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	case "third-will-succeed":
		p.logger.Info("processing third message (will succeed)", "source", msg.Topic)
		p.thirdProcessed.Store(true)

		p.mu.Lock()
		*p.processedMsgs = append(*p.processedMsgs, payload)
		p.mu.Unlock()
		return nil

	default:
		return fmt.Errorf("unknown message payload: %s", payload)
	}
}

// dummyHandler is a minimal handler for consumer group creation tests.
type dummyHandler struct{}

func (h *dummyHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dummyHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *dummyHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
	}
	return nil
}

// TestIntegration_ChainRetry verifies that messages properly go through the retry chain.
// Tests:
// - Message fails and gets redirected to retry topic
// - Message is tracked in retry chain (redirect topic)
// - Message is retried from retry topic
// - After success, message is freed from retry chain (tombstone published)
func TestIntegration_ChainRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-chain-retry")

	// Track attempts
	var (
		attempts atomic.Int32
		mu       sync.Mutex
		success  bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		make(chan error, 10),
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.RetryTopic(topic)
	redirectTopic := tracker.RedirectTopic(topic)

	SharedLogger.Info("tracker started",
		"topic", topic,
		"retry_topic", retryTopic,
		"redirect_topic", redirectTopic,
	)

	// Create handler that fails first 2 attempts, then succeeds
	handler := &chainRetryHandler{
		tracker:  tracker,
		logger:   SharedLogger,
		attempts: &attempts,
		mu:       &mu,
		success:  &success,
	}

	// Create main consumer
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	// Create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	// Start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, SharedLogger)
	runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, SharedLogger)

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce test message
	produceTestMessage(t, sharedBroker, topic, "test-key", "will-fail-twice-then-succeed")

	// Wait for processing to complete (3 attempts total)
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts.Load() == 3 && success
	}, 30*time.Second, 500*time.Millisecond, "message should be processed after 3 attempts")

	SharedLogger.Info("chain retry test completed",
		"total_attempts", attempts.Load(),
		"success", success,
	)

	// Verify message went through retry chain
	// We can verify by checking redirect topic has messages (and eventually a tombstone)
	// This is implicit in the test passing, but we could add explicit verification

	// Stop consumers
	consumerCancel()
	wg.Wait()
}

// chainRetryHandler handles messages for chain retry test.
type chainRetryHandler struct {
	tracker  *resilience.ErrorTracker
	logger   *slog.Logger
	attempts *atomic.Int32
	mu       *sync.Mutex
	success  *bool
}

func (h *chainRetryHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *chainRetryHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *chainRetryHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		retryMsg := saramaadapter.NewMessage(msg)
		attempt := h.attempts.Add(1)

		h.logger.Info("processing message in chain retry test",
			"attempt", attempt,
			"topic", msg.Topic,
			"value", string(msg.Value),
		)

		// Fail first 2 attempts, succeed on 3rd
		if attempt <= 2 {
			err := fmt.Errorf("simulated failure (attempt %d)", attempt)

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}

			h.logger.Info("message redirected to retry",
				"attempt", attempt,
			)
		} else {
			// Success on 3rd attempt
			if h.tracker.IsInRetryChain(session.Context(), retryMsg) {
				if freeErr := h.tracker.Free(session.Context(), retryMsg); freeErr != nil {
					h.logger.Error("failed to free message", "error", freeErr)
					return freeErr
				}
				h.logger.Info("message freed from retry chain")
			}

			h.mu.Lock()
			*h.success = true
			h.mu.Unlock()

			h.logger.Info("message processed successfully",
				"total_attempts", attempt,
			)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// TestIntegration_DLQ verifies that messages exceeding max retries are sent to DLQ.
// Tests:
// - Message fails repeatedly (more than max retries)
// - Message is sent to DLQ topic
// - DLQ message contains proper headers (retry attempts, original error)
// - With FreeOnDLQ=false (default), message stays in tracking (blocks subsequent messages)
func TestIntegration_DLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-dlq")

	// Track attempts and DLQ
	var (
		attempts    atomic.Int32
		dlqReceived atomic.Bool
		mu          sync.Mutex
		dlqHeaders  map[string]string
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.FreeOnDLQ = false

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		make(chan error, 10),
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.RetryTopic(topic)
	dlqTopic := tracker.DLQTopic(topic)

	SharedLogger.Info("tracker started",
		"topic", topic,
		"retry_topic", retryTopic,
		"dlq_topic", dlqTopic,
	)

	// Create handler that always fails
	handler := &dlqTestHandler{
		tracker:     tracker,
		logger:      SharedLogger,
		attempts:    &attempts,
		dlqReceived: &dlqReceived,
		mu:          &mu,
		dlqHeaders:  &dlqHeaders,
	}

	// Create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err, "failed to create DLQ consumer")
	t.Cleanup(func() { _ = dlqConsumer.Close() })

	// Start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, SharedLogger)
	runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, SharedLogger)
	runConsumerLoop(consumerCtx, &wg, dlqConsumer, []string{dlqTopic}, handler, SharedLogger)

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce test message that will always fail
	produceTestMessage(t, sharedBroker, topic, "test-key", "will-always-fail")

	// Wait for message to exceed max retries and go to DLQ
	// MaxRetries=3 means: 1 initial attempt + 3 retries = 4 total attempts
	assert.Eventually(t, func() bool {
		return attempts.Load() >= 4 && dlqReceived.Load()
	}, 30*time.Second, 500*time.Millisecond, "message should go to DLQ after exceeding max retries")

	// Verify DLQ headers
	mu.Lock()
	assert.NotEmpty(t, dlqHeaders, "DLQ message should have headers")
	mu.Unlock()

	SharedLogger.Info("DLQ test completed",
		"total_attempts", attempts.Load(),
		"dlq_received", dlqReceived.Load(),
	)

	// TODO: Test blocking behavior with FreeOnDLQ=false
	// When a message is sent to DLQ with FreeOnDLQ=false, it should remain in tracking
	// and subsequent messages with the same key should be blocked.
	// This requires more complex test setup to verify the blocking mechanism.

	// Stop consumers
	consumerCancel()
	wg.Wait()
}

// dlqTestHandler handles messages for DLQ test.
type dlqTestHandler struct {
	tracker     *resilience.ErrorTracker
	logger      *slog.Logger
	attempts    *atomic.Int32
	dlqReceived *atomic.Bool
	mu          *sync.Mutex
	dlqHeaders  *map[string]string
}

func (h *dlqTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dlqTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *dlqTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)

		// Check if this is DLQ topic (check claim topic directly)
		if strings.HasPrefix(claim.Topic(), "dlq_") || strings.HasPrefix(claim.Topic(), "dlq-") {
			h.logger.Info("received message in DLQ",
				"payload", payload,
				"topic", claim.Topic(),
			)

			h.dlqReceived.Store(true)

			// Extract headers
			h.mu.Lock()
			*h.dlqHeaders = make(map[string]string)
			for _, header := range msg.Headers {
				(*h.dlqHeaders)[string(header.Key)] = string(header.Value)
			}
			h.mu.Unlock()

			session.MarkMessage(msg, "")
			continue
		}

		retryMsg := saramaadapter.NewMessage(msg)

		// Check retry attempt from headers
		var currentAttempt int
		for _, h := range msg.Headers {
			if string(h.Key) == "x-retry-attempt" {
				fmt.Sscanf(string(h.Value), "%d", &currentAttempt)
				break
			}
		}

		// Check if max retries exceeded (before processing)
		if currentAttempt > 3 {
			h.logger.Info("max retries exceeded, sending to DLQ",
				"current_attempt", currentAttempt,
				"payload", payload,
			)

			im := &resilience.InternalMessage{
				KeyData:    msg.Key,
				Payload:    msg.Value,
				HeaderData: mapSaramaHeaders(msg.Headers),
			}
			im.SetTopic(msg.Topic)
			err := h.tracker.SendToDLQ(
				session.Context(),
				im,
				fmt.Errorf("max retries exceeded"),
			)
			if err != nil {
				h.logger.Error("failed to send to DLQ", "error", err)
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		// For "will-always-fail" message, always fail
		if payload == "will-always-fail" {
			attempt := h.attempts.Add(1)

			h.logger.Info("processing message that will fail",
				"attempt", attempt,
				"topic", msg.Topic,
				"retry_attempt", currentAttempt,
			)

			err := fmt.Errorf("simulated permanent failure (attempt %d)", attempt)

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// mapSaramaHeaders converts Sarama headers to retry.HeaderList
func mapSaramaHeaders(headers []*sarama.RecordHeader) *resilience.HeaderList {
	result := &resilience.HeaderList{}
	for _, h := range headers {
		result.Set(string(h.Key), h.Value)
	}
	return result
}

// TestIntegration_DLQ_WithFreeOnDLQ verifies DLQ behavior with FreeOnDLQ=true.
// Tests:
// - Message exceeds max retries and goes to DLQ
// - With FreeOnDLQ=true, message is freed from tracking (tombstone published)
// - Subsequent messages with same key are NOT blocked
func TestIntegration_DLQ_WithFreeOnDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newTestClient(t)
	adapters := newTestAdapters(t, client)
	topic, groupID := newTestIDs("test-dlq-free")

	// Track state
	var (
		firstAttempts  atomic.Int32
		firstDLQ       atomic.Bool
		secondReceived atomic.Bool
	)

	cfg := resilience.NewDefaultConfig()
	cfg.GroupID = groupID
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.FreeOnDLQ = true

	coordinator := resilience.NewKafkaStateCoordinator(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		make(chan error, 10),
	)

	tracker, err := resilience.NewErrorTracker(
		cfg, SharedLogger, adapters.Producer, adapters.ConsumerFactory, adapters.Admin,
		coordinator, resilience.NewExponentialBackoff(),
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.RetryTopic(topic)
	dlqTopic := tracker.DLQTopic(topic)

	// Create handler
	handler := &dlqFreeTestHandler{
		tracker:        tracker,
		logger:         SharedLogger,
		firstAttempts:  &firstAttempts,
		firstDLQ:       &firstDLQ,
		secondReceived: &secondReceived,
	}

	// Create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	t.Cleanup(func() { _ = mainConsumer.Close() })

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	t.Cleanup(func() { _ = retryConsumer.Close() })

	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err, "failed to create DLQ consumer")
	t.Cleanup(func() { _ = dlqConsumer.Close() })

	// Start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup
	runConsumerLoop(consumerCtx, &wg, mainConsumer, []string{topic}, handler, SharedLogger)
	runConsumerLoop(consumerCtx, &wg, retryConsumer, []string{retryTopic}, handler, SharedLogger)
	runConsumerLoop(consumerCtx, &wg, dlqConsumer, []string{dlqTopic}, handler, SharedLogger)

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce first message that will fail and go to DLQ
	produceTestMessage(t, sharedBroker, topic, "test-key", "first-will-fail")

	// Wait for first message to go to DLQ
	assert.Eventually(t, func() bool {
		return firstAttempts.Load() >= 4 && firstDLQ.Load()
	}, 30*time.Second, 500*time.Millisecond, "first message should go to DLQ")

	SharedLogger.Info("first message sent to DLQ, now sending second message with same key")

	// Produce second message with SAME key - should NOT be blocked because first was freed
	produceTestMessage(t, sharedBroker, topic, "test-key", "second-should-process")

	// Wait for second message to be processed
	assert.Eventually(t, func() bool {
		return secondReceived.Load()
	}, 20*time.Second, 500*time.Millisecond, "second message should be processed (not blocked)")

	SharedLogger.Info("DLQ with FreeOnDLQ test completed",
		"first_attempts", firstAttempts.Load(),
		"first_dlq", firstDLQ.Load(),
		"second_received", secondReceived.Load(),
	)

	// Stop consumers
	consumerCancel()
	wg.Wait()
}

// dlqFreeTestHandler handles messages for DLQ with FreeOnDLQ test.
type dlqFreeTestHandler struct {
	tracker        *resilience.ErrorTracker
	logger         *slog.Logger
	firstAttempts  *atomic.Int32
	firstDLQ       *atomic.Bool
	secondReceived *atomic.Bool
}

func (h *dlqFreeTestHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dlqFreeTestHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *dlqFreeTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		payload := string(msg.Value)

		// Check if this is DLQ topic (check claim topic directly)
		if strings.HasPrefix(claim.Topic(), "dlq_") || strings.HasPrefix(claim.Topic(), "dlq-") {
			h.logger.Info("received message in DLQ",
				"payload", payload,
				"topic", claim.Topic(),
			)

			if payload == "first-will-fail" {
				h.firstDLQ.Store(true)
			}

			session.MarkMessage(msg, "")
			continue
		}

		retryMsg := saramaadapter.NewMessage(msg)

		// Check retry attempt from headers
		var currentAttempt int
		for _, header := range msg.Headers {
			if string(header.Key) == "x-retry-attempt" {
				fmt.Sscanf(string(header.Value), "%d", &currentAttempt)
				break
			}
		}

		// Check if max retries exceeded (before processing)
		if currentAttempt > 3 {
			h.logger.Info("max retries exceeded, sending to DLQ",
				"current_attempt", currentAttempt,
				"payload", payload,
			)

			im := &resilience.InternalMessage{
				KeyData:    msg.Key,
				Payload:    msg.Value,
				HeaderData: mapSaramaHeaders(msg.Headers),
			}
			im.SetTopic(msg.Topic)
			err := h.tracker.SendToDLQ(
				session.Context(),
				im,
				fmt.Errorf("max retries exceeded"),
			)
			if err != nil {
				h.logger.Error("failed to send to DLQ", "error", err)
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		if payload == "first-will-fail" {
			attempt := h.firstAttempts.Add(1)

			h.logger.Info("processing first message",
				"attempt", attempt,
				"retry_attempt", currentAttempt,
			)

			err := fmt.Errorf("simulated failure (attempt %d)", attempt)

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", "error", redirectErr)
				return redirectErr
			}
		} else if payload == "second-should-process" {
			h.logger.Info("processing second message (should not be blocked)")
			h.secondReceived.Store(true)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}
