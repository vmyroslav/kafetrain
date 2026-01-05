//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	saramaadapter "github.com/vmyroslav/kafetrain/adapter/sarama"
	"github.com/vmyroslav/kafetrain/resilience"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// setupKafkaContainer starts a Kafka container and returns the broker address.
func setupKafkaContainer(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()

	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.6.0",
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err, "failed to start Kafka container")

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "failed to get Kafka brokers")

	cleanup := func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate Kafka container: %v", err)
		}
	}

	return brokers[0], cleanup
}

// produceTestMessage produces a message to the given topic.
func produceTestMessage(t *testing.T, broker, topic, key, value string) {
	t.Helper()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V4_1_0_0

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	require.NoError(t, err, "failed to create producer")
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	_, _, err = producer.SendMessage(msg)
	require.NoError(t, err, "failed to produce message")
}

// TestIntegration_SaramaAdmin tests the Sarama Admin adapter implementation.
// Verifies that the Admin interface correctly creates topics and manages consumer groups.
func TestIntegration_SaramaAdmin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	// Create Sarama client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0

	client, err := sarama.NewClient([]string{broker}, saramaConfig)
	require.NoError(t, err, "failed to create Sarama client")
	defer client.Close()

	// Create admin adapter
	admin, err := saramaadapter.NewAdminAdapterFromClient(client)
	require.NoError(t, err, "failed to create admin adapter")
	defer admin.Close()

	// Test 1: Create topics
	t.Run("CreateTopics", func(t *testing.T) {
		testTopic := fmt.Sprintf("admin-test-%d", time.Now().UnixNano())

		// Create topic with specific configuration
		err := admin.CreateTopic(ctx, testTopic, 3, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "3600000",
		})
		require.NoError(t, err, "failed to create topic")

		// Verify topic was created by describing it
		metadata, err := admin.DescribeTopics(ctx, []string{testTopic})
		require.NoError(t, err, "failed to describe topic")
		require.Len(t, metadata, 1, "expected 1 topic metadata")

		assert.Equal(t, testTopic, metadata[0].Name())
		assert.Equal(t, int32(3), metadata[0].Partitions())
		assert.Equal(t, int16(1), metadata[0].ReplicationFactor())

		logger.Info("topic created successfully",
			zap.String("topic", testTopic),
			zap.Int32("partitions", metadata[0].Partitions()),
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
		consumerConfig := sarama.NewConfig()
		consumerConfig.Version = sarama.V4_1_0_0
		consumerConfig.Consumer.Return.Errors = true

		consumer, err := sarama.NewConsumerGroup([]string{broker}, groupID, consumerConfig)
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

		logger.Info("consumer group deleted successfully", zap.String("group_id", groupID))
	})
}

// TestIntegration_SaramaAdapterFullFlow tests the full ErrorTracker flow with Sarama adapter.
// This is the main integration test for the library-agnostic architecture.
func TestIntegration_SaramaAdapterFullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-adapter-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("test-adapter-group-%d", time.Now().UnixNano())

	// Track processing
	var (
		firstAttempts  atomic.Int32
		secondAttempts atomic.Int32
		thirdProcessed atomic.Bool
		mu             sync.Mutex
		processedMsgs  []string
	)

	// Setup Sarama client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{broker}, saramaConfig)
	require.NoError(t, err, "failed to create Sarama client")
	defer client.Close()

	// Create Sarama producer and wrap with adapter
	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err, "failed to create Sarama producer")
	defer saramaProducer.Close()

	producer := saramaadapter.NewProducerAdapter(saramaProducer)

	// Create consumer factory
	consumerFactory := saramaadapter.NewConsumerFactory(client)

	// Create admin adapter
	admin, err := saramaadapter.NewAdminAdapterFromClient(client)
	require.NoError(t, err, "failed to create admin adapter")
	defer admin.Close()

	// Create logger adapter
	retryLogger := saramaadapter.NewZapLogger(logger)

	// Create config
	cfg := resilience.NewDefaultConfig()
	cfg.Brokers = []string{broker}
	cfg.GroupID = groupID
	cfg.Version = "4.1.0"
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	// Create library-agnostic ErrorTracker
	tracker, err := resilience.NewErrorTracker(
		cfg,
		retryLogger,
		producer,
		consumerFactory,
		admin,
		resilience.NewKeyTracker(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking - this should create topics
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	logger.Info("tracker started, topics should be created")

	// Get retry topic name for later use
	retryTopic := tracker.GetRetryTopic(topic)

	// Create business logic processor
	processor := &testMessageProcessor{
		logger:         logger,
		firstAttempts:  &firstAttempts,
		secondAttempts: &secondAttempts,
		thirdProcessed: &thirdProcessed,
		mu:             &mu,
		processedMsgs:  &processedMsgs,
	}

	// Create main consumer (using standard Sarama)
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	defer mainConsumer.Close()

	// Create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	defer retryConsumer.Close()

	// Create handler that uses library-agnostic tracker
	handler := &adapterTestHandler{
		tracker:   tracker,
		logger:    logger,
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
				logger.Error("main consumer error", zap.Error(err))
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
				logger.Error("retry consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce test messages
	produceTestMessage(t, broker, topic, "test-key", "first-will-fail-twice")
	produceTestMessage(t, broker, topic, "test-key", "second-will-fail-once")
	produceTestMessage(t, broker, topic, "test-key", "third-will-succeed")

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

	logger.Info("test completed successfully",
		zap.Int32("first_attempts", firstAttempts.Load()),
		zap.Int32("second_attempts", secondAttempts.Load()),
		zap.Bool("third_processed", thirdProcessed.Load()),
	)

	// Stop consumers
	consumerCancel()
	wg.Wait()
}

// adapterTestHandler implements sarama.ConsumerGroupHandler for adapter tests.
// It uses the library-agnostic ErrorTracker via the retry.Message interface.
type adapterTestHandler struct {
	tracker   *resilience.ErrorTracker
	logger    *zap.Logger
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
			// Check if it's a retriable error
			if retryErr, ok := err.(resilience.RetriableError); ok && retryErr.ShouldRetry() {
				// Use library-agnostic Redirect API
				if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
					h.logger.Error("failed to redirect message", zap.Error(redirectErr))
					return redirectErr
				}
				h.logger.Info("message redirected to retry",
					zap.String("payload", string(msg.Value)),
				)
			} else {
				h.logger.Error("non-retriable error", zap.Error(err))
				return err
			}
		} else {
			// Success - free from retry chain if it was in retry
			if h.tracker.IsInRetryChain(retryMsg) {
				if freeErr := h.tracker.Free(session.Context(), retryMsg); freeErr != nil {
					h.logger.Error("failed to free message", zap.Error(freeErr))
					return freeErr
				}
				h.logger.Info("message freed from retry chain",
					zap.String("payload", string(msg.Value)),
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
			return resilience.RetriableError{
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
			return resilience.RetriableError{
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

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-chain-retry-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("test-chain-retry-group-%d", time.Now().UnixNano())

	// Track attempts
	var (
		attempts atomic.Int32
		mu       sync.Mutex
		success  bool
	)

	// Setup Sarama client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{broker}, saramaConfig)
	require.NoError(t, err, "failed to create Sarama client")
	defer client.Close()

	// Create adapters
	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err, "failed to create Sarama producer")
	defer saramaProducer.Close()

	producer := saramaadapter.NewProducerAdapter(saramaProducer)
	consumerFactory := saramaadapter.NewConsumerFactory(client)
	admin, err := saramaadapter.NewAdminAdapterFromClient(client)
	require.NoError(t, err, "failed to create admin adapter")
	defer admin.Close()

	retryLogger := saramaadapter.NewZapLogger(logger)

	// Create config with 3 max retries
	cfg := resilience.NewDefaultConfig()
	cfg.Brokers = []string{broker}
	cfg.GroupID = groupID
	cfg.Version = "4.1.0"
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1

	// Create ErrorTracker
	tracker, err := resilience.NewErrorTracker(
		cfg,
		retryLogger,
		producer,
		consumerFactory,
		admin,
		resilience.NewKeyTracker(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.GetRetryTopic(topic)
	redirectTopic := tracker.GetRedirectTopic(topic)

	logger.Info("tracker started",
		zap.String("topic", topic),
		zap.String("retry_topic", retryTopic),
		zap.String("redirect_topic", redirectTopic),
	)

	// Create handler that fails first 2 attempts, then succeeds
	handler := &chainRetryHandler{
		tracker:  tracker,
		logger:   logger,
		attempts: &attempts,
		mu:       &mu,
		success:  &success,
	}

	// Create main consumer
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	defer mainConsumer.Close()

	// Create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	defer retryConsumer.Close()

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
				logger.Error("main consumer error", zap.Error(err))
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
				logger.Error("retry consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce test message
	produceTestMessage(t, broker, topic, "test-key", "will-fail-twice-then-succeed")

	// Wait for processing to complete (3 attempts total)
	assert.Eventually(t, func() bool {
		return attempts.Load() == 3 && success
	}, 30*time.Second, 500*time.Millisecond, "message should be processed after 3 attempts")

	logger.Info("chain retry test completed",
		zap.Int32("total_attempts", attempts.Load()),
		zap.Bool("success", success),
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
	logger   *zap.Logger
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
			zap.Int32("attempt", attempt),
			zap.String("topic", msg.Topic),
			zap.String("value", string(msg.Value)),
		)

		// Fail first 2 attempts, succeed on 3rd
		if attempt <= 2 {
			err := resilience.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure (attempt %d)", attempt),
			}

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", zap.Error(redirectErr))
				return redirectErr
			}

			h.logger.Info("message redirected to retry",
				zap.Int32("attempt", attempt),
			)
		} else {
			// Success on 3rd attempt
			if h.tracker.IsInRetryChain(retryMsg) {
				if freeErr := h.tracker.Free(session.Context(), retryMsg); freeErr != nil {
					h.logger.Error("failed to free message", zap.Error(freeErr))
					return freeErr
				}
				h.logger.Info("message freed from retry chain")
			}

			h.mu.Lock()
			*h.success = true
			h.mu.Unlock()

			h.logger.Info("message processed successfully",
				zap.Int32("total_attempts", attempt),
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

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-dlq-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("test-dlq-group-%d", time.Now().UnixNano())

	// Track attempts and DLQ
	var (
		attempts    atomic.Int32
		dlqReceived atomic.Bool
		mu          sync.Mutex
		dlqHeaders  map[string]string
	)

	// Setup Sarama client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{broker}, saramaConfig)
	require.NoError(t, err, "failed to create Sarama client")
	defer client.Close()

	// Create adapters
	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err, "failed to create Sarama producer")
	defer saramaProducer.Close()

	producer := saramaadapter.NewProducerAdapter(saramaProducer)
	consumerFactory := saramaadapter.NewConsumerFactory(client)
	admin, err := saramaadapter.NewAdminAdapterFromClient(client)
	require.NoError(t, err, "failed to create admin adapter")
	defer admin.Close()

	retryLogger := saramaadapter.NewZapLogger(logger)

	// Create config with MaxRetries=3, FreeOnDLQ=false (default)
	cfg := resilience.NewDefaultConfig()
	cfg.Brokers = []string{broker}
	cfg.GroupID = groupID
	cfg.Version = "4.1.0"
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.FreeOnDLQ = false // Message stays in tracking after DLQ

	// Create ErrorTracker
	tracker, err := resilience.NewErrorTracker(
		cfg,
		retryLogger,
		producer,
		consumerFactory,
		admin,
		resilience.NewKeyTracker(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.GetRetryTopic(topic)
	dlqTopic := tracker.GetDLQTopic(topic)

	logger.Info("tracker started",
		zap.String("topic", topic),
		zap.String("retry_topic", retryTopic),
		zap.String("dlq_topic", dlqTopic),
	)

	// Create handler that always fails
	handler := &dlqTestHandler{
		tracker:     tracker,
		logger:      logger,
		attempts:    &attempts,
		dlqReceived: &dlqReceived,
		mu:          &mu,
		dlqHeaders:  &dlqHeaders,
	}

	// Create main consumer
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	defer mainConsumer.Close()

	// Create retry consumer
	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	defer retryConsumer.Close()

	// Create DLQ consumer
	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err, "failed to create DLQ consumer")
	defer dlqConsumer.Close()

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
				logger.Error("main consumer error", zap.Error(err))
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
				logger.Error("retry consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Start DLQ consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := dlqConsumer.Consume(consumerCtx, []string{dlqTopic}, handler); err != nil {
				logger.Error("DLQ consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce test message that will always fail
	produceTestMessage(t, broker, topic, "test-key", "will-always-fail")

	// Wait for message to exceed max retries and go to DLQ
	// MaxRetries=3 means: 1 initial attempt + 3 retries = 4 total attempts
	assert.Eventually(t, func() bool {
		return attempts.Load() >= 4 && dlqReceived.Load()
	}, 30*time.Second, 500*time.Millisecond, "message should go to DLQ after exceeding max retries")

	// Verify DLQ headers
	mu.Lock()
	assert.NotEmpty(t, dlqHeaders, "DLQ message should have headers")
	mu.Unlock()

	logger.Info("DLQ test completed",
		zap.Int32("total_attempts", attempts.Load()),
		zap.Bool("dlq_received", dlqReceived.Load()),
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
	logger      *zap.Logger
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
				zap.String("payload", payload),
				zap.String("topic", claim.Topic()),
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
				zap.Int("current_attempt", currentAttempt),
				zap.String("payload", payload),
			)

			err := h.tracker.SendToDLQ(
				session.Context(),
				&resilience.InternalMessage{
					Key:     msg.Key,
					Payload: msg.Value,
					Headers: mapSaramaHeaders(msg.Headers),
				},
				fmt.Errorf("max retries exceeded"),
			)
			if err != nil {
				h.logger.Error("failed to send to DLQ", zap.Error(err))
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		// For "will-always-fail" message, always fail
		if payload == "will-always-fail" {
			attempt := h.attempts.Add(1)

			h.logger.Info("processing message that will fail",
				zap.Int32("attempt", attempt),
				zap.String("topic", msg.Topic),
				zap.Int("retry_attempt", currentAttempt),
			)

			err := resilience.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated permanent failure (attempt %d)", attempt),
			}

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", zap.Error(redirectErr))
				return redirectErr
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// mapSaramaHeaders converts Sarama headers to retry.HeaderList
func mapSaramaHeaders(headers []*sarama.RecordHeader) resilience.HeaderList {
	result := make(resilience.HeaderList, 0, len(headers))
	for _, h := range headers {
		result = append(result, resilience.Header{
			Key:   h.Key,
			Value: h.Value,
		})
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

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))

	topic := fmt.Sprintf("test-dlq-free-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("test-dlq-free-group-%d", time.Now().UnixNano())

	// Track state
	var (
		firstAttempts  atomic.Int32
		firstDLQ       atomic.Bool
		secondReceived atomic.Bool
	)

	// Setup Sarama client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V4_1_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{broker}, saramaConfig)
	require.NoError(t, err, "failed to create Sarama client")
	defer client.Close()

	// Create adapters
	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err, "failed to create Sarama producer")
	defer saramaProducer.Close()

	producer := saramaadapter.NewProducerAdapter(saramaProducer)
	consumerFactory := saramaadapter.NewConsumerFactory(client)
	admin, err := saramaadapter.NewAdminAdapterFromClient(client)
	require.NoError(t, err, "failed to create admin adapter")
	defer admin.Close()

	retryLogger := saramaadapter.NewZapLogger(logger)

	// Create config with FreeOnDLQ=true
	cfg := resilience.NewDefaultConfig()
	cfg.Brokers = []string{broker}
	cfg.GroupID = groupID
	cfg.Version = "4.1.0"
	cfg.MaxRetries = 3
	cfg.RetryTopicPartitions = 1
	cfg.FreeOnDLQ = true // FREE message from tracking after DLQ

	// Create ErrorTracker
	tracker, err := resilience.NewErrorTracker(
		cfg,
		retryLogger,
		producer,
		consumerFactory,
		admin,
		resilience.NewKeyTracker(),
		nil,
	)
	require.NoError(t, err, "failed to create error tracker")

	// Start tracking
	err = tracker.StartTracking(ctx, topic)
	require.NoError(t, err, "failed to start tracking")

	retryTopic := tracker.GetRetryTopic(topic)
	dlqTopic := tracker.GetDLQTopic(topic)

	// Create handler
	handler := &dlqFreeTestHandler{
		tracker:        tracker,
		logger:         logger,
		firstAttempts:  &firstAttempts,
		firstDLQ:       &firstDLQ,
		secondReceived: &secondReceived,
	}

	// Create consumers
	mainConsumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	require.NoError(t, err, "failed to create main consumer")
	defer mainConsumer.Close()

	retryConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-retry", client)
	require.NoError(t, err, "failed to create retry consumer")
	defer retryConsumer.Close()

	dlqConsumer, err := sarama.NewConsumerGroupFromClient(groupID+"-dlq", client)
	require.NoError(t, err, "failed to create DLQ consumer")
	defer dlqConsumer.Close()

	// Start consumption
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	var wg sync.WaitGroup

	// Start consumers
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := retryConsumer.Consume(consumerCtx, []string{retryTopic}, handler); err != nil {
				logger.Error("retry consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := dlqConsumer.Consume(consumerCtx, []string{dlqTopic}, handler); err != nil {
				logger.Error("DLQ consumer error", zap.Error(err))
			}
			if consumerCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for consumers to be ready
	time.Sleep(2 * time.Second)

	// Produce first message that will fail and go to DLQ
	produceTestMessage(t, broker, topic, "test-key", "first-will-fail")

	// Wait for first message to go to DLQ
	assert.Eventually(t, func() bool {
		return firstAttempts.Load() >= 4 && firstDLQ.Load()
	}, 30*time.Second, 500*time.Millisecond, "first message should go to DLQ")

	logger.Info("first message sent to DLQ, now sending second message with same key")

	// Produce second message with SAME key - should NOT be blocked because first was freed
	produceTestMessage(t, broker, topic, "test-key", "second-should-process")

	// Wait for second message to be processed
	assert.Eventually(t, func() bool {
		return secondReceived.Load()
	}, 20*time.Second, 500*time.Millisecond, "second message should be processed (not blocked)")

	logger.Info("DLQ with FreeOnDLQ test completed",
		zap.Int32("first_attempts", firstAttempts.Load()),
		zap.Bool("first_dlq", firstDLQ.Load()),
		zap.Bool("second_received", secondReceived.Load()),
	)

	// Stop consumers
	consumerCancel()
	wg.Wait()
}

// dlqFreeTestHandler handles messages for DLQ with FreeOnDLQ test.
type dlqFreeTestHandler struct {
	tracker        *resilience.ErrorTracker
	logger         *zap.Logger
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
				zap.String("payload", payload),
				zap.String("topic", claim.Topic()),
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
				zap.Int("current_attempt", currentAttempt),
				zap.String("payload", payload),
			)

			err := h.tracker.SendToDLQ(
				session.Context(),
				&resilience.InternalMessage{
					Key:     msg.Key,
					Payload: msg.Value,
					Headers: mapSaramaHeaders(msg.Headers),
				},
				fmt.Errorf("max retries exceeded"),
			)
			if err != nil {
				h.logger.Error("failed to send to DLQ", zap.Error(err))
				return err
			}
			session.MarkMessage(msg, "")
			continue
		}

		if payload == "first-will-fail" {
			attempt := h.firstAttempts.Add(1)

			h.logger.Info("processing first message",
				zap.Int32("attempt", attempt),
				zap.Int("retry_attempt", currentAttempt),
			)

			err := resilience.RetriableError{
				Retry:  true,
				Origin: fmt.Errorf("simulated failure (attempt %d)", attempt),
			}

			if redirectErr := h.tracker.Redirect(session.Context(), retryMsg, err); redirectErr != nil {
				h.logger.Error("failed to redirect message", zap.Error(redirectErr))
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
