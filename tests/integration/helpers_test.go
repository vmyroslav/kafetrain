//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/vmyroslav/kafetrain/resilience"
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

// createTestConfig creates a Config for testing with the given broker address.
func createTestConfig(broker string, groupID string) retryold.Config {
	return retryold.Config{
		Brokers:              []string{broker},
		Version:              "4.1.0",
		GroupID:              groupID,
		ClientID:             "test-client",
		InitialOffset:        retryold.OffsetOldest,
		MaxProcessingTime:    100,
		RetryTopicPrefix:     "retry",
		RedirectTopicPrefix:  "redirect",
		DLQTopicPrefix:       "dlq",
		MaxRetries:           3,
		RetryTopicPartitions: 1,
		BuffSize:             256,
		FreeOnDLQ:            false, // Default to strict ordering
	}
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
