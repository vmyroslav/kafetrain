//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

const kafkaImage = "confluentinc/confluent-local:7.6.0"

var (
	sharedBroker string
	SharedLogger *slog.Logger
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// setup shared logger
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	SharedLogger = slog.New(slog.NewTextHandler(os.Stdout, opts))

	// start shared Kafka container
	kafkaContainer, err := kafka.Run(ctx,
		kafkaImage,
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		fmt.Printf("failed to start shared Kafka container: %v\n", err)
		os.Exit(1)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		fmt.Printf("failed to get shared Kafka brokers: %v\n", err)
		kafkaContainer.Terminate(ctx)
		os.Exit(1)
	}

	sharedBroker = brokers[0]
	fmt.Printf("Shared Kafka container started at: %s\n", sharedBroker)

	// run tests
	code := m.Run()

	// cleanup
	if err := kafkaContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate shared Kafka container: %v\n", err)
	}

	os.Exit(code)
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
