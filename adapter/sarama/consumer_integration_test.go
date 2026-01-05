//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	saramaadapter "github.com/vmyroslav/kafetrain/adapter/sarama"
	"github.com/vmyroslav/kafetrain/resilience"
)

// TestIntegration_SaramaConsumer verifies that ConsumerAdapter can consume messages from a real Kafka broker.
func TestIntegration_SaramaConsumer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	topic := fmt.Sprintf("test-consumer-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

	// create Topic
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	require.NoError(t, err)
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	require.NoError(t, err)

	// produce Message
	produceTestMessage(t, broker, topic, "key-1", "value-1")

	// create Consumer
	client, err := sarama.NewClient([]string{broker}, config)
	require.NoError(t, err)
	defer client.Close()

	factory := saramaadapter.NewConsumerFactory(client)
	consumer, err := factory.NewConsumer(groupID)
	require.NoError(t, err)
	defer consumer.Close()

	handler := &integrationConsumerHandler{
		received: make(chan resilience.Message, 1),
	}

	consumeCtx, consumeCancel := context.WithCancel(ctx)

	done := make(chan error, 1)
	go func() {
		done <- consumer.Consume(consumeCtx, []string{topic}, handler)
	}()

	// verify Message Received
	select {
	case msg := <-handler.received:
		assert.Equal(t, "key-1", string(msg.Key()))
		assert.Equal(t, "value-1", string(msg.Value()))
		assert.Equal(t, topic, msg.Topic())
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	// verify Context Cancellation Stops Consumer
	consumeCancel()
	err = <-done
	assert.ErrorIs(t, err, context.Canceled)
}

// integrationConsumerHandler implements resilience.ConsumerHandler for integration tests.
type integrationConsumerHandler struct {
	received chan resilience.Message
	count    atomic.Int32
}

func (h *integrationConsumerHandler) Handle(ctx context.Context, msg resilience.Message) error {
	h.count.Add(1)

	select {
	case h.received <- msg:
	default:
		// Channel full, just count it
	}

	return nil
}
