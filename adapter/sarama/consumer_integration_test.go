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
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
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

// TestIntegration_SaramaConsumerRebalance verifies that ConsumerAdapter automatically
// restarts consumption after a rebalance occurs.
func TestIntegration_SaramaConsumerRebalance(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker, cleanup := setupKafkaContainer(t, ctx)
	defer cleanup()

	topic := fmt.Sprintf("test-rebalance-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("test-rebalance-group-%d", time.Now().UnixNano())

	// create topic with 2 partitions to facilitate rebalancing
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	require.NoError(t, err)
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, false)
	require.NoError(t, err)

	// create Consumer 1
	client1, err := sarama.NewClient([]string{broker}, config)
	require.NoError(t, err)
	defer client1.Close()

	factory1 := saramaadapter.NewConsumerFactory(client1)
	consumer1, err := factory1.NewConsumer(groupID)
	require.NoError(t, err)
	defer consumer1.Close()

	handler1 := &integrationConsumerHandler{
		received: make(chan resilience.Message, 100),
	}

	consumeCtx1, consumeCancel1 := context.WithCancel(ctx)
	defer consumeCancel1()

	done1 := make(chan error, 1)
	go func() {
		done1 <- consumer1.Consume(consumeCtx1, []string{topic}, handler1)
	}()

	// produce initial message and verify Consumer 1 receives it
	produceTestMessage(t, broker, topic, "key-1", "value-1")
	select {
	case msg := <-handler1.received:
		assert.Equal(t, "value-1", string(msg.Value()))
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for message 1")
	}

	// create Consumer 2 in the same group to trigger rebalance
	client2, err := sarama.NewClient([]string{broker}, config)
	require.NoError(t, err)
	defer client2.Close()

	factory2 := saramaadapter.NewConsumerFactory(client2)
	consumer2, err := factory2.NewConsumer(groupID)
	require.NoError(t, err)
	defer consumer2.Close()

	handler2 := &integrationConsumerHandler{
		received: make(chan resilience.Message, 100),
	}

	consumeCtx2, consumeCancel2 := context.WithCancel(ctx)
	defer consumeCancel2()

	done2 := make(chan error, 1)
	go func() {
		done2 <- consumer2.Consume(consumeCtx2, []string{topic}, handler2)
	}()

	// Continuously produce messages until we confirm both consumers are active.
	// This confirms that rebalance has completed and partitions are distributed.
	bothActive := false
	messagesSent := 0
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Drain any previous messages first to avoid confusion
loop:
	for {
		select {
		case <-handler1.received:
			// Drain existing
		case <-handler2.received:
			// Drain existing
		case <-timeout:
			t.Fatalf("timed out waiting for rebalance. Stats - C1: %d, C2: %d",
				handler1.count.Load(), handler2.count.Load())
		case <-ticker.C:
			messagesSent++
			produceTestMessage(t, broker, topic,
				fmt.Sprintf("poll-%d", messagesSent),
				fmt.Sprintf("value-%d", messagesSent))

			// Check if both have received messages *total* (including initial one for C1)
			// We want C1 > 1 (initial + at least one new) and C2 > 0
			c1Count := handler1.count.Load()
			c2Count := handler2.count.Load()

			if c1Count > 1 && c2Count > 0 {
				bothActive = true
				break loop
			}
		}
	}

	assert.True(t, bothActive, "both consumers should be active after rebalance")

	// Verify Consumer 1 is still running (done1 hasn't received anything)
	select {
	case err := <-done1:
		t.Fatalf("Consumer 1 stopped unexpectedly: %v", err)
	default:
		// OK
	}

	consumeCancel2()
	<-done2
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
