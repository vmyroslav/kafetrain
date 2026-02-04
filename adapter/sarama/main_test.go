//go:build integration

package sarama_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	saramaadapter "github.com/vmyroslav/kafka-resilience/adapter/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
)

const kafkaImage = "confluentinc/confluent-local:7.6.0"

var (
	sharedBroker string
	sharedKafka  *kafka.KafkaContainer
	sharedLogger *slog.Logger
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	sharedLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// start shared Kafka container
	var err error
	sharedKafka, err = kafka.Run(ctx, kafkaImage, kafka.WithClusterID("test-cluster"))
	if err != nil {
		fmt.Printf("failed to start Kafka container: %v\n", err)
		os.Exit(1)
	}

	brokers, err := sharedKafka.Brokers(ctx)
	if err != nil {
		fmt.Printf("failed to get Kafka brokers: %v\n", err)
		_ = sharedKafka.Terminate(ctx)
		os.Exit(1)
	}
	sharedBroker = brokers[0]
	fmt.Printf("Kafka container started at: %s\n", sharedBroker)

	// run tests
	code := m.Run()

	// cleanup
	if err := sharedKafka.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate Kafka container: %v\n", err)
	}

	os.Exit(code)
}

// setupIsolatedKafkaContainer starts a separate Kafka container for tests
// that need to control the container lifecycle (stop/restart).
func setupIsolatedKafkaContainer(t *testing.T, ctx context.Context) (string, *kafka.KafkaContainer, func()) {
	t.Helper()

	container, err := kafka.Run(ctx, kafkaImage, kafka.WithClusterID("test-cluster-isolated"))
	require.NoError(t, err)

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	return brokers[0], container, func() {
		_ = container.Terminate(ctx)
	}
}

// produceTestMessage produces a message to the given topic.
func produceTestMessage(t *testing.T, broker, topic, key, value string) {
	t.Helper()

	producer, err := sarama.NewSyncProducer([]string{broker}, newTestSaramaConfig())
	require.NoError(t, err)
	defer producer.Close()

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	})
	require.NoError(t, err)
}

// newTestSaramaConfig creates a standard Sarama config for tests.
func newTestSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V4_1_0_0
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	return cfg
}

// newTestClient creates a Sarama client connected to the shared broker.
// Cleanup is automatic via t.Cleanup.
func newTestClient(t *testing.T) sarama.Client {
	t.Helper()
	client, err := sarama.NewClient([]string{sharedBroker}, newTestSaramaConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

// testAdapters holds all adapters needed for tests.
type testAdapters struct {
	Producer        resilience.Producer
	ConsumerFactory resilience.ConsumerFactory
	Admin           resilience.Admin
}

// newTestAdapters creates all adapters from a Sarama client.
// Cleanup is automatic via t.Cleanup.
func newTestAdapters(t *testing.T, client sarama.Client) *testAdapters {
	t.Helper()

	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = saramaProducer.Close() })

	admin, err := saramaadapter.NewAdminAdapter(client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	return &testAdapters{
		Producer:        saramaadapter.NewProducerAdapter(saramaProducer),
		ConsumerFactory: saramaadapter.NewConsumerFactory(client),
		Admin:           admin,
	}
}

// newTestIDs generates unique topic and group IDs for a test.
func newTestIDs(prefix string) (topic, groupID string) {
	suffix := time.Now().UnixNano()

	return fmt.Sprintf("%s-topic-%d", prefix, suffix),
		fmt.Sprintf("%s-group-%d", prefix, suffix)
}

// runConsumerLoop starts a consumer in a goroutine that restarts on errors.
// Returns a channel that closes when consumer is ready (Setup called = joined group and got partitions).
// The goroutine exits when ctx is cancelled.
func runConsumerLoop(ctx context.Context, wg *sync.WaitGroup, consumer sarama.ConsumerGroup,
	topics []string, handler sarama.ConsumerGroupHandler, logger *slog.Logger,
) <-chan struct{} {
	ready := make(chan struct{})
	wrapped := &readyHandler{
		handler: handler,
		ready:   ready,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumer.Consume(ctx, topics, wrapped); err != nil {
				logger.Error("consumer error", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return ready
}

// readyHandler wraps a ConsumerGroupHandler and signals when Setup is called.
type readyHandler struct {
	handler sarama.ConsumerGroupHandler
	ready   chan struct{}
	once    sync.Once
}

func (h *readyHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.once.Do(func() { close(h.ready) })
	return h.handler.Setup(session)
}

func (h *readyHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return h.handler.Cleanup(session)
}

func (h *readyHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return h.handler.ConsumeClaim(session, claim)
}

// waitForReady waits for all provided ready channels to close or fails on context timeout.
func waitForReady(t *testing.T, ctx context.Context, readyChans ...<-chan struct{}) {
	t.Helper()
	for i, ready := range readyChans {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for consumer %d to be ready", i)
		}
	}
}
