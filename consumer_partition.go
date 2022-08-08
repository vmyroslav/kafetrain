package kafetrain

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// KafkaPartitionConsumer consumes topic.
type KafkaPartitionConsumer struct {
	cfg    Config
	logger *zap.Logger

	sConsumer   sarama.Consumer
	sClient     sarama.Client
	middlewares []Middleware
}

func NewKafkaPartitionConsumer(
	cfg Config,
	logger *zap.Logger,
	middlewares []Middleware,
	topic string,
	partitions []int32,
) (*KafkaPartitionConsumer, error) {
	sc, err := createSaramaConfig(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama configs")
	}

	sConsumer, err := sarama.NewConsumer(cfg.Brokers, sc)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama configs")
	}

	client, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama configs")
	}
	//sCons
	client.Close()

	return &KafkaPartitionConsumer{
		cfg:         cfg,
		logger:      logger,
		sConsumer:   sConsumer,
		middlewares: middlewares,
	}, nil
}

func (k *KafkaPartitionConsumer) Consume(ctx context.Context, topic string, messageHandler MessageHandler) error {
	a, err := k.sConsumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return errors.Wrapf(err, "unable to consume partition")
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case msg := <-a.Messages():
			messageHandler(msg)
		}

	}
	//	//TODO implement me
	return nil
}

func (k *KafkaPartitionConsumer) Close() error {
	//TODO implement me
	panic("implement me")
}
