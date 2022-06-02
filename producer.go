package kafetrain

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type Producer struct {
	brokers        []string
	saramaProducer sarama.SyncProducer
	cfg            *sarama.Config
}

var topicAlreadyExists = errors.New("topic already exists")

// NewProducer creates a new instance of Producer.
func NewProducer(cfg Config) (*Producer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Net.WriteTimeout = 1 * time.Second
	saramaConfig.Metadata.Retry.Max = 5

	p := Producer{
		brokers: cfg.Brokers,
		cfg:     saramaConfig,
	}

	saramaProducer, err := sarama.NewSyncProducer(cfg.Brokers, p.cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p.saramaProducer = saramaProducer

	return &p, nil
}

func (p *Producer) Publish(_ context.Context, msg Message) error {
	var sh []sarama.RecordHeader
	for _, v := range msg.Headers {
		sh = append(sh, sarama.RecordHeader{
			Key:   v.Key,
			Value: v.Value,
		})
	}

	sm := sarama.ProducerMessage{
		Topic:   msg.topic,
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Payload),
		Headers: sh,
	}

	_, _, err := p.saramaProducer.SendMessage(&sm)

	return errors.WithStack(err)
}

func (p *Producer) CreateTopic(topic string) error {
	admin, err := sarama.NewClusterAdmin(p.brokers, p.cfg)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		sErr, ok := err.(*sarama.TopicError)

		if !(ok && sErr.Err == sarama.ErrTopicAlreadyExists) {
			return topicAlreadyExists
		}

		return errors.WithStack(err)
	}

	return nil
}
