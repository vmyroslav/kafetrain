package sarama

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafetrain/resilience"
)

// ProducerAdapter wraps sarama.SyncProducer to implement retry.Producer interface.
type ProducerAdapter struct {
	producer sarama.SyncProducer
}

// NewProducerAdapter creates a retry.Producer from a Sarama SyncProducer.
func NewProducerAdapter(producer sarama.SyncProducer) resilience.Producer {
	return &ProducerAdapter{producer: producer}
}

// Produce implements retry.Producer interface.
func (p *ProducerAdapter) Produce(ctx context.Context, topic string, msg resilience.Message) error {
	saramaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(msg.Key()),
		Value:     sarama.ByteEncoder(msg.Value()),
		Headers:   ToSaramaHeaders(msg.Headers()),
		Timestamp: msg.Timestamp(),
	}

	_, _, err := p.producer.SendMessage(saramaMsg)
	return err
}

// ProduceBatch implements retry.Producer interface.
func (p *ProducerAdapter) ProduceBatch(ctx context.Context, messages []resilience.MessageTarget) error {
	if len(messages) == 0 {
		return nil
	}

	saramaMsgs := make([]*sarama.ProducerMessage, len(messages))
	for i, mt := range messages {
		saramaMsgs[i] = &sarama.ProducerMessage{
			Topic:     mt.Topic,
			Key:       sarama.ByteEncoder(mt.Message.Key()),
			Value:     sarama.ByteEncoder(mt.Message.Value()),
			Headers:   ToSaramaHeaders(mt.Message.Headers()),
			Timestamp: mt.Message.Timestamp(),
		}
	}

	return p.producer.SendMessages(saramaMsgs)
}

// Close implements retry.Producer interface.
func (p *ProducerAdapter) Close() error {
	return p.producer.Close()
}
