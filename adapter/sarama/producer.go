package sarama

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafka-resilience/resilience"
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
func (p *ProducerAdapter) Produce(_ context.Context, topic string, msg resilience.Message) error {
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

// Close implements retry.Producer interface.
func (p *ProducerAdapter) Close() error {
	return p.producer.Close()
}
