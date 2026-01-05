package sarama

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafetrain/resilience"
)

// ConsumerAdapter wraps sarama.ConsumerGroup to implement retry.Consumer interface.
type ConsumerAdapter struct {
	consumerGroup sarama.ConsumerGroup
}

// NewConsumerAdapter creates a retry.Consumer from a Sarama ConsumerGroup.
func NewConsumerAdapter(cg sarama.ConsumerGroup) resilience.Consumer {
	return &ConsumerAdapter{consumerGroup: cg}
}

// Consume implements retry.Consumer interface.
func (c *ConsumerAdapter) Consume(ctx context.Context, topics []string, handler resilience.ConsumerHandler) error {
	// Create Sarama handler that adapts retry.ConsumerHandler
	saramaHandler := &consumerGroupHandler{
		retryHandler: handler,
	}

	// Start consuming (blocks until context cancelled or error)
	for {
		if err := c.consumerGroup.Consume(ctx, topics, saramaHandler); err != nil {
			return err
		}

		// Check if context was cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Close implements retry.Consumer interface.
func (c *ConsumerAdapter) Close() error {
	return c.consumerGroup.Close()
}

// consumerGroupHandler adapts retry.ConsumerHandler to sarama.ConsumerGroupHandler.
// This is the bridge between kafetrain's library-agnostic handler and Sarama's specific handler.
type consumerGroupHandler struct {
	retryHandler resilience.ConsumerHandler
}

// Setup implements sarama.ConsumerGroupHandler interface.
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler interface.
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler interface.
func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		// Convert Sarama message to retry.Message
		retryMsg := NewMessage(msg)

		// Call the library-agnostic handler
		if err := h.retryHandler.Handle(session.Context(), retryMsg); err != nil {
			return err
		}

		// Mark message as processed
		session.MarkMessage(msg, "")
	}

	return nil
}

// ConsumerFactory implements retry.ConsumerFactory interface for Sarama.
// This factory creates new Sarama consumer groups with different group IDs.
type ConsumerFactory struct {
	client sarama.Client
}

// NewConsumerFactory creates a retry.ConsumerFactory from a Sarama Client.
func NewConsumerFactory(client sarama.Client) resilience.ConsumerFactory {
	return &ConsumerFactory{client: client}
}

// NewConsumer implements retry.ConsumerFactory interface.
func (f *ConsumerFactory) NewConsumer(groupID string) (resilience.Consumer, error) {
	cg, err := sarama.NewConsumerGroupFromClient(groupID, f.client)
	if err != nil {
		return nil, err
	}

	return NewConsumerAdapter(cg), nil
}
