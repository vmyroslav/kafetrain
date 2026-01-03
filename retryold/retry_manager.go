package retryold

import (
	"context"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// RetryManager manages retry consumer lifecycle for ErrorTracker.
// This is Layer 2 - convenience layer for users who want managed retry consumers.
// Users run their own main consumer but delegate retry consumer management to RetryManager.
type RetryManager struct {
	tracker *ErrorTracker
	handler MessageHandler
	logger  *zap.Logger
	cfg     *Config
}

// NewRetryManager creates a retry consumer manager with a MessageHandler.
// The handler will be used to process messages from the retry topic.
func NewRetryManager(
	tracker *ErrorTracker,
	handler MessageHandler,
) *RetryManager {
	return &RetryManager{
		tracker: tracker,
		handler: handler,
		logger:  tracker.logger,
		cfg:     tracker.cfg,
	}
}

// NewRetryManagerWithSaramaHandler creates a retry manager with a Sarama-based handler.
// This is the recommended constructor for most use cases - allows writing business logic
// once using *sarama.ConsumerMessage and reusing it for both main and retry consumers.
//
// Example:
//
//	processOrder := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
//	    // Business logic here
//	    return processOrder(msg)
//	}
//
//	retryMgr := resilience.NewRetryManagerWithSaramaHandler(tracker, processOrder)
func NewRetryManagerWithSaramaHandler(
	tracker *ErrorTracker,
	handler func(context.Context, *sarama.ConsumerMessage) error,
) *RetryManager {
	// Convert Sarama handler to MessageHandler using ToMessageHandler adapter
	messageHandler := ToMessageHandler(handler)
	return NewRetryManager(tracker, messageHandler)
}

// StartRetryConsumer starts the retry consumer for a topic.
// Uses the handler provided in constructor to process retry messages.
// The retry consumer runs in a separate consumer group (groupID + "-retry").
func (rm *RetryManager) StartRetryConsumer(ctx context.Context, topic string) error {
	return rm.tracker.startRetryConsumerWithHandler(ctx, topic, rm.handler)
}
