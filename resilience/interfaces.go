package resilience

import (
	"context"
)

// Producer publishes messages to Kafka topics (library-agnostic).
type Producer interface {
	// Produce publishes a single message to the specified topic
	Produce(ctx context.Context, topic string, msg Message) error

	// ProduceBatch publishes multiple messages atomically
	//TODO: use it insted of SAGA?
	ProduceBatch(ctx context.Context, messages []MessageTarget) error

	// Close releases producer resources
	Close() error
}

// MessageTarget represents a message with its destination topic.
// Used for batch operations where different messages go to different topics.
type MessageTarget struct {
	Message Message
	Topic   string
}

// ConsumerHandler processes messages (library-agnostic).
// Implementations contain the business logic for processing Kafka messages.
type ConsumerHandler interface {
	Handle(ctx context.Context, msg Message) error
}

// ConsumerHandlerFunc is a function adapter for ConsumerHandler.
type ConsumerHandlerFunc func(ctx context.Context, msg Message) error

func (f ConsumerHandlerFunc) Handle(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}

// Consumer consumes messages from Kafka topics (library-agnostic).
type Consumer interface {
	// Consume starts consuming from the specified topics
	// Blocks until context is canceled or an error occurs
	Consume(ctx context.Context, topics []string, handler ConsumerHandler) error

	// Close stops consumption and releases resources
	Close() error
}

// ConsumerFactory creates Consumer instances (library-agnostic).
// Used by ErrorTracker to create retry and redirect consumers.
type ConsumerFactory interface {
	// NewConsumer creates a new consumer with the specified group ID
	NewConsumer(groupID string) (Consumer, error)
}

// Logger is a minimal logging interface (library-agnostic).
// Allows kafetrain core to work with any logging library.
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
}

// Admin performs Kafka cluster administration operations (library-agnostic).
// Used by ErrorTracker to create topics and manage consumer groups.
type Admin interface {
	// CreateTopic creates a topic with specified configuration.
	// config keys: "cleanup.policy", "retention.ms", "segment.ms", etc.
	// Returns nil if topic already exists (idempotent).
	CreateTopic(ctx context.Context, name string, partitions int32, replicationFactor int16, config map[string]string) error

	// DescribeTopics retrieves metadata for specified topics.
	// Returns metadata for existing topics, skips non-existent ones.
	DescribeTopics(ctx context.Context, topics []string) ([]TopicMetadata, error)

	// DeleteConsumerGroup removes a consumer group from the cluster.
	// Used to clean up ephemeral groups created during state restoration.
	DeleteConsumerGroup(ctx context.Context, groupID string) error

	// Close releases admin client resources.
	Close() error
}

// TopicMetadata represents topic configuration and partition information.
type TopicMetadata interface {
	// Name returns the topic name.
	Name() string

	// Partitions returns the number of partitions.
	Partitions() int32

	// ReplicationFactor returns the replication factor.
	ReplicationFactor() int16
}
