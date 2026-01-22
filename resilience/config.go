package resilience

// Config holds configuration for the retry mechanism.
type Config struct {
	// RetryTopicPrefix is the prefix for retry topics where failed messages are sent for reprocessing.
	// Topic name format: {prefix}_{original_topic} (e.g., "retry_orders").
	RetryTopicPrefix string

	// RedirectTopicPrefix is the prefix for redirect topics used for distributed state tracking.
	// These compacted topics store locks to ensure message ordering guarantees during retries.
	// Topic name format: {prefix}_{original_topic} (e.g., "redirect_orders").
	RedirectTopicPrefix string

	// DLQTopicPrefix is the prefix for dead-letter queue topics where messages that exceed
	// max retries are sent for manual inspection and recovery.
	// Topic name format: {prefix}_{original_topic} (e.g., "dlq_orders").
	DLQTopicPrefix string

	// GroupID is the consumer group ID for the retry worker.
	// This is a required field and must be unique per application instance.
	GroupID string

	// MaxRetries is the maximum number of retry attempts before sending a message to the DLQ.
	// Default: 5
	MaxRetries int

	// InitialOffset specifies where to start consuming from retry and redirect topics.
	// -1 = newest, -2 = oldest.
	// Default: -2 (OffsetOldest)
	InitialOffset int64

	// StateRestoreTimeoutMs is the maximum time in milliseconds to wait for the redirect topic
	// state restoration to complete before starting message processing.
	// Default: 30000 (30 seconds)
	StateRestoreTimeoutMs int64

	// StateRestoreIdleTimeoutMs is the idle time in milliseconds to wait after the last message
	// during state restoration before considering the restoration complete.
	// Default: 5000 (5 seconds)
	StateRestoreIdleTimeoutMs int64

	// RetryTopicPartitions specifies the number of partitions for auto-created retry topics.
	// 0 means use the same number of partitions as the original topic.
	// Default: 0
	RetryTopicPartitions int32

	// FreeOnDLQ determines whether to release locks (send tombstones) when a message is sent to DLQ.
	// If false, locks remain until manually cleared, preserving ordering guarantees.
	// Default: false
	FreeOnDLQ bool

	// DisableAutoTopicCreation disables automatic creation of retry, redirect, and DLQ topics.
	// If true, topics must be created manually before use.
	// Default: false
	DisableAutoTopicCreation bool
}

// NewDefaultConfig creates a Config with sensible defaults.
func NewDefaultConfig() *Config {
	return &Config{
		RedirectTopicPrefix:       "redirect",
		DLQTopicPrefix:            "dlq",
		RetryTopicPrefix:          "retry",
		MaxRetries:                5,
		RetryTopicPartitions:      0,
		InitialOffset:             -2, // OffsetOldest
		FreeOnDLQ:                 false,
		StateRestoreTimeoutMs:     30000, // 30 seconds
		StateRestoreIdleTimeoutMs: 5000,  // 5 seconds
		DisableAutoTopicCreation:  false,
	}
}
