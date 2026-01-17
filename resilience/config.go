package resilience

// Config holds configuration for the retry mechanism.
type Config struct {
	// RetryTopicPrefix is the prefix for the retry topic (default: "retry").
	// The full name will be "{Prefix}_{OriginalTopic}".
	RetryTopicPrefix string

	// RedirectTopicPrefix is the prefix for the internal state tracking topic (default: "redirect").
	// This topic uses compaction to track locked message keys.
	RedirectTopicPrefix string

	// DLQTopicPrefix is the prefix for the Dead Letter Queue topic (default: "dlq").
	DLQTopicPrefix string

	// GroupID is the consumer group ID of the main application.
	// This is REQUIRED to ensure the coordinator uniquely identifies this consumer group's state.
	GroupID string

	// MaxRetries is the maximum number of retry attempts before sending to DLQ (default: 5).
	MaxRetries int

	// InitialOffset determines where to start consuming if no offset is present.
	InitialOffset int64

	// RetryTopicPartitions is the number of partitions for retry/redirect/DLQ topics.
	// If set to 0 (default), the library attempts to auto-detect the partition count
	// from the original topic.
	RetryTopicPartitions int32

	// FreeOnDLQ determines behavior when a message hits the DLQ.
	// If false (default), the message key remains "locked" in the tracker, preserving
	// strict FIFO order for that key.
	// If true, the lock is released, allowing subsequent messages to be processed.
	FreeOnDLQ bool

	// StateRestoreTimeoutMs is the maximum time to wait for state restoration to complete on startup (default: 30000ms).
	StateRestoreTimeoutMs int64

	// StateRestoreIdleTimeoutMs is the duration of inactivity on the redirect topic
	// that indicates restoration is complete (default: 5000ms).
	StateRestoreIdleTimeoutMs int64
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
	}
}
