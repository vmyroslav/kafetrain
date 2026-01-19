package resilience

// Config holds configuration for the retry mechanism.
type Config struct {
	RetryTopicPrefix          string
	RedirectTopicPrefix       string
	DLQTopicPrefix            string
	GroupID                   string
	MaxRetries                int
	InitialOffset             int64
	StateRestoreTimeoutMs     int64
	StateRestoreIdleTimeoutMs int64
	RetryTopicPartitions      int32
	FreeOnDLQ                 bool
	DisableAutoTopicCreation  bool
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
