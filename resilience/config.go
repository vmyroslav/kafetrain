package resilience

// Config holds configuration for the retry mechanism.
type Config struct {
	RetryTopicPrefix          string
	RedirectTopicPrefix       string
	GroupID                   string
	DLQTopicPrefix            string
	MaxRetries                int
	InitialOffset             int64
	RetryTopicPartitions      int32
	BuffSize                  uint16
	MaxProcessingTime         uint16
	FreeOnDLQ                 bool
	Silent                    bool
	StateRestoreTimeoutMs     int64
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
		BuffSize:                  256,
		MaxProcessingTime:         100,
		FreeOnDLQ:                 false,
		Silent:                    false,
		StateRestoreTimeoutMs:     30000, // 30 seconds
		StateRestoreIdleTimeoutMs: 5000,  // 5 seconds
	}
}
