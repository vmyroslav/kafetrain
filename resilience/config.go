package resilience

// Config holds configuration for the retry mechanism.
type Config struct {
	// Topic naming
	RedirectTopicPrefix string
	DLQTopicPrefix      string
	RetryTopicPrefix    string

	// Kafka connection
	Brokers []string
	GroupID string
	Version string

	// Optional authentication
	ClientID string
	Username string
	Password string
	CACert   string

	// Retry behavior
	MaxRetries           int
	RetryTopicPartitions int32
	FreeOnDLQ            bool

	// Consumer settings
	InitialOffset     int64
	BuffSize          uint16
	MaxProcessingTime uint16
	Silent            bool
}

// NewDefaultConfig creates a Config with sensible defaults.
func NewDefaultConfig() *Config {
	return &Config{
		RedirectTopicPrefix:  "redirect",
		DLQTopicPrefix:       "dlq",
		RetryTopicPrefix:     "retry",
		ClientID:             "kafetrain-consumer",
		MaxRetries:           5,
		RetryTopicPartitions: 1,
		InitialOffset:        -2, // OffsetOldest
		BuffSize:             256,
		MaxProcessingTime:    100,
		FreeOnDLQ:            false,
		Silent:               false,
	}
}
