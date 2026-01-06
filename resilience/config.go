package resilience

// Config holds configuration for the retry mechanism.
type Config struct {
	Password             string
	CACert               string
	RetryTopicPrefix     string
	RedirectTopicPrefix  string
	GroupID              string
	Version              string
	ClientID             string
	Username             string
	DLQTopicPrefix       string
	Brokers              []string
	MaxRetries           int
	InitialOffset        int64
	RetryTopicPartitions int32
	BuffSize             uint16
	MaxProcessingTime    uint16
	FreeOnDLQ            bool
	Silent               bool
}

// NewDefaultConfig creates a Config with sensible defaults.
func NewDefaultConfig() *Config {
	return &Config{
		RedirectTopicPrefix:  "redirect",
		DLQTopicPrefix:       "dlq",
		RetryTopicPrefix:     "retry",
		ClientID:             "kafetrain-consumer",
		MaxRetries:           5,
		RetryTopicPartitions: 0,
		InitialOffset:        -2, // OffsetOldest
		BuffSize:             256,
		MaxProcessingTime:    100,
		FreeOnDLQ:            false,
		Silent:               false,
	}
}
