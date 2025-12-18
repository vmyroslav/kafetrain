package kafetrain

type Config struct {
	// Base configuration.
	Brokers  []string `envconfig:"KAFKA_BROKERS" required:"true"`
	Version  string   `envconfig:"KAFKA_VERSION" required:"true"`
	GroupID  string   `envconfig:"KAFKA_GROUP_ID" required:"true"`
	ClientID string   `envconfig:"KAFKA_CLIENT_ID" default:"kafetrain-consumer"`

	// Security configuration.
	Username string `envconfig:"KAFKA_USERNAME"`
	Password string `envconfig:"KAFKA_PASSWORD"`
	CACert   string `envconfig:"KAFKA_CA_CERT"`

	// Consumer configuration.
	MaxProcessingTime uint16 `envconfig:"KAFKA_MAX_PROCESSING_TIME_MS" default:"100"`
	InitialOffset     int64  `envconfig:"KAFKA_CONSUMER_INITIAL_OFFSET" default:"-2"`
	Silent            bool   `envconfig:"KAFKA_CONSUMER_SILENT" default:"false"`

	// Streamer configuration.
	BuffSize uint16 `envconfig:"KAFKA_BUFF_SIZE" default:"256"` // Fixed: was using KAFKA_MAX_PROCESSING_TIME_MS

	// Retry configuration.
	RetryTopicPrefix     string `envconfig:"KAFKA_RETRY_TOPIC_PREFIX" default:"retry"`       // topic for messages to Retry
	RedirectTopicPrefix  string `envconfig:"KAFKA_REDIRECT_TOPIC_PREFIX" default:"redirect"` // topic with message ids that should be retried
	DLQTopicPrefix       string `envconfig:"KAFKA_DLQ_TOPIC_PREFIX" default:"dlq"`           // topic for messages that exceeded max retries
	MaxRetries           int    `envconfig:"KAFKA_MAX_RETRIES" default:"5"`                  // maximum retry attempts before sending to DLQ (0 = infinite retries)
	RetryTopicPartitions int32  `envconfig:"KAFKA_RETRY_TOPIC_PARTITIONS" default:"1"`       // number of partitions for retry topics
}
