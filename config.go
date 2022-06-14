package kafetrain

type Config struct {
	// Base configuration.
	Brokers []string `envconfig:"KAFKA_BROKERS" required:"true"`
	Version string   `envconfig:"KAFKA_VERSION" required:"true"`
	//Topic    string   `envconfig:"KAFKA_TOPIC" required:"true"`
	GroupID  string `envconfig:"KAFKA_GROUP_ID" required:"true"`
	ClientID string `envconfig:"KAFKA_CLIENT_ID" default:"kafetrain-consumer"`

	// Security configuration.
	Username string `envconfig:"KAFKA_USERNAME"`
	Password string `envconfig:"KAFKA_PASSWORD"`
	CACert   string `envconfig:"KAFKA_CA_CERT"`

	// Consumer configuration.
	MaxProcessingTime uint16 `envconfig:"KAFKA_MAX_PROCESSING_TIME_MS" default:"100"`
	InitialOffset     int64  `envconfig:"KAFKA_CONSUMER_INITIAL_OFFSET" default:"-2"`

	// Retry configuration.
	RetryTopicPrefix    string `envconfig:"KAFKA_RETRY_TOPIC_PREFIX" default:"retry"`       // topic for messages to retry
	RedirectTopicPrefix string `envconfig:"KAFKA_REDIRECT_TOPIC_PREFIX" default:"redirect"` // topic with message ids that should be retried
}
