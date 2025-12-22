package resilience

type Config struct {
	RedirectTopicPrefix  string   `envconfig:"KAFKA_REDIRECT_TOPIC_PREFIX" default:"redirect"`
	DLQTopicPrefix       string   `envconfig:"KAFKA_DLQ_TOPIC_PREFIX" default:"dlq"`
	GroupID              string   `envconfig:"KAFKA_GROUP_ID" required:"true"`
	ClientID             string   `envconfig:"KAFKA_CLIENT_ID" default:"kafetrain-consumer"`
	Username             string   `envconfig:"KAFKA_USERNAME"`
	Password             string   `envconfig:"KAFKA_PASSWORD"`
	Version              string   `envconfig:"KAFKA_VERSION" required:"true"`
	CACert               string   `envconfig:"KAFKA_CA_CERT"`
	RetryTopicPrefix     string   `envconfig:"KAFKA_RETRY_TOPIC_PREFIX" default:"retry"`
	Brokers              []string `envconfig:"KAFKA_BROKERS" required:"true"`
	InitialOffset        int64    `envconfig:"KAFKA_CONSUMER_INITIAL_OFFSET" default:"-2"`
	MaxRetries           int      `envconfig:"KAFKA_MAX_RETRIES" default:"5"`
	RetryTopicPartitions int32    `envconfig:"KAFKA_RETRY_TOPIC_PARTITIONS" default:"1"`
	BuffSize             uint16   `envconfig:"KAFKA_BUFF_SIZE" default:"256"`
	MaxProcessingTime    uint16   `envconfig:"KAFKA_MAX_PROCESSING_TIME_MS" default:"100"`
	Silent               bool     `envconfig:"KAFKA_CONSUMER_SILENT" default:"false"`
}
