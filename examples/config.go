package main

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"github.com/vmyroslav/kafetrain/examples/pkg/logging"
	"github.com/vmyroslav/kafetrain/resilience"
)

type Config struct {
	Topic string `envconfig:"KAFKA_TOPIC" required:"true"`

	KafkaConfig  resilience.Config
	LoggerConfig logging.Config
}

func NewConfig() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	return &cfg, nil
}
