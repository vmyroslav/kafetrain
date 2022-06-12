package main

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"github.com/vmyroslav/kafetrain"
)

// Config is the configuration for the logger.
type Config struct {
	Topic string `envconfig:"KAFKA_TOPIC" required:"true"`

	KafkaConfig  kafetrain.Config
	LoggerConfig kafetrain.Config
}

func NewConfig() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	return &cfg, nil
}
