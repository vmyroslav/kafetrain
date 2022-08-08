package logging

import "go.uber.org/zap/zapcore"

type Config struct {
	Level zapcore.Level `envconfig:"LOG_LEVEL" default:"info"`
}
