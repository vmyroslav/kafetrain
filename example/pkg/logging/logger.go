// Package logging is a wrapper around zapctxd.Logger that adds context to the log entries.
package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
)

// New initiates a new logger.
func New(cfg Config) *zap.Logger {
	zcfg := zap.NewProductionConfig()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zcfg.EncoderConfig),
		zapcore.AddSync(io.Discard),
		cfg.Level,
	)
	logger := zap.New(core)

	return logger
}
