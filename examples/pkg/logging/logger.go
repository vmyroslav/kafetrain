// Package logging is a wrapper around zapctxd.Logger that adds context to the log entries.
package logging

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New initiates a new logger.
func New(cfg Config) *zap.Logger {
	zcfg := zap.NewProductionConfig()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zcfg.EncoderConfig),
		zapcore.AddSync(os.Stdout),
		cfg.Level,
	)
	logger := zap.New(core, zap.AddCaller())

	return logger
}
