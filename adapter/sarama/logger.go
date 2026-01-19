package sarama

import (
	"github.com/vmyroslav/kafka-resilience/resilience"
	"go.uber.org/zap"
)

// ZapLogger adapts zap.Logger to implement retry.Logger interface.
type ZapLogger struct {
	logger *zap.Logger
}

// NewZapLogger creates a retry.Logger from a zap.Logger.
func NewZapLogger(logger *zap.Logger) resilience.Logger {
	return &ZapLogger{logger: logger}
}

// Debug implements retry.Logger interface.
func (l *ZapLogger) Debug(msg string, fields ...interface{}) {
	l.logger.Debug(msg, convertFields(fields)...)
}

// Info implements retry.Logger interface.
func (l *ZapLogger) Info(msg string, fields ...interface{}) {
	l.logger.Info(msg, convertFields(fields)...)
}

// Warn implements retry.Logger interface.
func (l *ZapLogger) Warn(msg string, fields ...interface{}) {
	l.logger.Warn(msg, convertFields(fields)...)
}

// Error implements retry.Logger interface.
func (l *ZapLogger) Error(msg string, fields ...interface{}) {
	l.logger.Error(msg, convertFields(fields)...)
}

// convertFields converts generic interface{} fields to zap.Field.
// Expects fields in pairs: key1, value1, key2, value2, ...
func convertFields(fields []interface{}) []zap.Field {
	if len(fields) == 0 {
		return nil
	}

	zapFields := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i < len(fields)-1; i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		zapFields = append(zapFields, zap.Any(key, fields[i+1]))
	}

	return zapFields
}
