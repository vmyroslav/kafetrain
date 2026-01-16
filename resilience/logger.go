package resilience

// NoOpLogger is a logger that discards all log messages.
type NoOpLogger struct{}

func (l *NoOpLogger) Debug(_ string, _ ...any) {}
func (l *NoOpLogger) Info(_ string, _ ...any)  {}
func (l *NoOpLogger) Warn(_ string, _ ...any)  {}
func (l *NoOpLogger) Error(_ string, _ ...any) {}

// NewNoOpLogger creates a new NoOpLogger.
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}
