package logging

import (
	"fmt"
	"go.uber.org/zap"
)

var source = zap.String("source", "sarama")

// SaramaAdapter is a wrapper around zap.Logger to fit sarama.StdLogger interface.
type SaramaAdapter struct {
	base *zap.Logger
}

// NewSaramaAdapter creates a new SaramaAdapter.
func NewSaramaAdapter(base *zap.Logger) *SaramaAdapter {
	return &SaramaAdapter{base: base}
}

// Print logs a message at level Debug.
func (s SaramaAdapter) Print(v ...interface{}) {
	s.base.Debug(fmt.Sprint(v...), source)
}

// Printf logs a message at level Debug with provided format.
func (s SaramaAdapter) Printf(format string, v ...interface{}) {
	s.base.Debug(fmt.Sprintf(format, v...), source)
}

// Println logs a message at level Debug.
func (s SaramaAdapter) Println(v ...interface{}) {
	s.base.Debug(fmt.Sprintln(v...), source)
}
