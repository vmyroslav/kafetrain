package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/vmyroslav/kafetrain/resilience"
	"go.uber.org/zap"
)

type HandlerExample struct {
	logger *zap.Logger
}

func NewHandlerExample(logger *zap.Logger) *HandlerExample {
	return &HandlerExample{logger: logger}
}

func (h *HandlerExample) Handle(ctx context.Context, msg resilience.Message) error {
	h.logger.Info(
		"handle",
		zap.String("key", string(msg.Key)),
		zap.String("payload", string(msg.Payload)),
	)

	_, isRetry := resilience.GetHeaderValue[string](&msg.Headers, "retry")
	if isRetry {
		h.logger.Info("retry in handler", zap.String("key", string(msg.Key)))
		//return nil
	}

	return resilience.RetriableError{
		Origin: errors.New("error"),
		Retry:  true,
	}
}
