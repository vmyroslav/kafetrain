package main

import (
	"context"
	"github.com/pkg/errors"
	"github.com/vmyroslav/kafetrain"
	"go.uber.org/zap"
)

type HandlerExample struct {
	logger *zap.Logger
}

func NewHandlerExample(logger *zap.Logger) *HandlerExample {
	return &HandlerExample{logger: logger}
}

func (h *HandlerExample) Handle(ctx context.Context, msg kafetrain.Message) error {
	h.logger.Info(
		"handle",
		zap.String("key", string(msg.Key)),
		zap.String("payload", string(msg.Payload)),
	)

	_, isRetry := msg.Headers.Get("retry")
	if isRetry {
		h.logger.Info("retry in handler", zap.String("key", string(msg.Key)))
		//return nil
	}

	return kafetrain.RetriableError{
		Origin: errors.New("error"),
		Retry:  true,
	}
}
