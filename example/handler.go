package main

import (
	"context"
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
	h.logger.Info("handle", zap.String("msg", string(msg.Key)))

	return nil
}
