package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/vmyroslav/kafetrain/examples/pkg/logging"
	"github.com/vmyroslav/kafetrain/resilience"
	"go.uber.org/zap"
)

var topic string

func main() {
	killSignal := make(chan os.Signal, 1)
	signal.Notify(killSignal, syscall.SIGINT, syscall.SIGTERM)

	logger := logging.New(logging.Config{Level: zap.InfoLevel})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kCfg := resilience.Config{
		Brokers:           []string{"localhost:9092"},
		Version:           "3.0.1",
		GroupID:           "example-simple-consumer",
		ClientID:          "example-simple-consumer",
		Username:          "",
		Password:          "",
		InitialOffset:     0,
		MaxProcessingTime: 100,
		Silent:            true,
	}

	kafkaConsumer, err := resilience.NewKafkaConsumer(
		kCfg,
		logger,
	)
	if err != nil {
		logger.Fatal("could not create kafka consumer", zap.Error(err))
	}

	msgCh, errCh := kafkaConsumer.WithMiddlewares(resilience.NewFilterMiddleware(func(msg resilience.Message) bool {
		return (string(msg.Key)) != "1"
	})).Stream(ctx, "hello-world")

	go func() {
		for msg := range msgCh {
			logger.Info("message received", zap.String(
				"key",
				string(msg.Key),
			), zap.String("value", string(msg.Payload)))
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("consumer failed, shutting down application", zap.Error(err))

		if err := kafkaConsumer.Close(); err != nil {
			logger.Error("could not gracefully stop consumer")
		}

	case s := <-killSignal:
		logger.Info(fmt.Sprintf("killSignal: %s received, shutting down application", s.String()))

		if err := kafkaConsumer.Close(); err != nil {
			logger.Error("could not gracefully stop consumer")
		}
	}

	logger.Info("application stopped successfully")
}
