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
	consumerErrors := make(chan error, 1)
	killSignal := make(chan os.Signal, 1)
	signal.Notify(killSignal, syscall.SIGINT, syscall.SIGTERM)

	logger := logging.New(logging.Config{Level: zap.InfoLevel})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kCfg := resilience.Config{
		Brokers:           []string{"localhost:9092"},
		Version:           "4.1.0",
		GroupID:           "example-simple-consumer",
		ClientID:          "example-simple-consumer",
		Username:          "",
		Password:          "",
		InitialOffset:     0,
		MaxProcessingTime: 100,
		Silent:            true,
	}
	topic = "hello-world"

	kafkaConsumer, err := resilience.NewKafkaConsumer(
		&kCfg,
		logger,
	)

	kafkaConsumer.WithMiddlewares(resilience.NewLoggingMiddleware(logger))

	if err != nil {
		logger.Fatal("could not create kafka consumer", zap.Error(err))
	}

	consumerErrors <- kafkaConsumer.Consume(
		ctx,
		topic,
		resilience.MessageHandleFunc(func(_ context.Context, msg *resilience.Message) error {
			logger.Info("message received", zap.String(
				"key",
				string(msg.Key),
			), zap.String("value", string(msg.Payload)))

			return nil
		}),
	)

	select {
	case err := <-consumerErrors:
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
