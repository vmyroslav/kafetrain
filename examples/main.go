package main

import (
	"context"
	"fmt"

	"github.com/vmyroslav/kafetrain/examples/pkg/logging"
	"github.com/vmyroslav/kafetrain/resilience"

	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "github.com/joho/godotenv/autoload"
	"go.uber.org/zap"
)

func main() {
	consumerErrors := make(chan error, 1)
	killSignal := make(chan os.Signal, 1)
	signal.Notify(killSignal, syscall.SIGINT, syscall.SIGTERM)

	cfg, err := NewConfig()
	if err != nil {
		panic(fmt.Errorf("could not load config: %w", err))
	}

	logger := logging.New(cfg.LoggerConfig)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := resilience.NewHandlerRegistry()
	registry.Add(cfg.Topic, NewHandlerExample(logger))

	t, err := resilience.NewTracker(cfg.KafkaConfig, logger, resilience.NewKeyTracker(), registry)
	if err != nil {
		logger.Fatal("could not initialize error tracker", zap.Error(err))
	}

	if err = t.Start(ctx, cfg.Topic); err != nil {
		logger.Fatal("could not start error tracker", zap.Error(err))
	}

	kafkaConsumer, err := resilience.NewKafkaConsumer(cfg.KafkaConfig, logger)

	if err != nil {
		logger.Fatal("could not create kafka consumer", zap.Error(err))
	}

	kafkaConsumer.WithMiddlewares(
		resilience.NewLoggingMiddleware(logger),
		resilience.NewErrorHandlingMiddleware(t),
	)

	go func() {
		handler, _ := registry.Get(cfg.Topic)
		consumerErrors <- kafkaConsumer.Consume(ctx, cfg.Topic, handler)
	}()

	select {
	case err := <-consumerErrors:
		logger.Error("consumer failed, shutting down application", zap.Error(err))

		shutdown(kafkaConsumer, logger)
	case <-killSignal:
		logger.Info("killSignal received, shutting down application")

		shutdown(kafkaConsumer, logger)
	}
}

func shutdown(c resilience.Consumer, logger *zap.Logger) {
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := c.Close(); err != nil {
			logger.Error("could not gracefully stop consumer")
		}

		logger.Info("consumer stopped")
	}()

	wg.Wait()
	logger.Info("stopped all")
}
