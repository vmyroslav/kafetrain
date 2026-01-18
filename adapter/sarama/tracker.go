package sarama

import (
	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafetrain/resilience"
)

type managerOptions struct {
	logger      resilience.Logger
	coordinator resilience.StateCoordinator
	backoff     resilience.BackoffStrategy
	errCh       chan<- error
}

// NewResilienceTracker creates a configured ErrorTracker using the Sarama client.
// It handles the creation of all necessary adapters and internal components.
func NewResilienceTracker(
	client sarama.Client,
	cfg *resilience.Config,
	opts ...ManagerOption,
) (*resilience.ErrorTracker, error) {
	options := &managerOptions{
		logger: resilience.NewNoOpLogger(),
	}
	for _, opt := range opts {
		opt(options)
	}

	// note: we don't close producer, because it shares the underlying client
	saramaProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	producerAdapter := NewProducerAdapter(saramaProducer)
	consumerFactory := NewConsumerFactory(client)
	adminAdapter, err := NewAdminAdapter(client)
	if err != nil {
		return nil, err
	}

	if options.errCh == nil {
		// default error channel draining by logger
		defaultErrCh := make(chan error, 10)
		go func() {
			for err := range defaultErrCh {
				options.logger.Error("kafka-resilience background error", "error", err)
			}
		}()
		options.errCh = defaultErrCh
	}

	coordinator := options.coordinator
	if coordinator == nil {
		coordinator = resilience.NewKafkaStateCoordinator(
			cfg,
			options.logger,
			producerAdapter,
			consumerFactory,
			adminAdapter,
			options.errCh,
		)
	}

	return resilience.NewErrorTracker(
		cfg,
		options.logger,
		producerAdapter,
		consumerFactory,
		adminAdapter,
		coordinator,
		options.backoff,
	)
}

// ManagerOption is a functional option for configuring the resilience manager.
type ManagerOption func(*managerOptions)

// WithLogger sets the logger for the manager.
func WithLogger(logger resilience.Logger) ManagerOption {
	return func(o *managerOptions) {
		o.logger = logger
	}
}

// WithCoordinator sets a custom state coordinator.
func WithCoordinator(coordinator resilience.StateCoordinator) ManagerOption {
	return func(o *managerOptions) {
		o.coordinator = coordinator
	}
}

// WithBackoff sets a custom backoff strategy.
func WithBackoff(backoff resilience.BackoffStrategy) ManagerOption {
	return func(o *managerOptions) {
		o.backoff = backoff
	}
}

// WithErrorChannel sets a custom error channel for background errors.
func WithErrorChannel(errCh chan<- error) ManagerOption {
	return func(o *managerOptions) {
		o.errCh = errCh
	}
}
