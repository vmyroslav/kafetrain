package kafetrain

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"go.uber.org/zap"
)

type Consumer interface {
	Consume(ctx context.Context, topic string, messageHandler MessageHandler) error
	Close() error
}

// KafkaConsumer consumes topic.
type KafkaConsumer struct {
	cfg    Config
	logger *zap.Logger

	consumerGroup sarama.ConsumerGroup
	middlewares   []Middleware
}

func NewKafkaConsumer(
	cfg Config,
	logger *zap.Logger,
	middlewares ...Middleware,
) (*KafkaConsumer, error) {
	sc, err := createSaramaConfig(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama configs")
	}

	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, sc)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama consumer group")
	}

	return &KafkaConsumer{
		cfg:           cfg,
		consumerGroup: cg,
		middlewares:   middlewares,
		logger:        logger,
	}, nil
}

func createSaramaConfig(config Config) (*sarama.Config, error) {
	c := sarama.NewConfig()

	v, err := sarama.ParseKafkaVersion(config.Version)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse Kafka version: %s", config.Version)
	}

	c.Version = v
	c.ClientID = config.ClientID

	c.Consumer.Offsets.AutoCommit.Enable = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.Return.Errors = true
	c.Consumer.MaxProcessingTime = time.Duration(config.MaxProcessingTime) * time.Millisecond

	c.Net.SASL.Password = config.Password
	c.Net.SASL.User = config.Username
	c.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	if config.CACert != "" {
		c.Net.SASL.Enable = true
		c.Net.TLS.Enable = true

		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New("could not create ca cert")
		}

		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}

		if ok := rootCAs.AppendCertsFromPEM([]byte(config.CACert)); !ok {
			return nil, errors.New("could not append ca cert")
		}

		c.Net.TLS.Config = &tls.Config{
			RootCAs:    rootCAs,
			MinVersion: tls.VersionTLS12,
		}
	}

	return c, nil
}

// Consume consume messages from kafka.
func (c KafkaConsumer) Consume(ctx context.Context, topic string, messageHandler MessageHandler) error {
	handler := newHandler(messageHandler, c.middlewares, c.logger, !c.cfg.Silent)
	c.logger.Info("started consuming")

	// Consume errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			c.logger.Error("consumerGroup handlers error", zap.Error(err))
		}
	}()

	for {
		if errors.Is(ctx.Err(), context.Canceled) {
			c.logger.Error("consumerGroup handlers error", zap.Error(errors.New("context was canceled")))

			break
		}

		if err := c.consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// Close stops the consumer group.
func (c *KafkaConsumer) Close() error {
	return errors.WithStack(c.consumerGroup.Close())
}

// MessageHandleFunc handles messages.
type MessageHandleFunc func(ctx context.Context, message Message) error

// Handle type is an adapter to allow the use of ordinary functions as MessageHandler.
func (f MessageHandleFunc) Handle(ctx context.Context, message Message) error { return f(ctx, message) }

// Middleware function.
type Middleware func(next MessageHandleFunc) MessageHandleFunc

// MessageHandler implementation of business logic to process received message.
type MessageHandler interface {
	Handle(ctx context.Context, msg Message) error
}

// Wrapper from our domain handlers to sarama ConsumerGroupHandler to avoid abstraction leak.
type saramaHandler struct {
	messageHandler MessageHandleFunc
	logger         *zap.Logger

	shouldCommit bool
}

func newHandler(handler MessageHandler, middlewares []Middleware, logger *zap.Logger, commit bool) *saramaHandler {
	h := saramaHandler{
		messageHandler: newMessageHandlerFunc(handler),
		logger:         logger,
		shouldCommit:   commit,
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		h.messageHandler = middlewares[i](h.messageHandler)
	}

	return &h
}

func newMessageHandlerFunc(handler MessageHandler) MessageHandleFunc {
	return func(ctx context.Context, message Message) error {
		err := handler.Handle(ctx, message)

		return errors.Wrapf(err, "handler failed")
	}
}

func (h *saramaHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *saramaHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *saramaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for message := range claim.Messages() {
		m := Message{
			Key:       message.Key,
			Payload:   message.Value,
			Headers:   h.mapHeaders(message.Headers),
			topic:     message.Topic,
			offset:    message.Offset,
			partition: message.Partition,
		}

		if err := h.messageHandler(ctx, m); err != nil {
			return errors.WithStack(err)
		}

		if h.shouldCommit {
			session.MarkMessage(message, "") // mark message as processed (not an offset commit)
		}
	}

	return nil
}

func (h *saramaHandler) mapHeaders(headers []*sarama.RecordHeader) HeaderList {
	headerList := HeaderList{}

	for _, header := range headers {
		headerList.Set(string(header.Key), string(header.Value))
	}

	return headerList
}
