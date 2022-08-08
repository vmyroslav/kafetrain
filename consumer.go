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

const (
	OffsetOldest = sarama.OffsetOldest
	OffsetNewest = sarama.OffsetNewest
)

type Consumer interface {
	Consume(ctx context.Context, topic string, messageHandler MessageHandler) error
	Close() error
}

type Streamer interface {
	Stream(ctx context.Context, topic string) (<-chan Message, <-chan error)
	Close() error
}

// KafkaConsumer wrapper around sarama.ConsumerGroup.
// It provides a way to consume messages from kafka topic in a consumer group.
type KafkaConsumer struct {
	cfg    Config
	logger *zap.Logger

	client        sarama.Client
	consumerGroup sarama.ConsumerGroup
	middlewares   []Middleware
	optionsCfg    *consumerOptionConfig
}

func NewKafkaConsumer(
	cfg Config,
	logger *zap.Logger,
	options ...Option,
) (*KafkaConsumer, error) {
	sc, err := createSaramaConfig(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama configs")
	}

	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, sc)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama consumer group")
	}

	client, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create sarama consumer group")
	}

	optionsCfg := newConsumerOptionConfig()

	for _, o := range options {
		o.Apply(optionsCfg)
	}

	return &KafkaConsumer{
		cfg:           cfg,
		logger:        logger,
		consumerGroup: cg,
		client:        client,
		middlewares:   make([]Middleware, 0),
		optionsCfg:    optionsCfg,
	}, nil
}

func createSaramaConfig(cfg Config) (*sarama.Config, error) {
	c := sarama.NewConfig()

	v, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse Kafka version: %s", cfg.Version)
	}

	c.Version = v
	c.ClientID = cfg.ClientID

	c.Consumer.Offsets.AutoCommit.Enable = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.Return.Errors = true
	c.Consumer.MaxProcessingTime = time.Duration(cfg.MaxProcessingTime) * time.Millisecond

	c.Net.SASL.Password = cfg.Password
	c.Net.SASL.User = cfg.Username
	c.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	if cfg.CACert != "" {
		c.Net.SASL.Enable = true
		c.Net.TLS.Enable = true

		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New("could not create ca cert")
		}

		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}

		if ok := rootCAs.AppendCertsFromPEM([]byte(cfg.CACert)); !ok {
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
func (c *KafkaConsumer) Consume(ctx context.Context, topic string, messageHandler MessageHandler) error {
	if c.optionsCfg.Offset != 0 {
		partitions, err := c.partitions()(ctx, topic)
		if err != nil {
			return errors.Wrapf(err, "unable to get partitions for topic: %s", topic)
		}

		c.optionsCfg.partitionsMap = make(map[int32]int64)
		for _, partition := range partitions {
			c.optionsCfg.partitionsMap[partition] = c.optionsCfg.Offset
		}
	}

	if !c.optionsCfg.StartFrom.IsZero() {
		partitions, err := c.partitions()(ctx, topic)
		if err != nil {
			return errors.Wrapf(err, "unable to get partitions for topic: %s", topic)
		}

		for _, partition := range partitions {
			offset, err := c.client.GetOffset(topic, partition, c.optionsCfg.StartFrom.Unix())
			if err != nil {
				return errors.Wrapf(err, "unable to get partitions for topic: %s", topic)
			}

			c.optionsCfg.partitionsMap[partition] = offset
		}
	}

	handler := newHandler(topic, messageHandler, c.middlewares, c.logger, c.optionsCfg)
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

func (c *KafkaConsumer) Stream(ctx context.Context, topic string) (<-chan Message, <-chan error) {
	msgCh := make(chan Message, c.cfg.BuffSize)
	errCh := make(chan error, 1)
	handler := newHandler(topic, MessageHandleFunc(func(ctx context.Context, message Message) error {
		msgCh <- message

		return nil
	}), c.middlewares, c.logger, c.optionsCfg)

	go func() {
		defer close(msgCh)
		defer close(errCh)
		if err := c.consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			errCh <- errors.WithStack(err)
		}
	}()

	return msgCh, errCh
}

func (c *KafkaConsumer) WithMiddlewares(middlewares ...Middleware) *KafkaConsumer {
	c.middlewares = append(c.middlewares, middlewares...)
	return c
}

func (c *KafkaConsumer) partitions() func(ctx context.Context, topic string) ([]int32, error) {
	var partitions []int32

	return func(ctx context.Context, topic string) ([]int32, error) {
		if len(partitions) > 0 {
			return partitions, nil
		}

		return c.client.Partitions(topic)
	}
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
	topic          string
	messageHandler MessageHandleFunc
	logger         *zap.Logger

	params *consumerOptionConfig
}

func newHandler(
	topic string,
	baseHandler MessageHandler,
	middlewares []Middleware,
	logger *zap.Logger,
	params *consumerOptionConfig,
) *saramaHandler {
	h := saramaHandler{
		topic:          topic,
		messageHandler: newMessageHandlerFunc(baseHandler),
		logger:         logger,
		params:         params,
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
	for partition, offset := range h.params.partitionsMap {
		session.ResetOffset(h.topic, partition, offset, "")
	}

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
			Timestamp: message.Timestamp,
		}

		if err := h.messageHandler(ctx, m); err != nil {
			return errors.WithStack(err)
		}

		if !h.params.Silent {
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

func partitions(client sarama.Client) func(ctx context.Context, topic string) ([]int32, error) {
	var partitions []int32

	return func(ctx context.Context, topic string) ([]int32, error) {
		if len(partitions) > 0 {
			return partitions, nil
		}

		return client.Partitions(topic)
	}
}
