package kafetrain

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	HeaderTopic = "topic"
	headerID    = "id"
	headerRetry = "retry"
	headerKey   = "key"
)

//TODO: implement error_train
type ErrorTracker struct {
	cfg    Config
	logger *zap.Logger

	producer   *producer
	comparator MessageChainTracker
	lm         lockMap
	registry   *HandlerRegistry

	errors chan error

	sync.Mutex
}

func NewTracker(
	cfg Config,
	logger *zap.Logger,
	comparator MessageChainTracker,
	registry *HandlerRegistry,
) (*ErrorTracker, error) {
	if comparator == nil {
		comparator = NewKeyTracker()
	}

	producer, err := newProducer(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t := ErrorTracker{
		cfg:        cfg,
		logger:     logger,
		registry:   registry,
		producer:   producer,
		lm:         make(lockMap),
		comparator: comparator,
		errors:     make(chan error, 256),
	}

	return &t, nil
}

func (t *ErrorTracker) Start(ctx context.Context, topic string) error {
	// init sarama Config and all dependencies for ErrorTracker
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Net.WriteTimeout = 1 * time.Second
	saramaConfig.Metadata.Retry.Max = 5
	// Create redirect and Retry topics if not exist
	admin, err := sarama.NewClusterAdmin(t.cfg.Brokers, saramaConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() { _ = admin.Close() }()

	var g = errgroup.Group{}

	g.Go(func() error {
		err := admin.CreateTopic(t.retryTopic(topic), &sarama.TopicDetail{
			NumPartitions:     1, //TODO: configurable
			ReplicationFactor: 1,
			ConfigEntries: map[string]*string{
				"cleanup.policy": toPtr("compact"),
				"segment.ms":     toPtr("100"),
			},
		}, false)
		if err != nil {
			sErr, ok := err.(*sarama.TopicError)

			if !(ok && sErr.Err == sarama.ErrTopicAlreadyExists) {
				return errors.WithStack(err)
			}
		}

		return nil
	})

	g.Go(func() error {
		err := admin.CreateTopic(t.redirectTopic(topic), &sarama.TopicDetail{
			NumPartitions:     1, //TODO: configurable
			ReplicationFactor: 1,
			ConfigEntries: map[string]*string{
				"cleanup.policy": toPtr("compact"),
				"segment.ms":     toPtr("100"),
			},
		}, false)
		if err != nil {
			sErr, ok := err.(*sarama.TopicError) // nolint: errorlint

			if !(ok && sErr.Err == sarama.ErrTopicAlreadyExists) {
				return errors.WithStack(err)
			}
		}

		return nil
	})

	err = g.Wait()
	if err != nil {
		return errors.WithStack(err)
	}

	// Fill internal store with data from redirect topic
	//TODO: implement later
	cfg2 := t.cfg
	cfg2.GroupID = uuid.New().String()

	refillConsumer, err := NewKafkaConsumer(
		t.cfg,
		t.logger,
	)

	if err != nil {
		return errors.WithStack(err)
	}
	//
	////TODO: delete consumer group
	errCh := make(chan error, 10)

	tctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	go func() {
		errCh <- refillConsumer.Consume(tctx, t.redirectTopic(topic), newRedirectFillHandler(t))
	}()

	<-tctx.Done()

	if err := refillConsumer.Close(); err != nil {
		return errors.WithStack(err)
	}

	err = admin.DeleteConsumerGroup(cfg2.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}

	// start Retry consumer
	// try to handle events from this topic in the same order they were received
	// if message was handled successfully publish tombstone event to redirect topic

	//cfg2 := t.cfg
	//cfg2.To
	//errCh := make(chan error, 10)

	retryConsumer, err := NewKafkaConsumer(
		t.cfg,
		t.logger,
	)

	if err != nil {
		return errors.WithStack(err)
	}

	handler, ok := t.registry.Get(topic)
	if !ok || handler == nil {
		return errors.New(fmt.Sprintf("handler for topic: %s not found", topic))
	}

	go func() {
		errCh <- retryConsumer.WithMiddlewares(NewRetryMiddleware(t)).Consume(ctx, t.retryTopic(topic), handler)
	}()
	println(errCh)
	//
	//// start redirect consumer
	//// listen for tombstone events and remove successfully handled messages from in memory store
	////TODO
	redirectConsumer, err := NewKafkaConsumer(
		t.cfg,
		t.logger,
	)

	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		t.errors <- redirectConsumer.Consume(ctx, t.redirectTopic(topic), NewRedirectHandler(t))
	}()

	return nil
}
func (t *ErrorTracker) Errors() <-chan error {
	return t.errors
}

func (t *ErrorTracker) IsRelated(topic string, msg Message) bool {
	return t.comparator.IsRelated(context.Background(), msg)
}

func (t *ErrorTracker) Redirect(ctx context.Context, msg Message) error {
	g, ctx := errgroup.WithContext(ctx)

	idH, ok := msg.Headers.Get(headerRetry)
	if ok {
		log.Print(idH)
		// msg was already handled
	}

	id, err := t.comparator.AddMessage(ctx, msg)
	if err != nil {
		return errors.WithStack(err)
	}

	g.Go(func() error {
		retryMsg := Message{
			topic:   t.retryTopic(msg.topic),
			Key:     msg.Key,
			Payload: msg.Payload,
			Headers: HeaderList{
				{
					Key:   []byte(headerID),
					Value: []byte(id),
				},
				{
					Key:   []byte(headerRetry), //temporary header
					Value: []byte("true"),
				},
				{
					Key:   []byte(HeaderTopic),
					Value: []byte(msg.topic),
				},
			},
		}

		return t.producer.publish(ctx, retryMsg)
	})

	g.Go(func() error {
		newMessage := Message{
			topic:   t.redirectTopic(msg.topic),
			Key:     []byte(id),
			Payload: []byte(id),
			Headers: HeaderList{
				{
					Key:   []byte(HeaderTopic),
					Value: []byte(msg.topic),
				},
				{
					Key:   []byte("key"),
					Value: msg.Key,
				},
			},
		}

		return t.producer.publish(ctx, newMessage)
	})

	if err := g.Wait(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (t *ErrorTracker) LockMessage(ctx context.Context, m Message) error {
	topic, ok := m.Headers.Get("topic")
	if !ok {
		return errors.New("topic header not found")
	}

	k, ok := m.Headers.Get("key")
	if !ok {
		return errors.New("topic header not found")
	}

	t.Lock()

	_, ok = t.lm[topic][k]
	if !ok {
		t.lm[topic][k] = make([]string, 0)
	}

	t.lm[topic][k] = append(t.lm[topic][k], string(m.Key))
	t.Unlock()

	return nil
}

func (t *ErrorTracker) Free(ctx context.Context, msg Message) error {
	id, ok := msg.Headers.Get(headerRetry)
	if !ok {
		return errors.New("topic id not found")
	}

	topic, ok := msg.Headers.Get(HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}

	newMessage := Message{
		topic:   t.redirectTopic(topic),
		Key:     []byte(id),
		Payload: nil,
		Headers: HeaderList{
			{
				Key:   []byte(HeaderTopic),
				Value: []byte(topic),
			},
			{
				Key:   []byte(headerKey),
				Value: msg.Key,
			},
		},
	}

	return t.producer.publish(ctx, newMessage)
}

func (t *ErrorTracker) ReleaseMessage(ctx context.Context, msg Message) error {
	topic, ok := msg.Headers.Get(HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}

	key, ok := msg.Headers.Get(headerKey)
	if !ok {
		return errors.New("key header not found")
	}

	if err := t.comparator.ReleaseMessage(ctx, Message{
		Key:   []byte(key),
		topic: topic,
	}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (t *ErrorTracker) retryTopic(topic string) string {
	return t.cfg.RetryTopicPrefix + "_" + topic
}

func (t *ErrorTracker) redirectTopic(topic string) string {
	return t.cfg.RedirectTopicPrefix + "_" + topic
}

type redirectFillHandler struct {
	t *ErrorTracker
}

func newRedirectFillHandler(t *ErrorTracker) *redirectFillHandler {
	return &redirectFillHandler{t: t}
}

func (r *redirectFillHandler) Handle(ctx context.Context, msg Message) error {
	// no payload means tombstone event
	if msg.Payload == nil {
		return nil
	}

	err := r.t.LockMessage(ctx, msg)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

type RedirectHandler struct {
	t *ErrorTracker
}

func NewRedirectHandler(t *ErrorTracker) *RedirectHandler {
	return &RedirectHandler{t: t}
}

func (r *RedirectHandler) Handle(ctx context.Context, msg Message) error {
	if msg.Payload != nil {
		return nil
	}

	err := r.t.ReleaseMessage(ctx, msg)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func toPtr(s string) *string {
	return &s
}

type RetriableError struct {
	Origin error
	Retry  bool
}

func NewRetriableError(origin error, retry bool) *RetriableError {
	return &RetriableError{Origin: origin, Retry: retry}
}

func (e RetriableError) Error() string {
	return e.Origin.Error()
}

func (e RetriableError) ShouldRetry() bool {
	return e.Retry
}
