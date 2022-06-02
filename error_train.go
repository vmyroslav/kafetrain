package kafetrain

import (
	"context"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

//TODO: implement error_train
type ErrorTracker struct {
	cfg    Config
	logger zap.Logger

	producer *Producer
	lm       lockMap
	registry *HandlerRegistry

	sync.Mutex
}

func InitTracker(
	ctx context.Context,
	cfg Config,
	logger zap.Logger,
	topic string,
	registry *HandlerRegistry,
) (*ErrorTracker, error) {
	// init sarama Config and all dependencies for ErrorTracker
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Net.WriteTimeout = 1 * time.Second
	saramaConfig.Metadata.Retry.Max = 5

	// initialize topic map
	lm := make(lockMap)
	lm[topic] = make(map[string][]string)

	producer, err := NewProducer(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t := ErrorTracker{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		producer: producer,
		lm:       lm,
	}

	// Create redirect and retry topics if not exist
	admin, err := sarama.NewClusterAdmin(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() { _ = admin.Close() }()

	var g = errgroup.Group{}

	g.Go(func() error {
		err := admin.CreateTopic(t.retryTopic(""), &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			sErr, ok := err.(*sarama.TopicError) // nolint: errorlint

			if !(ok && sErr.Err == sarama.ErrTopicAlreadyExists) {
				return errors.WithStack(err)
			}
		}

		return nil
	})

	g.Go(func() error {
		err := admin.CreateTopic(t.redirectTopic(""), &sarama.TopicDetail{
			NumPartitions:     1,
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
		return nil, errors.WithStack(err)
	}

	// Fill internal store with data from redirect topic
	cfg2 := cfg
	cfg2.GroupID = uuid.New().String()

	if err != nil {
		return nil, errors.WithStack(err)
	}

	refillConsumer, err := NewKafkaConsumer(
		t.cfg,
		logger,
	)

	errCh := make(chan error, 10)

	tctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	go func() {
		errCh <- refillConsumer.Consume(tctx, t.redirectTopic(""), NewRedirectFillHandler(&t))
	}()

	<-tctx.Done()

	if err := refillConsumer.Close(); err != nil {
		return nil, errors.WithStack(err)
	}

	// start retry consumer
	// try to handle events from this topic in the same order they were received
	// if message was handled successfully publish tombstone event to redirect topic
	if err != nil {
		return nil, errors.WithStack(err)
	}

	retryConsumer, err := NewKafkaConsumer(
		t.cfg,
		logger,
		NewRetryMiddleware(&t),
	)

	go func() {
		handler, _ := t.registry.Get("")
		errCh <- retryConsumer.Consume(ctx, t.retryTopic(""), handler)
	}()

	//consumerGroup2, err := NewConsumerGroup(t.cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// start redirect consumer
	// listen for tombstone events and remove successfully handled messages from in memory store
	//TODO
	redirectConsumer, err := NewKafkaConsumer(
		t.cfg,
		logger,
	)

	go func() {
		errCh <- redirectConsumer.Consume(ctx, t.redirectTopic(""), NewRedirectHandler(&t))
	}()

	go func() {
		for err := range errCh {
			if err != nil {
				logger.Error("error", zap.Error(err))
			}
		}
	}()

	return &t, nil
}

func (t *ErrorTracker) IsRelated(topic string, key []byte) bool {
	if _, ok := t.lm[topic][string(key)]; ok {
		return true
	}

	return false
}

func (t *ErrorTracker) Redirect(ctx context.Context, m Message) error {
	g, ctx := errgroup.WithContext(ctx)

	id := uuid.New().String()

	t.Lock()
	t.lm[m.topic][string(m.Key)] = append(t.lm[m.topic][string(m.Key)], id)
	t.Unlock()

	g.Go(func() error {
		newMessage := Message{
			topic:   t.retryTopic(""),
			Key:     m.Key,
			Payload: m.Payload,
			Headers: HeaderList{
				{
					Key:   []byte("id"),
					Value: []byte(id),
				},
				{
					Key:   []byte("retry"),
					Value: []byte("true"),
				},
			},
		}

		return t.producer.Publish(ctx, newMessage)
	})

	g.Go(func() error {
		newMessage := Message{
			topic:   t.redirectTopic(""),
			Key:     []byte(id),
			Payload: []byte(id),
			Headers: HeaderList{
				{
					Key:   []byte("topic"),
					Value: []byte(m.topic),
				},
				{
					Key:   []byte("key"),
					Value: m.Key,
				},
			},
		}

		return t.producer.Publish(ctx, newMessage)
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

func (t *ErrorTracker) Free(ctx context.Context, m Message) error {
	id, ok := m.Headers.Get("id")
	if !ok {
		return errors.New("topic header not found")
	}

	newMessage := Message{
		topic:   t.redirectTopic(m.topic),
		Key:     []byte(id),
		Payload: nil,
		Headers: HeaderList{
			{
				Key:   []byte("topic"),
				Value: []byte(m.topic),
			},
			{
				Key:   []byte("key"),
				Value: m.Key,
			},
		},
	}

	return t.producer.Publish(ctx, newMessage)
}

func (t *ErrorTracker) ReleaseMessage(_ context.Context, m Message) error {
	topic, ok := m.Headers.Get("topic")
	if !ok {
		return errors.New("topic header not found")
	}

	k, ok := m.Headers.Get("key")
	if !ok {
		return errors.New("topic header not found")
	}

	t.Lock()
	mm := remove(t.lm[topic][k], string(m.Key))

	if len(mm) == 0 {
		delete(t.lm[topic], k)
	} else {
		t.lm[topic][k] = mm
	}

	t.Unlock()

	return nil
}

func (t *ErrorTracker) retryTopic(topic string) string {
	return t.cfg.RetryTopicPrefix + "_" + topic
}

func (t *ErrorTracker) redirectTopic(topic string) string {
	return t.cfg.RedirectTopicPrefix + "_" + topic
}

type RedirectFillHandler struct {
	t *ErrorTracker
}

func NewRedirectFillHandler(t *ErrorTracker) *RedirectFillHandler {
	return &RedirectFillHandler{t: t}
}

func (r RedirectFillHandler) Handle(ctx context.Context, msg Message) error {
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

func (r RedirectHandler) Handle(ctx context.Context, msg Message) error {
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
	origin error
	retry  bool
}

func NewRetriableError(origin error, retry bool) *RetriableError {
	return &RetriableError{origin: origin, retry: retry}
}

func (e RetriableError) Error() string {
	return e.origin.Error()
}

func (e RetriableError) ShouldRetry() bool {
	return e.retry
}

// topic map ['topic-name' => ['topic-key' => ['id1', 'id2', 'id3'], 'topic-key-2' => ['id4', 'id5', 'id6']]].
type lockMap map[string]map[string][]string

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}

	return s
}
