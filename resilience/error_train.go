package resilience

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// HeaderTopic stores the original topic name
	HeaderTopic = "topic"
	headerID    = "id"
	headerRetry = "retry"
	headerKey   = "key"
)

type ErrorTracker struct {
	comparator      MessageChainTracker
	backoffStrategy BackoffStrategy
	logger          *zap.Logger
	producer        *producer
	registry        *HandlerRegistry
	errors          chan error
	cfg             *Config
}

func NewTracker(
	cfg *Config,
	logger *zap.Logger,
	comparator MessageChainTracker,
	registry *HandlerRegistry,
) (*ErrorTracker, error) {
	return NewTrackerWithBackoff(cfg, logger, comparator, registry, nil)
}

func NewTrackerWithBackoff(
	cfg *Config,
	logger *zap.Logger,
	comparator MessageChainTracker,
	registry *HandlerRegistry,
	backoff BackoffStrategy,
) (*ErrorTracker, error) {
	if comparator == nil {
		comparator = NewKeyTracker()
	}

	if backoff == nil {
		backoff = NewExponentialBackoff()
	}

	producer, err := newProducer(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t := ErrorTracker{
		cfg:             cfg,
		logger:          logger,
		registry:        registry,
		producer:        producer,
		comparator:      comparator,
		backoffStrategy: backoff,
		errors:          make(chan error, 256),
	}

	return &t, nil
}

func (t *ErrorTracker) Start(ctx context.Context, topic string) error {
	if err := t.ensureTopicsExist(topic); err != nil {
		return err
	}

	if err := t.restoreState(ctx, topic); err != nil {
		return err
	}

	return t.startConsumers(ctx, topic)
}

func (t *ErrorTracker) ensureTopicsExist(topic string) error {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Net.WriteTimeout = 1 * time.Second
	saramaConfig.Metadata.Retry.Max = 5

	admin, err := sarama.NewClusterAdmin(t.cfg.Brokers, saramaConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() { _ = admin.Close() }()

	g := errgroup.Group{}
	topics := []struct {
		detail *sarama.TopicDetail
		name   string
	}{
		{
			name: t.retryTopic(topic),
			detail: &sarama.TopicDetail{
				NumPartitions:     t.cfg.RetryTopicPartitions,
				ReplicationFactor: 1,
				ConfigEntries: map[string]*string{
					"cleanup.policy": toPtr("compact"),
					"segment.ms":     toPtr("100"),
				},
			},
		},
		{
			name: t.redirectTopic(topic),
			detail: &sarama.TopicDetail{
				NumPartitions:     t.cfg.RetryTopicPartitions,
				ReplicationFactor: 1,
				ConfigEntries: map[string]*string{
					"cleanup.policy": toPtr("compact"),
					"segment.ms":     toPtr("100"),
				},
			},
		},
		{
			name: t.dlqTopic(topic),
			detail: &sarama.TopicDetail{
				NumPartitions:     t.cfg.RetryTopicPartitions,
				ReplicationFactor: 1,
				ConfigEntries: map[string]*string{
					"cleanup.policy": toPtr("delete"),
					"retention.ms":   toPtr("-1"),
				},
			},
		},
	}

	for _, tc := range topics {
		g.Go(func() error {
			err := admin.CreateTopic(tc.name, tc.detail, false)
			if err != nil {
				sErr, ok := err.(*sarama.TopicError)
				if !ok || sErr.Err != sarama.ErrTopicAlreadyExists {
					return errors.WithStack(err)
				}
			}

			return nil
		})
	}

	return errors.WithStack(g.Wait())
}

func (t *ErrorTracker) restoreState(ctx context.Context, topic string) error {
	refillCfg := *t.cfg
	refillCfg.GroupID = uuid.New().String()

	refillConsumer, err := NewKafkaConsumer(&refillCfg, t.logger)
	if err != nil {
		return errors.WithStack(err)
	}

	refillCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		errCh <- refillConsumer.Consume(refillCtx, t.redirectTopic(topic), newRedirectFillHandler(t))
	}()

	<-refillCtx.Done()

	if err := refillConsumer.Close(); err != nil {
		return errors.WithStack(err)
	}

	saramaConfig := sarama.NewConfig()

	admin, err := sarama.NewClusterAdmin(t.cfg.Brokers, saramaConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() { _ = admin.Close() }()

	if err := admin.DeleteConsumerGroup(refillCfg.GroupID); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (t *ErrorTracker) startConsumers(ctx context.Context, topic string) error {
	retryConsumer, err := NewKafkaConsumer(t.cfg, t.logger)
	if err != nil {
		return errors.WithStack(err)
	}

	handler, ok := t.registry.Get(topic)
	if !ok || handler == nil {
		return fmt.Errorf("handler for topic: %s not found", topic)
	}

	go func() {
		_ = retryConsumer.WithMiddlewares(NewRetryMiddleware(t)).Consume(ctx, t.retryTopic(topic), handler)
	}()

	redirectConsumer, err := NewKafkaConsumer(t.cfg, t.logger)
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

func (t *ErrorTracker) IsRelated(_ string, msg *Message) bool {
	return t.comparator.IsRelated(context.Background(), msg)
}

func (t *ErrorTracker) Redirect(ctx context.Context, msg *Message) error {
	return t.RedirectWithError(ctx, msg, nil)
}

func (t *ErrorTracker) RedirectWithError(ctx context.Context, msg *Message, lastError error) error {
	// Calculate retry metadata
	currentAttempt, _ := GetHeaderValue[int](&msg.Headers, HeaderRetryAttempt)

	originalTime, ok := GetHeaderValue[time.Time](&msg.Headers, HeaderRetryOriginalTime)
	if !ok {
		originalTime = time.Now()
	}

	nextDelay := t.backoffStrategy.NextDelay(currentAttempt)
	nextRetryTime := time.Now().Add(nextDelay)
	nextAttempt := currentAttempt + 1

	id, err := t.comparator.AddMessage(ctx, msg)
	if err != nil {
		return errors.WithStack(err)
	}

	t.logger.Info("redirecting message to retry topic",
		zap.String("topic", msg.topic),
		zap.Int("attempt", nextAttempt),
		zap.Int("max_retries", t.cfg.MaxRetries),
		zap.Duration("next_delay", nextDelay),
		zap.Time("next_retry_time", nextRetryTime),
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return t.publishToRetry(ctx, msg, id, nextAttempt, nextRetryTime, originalTime, lastError)
	})

	g.Go(func() error {
		return t.publishToRedirect(ctx, msg, id)
	})

	return errors.WithStack(g.Wait())
}

func (t *ErrorTracker) publishToRetry(
	ctx context.Context,
	msg *Message,
	id string,
	nextAttempt int,
	nextRetryTime time.Time,
	originalTime time.Time,
	lastError error,
) error {
	retryHeaders := make(HeaderList, 0, len(msg.Headers)+6)
	for _, h := range msg.Headers {
		key := string(h.Key)
		if key != HeaderRetryAttempt && key != HeaderRetryMax &&
			key != HeaderRetryNextTime && key != HeaderRetryOriginalTime &&
			key != HeaderRetryReason && key != headerID && key != headerRetry {
			retryHeaders = append(retryHeaders, h)
		}
	}

	SetHeader[int](&retryHeaders, HeaderRetryAttempt, nextAttempt)
	SetHeader[int](&retryHeaders, HeaderRetryMax, t.cfg.MaxRetries)
	SetHeader[time.Time](&retryHeaders, HeaderRetryNextTime, nextRetryTime)
	SetHeader[time.Time](&retryHeaders, HeaderRetryOriginalTime, originalTime)

	if lastError != nil {
		SetHeader[string](&retryHeaders, HeaderRetryReason, lastError.Error())
	}

	SetHeader[string](&retryHeaders, headerID, id)
	SetHeader[string](&retryHeaders, headerRetry, "true")
	SetHeader[string](&retryHeaders, HeaderTopic, msg.topic)

	retryMsg := &Message{
		topic:   t.retryTopic(msg.topic),
		Key:     msg.Key,
		Payload: msg.Payload,
		Headers: retryHeaders,
	}

	return t.producer.publish(ctx, retryMsg)
}

func (t *ErrorTracker) publishToRedirect(ctx context.Context, msg *Message, id string) error {
	newMessage := &Message{
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
}

func (t *ErrorTracker) Free(ctx context.Context, msg *Message) error {
	id, ok := GetHeaderValue[string](&msg.Headers, headerRetry)
	if !ok {
		return errors.New("topic id not found")
	}

	topic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}

	newMessage := &Message{
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

func (t *ErrorTracker) ReleaseMessage(ctx context.Context, msg *Message) error {
	topic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}

	key, ok := GetHeaderValue[string](&msg.Headers, headerKey)
	if !ok {
		return errors.New("key header not found")
	}

	if err := t.comparator.ReleaseMessage(ctx, &Message{
		Key:   []byte(key),
		topic: topic,
	}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// SendToDLQ sends a message that has exceeded max retries to the Dead Letter Queue.
// It adds DLQ metadata headers and publishes to the DLQ topic.
func (t *ErrorTracker) SendToDLQ(ctx context.Context, msg *Message, lastError error) error {
	// Get original topic from headers
	originalTopic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		originalTopic = msg.topic
	}

	// Get retry metadata
	attempt, _ := GetHeaderValue[int](&msg.Headers, HeaderRetryAttempt)
	originalTime, _ := GetHeaderValue[time.Time](&msg.Headers, HeaderRetryOriginalTime)

	// Copy original message headers (excluding retry headers)
	dlqHeaders := make(HeaderList, 0, len(msg.Headers)+4)
	for _, h := range msg.Headers {
		key := string(h.Key)
		// Keep original headers but not retry-specific ones
		if key != HeaderRetryAttempt && key != HeaderRetryMax &&
			key != HeaderRetryNextTime && key != headerID && key != headerRetry {
			dlqHeaders = append(dlqHeaders, h)
		}
	}

	// Add DLQ metadata headers
	SetHeader[string](&dlqHeaders, "x-dlq-reason", lastError.Error())
	SetHeader[time.Time](&dlqHeaders, "x-dlq-timestamp", time.Now())
	SetHeader[string](&dlqHeaders, "x-dlq-source-topic", originalTopic)
	SetHeader[int](&dlqHeaders, "x-dlq-retry-attempts", attempt)

	if !originalTime.IsZero() {
		SetHeader[time.Time](&dlqHeaders, "x-dlq-original-failure-time", originalTime)
	}

	dlqMsg := &Message{
		topic:   t.dlqTopic(originalTopic),
		Key:     msg.Key,
		Payload: msg.Payload,
		Headers: dlqHeaders,
	}

	t.logger.Error("sending message to DLQ (max retries exceeded)",
		zap.String("original_topic", originalTopic),
		zap.String("dlq_topic", dlqMsg.topic),
		zap.Int("attempts", attempt),
		zap.Int("max_retries", t.cfg.MaxRetries),
		zap.Error(lastError),
	)

	return t.producer.publish(ctx, dlqMsg)
}

func (t *ErrorTracker) retryTopic(topic string) string {
	return t.cfg.RetryTopicPrefix + "_" + topic
}

func (t *ErrorTracker) redirectTopic(topic string) string {
	return t.cfg.RedirectTopicPrefix + "_" + topic
}

func (t *ErrorTracker) dlqTopic(topic string) string {
	return t.cfg.DLQTopicPrefix + "_" + topic
}

func (t *ErrorTracker) HandleMaxRetriesExceeded(ctx context.Context, message *Message, currentAttempt, maxRetries int) error {
	t.logger.Warn("max retries exceeded, sending to DLQ",
		zap.String("topic", message.topic),
		zap.Int("current_attempt", currentAttempt),
		zap.Int("max_retries", maxRetries),
	)

	// Get the last error reason from headers
	lastErrorReason, _ := GetHeaderValue[string](&message.Headers, HeaderRetryReason)

	lastError := errors.New(lastErrorReason)
	if lastErrorReason == "" {
		lastError = errors.New("max retries exceeded")
	}

	// Send to DLQ
	if err := t.SendToDLQ(ctx, message, lastError); err != nil {
		t.logger.Error("failed to send message to DLQ",
			zap.String("topic", message.topic),
			zap.Error(err),
		)

		return errors.Wrap(err, "failed to send message to DLQ")
	}

	// Still publish tombstone to remove from tracking
	if err := t.Free(ctx, message); err != nil {
		t.logger.Error("failed to publish tombstone after DLQ",
			zap.String("topic", message.topic),
			zap.Error(err),
		)

		return errors.Wrap(err, "failed to free message after DLQ")
	}

	t.logger.Info("successfully sent to DLQ and freed from tracking",
		zap.String("topic", message.topic),
	)

	return nil
}

func (t *ErrorTracker) WaitForRetryTime(ctx context.Context, message *Message) error {
	// Check if this message has a scheduled retry time
	nextRetryTime, ok := GetHeaderValue[time.Time](&message.Headers, HeaderRetryNextTime)
	if !ok {
		return nil
	}

	now := time.Now()
	if !now.Before(nextRetryTime) {
		return nil
	}

	// Calculate remaining delay
	delay := nextRetryTime.Sub(now)

	t.logger.Debug("waiting before retry processing",
		zap.String("topic", message.topic),
		zap.Duration("delay", delay),
		zap.Time("scheduled_for", nextRetryTime),
	)

	// Wait until the scheduled time or context is canceled
	select {
	case <-time.After(delay):
		// Continue processing after delay
		t.logger.Debug("delay complete, processing message",
			zap.String("topic", message.topic),
		)

		return nil
	case <-ctx.Done():
		// Context canceled during delay
		return errors.WithStack(ctx.Err())
	}
}

type redirectFillHandler struct {
	t *ErrorTracker
}

func newRedirectFillHandler(t *ErrorTracker) *redirectFillHandler {
	return &redirectFillHandler{t: t}
}

func (r *redirectFillHandler) Handle(ctx context.Context, msg *Message) error {
	// Skip tombstone events (they represent already-completed retries)
	if msg.Payload == nil {
		return nil
	}

	// Extract original topic and key from headers
	originalTopic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		return errors.New("redirect message missing topic header")
	}

	originalKey, ok := GetHeaderValue[string](&msg.Headers, headerKey)
	if !ok {
		return errors.New("redirect message missing key header")
	}

	// Reconstruct original message to restore to comparator
	originalMsg := &Message{
		topic: originalTopic,
		Key:   []byte(originalKey),
	}

	// Restore to comparator's tracking state
	_, err := r.t.comparator.AddMessage(ctx, originalMsg)
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

func (r *RedirectHandler) Handle(ctx context.Context, msg *Message) error {
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
