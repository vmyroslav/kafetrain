package resilience

import (
	"context"
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
	errors          chan error
	cfg             *Config
}

// NewErrorTracker creates a new ErrorTracker for standalone usage (Layer 1).
// This tracker only manages redirect topic and tracking logic.
// Users must run their own retry consumer.
func NewErrorTracker(
	cfg *Config,
	logger *zap.Logger,
	comparator MessageChainTracker,
) (*ErrorTracker, error) {
	return NewErrorTrackerWithBackoff(cfg, logger, comparator, nil)
}

// NewErrorTrackerWithBackoff creates a new ErrorTracker with custom backoff strategy.
func NewErrorTrackerWithBackoff(
	cfg *Config,
	logger *zap.Logger,
	comparator MessageChainTracker,
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
		producer:        producer,
		comparator:      comparator,
		backoffStrategy: backoff,
		errors:          make(chan error, 256),
	}

	return &t, nil
}

// StartTracking starts the redirect topic consumer to manage message chain tracking.
// This is the Layer 1 API - users run their own retry consumer.
// For managed retry consumers, use RetryManager (Layer 2).
func (t *ErrorTracker) StartTracking(ctx context.Context, topic string) error {
	if err := t.ensureTopicsExist(topic); err != nil {
		return err
	}

	if err := t.restoreState(ctx, topic); err != nil {
		return err
	}

	return t.startRedirectConsumer(ctx, topic)
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

	// Query primary topic partition count for partition affinity
	primaryPartitions := t.cfg.RetryTopicPartitions // fallback to config

	metadata, err := admin.DescribeTopics([]string{topic})
	if err == nil && len(metadata) > 0 && len(metadata[0].Partitions) > 0 {
		primaryPartitions = int32(len(metadata[0].Partitions))

		t.logger.Debug("using primary topic partition count for retry topics",
			zap.String("topic", topic),
			zap.Int32("partitions", primaryPartitions),
		)
	} else {
		t.logger.Warn("could not query primary topic, using config value",
			zap.String("topic", topic),
			zap.Int32("partitions", primaryPartitions),
			zap.Error(err),
		)
	}

	g := errgroup.Group{}
	topics := []struct {
		detail *sarama.TopicDetail
		name   string
	}{
		{
			name: t.retryTopic(topic),
			detail: &sarama.TopicDetail{
				NumPartitions:     primaryPartitions, // Match primary topic
				ReplicationFactor: 1,
				ConfigEntries: map[string]*string{
					"cleanup.policy": toPtr("delete"),
					"retention.ms":   toPtr("3600000"), // 1 hour retention
				},
			},
		},
		{
			name: t.redirectTopic(topic),
			detail: &sarama.TopicDetail{
				NumPartitions:     primaryPartitions, // Match primary topic
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
				NumPartitions:     primaryPartitions, // Match primary topic
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

// startRedirectConsumer starts only the redirect topic consumer.
// This consumer processes tombstones to release messages from tracking.
func (t *ErrorTracker) startRedirectConsumer(ctx context.Context, topic string) error {
	redirectCfg := *t.cfg
	redirectCfg.GroupID = t.cfg.GroupID + "-redirect"

	redirectConsumer, err := NewKafkaConsumer(&redirectCfg, t.logger)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		t.errors <- redirectConsumer.Consume(ctx, t.redirectTopic(topic), NewRedirectHandler(t))
	}()

	return nil
}

// startRetryConsumerWithHandler is an internal helper for starting retry consumer.
// This is used by RetryManager (Layer 2) to manage retry consumer lifecycle.
func (t *ErrorTracker) startRetryConsumerWithHandler(
	ctx context.Context,
	topic string,
	handler MessageHandler,
) error {
	retryCfg := *t.cfg
	retryCfg.GroupID = t.cfg.GroupID + "-retry"

	retryConsumer, err := NewKafkaConsumer(&retryCfg, t.logger)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		_ = retryConsumer.WithMiddlewares(NewRetryMiddleware(t)).
			Consume(ctx, t.retryTopic(topic), handler)
	}()

	return nil
}

func (t *ErrorTracker) Errors() <-chan error {
	return t.errors
}

// GetRetryTopic returns the retry topic name for a given primary topic.
// This allows users to manually consume retry messages (Layer 1 usage).
func (t *ErrorTracker) GetRetryTopic(topic string) string {
	return t.retryTopic(topic)
}

// IsInRetryChain checks if a Sarama message is already in the retry chain.
// Returns true if the message key is currently being tracked for retries.
func (t *ErrorTracker) IsInRetryChain(msg *sarama.ConsumerMessage) bool {
	internalMsg := t.toInternalMessage(msg)
	return t.comparator.IsRelated(context.Background(), internalMsg)
}

// Redirect sends a failed Sarama message to the retry topic for reprocessing.
// This is the primary public API for standalone ErrorTracker usage.
// The message will be tracked and retried according to the configured backoff strategy.
func (t *ErrorTracker) Redirect(ctx context.Context, msg *sarama.ConsumerMessage, lastError error) error {
	internalMsg := t.toInternalMessage(msg)

	// Check if already in retry chain
	if t.comparator.IsRelated(ctx, internalMsg) {
		t.logger.Debug("message already in retry chain, skipping redirect",
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
		)
		return nil
	}

	return t.redirectMessageWithError(ctx, internalMsg, lastError)
}

// Free removes a successfully processed Sarama message from the retry chain.
// This is the primary public API for standalone ErrorTracker usage.
// Publishes a tombstone to the redirect topic to release the message key.
func (t *ErrorTracker) Free(ctx context.Context, msg *sarama.ConsumerMessage) error {
	internalMsg := t.toInternalMessage(msg)
	return t.freeMessage(ctx, internalMsg)
}

// toInternalMessage converts a Sarama ConsumerMessage to internal Message type.
func (t *ErrorTracker) toInternalMessage(msg *sarama.ConsumerMessage) *Message {
	headers := make(HeaderList, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = Header{
			Key:   h.Key,
			Value: h.Value,
		}
	}

	return &Message{
		topic:     msg.Topic,
		partition: msg.Partition,
		offset:    msg.Offset,
		Key:       msg.Key,
		Payload:   msg.Value,
		Headers:   headers,
		Timestamp: msg.Timestamp,
	}
}

// redirectMessage is the internal Message-based redirect used by middlewares.
// For standalone usage, use Redirect(*sarama.ConsumerMessage, error) instead.
func (t *ErrorTracker) redirectMessage(ctx context.Context, msg *Message) error {
	return t.redirectMessageWithError(ctx, msg, nil)
}

// redirectMessageWithError is the internal Message-based redirect implementation.
// For standalone usage, use Redirect(*sarama.ConsumerMessage, error) instead.
func (t *ErrorTracker) redirectMessageWithError(ctx context.Context, msg *Message, lastError error) error {
	// Calculate retry metadata
	currentAttempt, _ := GetHeaderValue[int](&msg.Headers, HeaderRetryAttempt)

	originalTime, ok := GetHeaderValue[time.Time](&msg.Headers, HeaderRetryOriginalTime)
	if !ok {
		originalTime = time.Now()
	}

	// Get the ORIGINAL topic from headers (if this is already a retry message)
	// This prevents creating "retry_retry_..." topic names
	originalTopic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		// This is the first redirect, use the current topic
		originalTopic = msg.topic
	}

	nextDelay := t.backoffStrategy.NextDelay(currentAttempt)
	nextRetryTime := time.Now().Add(nextDelay)
	nextAttempt := currentAttempt + 1

	// Use key directly - tracking already done in middleware
	key := string(msg.Key)

	t.logger.Debug("redirecting message to retry topic",
		zap.String("topic", msg.topic),
		zap.String("original_topic", originalTopic),
		zap.String("key", key),
		zap.Int("attempt", nextAttempt),
		zap.Int("max_retries", t.cfg.MaxRetries),
		zap.Duration("next_delay", nextDelay),
		zap.Time("next_retry_time", nextRetryTime),
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := t.publishToRetry(ctx, msg, key, nextAttempt, nextRetryTime, originalTime, lastError, originalTopic); err != nil {
			t.logger.Error("failed to publish to retry topic",
				zap.String("topic", originalTopic),
				zap.Error(err),
			)
			return err
		}
		t.logger.Debug("published to retry topic",
			zap.String("retry_topic", t.retryTopic(originalTopic)),
			zap.Int("next_attempt", nextAttempt),
		)
		return nil
	})

	g.Go(func() error {
		if err := t.publishToRedirect(ctx, msg, key, originalTopic); err != nil {
			t.logger.Error("failed to publish to redirect topic",
				zap.String("topic", originalTopic),
				zap.Error(err),
			)
			return err
		}
		return nil
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
	originalTopic string,
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
	SetHeader[string](&retryHeaders, HeaderTopic, originalTopic)

	retryMsg := &Message{
		topic:   t.retryTopic(originalTopic),
		Key:     msg.Key,
		Payload: msg.Payload,
		Headers: retryHeaders,
	}

	return t.producer.publish(ctx, retryMsg)
}

func (t *ErrorTracker) publishToRedirect(ctx context.Context, msg *Message, id string, originalTopic string) error {
	newMessage := &Message{
		topic:   t.redirectTopic(originalTopic),
		Key:     []byte(id),
		Payload: []byte(id),
		Headers: HeaderList{
			{
				Key:   []byte(HeaderTopic),
				Value: []byte(originalTopic),
			},
			{
				Key:   []byte("key"),
				Value: msg.Key,
			},
		},
	}

	return t.producer.publish(ctx, newMessage)
}

// freeMessage is the internal Message-based free implementation used by middlewares.
// For standalone usage, use Free(*sarama.ConsumerMessage) instead.
func (t *ErrorTracker) freeMessage(ctx context.Context, msg *Message) error {
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

	// conditionally free from tracking based on config
	if t.cfg.FreeOnDLQ {
		if err := t.freeMessage(ctx, message); err != nil {
			t.logger.Error("failed to publish tombstone after DLQ",
				zap.String("topic", message.topic),
				zap.Error(err),
			)

			return errors.Wrap(err, "failed to free message after DLQ")
		}

		t.logger.Debug("successfully sent to DLQ and freed from tracking",
			zap.String("topic", message.topic),
			zap.Bool("free_on_dlq", t.cfg.FreeOnDLQ),
		)
	}

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
