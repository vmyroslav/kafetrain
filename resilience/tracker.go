package resilience

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ErrorTracker is the library-agnostic core retry tracking implementation.
// It uses interfaces to remain independent of any specific Kafka library.
type ErrorTracker struct {
	comparator      MessageChainTracker
	backoffStrategy BackoffStrategy
	logger          Logger
	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	errors          chan error
	cfg             *Config
}

// NewErrorTracker creates a new ErrorTracker using library-agnostic interfaces.
// This is the core constructor that works with any Kafka library via adapters.
func NewErrorTracker(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	comparator MessageChainTracker,
) (*ErrorTracker, error) {
	return NewErrorTrackerWithBackoff(cfg, logger, producer, consumerFactory, admin, comparator, nil)
}

// NewErrorTrackerWithBackoff creates a new ErrorTracker with custom backoff strategy.
func NewErrorTrackerWithBackoff(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	comparator MessageChainTracker,
	backoff BackoffStrategy,
) (*ErrorTracker, error) {
	if comparator == nil {
		comparator = NewKeyTracker()
	}

	if backoff == nil {
		backoff = NewExponentialBackoff()
	}

	t := ErrorTracker{
		cfg:             cfg,
		logger:          logger,
		producer:        producer,
		consumerFactory: consumerFactory,
		admin:           admin,
		comparator:      comparator,
		backoffStrategy: backoff,
		errors:          make(chan error, 256),
	}

	return &t, nil
}

// StartTracking starts the redirect topic consumer to manage message chain tracking.
func (t *ErrorTracker) StartTracking(ctx context.Context, topic string) error {
	// ensure topics exist before starting
	if err := t.ensureTopicsExist(ctx, topic); err != nil {
		return err
	}

	if err := t.restoreState(ctx, topic); err != nil {
		return err
	}

	return t.startRedirectConsumer(ctx, topic)
}

// ensureTopicsExist creates retry, redirect, and DLQ topics if they don't exist.
// Queries primary topic partition count and creates topics with matching partitions.
func (t *ErrorTracker) ensureTopicsExist(ctx context.Context, topic string) error {
	// Query primary topic partition count
	primaryPartitions := t.cfg.RetryTopicPartitions // fallback

	metadata, err := t.admin.DescribeTopics(ctx, []string{topic})
	if err == nil && len(metadata) > 0 {
		primaryPartitions = metadata[0].Partitions()

		t.logger.Debug("using primary topic partition count for retry topics",
			"topic", topic,
			"partitions", primaryPartitions,
		)
	} else {
		t.logger.Warn("could not query primary topic, using config value",
			"topic", topic,
			"partitions", primaryPartitions,
			"error", err,
		)
	}

	// Create retry topic
	if err := t.admin.CreateTopic(ctx, t.retryTopic(topic), primaryPartitions, 1, map[string]string{
		"cleanup.policy": "delete",
		"retention.ms":   "3600000", // 1 hour
	}); err != nil {
		return errors.WithStack(err)
	}

	// Create redirect topic
	if err := t.admin.CreateTopic(ctx, t.redirectTopic(topic), primaryPartitions, 1, map[string]string{
		"cleanup.policy": "compact",
		"segment.ms":     "100",
	}); err != nil {
		return errors.WithStack(err)
	}

	// Create DLQ topic
	if err := t.admin.CreateTopic(ctx, t.dlqTopic(topic), primaryPartitions, 1, map[string]string{
		"cleanup.policy": "delete",
		"retention.ms":   "-1", // infinite
	}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (t *ErrorTracker) restoreState(ctx context.Context, topic string) error {
	refillCfg := *t.cfg
	refillCfg.GroupID = uuid.New().String()

	// Create temporary consumer for state restoration
	refillConsumer, err := t.consumerFactory.NewConsumer(refillCfg.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}

	refillCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		errCh <- refillConsumer.Consume(refillCtx, []string{t.redirectTopic(topic)}, &redirectFillHandler{t: t})
	}()

	<-refillCtx.Done()

	if err := refillConsumer.Close(); err != nil {
		return errors.WithStack(err)
	}

	// Delete ephemeral consumer group (critical: prevents orphaning)
	if err := t.admin.DeleteConsumerGroup(ctx, refillCfg.GroupID); err != nil {
		t.logger.Warn("failed to delete ephemeral consumer group",
			"group_id", refillCfg.GroupID,
			"error", err,
		)
		// Non-fatal: group will be cleaned up by Kafka eventually
	}

	return nil
}

// startRedirectConsumer starts only the redirect topic consumer.
// This consumer processes tombstones to release messages from tracking.
func (t *ErrorTracker) startRedirectConsumer(ctx context.Context, topic string) error {
	redirectCfg := *t.cfg
	redirectCfg.GroupID = t.cfg.GroupID + "-redirect"

	redirectConsumer, err := t.consumerFactory.NewConsumer(redirectCfg.GroupID)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		t.errors <- redirectConsumer.Consume(ctx, []string{t.redirectTopic(topic)}, &RedirectHandler{t: t})
	}()

	return nil
}

func (t *ErrorTracker) Errors() <-chan error {
	return t.errors
}

// GetRetryTopic returns the retry topic name for a given primary topic.
func (t *ErrorTracker) GetRetryTopic(topic string) string {
	return t.retryTopic(topic)
}

// GetRedirectTopic returns the redirect topic name for a given primary topic.
func (t *ErrorTracker) GetRedirectTopic(topic string) string {
	return t.redirectTopic(topic)
}

// GetDLQTopic returns the DLQ topic name for a given primary topic.
func (t *ErrorTracker) GetDLQTopic(topic string) string {
	return t.dlqTopic(topic)
}

// IsInRetryChain checks if a message is already in the retry chain.
// Returns true if the message key is currently being tracked for retries.
func (t *ErrorTracker) IsInRetryChain(msg Message) bool {
	internalMsg := t.toInternalMessage(msg)
	return t.comparator.IsRelated(context.Background(), internalMsg)
}

// Redirect sends a failed message to the retry topic for reprocessing.
// This is the primary public API for standalone ErrorTracker usage.
func (t *ErrorTracker) Redirect(ctx context.Context, msg Message, lastError error) error {
	internalMsg := t.toInternalMessage(msg)

	// Check if already in retry chain
	if t.comparator.IsRelated(ctx, internalMsg) {
		t.logger.Debug("message already in retry chain, skipping redirect",
			"topic", msg.Topic(),
			"offset", msg.Offset(),
		)
		return nil
	}

	return t.redirectMessageWithError(ctx, internalMsg, lastError)
}

// Free removes a successfully processed message from the retry chain.
// Publishes a tombstone to the redirect topic to release the message key.
func (t *ErrorTracker) Free(ctx context.Context, msg Message) error {
	internalMsg := t.toInternalMessage(msg)
	return t.freeMessage(ctx, internalMsg)
}

// toInternalMessage converts a Message interface to internal InternalMessage type.
func (t *ErrorTracker) toInternalMessage(msg Message) *InternalMessage {
	// Convert Headers interface to HeaderList
	headers := make(HeaderList, 0)
	for key, value := range msg.Headers().All() {
		headers = append(headers, Header{
			Key:   []byte(key),
			Value: value,
		})
	}

	return &InternalMessage{
		topic:     msg.Topic(),
		partition: msg.Partition(),
		offset:    msg.Offset(),
		Key:       msg.Key(),
		Payload:   msg.Value(),
		Headers:   headers,
		Timestamp: msg.Timestamp(),
	}
}

// redirectMessageWithError is the internal InternalMessage-based redirect implementation.
func (t *ErrorTracker) redirectMessageWithError(ctx context.Context, msg *InternalMessage, lastError error) error {
	// Calculate retry metadata
	currentAttempt, _ := GetHeaderValue[int](&msg.Headers, HeaderRetryAttempt)

	originalTime, ok := GetHeaderValue[time.Time](&msg.Headers, HeaderRetryOriginalTime)
	if !ok {
		originalTime = time.Now()
	}

	// Get the ORIGINAL topic from headers (if this is already a retry message)
	originalTopic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		// This is the first redirect, use the current topic
		originalTopic = msg.topic
	}

	nextDelay := t.backoffStrategy.NextDelay(currentAttempt)
	nextRetryTime := time.Now().Add(nextDelay)
	nextAttempt := currentAttempt + 1

	key := string(msg.Key)

	t.logger.Debug("redirecting message to retry topic",
		"topic", msg.topic,
		"original_topic", originalTopic,
		"key", key,
		"attempt", nextAttempt,
		"max_retries", t.cfg.MaxRetries,
		"next_delay", nextDelay,
		"next_retry_time", nextRetryTime,
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := t.publishToRetry(ctx, msg, key, nextAttempt, nextRetryTime, originalTime, lastError, originalTopic); err != nil {
			t.logger.Error("failed to publish to retry topic",
				"topic", originalTopic,
				"error", err,
			)
			return err
		}
		t.logger.Debug("published to retry topic",
			"retry_topic", t.retryTopic(originalTopic),
			"next_attempt", nextAttempt,
		)
		return nil
	})

	g.Go(func() error {
		if err := t.publishToRedirect(ctx, msg, key, originalTopic); err != nil {
			t.logger.Error("failed to publish to redirect topic",
				"topic", originalTopic,
				"error", err,
			)
			return err
		}
		return nil
	})

	return errors.WithStack(g.Wait())
}

func (t *ErrorTracker) publishToRetry(
	ctx context.Context,
	msg *InternalMessage,
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

	// Create wrapper that implements Message interface
	retryMsg := &messageWrapper{
		topic:     t.retryTopic(originalTopic),
		key:       msg.Key,
		value:     msg.Payload,
		headers:   &headerListWrapper{headers: retryHeaders},
		timestamp: time.Now(),
	}

	return t.producer.Produce(ctx, retryMsg.Topic(), retryMsg)
}

func (t *ErrorTracker) publishToRedirect(ctx context.Context, msg *InternalMessage, id string, originalTopic string) error {
	headers := HeaderList{
		{
			Key:   []byte(HeaderTopic),
			Value: []byte(originalTopic),
		},
		{
			Key:   []byte("key"),
			Value: msg.Key,
		},
	}

	redirectMsg := &messageWrapper{
		topic:     t.redirectTopic(originalTopic),
		key:       []byte(id),
		value:     []byte(id),
		headers:   &headerListWrapper{headers: headers},
		timestamp: time.Now(),
	}

	return t.producer.Produce(ctx, redirectMsg.Topic(), redirectMsg)
}

// freeMessage is the internal InternalMessage-based free implementation.
func (t *ErrorTracker) freeMessage(ctx context.Context, msg *InternalMessage) error {
	id, ok := GetHeaderValue[string](&msg.Headers, headerRetry)
	if !ok {
		return errors.New("topic id not found")
	}

	topic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}

	headers := HeaderList{
		{
			Key:   []byte(HeaderTopic),
			Value: []byte(topic),
		},
		{
			Key:   []byte(headerKey),
			Value: msg.Key,
		},
	}

	tombstoneMsg := &messageWrapper{
		topic:     t.redirectTopic(topic),
		key:       []byte(id),
		value:     nil, // Tombstone
		headers:   &headerListWrapper{headers: headers},
		timestamp: time.Now(),
	}

	return t.producer.Produce(ctx, tombstoneMsg.Topic(), tombstoneMsg)
}

func (t *ErrorTracker) ReleaseMessage(ctx context.Context, msg *InternalMessage) error {
	topic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}

	key, ok := GetHeaderValue[string](&msg.Headers, headerKey)
	if !ok {
		return errors.New("key header not found")
	}

	if err := t.comparator.ReleaseMessage(ctx, &InternalMessage{
		Key:   []byte(key),
		topic: topic,
	}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// SendToDLQ sends a message that has exceeded max retries to the Dead Letter Queue.
func (t *ErrorTracker) SendToDLQ(ctx context.Context, msg *InternalMessage, lastError error) error {
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

	dlqMsg := &messageWrapper{
		topic:     t.dlqTopic(originalTopic),
		key:       msg.Key,
		value:     msg.Payload,
		headers:   &headerListWrapper{headers: dlqHeaders},
		timestamp: time.Now(),
	}

	t.logger.Error("sending message to DLQ (max retries exceeded)",
		"original_topic", originalTopic,
		"dlq_topic", dlqMsg.topic,
		"attempts", attempt,
		"max_retries", t.cfg.MaxRetries,
		"error", lastError,
	)

	return t.producer.Produce(ctx, dlqMsg.Topic(), dlqMsg)
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

func (t *ErrorTracker) HandleMaxRetriesExceeded(ctx context.Context, message *InternalMessage, currentAttempt, maxRetries int) error {
	t.logger.Warn("max retries exceeded, sending to DLQ",
		"topic", message.topic,
		"current_attempt", currentAttempt,
		"max_retries", maxRetries,
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
			"topic", message.topic,
			"error", err,
		)

		return errors.Wrap(err, "failed to send message to DLQ")
	}

	// conditionally free from tracking based on config
	if t.cfg.FreeOnDLQ {
		if err := t.freeMessage(ctx, message); err != nil {
			t.logger.Error("failed to publish tombstone after DLQ",
				"topic", message.topic,
				"error", err,
			)

			return errors.Wrap(err, "failed to free message after DLQ")
		}

		t.logger.Debug("successfully sent to DLQ and freed from tracking",
			"topic", message.topic,
			"free_on_dlq", t.cfg.FreeOnDLQ,
		)
	}

	return nil
}

func (t *ErrorTracker) WaitForRetryTime(ctx context.Context, message *InternalMessage) error {
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
		"topic", message.topic,
		"delay", delay,
		"scheduled_for", nextRetryTime,
	)

	// Wait until the scheduled time or context is canceled
	select {
	case <-time.After(delay):
		t.logger.Debug("delay complete, processing message",
			"topic", message.topic,
		)
		return nil
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
}

// messageWrapper is an internal wrapper that implements the Message interface.
// Used for producing messages via the library-agnostic Producer interface.
type messageWrapper struct {
	topic     string
	partition int32
	offset    int64
	key       []byte
	value     []byte
	headers   Headers
	timestamp time.Time
}

func (m *messageWrapper) Topic() string        { return m.topic }
func (m *messageWrapper) Partition() int32     { return m.partition }
func (m *messageWrapper) Offset() int64        { return m.offset }
func (m *messageWrapper) Key() []byte          { return m.key }
func (m *messageWrapper) Value() []byte        { return m.value }
func (m *messageWrapper) Headers() Headers     { return m.headers }
func (m *messageWrapper) Timestamp() time.Time { return m.timestamp }

// headerListWrapper wraps HeaderList to implement the Headers interface.
type headerListWrapper struct {
	headers HeaderList
}

func (h *headerListWrapper) Get(key string) ([]byte, bool) {
	for _, header := range h.headers {
		if string(header.Key) == key {
			return header.Value, true
		}
	}
	return nil, false
}

func (h *headerListWrapper) Set(key string, value []byte) {
	// Find and update existing header
	for i, header := range h.headers {
		if string(header.Key) == key {
			h.headers[i].Value = value
			return
		}
	}
	// Add new header if not found
	h.headers = append(h.headers, Header{
		Key:   []byte(key),
		Value: value,
	})
}

func (h *headerListWrapper) All() map[string][]byte {
	result := make(map[string][]byte, len(h.headers))
	for _, header := range h.headers {
		result[string(header.Key)] = header.Value
	}
	return result
}

func (h *headerListWrapper) Delete(key string) {
	for i, header := range h.headers {
		if string(header.Key) == key {
			// Remove the header by slicing
			h.headers = append(h.headers[:i], h.headers[i+1:]...)
			return
		}
	}
}

func (h *headerListWrapper) Clone() Headers {
	// Deep copy the headers
	cloned := make(HeaderList, len(h.headers))
	for i, header := range h.headers {
		cloned[i] = Header{
			Key:   append([]byte(nil), header.Key...),
			Value: append([]byte(nil), header.Value...),
		}
	}
	return &headerListWrapper{headers: cloned}
}

// Handler types for internal consumers

type redirectFillHandler struct {
	t *ErrorTracker
}

func (r *redirectFillHandler) Handle(ctx context.Context, msg Message) error {
	// Skip tombstone events
	if msg.Value() == nil {
		return nil
	}

	// Extract original topic and key from headers
	originalTopicBytes, ok := msg.Headers().Get(HeaderTopic)
	if !ok {
		return errors.New("redirect message missing topic header")
	}

	originalKeyBytes, ok := msg.Headers().Get(headerKey)
	if !ok {
		return errors.New("redirect message missing key header")
	}

	// Reconstruct original message to restore to comparator
	originalMsg := &InternalMessage{
		topic: string(originalTopicBytes),
		Key:   originalKeyBytes,
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

func (r *RedirectHandler) Handle(ctx context.Context, msg Message) error {
	// Only process tombstones
	if msg.Value() != nil {
		return nil
	}

	internalMsg := r.t.toInternalMessage(msg)
	err := r.t.ReleaseMessage(ctx, internalMsg)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// RetriableError wraps an error to indicate if it should be retried.
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
