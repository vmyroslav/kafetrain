package resilience

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/avast/retry-go/v4"
	"golang.org/x/sync/errgroup"
)

// ErrorTracker is the library-agnostic core retry tracking implementation.
// It uses interfaces to remain independent of any specific Kafka library.
type ErrorTracker struct {
	coordinator     StateCoordinator
	backoff         BackoffStrategy
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
	backoff BackoffStrategy,
) (*ErrorTracker, error) {
	if backoff == nil {
		backoff = NewExponentialBackoff()
	}

	errCh := make(chan error, 256)

	// Default to Kafka-based coordination
	coordinator := NewKafkaStateCoordinator(
		cfg,
		logger,
		producer,
		consumerFactory,
		admin,
		comparator,
		errCh,
	)

	t := ErrorTracker{
		cfg:             cfg,
		logger:          logger,
		producer:        producer,
		consumerFactory: consumerFactory,
		admin:           admin,
		coordinator:     coordinator,
		backoff:         backoff,
		errors:          errCh,
	}

	return &t, nil
}

// Start initializes the complete resilience loop for a topic.
// 1. It starts the control plane (Coordinator) to manage ordering locks.
// 2. It starts the data plane (StartRetryWorker) to process retry messages.
func (t *ErrorTracker) Start(ctx context.Context, topic string, handler ConsumerHandler) error {
	if err := t.coordinator.Start(ctx, topic); err != nil {
		return err
	}
	return t.StartRetryWorker(ctx, topic, handler)
}

// ensureTopicsExist creates retry and DLQ topics if they don't exist.
// Queries primary topic partition count and creates topics with matching partitions.
func (t *ErrorTracker) ensureTopicsExist(ctx context.Context, topic string) error {
	// determine partition count
	partitions := t.cfg.RetryTopicPartitions

	// if config is 0 (default), query primary topic
	if partitions == 0 {
		metadata, err := t.admin.DescribeTopics(ctx, []string{topic})
		if err != nil {
			return fmt.Errorf("failed to describe primary topic '%s' for partition count auto-detection: %w", topic, err)
		}
		if len(metadata) == 0 {
			return fmt.Errorf("no metadata found for primary topic '%s' during partition count auto-detection", topic)
		}

		partitions = metadata[0].Partitions()
		t.logger.Debug("using primary topic partition count",
			"topic", topic,
			"partitions", partitions,
		)
	}

	g, ctx := errgroup.WithContext(ctx)

	// create retry topic
	g.Go(func() error {
		if err := t.admin.CreateTopic(ctx, t.retryTopic(topic), partitions, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "3600000", // 1 hour
		}); err != nil {
			return fmt.Errorf("failed to create retry topic: %w", err)
		}
		return nil
	})

	// create DLQ topic
	g.Go(func() error {
		if err := t.admin.CreateTopic(ctx, t.dlqTopic(topic), partitions, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "-1", // infinite
		}); err != nil {
			return fmt.Errorf("failed to create DLQ topic: %w", err)
		}
		return nil
	})

	return g.Wait()
}

// StartRetryWorker starts a background consumer for the retry topic.
// This worker automatically handles backoff delays, retries, and lifecycle management (Redirect/Free).
// It implements the Bulkhead Pattern, keeping retry processing separate from the main consumer.
func (t *ErrorTracker) StartRetryWorker(ctx context.Context, topic string, handler ConsumerHandler) error {
	// ensure retry/dlq topics exist
	if err := t.ensureTopicsExist(ctx, topic); err != nil {
		return err
	}

	retryTopic := t.retryTopic(topic)
	retryConsumerGroup := t.cfg.GroupID + "-retry"

	retryConsumer, err := t.consumerFactory.NewConsumer(retryConsumerGroup)
	if err != nil {
		return fmt.Errorf("failed to create retry consumer: %w", err)
	}

	workerHandler := &retryWorkerHandler{
		t:           t,
		userHandler: handler,
	}

	go func() {
		defer retryConsumer.Close()
		_ = retryConsumer.Consume(ctx, []string{retryTopic}, workerHandler)
	}()

	t.logger.Info("started background retry worker",
		"topic", retryTopic,
		"group_id", retryConsumerGroup,
	)

	return nil
}

type retryWorkerHandler struct {
	t           *ErrorTracker
	userHandler ConsumerHandler
}

func (h *retryWorkerHandler) Handle(ctx context.Context, msg Message) error {
	// 1. Wait for backoff delay
	if err := h.t.WaitForRetryTime(ctx, msg); err != nil {
		return err
	}

	// 2. Call user's business logic
	err := h.userHandler.Handle(ctx, msg)

	// 3. Handle result
	if err != nil {
		// Failed again -> Redirect (will check max retries internally)
		return h.t.Redirect(ctx, msg, err)
	}

	// Success -> Free the lock
	return h.t.Free(ctx, msg)
}

func (t *ErrorTracker) Errors() <-chan error {
	return t.errors
}

// GetRetryTopic returns the retry topic name for a given primary topic.
func (t *ErrorTracker) GetRetryTopic(topic string) string {
	return t.retryTopic(topic)
}

// GetDLQTopic returns the DLQ topic name for a given primary topic.
func (t *ErrorTracker) GetDLQTopic(topic string) string {
	return t.dlqTopic(topic)
}

// IsInRetryChain checks if a message is already in the retry chain.
// Returns true if the message key is currently being tracked for retries.
func (t *ErrorTracker) IsInRetryChain(msg Message) bool {
	internalMsg := t.toInternalMessage(msg)
	return t.coordinator.IsLocked(context.Background(), internalMsg)
}

// NewResilientHandler wraps a user handler with resilience logic for the MAIN topic.
// It automatically handles strict ordering checks and error redirection.
// Usage: mainConsumer.Consume(ctx, topics, tracker.NewResilientHandler(myHandler))
func (t *ErrorTracker) NewResilientHandler(handler ConsumerHandler) ConsumerHandler {
	return ConsumerHandlerFunc(func(ctx context.Context, msg Message) error {
		// 1. Strict Ordering Check
		// If this key is currently retrying (from a previous message), we must
		// redirect this new message to the retry queue to maintain order.
		if t.IsInRetryChain(msg) {
			return t.Redirect(ctx, msg, errors.New("strict ordering: predecessor in retry"))
		}

		// 2. Process Message
		if err := handler.Handle(ctx, msg); err != nil {
			// Failure -> Start retry chain
			return t.Redirect(ctx, msg, err)
		}

		// 3. Success
		// No need to call Free() here because main topic messages don't hold a lock.
		return nil
	})
}

// Redirect sends a failed message to the retry topic for reprocessing.
// This is the primary public API for standalone ErrorTracker usage.
func (t *ErrorTracker) Redirect(ctx context.Context, msg Message, lastError error) error {
	internalMsg := t.toInternalMessage(msg)
	return t.redirectMessageWithError(ctx, internalMsg, lastError)
}

// Free removes a successfully processed message from the retry chain.
// Releases the lock via the coordinator.
func (t *ErrorTracker) Free(ctx context.Context, msg Message) error {
	internalMsg := t.toInternalMessage(msg)
	return t.coordinator.Release(ctx, internalMsg)
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

	// Check if max retries would be exceeded
	if currentAttempt >= t.cfg.MaxRetries {
		return t.HandleMaxRetriesExceeded(ctx, msg, currentAttempt, t.cfg.MaxRetries)
	}

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

	nextDelay := t.backoff.NextDelay(currentAttempt)
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

	// Sequential publish with compensating rollback to ensure atomicity.
	// 1. Acquire Lock (Coordinator)
	if err := t.coordinator.Acquire(ctx, msg, originalTopic); err != nil {
		return fmt.Errorf("failed to acquire lock (topic=%s, key=%s): %w", originalTopic, key, err)
	}

	// 2. Publish to Retry Topic
	if err := t.publishToRetry(ctx, msg, key, nextAttempt, nextRetryTime, originalTime, lastError, originalTopic); err != nil {
		t.logger.Error("failed to publish to retry topic, attempting rollback of lock",
			"topic", originalTopic,
			"key", key,
			"error", err,
		)

		// Rollback: try to unlock the key since we failed to send the retry message.
		// We retry this critical operation to avoid leaving a "Zombie Key" (locked forever).
		rollbackErr := retry.Do(
			func() error {
				return t.coordinator.Release(ctx, msg)
			},
			retry.Attempts(3),
			retry.Delay(200*time.Millisecond),
			retry.Context(ctx),
		)

		if rollbackErr != nil {
			t.logger.Error("CRITICAL: failed to rollback lock after retry publish failure. Key may be permanently locked!",
				"topic", originalTopic,
				"key", key,
				"original_error", err,
				"rollback_error", rollbackErr,
			)
		}

		return fmt.Errorf("failed to publish to retry topic (topic=%s, key=%s): %w", originalTopic, key, err)
	}

	return nil
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

		return fmt.Errorf("failed to send message to DLQ: %w", err)
	}

	// conditionally free from tracking based on config
	if t.cfg.FreeOnDLQ {
		if err := t.coordinator.Release(ctx, message); err != nil {
			t.logger.Error("failed to release lock after DLQ",
				"topic", message.topic,
				"error", err,
			)

			return fmt.Errorf("failed to free message after DLQ: %w", err)
		}

		t.logger.Debug("successfully sent to DLQ and released lock",
			"topic", message.topic,
			"free_on_dlq", t.cfg.FreeOnDLQ,
		)
	}

	return nil
}

func (t *ErrorTracker) WaitForRetryTime(ctx context.Context, msg Message) error {
	// check if this message has a scheduled retry time
	nextTimeBytes, ok := msg.Headers().Get(HeaderRetryNextTime)
	if !ok {
		return nil
	}

	// Parse timestamp (stored as Unix string)
	ts, err := strconv.ParseInt(string(nextTimeBytes), 10, 64)
	if err != nil {
		// Log warning but don't block if header is corrupted
		t.logger.Warn("invalid retry timestamp header", "error", err)
		return nil
	}

	nextRetryTime := time.Unix(ts, 0)
	now := time.Now()
	if !now.Before(nextRetryTime) {
		return nil
	}

	// Calculate remaining delay
	delay := nextRetryTime.Sub(now)

	t.logger.Debug("waiting before retry processing",
		"topic", msg.Topic(),
		"delay", delay,
		"scheduled_for", nextRetryTime,
	)

	// Wait until the scheduled time or context is canceled
	select {
	case <-time.After(delay):
		t.logger.Debug("delay complete, processing message",
			"topic", msg.Topic(),
		)

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TODO: remove it
// messageWrapper is an internal wrapper that implements the Message interface.
// Used for producing messages via the library-agnostic Producer interface.
type messageWrapper struct {
	timestamp time.Time
	headers   Headers
	topic     string
	key       []byte
	value     []byte
	offset    int64
	partition int32
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
	newHeaders := make(HeaderList, 0, len(h.headers))
	for _, header := range h.headers {
		if string(header.Key) != key {
			newHeaders = append(newHeaders, header)
		}
	}
	h.headers = newHeaders
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
