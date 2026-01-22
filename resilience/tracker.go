package resilience

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"golang.org/x/sync/errgroup"
)

// ErrorTracker is the library-agnostic retry tracking implementation.
type ErrorTracker struct {
	coordinator     StateCoordinator
	backoff         BackoffStrategy
	logger          Logger
	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	cfg             *Config
	mu              sync.RWMutex
	startedTopics   map[string]struct{}
	cancels         map[string]context.CancelFunc
	wg              sync.WaitGroup
}

// NewErrorTracker creates a new ErrorTracker.
func NewErrorTracker(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	coordinator StateCoordinator,
	backoff BackoffStrategy,
) (*ErrorTracker, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("config.GroupID is required")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}
	if producer == nil {
		return nil, errors.New("producer cannot be nil")
	}
	if consumerFactory == nil {
		return nil, errors.New("consumerFactory cannot be nil")
	}
	if admin == nil {
		return nil, errors.New("admin cannot be nil")
	}
	if coordinator == nil {
		return nil, errors.New("coordinator cannot be nil")
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
		coordinator:     coordinator,
		backoff:         backoff,
		startedTopics:   make(map[string]struct{}),
		cancels:         make(map[string]context.CancelFunc),
	}

	return &t, nil
}

// Start initializes the complete resilience loop for a topic.
// 1. It starts the control plane (Coordinator) to manage ordering locks.
// 2. It starts the data plane (StartRetryWorker) to process retry messages.
func (t *ErrorTracker) Start(ctx context.Context, topic string, handler ConsumerHandler) error {
	t.mu.Lock()
	if _, started := t.startedTopics[topic]; started {
		t.mu.Unlock()
		return nil
	}
	t.startedTopics[topic] = struct{}{}

	// Create a cancellable context for this topic's workers, derived from the input context
	workerCtx, cancel := context.WithCancel(ctx)
	t.cancels[topic] = cancel
	t.mu.Unlock()

	// Use derived context for both setup and background workers
	// If setup fails, we cancel the context to clean up any partial state
	if err := t.coordinator.Start(workerCtx, topic); err != nil {
		cancel()
		t.mu.Lock()
		delete(t.startedTopics, topic)
		delete(t.cancels, topic)
		t.mu.Unlock()
		return err
	}

	// background workers use the same derived context
	t.wg.Add(1)
	return t.StartRetryWorker(workerCtx, topic, handler)
}

// Close stops all background workers and releases resources.
func (t *ErrorTracker) Close() error {
	t.mu.Lock()
	for _, cancel := range t.cancels {
		cancel()
	}
	// Clear maps to prevent reuse without restart
	t.cancels = make(map[string]context.CancelFunc)
	t.startedTopics = make(map[string]struct{})
	t.mu.Unlock()

	// Wait for all background workers to finish
	t.wg.Wait()

	return t.coordinator.Close()
}

// ensureTopicsExist creates retry and DLQ topics if they don't exist.
// Queries primary topic partition count and creates topics with matching partitions.
func (t *ErrorTracker) ensureTopicsExist(ctx context.Context, topic string) error {
	if t.cfg.DisableAutoTopicCreation {
		retryTopic := t.RetryTopic(topic)
		dlqTopic := t.DLQTopic(topic)

		metadata, err := t.admin.DescribeTopics(ctx, []string{retryTopic, dlqTopic})
		if err != nil {
			return fmt.Errorf("failed to verify existence of topics (auto-creation disabled): %w", err)
		}

		foundRetry := slices.ContainsFunc(metadata, func(m TopicMetadata) bool { return m.Name() == retryTopic })
		foundDLQ := slices.ContainsFunc(metadata, func(m TopicMetadata) bool { return m.Name() == dlqTopic })

		if !foundRetry || !foundDLQ {
			return fmt.Errorf("required topics missing (auto-creation disabled): retry_exists=%v, dlq_exists=%v", foundRetry, foundDLQ)
		}

		return nil
	}

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
		if err := t.admin.CreateTopic(ctx, t.RetryTopic(topic), partitions, 1, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "3600000", // 1 hour
		}); err != nil {
			return fmt.Errorf("failed to create retry topic: %w", err)
		}

		return nil
	})

	// create DLQ topic
	g.Go(func() error {
		if err := t.admin.CreateTopic(ctx, t.DLQTopic(topic), partitions, 1, map[string]string{
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

	retryTopic := t.RetryTopic(topic)
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
		defer t.wg.Done()
		defer retryConsumer.Close()

		backoff := 5 * time.Second

		for {
			err := retryConsumer.Consume(ctx, []string{retryTopic}, workerHandler)

			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}

			if err != nil {
				t.logger.Error("retry worker failed, restarting",
					"topic", retryTopic,
					"error", err,
					"retry_in", backoff,
				)
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
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

// StartTracking starts the coordination layer (control plane) for the topic.
// This is required for Redirect/Free/IsInRetryChain to work.
func (t *ErrorTracker) StartTracking(ctx context.Context, topic string) error {
	// ensure topics exist
	if err := t.ensureTopicsExist(ctx, topic); err != nil {
		return err
	}

	return t.coordinator.Start(ctx, topic)
}

// Synchronize ensures the internal state is consistent with the distributed log.
// This should be called during the Rebalance phase (e.g. in Sarama's Setup()).
func (t *ErrorTracker) Synchronize(ctx context.Context) error {
	return t.coordinator.Synchronize(ctx)
}

// IsInRetryChain checks if a message is already in the retry chain.
// Returns true if the message key is currently being tracked for retries.
func (t *ErrorTracker) IsInRetryChain(msg Message) bool {
	internalMsg := NewFromMessage(msg)
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
	internalMsg := NewFromMessage(msg)

	return t.redirectMessageWithError(ctx, internalMsg, lastError)
}

// Free removes a successfully processed message from the retry chain.
// Releases the lock via the coordinator.
func (t *ErrorTracker) Free(ctx context.Context, msg Message) error {
	internalMsg := NewFromMessage(msg)
	return t.coordinator.Release(ctx, internalMsg)
}

// redirectMessageWithError is the internal InternalMessage-based redirect implementation.
func (t *ErrorTracker) redirectMessageWithError(ctx context.Context, msg *InternalMessage, lastError error) error {
	// Calculate retry metadata
	currentAttempt, _ := GetHeaderValue[int](&msg.HeaderData, HeaderRetryAttempt)

	// Check if max retries would be exceeded
	if currentAttempt >= t.cfg.MaxRetries {
		return t.handleMaxRetriesExceeded(ctx, msg, currentAttempt, t.cfg.MaxRetries)
	}

	originalTime, ok := GetHeaderValue[time.Time](&msg.HeaderData, HeaderRetryOriginalTime)
	if !ok {
		originalTime = time.Now()
	}

	// Get the ORIGINAL topic from headers (if this is already a retry message)
	originalTopic, ok := GetHeaderValue[string](&msg.HeaderData, HeaderTopic)
	if !ok {
		// This is the first redirect, use the current topic
		originalTopic = msg.topic
	}

	nextDelay := t.backoff.NextDelay(currentAttempt)
	nextRetryTime := time.Now().Add(nextDelay)
	nextAttempt := currentAttempt + 1

	key := string(msg.KeyData)
	destinationTopic := t.RetryTopic(originalTopic)

	t.logger.Debug("redirecting message to retry topic",
		"destination_topic", destinationTopic,
		"source_topic", msg.topic,
		"original_topic", originalTopic,
		"key", key,
		"attempt", nextAttempt,
		"max_retries", t.cfg.MaxRetries,
		"next_delay_ms", nextDelay.Milliseconds(),
		"next_retry_time", nextRetryTime.Format(time.RFC3339),
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
	retryHeaders := HeaderList{
		list: make([]Header, 0, len(msg.HeaderData.list)+6),
	}
	for _, h := range msg.HeaderData.list {
		key := string(h.Key)
		if key != HeaderRetryAttempt && key != HeaderRetryMax &&
			key != HeaderRetryNextTime && key != HeaderRetryOriginalTime &&
			key != HeaderRetryReason && key != HeaderID && key != HeaderRetry {
			retryHeaders.Set(key, h.Value)
		}
	}

	// all SetHeader calls below use supported types
	_ = SetHeader[int](&retryHeaders, HeaderRetryAttempt, nextAttempt)
	_ = SetHeader[int](&retryHeaders, HeaderRetryMax, t.cfg.MaxRetries)
	_ = SetHeader[time.Time](&retryHeaders, HeaderRetryNextTime, nextRetryTime)
	_ = SetHeader[time.Time](&retryHeaders, HeaderRetryOriginalTime, originalTime)

	if lastError != nil {
		_ = SetHeader[string](&retryHeaders, HeaderRetryReason, lastError.Error())
	}

	_ = SetHeader[string](&retryHeaders, HeaderID, id)
	_ = SetHeader[string](&retryHeaders, HeaderRetry, "true")
	_ = SetHeader[string](&retryHeaders, HeaderTopic, originalTopic)

	retryMsg := &InternalMessage{
		topic:         t.RetryTopic(originalTopic),
		KeyData:       msg.KeyData,
		Payload:       msg.Payload,
		HeaderData:    retryHeaders,
		TimestampData: time.Now(),
	}

	return t.producer.Produce(ctx, retryMsg.Topic(), retryMsg)
}

// SendToDLQ sends a message that has exceeded max retries to the Dead Letter Queue.
func (t *ErrorTracker) SendToDLQ(ctx context.Context, msg Message, lastError error) error {
	internalMsg := NewFromMessage(msg)

	// Get original topic from headers
	originalTopic, ok := GetHeaderValue[string](&internalMsg.HeaderData, HeaderTopic)
	if !ok {
		originalTopic = internalMsg.topic
	}

	// Get retry metadata
	attempt, _ := GetHeaderValue[int](&internalMsg.HeaderData, HeaderRetryAttempt)
	originalTime, _ := GetHeaderValue[time.Time](&internalMsg.HeaderData, HeaderRetryOriginalTime)

	// Copy original message headers (excluding retry headers)
	dlqHeaders := HeaderList{
		list: make([]Header, 0, len(internalMsg.HeaderData.list)+4),
	}
	for _, h := range internalMsg.HeaderData.list {
		key := string(h.Key)
		if key != HeaderRetryAttempt && key != HeaderRetryMax &&
			key != HeaderRetryNextTime && key != HeaderID && key != HeaderRetry {
			dlqHeaders.Set(key, h.Value)
		}
	}

	// Add DLQ metadata headers (all use supported types, so errors are impossible)
	_ = SetHeader[string](&dlqHeaders, HeaderDLQReason, lastError.Error())
	_ = SetHeader[time.Time](&dlqHeaders, HeaderDLQTimestamp, time.Now())
	_ = SetHeader[string](&dlqHeaders, HeaderDLQSourceTopic, originalTopic)
	_ = SetHeader[int](&dlqHeaders, HeaderDLQRetryAttempts, attempt)

	if !originalTime.IsZero() {
		_ = SetHeader[time.Time](&dlqHeaders, HeaderDLQOriginalFailureTime, originalTime)
	}

	dlqMsg := &InternalMessage{
		topic:         t.DLQTopic(originalTopic),
		KeyData:       internalMsg.KeyData,
		Payload:       internalMsg.Payload,
		HeaderData:    dlqHeaders,
		TimestampData: time.Now(),
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

func (t *ErrorTracker) RetryTopic(topic string) string {
	return t.cfg.RetryTopicPrefix + "_" + topic
}

func (t *ErrorTracker) RedirectTopic(topic string) string {
	return t.cfg.RedirectTopicPrefix + "_" + topic
}

func (t *ErrorTracker) DLQTopic(topic string) string {
	return t.cfg.DLQTopicPrefix + "_" + topic
}

func (t *ErrorTracker) handleMaxRetriesExceeded(ctx context.Context, message *InternalMessage, currentAttempt, maxRetries int) error {
	t.logger.Warn("max retries exceeded, sending to DLQ",
		"topic", message.topic,
		"current_attempt", currentAttempt,
		"max_retries", maxRetries,
	)

	// Get the last error reason from headers
	lastErrorReason, _ := GetHeaderValue[string](&message.HeaderData, HeaderRetryReason)

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
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		t.logger.Debug("delay complete, processing message",
			"topic", msg.Topic(),
		)

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

func (e RetriableError) Unwrap() error {
	return e.Origin
}

func (e RetriableError) ShouldRetry() bool {
	return e.Retry
}
