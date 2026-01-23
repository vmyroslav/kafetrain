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

// ErrorTracker is the main entry point for the resilience library.
// It orchestrates the complete 3-topic retry pattern: retry topic for messages,
// redirect topic for distributed locking, and DLQ for unrecoverable failures.
// This implementation is library-agnostic, working with any Kafka client through
// the Producer/Consumer/Admin interfaces.
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

// NewErrorTracker creates a new ErrorTracker with the provided dependencies.
// All parameters except backoff are required. If backoff is nil, it defaults
// to ExponentialBackoff. Returns an error if any required parameter is missing
// or if the configuration is invalid (e.g., missing GroupID).
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

	if err := cfg.Validate(); err != nil {
		return nil, err
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

// Start initializes the complete resilience infrastructure for a topic.
// This method sets up both the control plane and data plane:
//  1. Control plane: Starts the coordinator to manage distributed ordering locks
//  2. Data plane: Starts the retry worker to process messages from the retry topic
//
// This is a blocking call during coordinator startup (state restoration), then launches
// a background retry worker. Idempotent: calling Start multiple times for the same
// topic is safe and will no-op. Returns an error if initialization fails.
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

// Close gracefully shuts down all background workers and releases resources.
// It cancels all topic-specific contexts, waits for retry workers to complete,
// and closes the coordinator. After Close returns, the ErrorTracker should not
// be used. This method blocks until all goroutines exit cleanly.
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
		if err := t.admin.CreateTopic(ctx, t.RetryTopic(topic), partitions, t.cfg.ReplicationFactor, map[string]string{
			"cleanup.policy": "delete",
			"retention.ms":   "3600000", // 1 hour
		}); err != nil {
			return fmt.Errorf("failed to create retry topic: %w", err)
		}

		return nil
	})

	// create DLQ topic
	g.Go(func() error {
		if err := t.admin.CreateTopic(ctx, t.DLQTopic(topic), partitions, t.cfg.ReplicationFactor, map[string]string{
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
		defer func() { _ = retryConsumer.Close() }()

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

// retryWorkerHandler wraps user business logic with retry orchestration.
// It handles the complete retry lifecycle: backoff waiting, user processing,
// and result handling (Free on success, Redirect on failure).
type retryWorkerHandler struct {
	t           *ErrorTracker
	userHandler ConsumerHandler
}

// Handle processes a message from the retry topic through the complete lifecycle:
//  1. Wait for backoff delay (reads HeaderRetryNextTime)
//  2. Execute user's business logic
//  3. On success: Free the lock (allows subsequent messages with same key)
//  4. On failure: Redirect again (or to DLQ if max retries exceeded)
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

// StartTracking starts only the coordination layer (control plane) for the topic.
// This is a lightweight alternative to Start() when you want to manage retry workers
// manually. It ensures topics exist and initializes the coordinator for lock management.
// Use this when you need Redirect/Free/IsInRetryChain functionality without the
// automatic retry worker. This is a blocking call during coordinator startup.
func (t *ErrorTracker) StartTracking(ctx context.Context, topic string) error {
	// ensure topics exist
	if err := t.ensureTopicsExist(ctx, topic); err != nil {
		return err
	}

	return t.coordinator.Start(ctx, topic)
}

// Synchronize blocks until the coordinator's local state is fully synchronized
// with the distributed redirect topic. This ensures the coordinator has consumed
// all pending lock updates before processing resumes. Call this during partition
// rebalancing (e.g., in Sarama's ConsumerGroupHandler.Setup()) to guarantee
// consistent ordering guarantees after partition reassignment.
func (t *ErrorTracker) Synchronize(ctx context.Context) error {
	return t.coordinator.Synchronize(ctx)
}

// IsInRetryChain checks if a message key is currently locked in the retry chain.
// Returns true if the key is being processed or waiting in the retry topic.
// This is used to enforce strict ordering: new messages with locked keys must
// wait until the predecessor completes. This is a fast, local lock check with
// no network I/O.
func (t *ErrorTracker) IsInRetryChain(ctx context.Context, msg Message) bool {
	internalMsg := NewFromMessage(msg)
	return t.coordinator.IsLocked(ctx, internalMsg)
}

// NewResilientHandler wraps a user handler with automatic resilience for MAIN topic consumption.
// It provides two key guarantees:
//  1. Strict Ordering: If a message key is in the retry chain, new messages with the
//     same key are automatically redirected to preserve ordering
//  2. Automatic Retry: Failed messages are sent to the retry topic with backoff metadata
//
// This is the recommended pattern for main topic consumers. Retry workers should NOT
// use this wrapper (they need Free() on success, which this handler doesn't call).
func (t *ErrorTracker) NewResilientHandler(handler ConsumerHandler) ConsumerHandler {
	return ConsumerHandlerFunc(func(ctx context.Context, msg Message) error {
		// 1. Strict Ordering Check
		// If this key is currently retrying (from a previous message), we must
		// redirect this new message to the retry queue to maintain order.
		if t.IsInRetryChain(ctx, msg) {
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

// Redirect sends a failed message to the retry topic for reprocessing with backoff.
// This method:
//  1. Acquires a distributed lock on the message key (via coordinator)
//  2. Publishes the message to the retry topic with updated retry metadata
//  3. Checks max retries and sends to DLQ if exceeded
//
// If the publish fails, the lock is rolled back to prevent "zombie keys".
// This is the primary public API for manual ErrorTracker usage.
func (t *ErrorTracker) Redirect(ctx context.Context, msg Message, lastError error) error {
	internalMsg := NewFromMessage(msg)

	return t.redirectMessageWithError(ctx, internalMsg, lastError)
}

// Free releases the distributed lock for a successfully processed message.
// This signals to all coordinators that the message key is no longer in the
// retry chain, allowing subsequent messages with the same key to be processed.
// Call this after successfully processing a message from the RETRY topic.
// Never call this for main topic messages (they don't hold locks).
func (t *ErrorTracker) Free(ctx context.Context, msg Message) error {
	internalMsg := NewFromMessage(msg)
	return t.coordinator.Release(ctx, internalMsg)
}

// redirectMessageWithError is the internal redirect implementation using InternalMessage.
// It calculates retry metadata (attempt count, backoff delay, next retry time),
// performs atomic lock acquisition and message publishing with compensating rollback,
// and handles max retries exceeded by sending to DLQ. This is where the core
// retry orchestration logic lives.
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

// publishToRetry constructs and publishes a retry message with updated metadata headers.
// It preserves the original message payload and most headers while updating retry-specific
// headers (attempt count, next retry time, error reason, etc.). Original user headers
// are preserved to maintain context across retries.
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
// It preserves the original payload and most headers while adding DLQ-specific metadata
// (failure reason, timestamp, source topic, retry attempts). The DLQ topic uses infinite
// retention (-1) to preserve failed messages for investigation and manual recovery.
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

// RetryTopic returns the retry topic name for a given primary topic.
// Naming convention: {RetryTopicPrefix}_{topic}
func (t *ErrorTracker) RetryTopic(topic string) string {
	return t.cfg.RetryTopicPrefix + "_" + topic
}

// RedirectTopic returns the redirect (compacted) topic name for a given primary topic.
// Naming convention: {RedirectTopicPrefix}_{topic}
func (t *ErrorTracker) RedirectTopic(topic string) string {
	return t.cfg.RedirectTopicPrefix + "_" + topic
}

// DLQTopic returns the Dead Letter Queue topic name for a given primary topic.
// Naming convention: {DLQTopicPrefix}_{topic}
func (t *ErrorTracker) DLQTopic(topic string) string {
	return t.cfg.DLQTopicPrefix + "_" + topic
}

// handleMaxRetriesExceeded is called when a message has exhausted all retry attempts.
// It sends the message to DLQ and optionally releases the lock based on FreeOnDLQ config.
// If FreeOnDLQ is false, the key remains locked, preventing new messages with the same
// key from processing until manual intervention.
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

// WaitForRetryTime blocks until the scheduled retry time arrives.
// It reads the HeaderRetryNextTime header and sleeps until that timestamp.
// This implements the backoff delay: messages are consumed from the retry topic
// immediately but processing is deferred until the backoff period expires.
// Returns immediately if no retry time header exists or if the time has passed.
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
// This allows fine-grained control over retry behavior: some errors may be
// permanent (e.g., validation failures) and shouldn't be retried, while others
// are transient (e.g., network timeouts) and should be retried.
type RetriableError struct {
	Origin error
	Retry  bool
}

// NewRetriableError creates a RetriableError with the specified retry behavior.
// Use shouldRetry=true for transient errors, false for permanent errors.
func NewRetriableError(origin error, shouldRetry bool) *RetriableError {
	return &RetriableError{Origin: origin, Retry: shouldRetry}
}

// Error returns the error message from the wrapped origin error.
func (e RetriableError) Error() string {
	return e.Origin.Error()
}

// Unwrap returns the wrapped origin error for use with errors.Is/As.
func (e RetriableError) Unwrap() error {
	return e.Origin
}

// ShouldRetry returns true if this error should trigger a retry, false otherwise.
func (e RetriableError) ShouldRetry() bool {
	return e.Retry
}
