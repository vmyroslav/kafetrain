package resilience

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
)

// KafkaStateCoordinator implements StateCoordinator using a compacted Kafka topic and local memory.
// It decorates LocalStateCoordinator to add distributed synchronization.
type KafkaStateCoordinator struct {
	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	logger          Logger
	local           *LocalStateCoordinator
	cfg             *Config
	errors          chan<- error
	consumedOffsets map[int32]int64
	instanceID      string
	topic           string
	mu              sync.RWMutex
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NewKafkaStateCoordinator creates a coordinator using a compacted Kafka topic for distributed state.
// Note: Consider extracting coordinator-specific config fields if the Config struct grows too large.
func NewKafkaStateCoordinator(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	errCh chan<- error,
) *KafkaStateCoordinator {
	return &KafkaStateCoordinator{
		local:           NewLocalStateCoordinator(),
		producer:        producer,
		consumerFactory: consumerFactory,
		admin:           admin,
		cfg:             cfg,
		logger:          logger,
		errors:          errCh,
		instanceID:      uuid.New().String(),
		consumedOffsets: make(map[int32]int64),
	}
}

func (k *KafkaStateCoordinator) IsLocked(ctx context.Context, msg *InternalMessage) bool {
	return k.local.IsLocked(ctx, msg)
}

func (k *KafkaStateCoordinator) Start(ctx context.Context, topic string) error {
	k.mu.Lock()
	k.topic = topic
	// Create a derived context for background workers that we can cancel via Close()
	workerCtx, cancel := context.WithCancel(ctx)
	k.cancel = cancel
	k.mu.Unlock()

	// ensure redirect topic exists
	if err := k.ensureRedirectTopic(ctx, topic); err != nil {
		return err
	}

	// restore state from redirect topic
	if err := k.restoreState(ctx, topic); err != nil {
		return err
	}

	// start monitoring of redirect topic
	return k.startRedirectConsumer(workerCtx, topic)
}

func (k *KafkaStateCoordinator) Close() error {
	k.mu.Lock()

	if k.cancel != nil {
		k.cancel()
	}

	k.mu.Unlock()

	k.wg.Wait()

	return nil
}

// Acquire locks the key for the given message.
func (k *KafkaStateCoordinator) Acquire(ctx context.Context, msg *InternalMessage, originalTopic string) error {
	id, ok := GetHeaderValue[string](&msg.HeaderData, HeaderID)
	if !ok {
		id = string(msg.KeyData)
	}

	// optimistic locking: update local state immediately
	if err := k.local.Acquire(ctx, msg, originalTopic); err != nil {
		return err
	}

	headers := HeaderList{}
	headers.Set(HeaderTopic, []byte(originalTopic))
	headers.Set(HeaderKey, msg.KeyData)
	headers.Set(HeaderCoordinatorID, []byte(k.instanceID))

	redirectMsg := &InternalMessage{
		topic:         k.redirectTopic(originalTopic),
		KeyData:       []byte(id),
		Payload:       []byte(id),
		HeaderData:    headers,
		TimestampData: time.Now(),
	}

	err := retry.Do(
		func() error {
			return k.producer.Produce(ctx, redirectMsg.Topic(), redirectMsg)
		},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.MaxDelay(2*time.Second),
		retry.OnRetry(func(n uint, err error) {
			k.logger.Warn(
				"Failed to produce acquire message",
				"topic",
				originalTopic,
				"attempt",
				n+1,
				"error",
				err,
			)
		}),
	)
	if err != nil {
		// rollback local state on failure
		_ = k.local.Release(ctx, msg)

		return fmt.Errorf("failed to produce acquire message: %w", err)
	}

	return nil
}

func (k *KafkaStateCoordinator) Release(ctx context.Context, msg *InternalMessage) error {
	id, ok := GetHeaderValue[string](&msg.HeaderData, HeaderID)
	if !ok {
		id = string(msg.KeyData)
	}

	topic, ok := GetHeaderValue[string](&msg.HeaderData, HeaderTopic)
	if !ok {
		topic = msg.topic
	}

	// optimistic release: update local state immediately
	if err := k.local.Release(ctx, msg); err != nil {
		return err
	}

	headers := HeaderList{}
	headers.Set(HeaderTopic, []byte(topic))
	headers.Set(HeaderKey, msg.KeyData)
	headers.Set(HeaderCoordinatorID, []byte(k.instanceID))

	tombstoneMsg := &InternalMessage{
		topic:         k.redirectTopic(topic),
		KeyData:       []byte(id),
		Payload:       nil, // Tombstone
		HeaderData:    headers,
		TimestampData: time.Now(),
	}

	err := retry.Do(
		func() error {
			return k.producer.Produce(ctx, tombstoneMsg.Topic(), tombstoneMsg)
		},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.MaxDelay(2*time.Second),
		retry.OnRetry(func(n uint, err error) {
			k.logger.Warn(
				"Failed to produce release message",
				"topic",
				topic,
				"attempt",
				n+1,
				"error",
				err,
			)
		}),
	)
	if err != nil {
		// rollback local state on failure
		_ = k.local.Acquire(ctx, msg, topic)

		return fmt.Errorf("failed to produce release message: %w", err)
	}

	return nil
}

// Synchronize ensures the coordinator's local state is up-to-date with the distributed source of truth.
// It blocks until the internal redirect consumer has reached the High Water Mark of the redirect topic.
func (k *KafkaStateCoordinator) Synchronize(ctx context.Context) error {
	k.mu.RLock()
	topic := k.topic
	k.mu.RUnlock()

	if topic == "" {
		return errors.New("coordinator not started: topic is empty")
	}

	redirectTopic := k.redirectTopic(topic)

	// 1. Get High Water Marks for the redirect topic
	metadata, err := k.admin.DescribeTopics(ctx, []string{redirectTopic})
	if err != nil {
		return fmt.Errorf("failed to describe redirect topic for sync: %w", err)
	}

	if len(metadata) == 0 {
		return nil
	}

	targetOffsets := metadata[0].PartitionOffsets()

	// 2. Wait until we reach them
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			k.mu.RLock()

			allCaughtUp := true

			for p, target := range targetOffsets {
				if target == 0 {
					continue
				}

				consumed, ok := k.consumedOffsets[p]
				// target is HWM (next offset to be written).
				// We are caught up if we processed (target - 1).
				if !ok || consumed < target-1 {
					allCaughtUp = false
					break
				}
			}

			k.mu.RUnlock()

			if allCaughtUp {
				return nil
			}
		}
	}
}

func (k *KafkaStateCoordinator) ensureRedirectTopic(ctx context.Context, topic string) error {
	redirectTopic := k.redirectTopic(topic)

	if k.cfg.DisableAutoTopicCreation {
		metadata, err := k.admin.DescribeTopics(ctx, []string{redirectTopic})
		if err != nil {
			return fmt.Errorf("failed to verify existence of redirect topic (auto-creation disabled): %w", err)
		}

		if !slices.ContainsFunc(metadata, func(m TopicMetadata) bool { return m.Name() == redirectTopic }) {
			return fmt.Errorf("required redirect topic '%s' missing (auto-creation disabled)", redirectTopic)
		}

		return nil
	}

	partitions := k.cfg.RetryTopicPartitions
	if partitions == 0 {
		metadata, err := k.admin.DescribeTopics(ctx, []string{topic})
		if err != nil {
			return fmt.Errorf("failed to describe primary topic '%s': %w", topic, err)
		}

		if len(metadata) > 0 {
			partitions = metadata[0].Partitions()
		}
	}

	// segment.ms=100 ensures fast compaction for lock state visibility.
	// Lower values = faster tombstone propagation, higher CPU usage.
	return k.admin.CreateTopic(ctx, redirectTopic, partitions, 1, map[string]string{
		"cleanup.policy": "compact",
		"segment.ms":     "100",
	})
}

func (k *KafkaStateCoordinator) restoreState(ctx context.Context, topic string) error {
	// 1. Get High Water Marks for the redirect topic
	metadata, err := k.admin.DescribeTopics(ctx, []string{k.redirectTopic(topic)})
	if err != nil {
		return fmt.Errorf("failed to describe redirect topic for restore: %w", err)
	}

	if len(metadata) == 0 {
		k.logger.Debug("redirect topic metadata not found, skipping restore")
		return nil
	}

	targetOffsets := metadata[0].PartitionOffsets()

	// 2. Check if there's anything to restore
	hasData := false

	for _, offset := range targetOffsets {
		if offset > 0 {
			hasData = true
			break
		}
	}

	if !hasData {
		k.logger.Debug("redirect topic is empty, skipping restore")
		return nil
	}

	refillCfg := *k.cfg
	refillCfg.GroupID = uuid.New().String()

	refillConsumer, err := k.consumerFactory.NewConsumer(refillCfg.GroupID)
	if err != nil {
		return err
	}

	defer func() {
		// clean up ephemeral group
		go func() {
			_ = k.admin.DeleteConsumerGroup(context.Background(), refillCfg.GroupID)
		}()
	}()

	// 3. Consume until we reach HWM
	// We use a separate context for the refill loop that we cancel when done
	refillCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Safety timeout in case we never reach HWM (e.g. if HWM moves while consuming?)
	// Ideally, we should read until the *snapshot* of HWM we took.
	timeout := time.Duration(k.cfg.StateRestoreTimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	// Wrap context with timeout
	refillCtx, cancelTimeout := context.WithTimeout(refillCtx, timeout)
	defer cancelTimeout()

	handler := &redirectFillHandler{
		k:               k,
		targetOffsets:   targetOffsets,
		consumedOffsets: make(map[int32]int64),
		cancel:          cancel, // Cancel the *outer* refillCtx (well, the one we passed to Consume)
	}

	err = refillConsumer.Consume(refillCtx, []string{k.redirectTopic(topic)}, handler)
	_ = refillConsumer.Close()

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("restore failed: %w", err)
	}

	return nil
}

func (k *KafkaStateCoordinator) startRedirectConsumer(ctx context.Context, topic string) error {
	redirectCfg := *k.cfg
	redirectCfg.GroupID = k.cfg.GroupID + "-redirect"

	consumer, err := k.consumerFactory.NewConsumer(redirectCfg.GroupID)
	if err != nil {
		return err
	}

	// TODO: propagate errors back to the tracker
	k.wg.Add(1)

	go func() {
		defer k.wg.Done()
		defer func() { _ = consumer.Close() }()

		backoff := 5 * time.Second

		for {
			err := consumer.Consume(ctx, []string{k.redirectTopic(topic)}, &RedirectHandler{k: k})

			// check for clean shutdown
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}

			// 2. Handle errors
			if err != nil {
				if k.errors != nil {
					select {
					case k.errors <- err:
					default:
						k.logger.Warn("error channel full, dropping background error", "error", err)
					}
				} else {
					k.logger.Error("background redirect consumer failed, restarting",
						"error", err,
						"retry_in", backoff,
					)
				}
			}
			// wait before restarting
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				// continue loop
			}
		}
	}()

	return nil
}

func (k *KafkaStateCoordinator) processRedirectMessage(ctx context.Context, msg Message) error {
	// ignore echo messages from self
	if originID, ok := msg.Headers().Get(HeaderCoordinatorID); ok {
		if string(originID) == k.instanceID {
			return nil
		}
	}

	if msg.Value() == nil {
		return k.releaseMessage(ctx, NewFromMessage(msg))
	}

	originalTopicBytes, ok := msg.Headers().Get(HeaderTopic)
	if !ok {
		return errors.New("redirect message missing topic header")
	}

	originalKeyBytes, ok := msg.Headers().Get(HeaderKey)
	if !ok {
		return errors.New("redirect message missing key header")
	}

	originalMsg := &InternalMessage{
		topic:   string(originalTopicBytes),
		KeyData: originalKeyBytes,
	}

	// delegate to local coordinator
	return k.local.Acquire(ctx, originalMsg, originalMsg.topic)
}

// releaseMessage releases a lock based on a received message.
// This is used when processing tombstones from other coordinators.
func (k *KafkaStateCoordinator) releaseMessage(ctx context.Context, msg *InternalMessage) error {
	if _, ok := GetHeaderValue[string](&msg.HeaderData, HeaderTopic); !ok {
		return errors.New("topic header not found")
	}

	if _, ok := GetHeaderValue[string](&msg.HeaderData, HeaderKey); !ok {
		return errors.New("key header not found")
	}

	// delegate to local coordinator
	return k.local.Release(ctx, msg)
}

// redirectTopic returns the compacted topic name used for lock state.
// The naming convention follows the pattern: {prefix}_{original_topic}
func (k *KafkaStateCoordinator) redirectTopic(topic string) string {
	return k.cfg.RedirectTopicPrefix + "_" + topic
}

type redirectFillHandler struct {
	k               *KafkaStateCoordinator
	targetOffsets   map[int32]int64
	consumedOffsets map[int32]int64
	cancel          context.CancelFunc
	mu              sync.Mutex
}

func (r *redirectFillHandler) Handle(ctx context.Context, msg Message) error {
	if err := r.k.processRedirectMessage(ctx, msg); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	p := msg.Partition()
	r.consumedOffsets[p] = msg.Offset()

	// Check if all partitions have reached their target offset
	allDone := true

	for p, target := range r.targetOffsets {
		if target == 0 {
			// Empty partition, effectively done
			continue
		}

		consumed, ok := r.consumedOffsets[p]
		// target is the HWM (next offset to be written).
		// So if we processed (target - 1), we are up to date.
		if !ok || consumed < target-1 {
			allDone = false
			break
		}
	}

	if allDone {
		r.cancel()
	}

	return nil
}

type RedirectHandler struct {
	k *KafkaStateCoordinator
}

func (r *RedirectHandler) Handle(ctx context.Context, msg Message) error {
	if err := r.k.processRedirectMessage(ctx, msg); err != nil {
		return err
	}

	r.k.mu.Lock()
	r.k.consumedOffsets[msg.Partition()] = msg.Offset()
	r.k.mu.Unlock()

	return nil
}
