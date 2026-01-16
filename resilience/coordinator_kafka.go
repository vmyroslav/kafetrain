package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
)

// KafkaStateCoordinator implements StateCoordinator using a compacted Kafka topic and local memory.
// It decorates LocalStateCoordinator to add distributed synchronization.
type KafkaStateCoordinator struct {
	local           *LocalStateCoordinator
	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	cfg             *Config
	logger          Logger
	errors          chan<- error
	instanceID      string
}

// TODO: add a seprate smaller config struct for coordinator only
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
	}
}

func (k *KafkaStateCoordinator) IsLocked(ctx context.Context, msg *InternalMessage) bool {
	return k.local.IsLocked(ctx, msg)
}

func (k *KafkaStateCoordinator) Start(ctx context.Context, topic string) error {
	// ensure redirect topic exists
	if err := k.ensureRedirectTopic(ctx, topic); err != nil {
		return err
	}

	// restore state from redirect topic
	if err := k.restoreState(ctx, topic); err != nil {
		return err
	}

	// start monitoring of redirect topic
	return k.startRedirectConsumer(ctx, topic)
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

	headers := HeaderList{
		{
			Key:   []byte(HeaderTopic),
			Value: []byte(originalTopic),
		},
		{
			Key:   []byte("key"),
			Value: msg.KeyData,
		},
		{
			Key:   []byte(HeaderCoordinatorID),
			Value: []byte(k.instanceID),
		},
	}

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

	headers := HeaderList{
		{
			Key:   []byte(HeaderTopic),
			Value: []byte(topic),
		},
		{
			Key:   []byte(HeaderKey),
			Value: msg.KeyData,
		},
		{
			Key:   []byte(HeaderCoordinatorID),
			Value: []byte(k.instanceID),
		},
	}

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

func (k *KafkaStateCoordinator) ensureRedirectTopic(ctx context.Context, topic string) error {
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

	return k.admin.CreateTopic(ctx, k.redirectTopic(topic), partitions, 1, map[string]string{
		"cleanup.policy": "compact",
		"segment.ms":     "100", // todo: make configurable and find optimal default value
	})
}

func (k *KafkaStateCoordinator) restoreState(ctx context.Context, topic string) error {
	refillCfg := *k.cfg
	refillCfg.GroupID = uuid.New().String()

	refillConsumer, err := k.consumerFactory.NewConsumer(refillCfg.GroupID)
	if err != nil {
		return err
	}

	timeout := time.Duration(k.cfg.StateRestoreTimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	refillCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	idleTimeout := time.Duration(k.cfg.StateRestoreIdleTimeoutMs) * time.Millisecond
	if idleTimeout == 0 {
		idleTimeout = 5 * time.Second
	}

	lastActivity := new(int64)
	atomic.StoreInt64(lastActivity, time.Now().UnixNano())

	handler := &redirectFillHandler{
		k:            k,
		lastActivity: lastActivity,
	}

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-refillCtx.Done():
				return
			case <-ticker.C:
				last := atomic.LoadInt64(lastActivity)
				if time.Since(time.Unix(0, last)) > idleTimeout {
					cancel()
					return
				}
			}
		}
	}()

	_ = refillConsumer.Consume(refillCtx, []string{k.redirectTopic(topic)}, handler)
	refillConsumer.Close()

	// clean up ephemeral group
	go func() {
		_ = k.admin.DeleteConsumerGroup(context.Background(), refillCfg.GroupID)
	}()

	return nil
}

func (k *KafkaStateCoordinator) startRedirectConsumer(ctx context.Context, topic string) error {
	redirectCfg := *k.cfg
	redirectCfg.GroupID = k.cfg.GroupID + "-redirect"

	consumer, err := k.consumerFactory.NewConsumer(redirectCfg.GroupID)
	if err != nil {
		return err
	}

	go func() {
		k.errors <- consumer.Consume(ctx, []string{k.redirectTopic(topic)}, &RedirectHandler{k: k})
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

// TODO: set ready to use name, do not generate it in coordinator
func (k *KafkaStateCoordinator) redirectTopic(topic string) string {
	return k.cfg.RedirectTopicPrefix + "_" + topic
}

type redirectFillHandler struct {
	k            *KafkaStateCoordinator
	lastActivity *int64
}

func (r *redirectFillHandler) Handle(ctx context.Context, msg Message) error {
	atomic.StoreInt64(r.lastActivity, time.Now().UnixNano())
	return r.k.processRedirectMessage(ctx, msg)
}

type RedirectHandler struct {
	k *KafkaStateCoordinator
}

func (r *RedirectHandler) Handle(ctx context.Context, msg Message) error {
	return r.k.processRedirectMessage(ctx, msg)
}
