package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// StateCoordinator abstracts the mechanism for acquiring and releasing locks on message keys.
type StateCoordinator interface {
	// Start initializes the coordinator (e.g., starts background listeners).
	Start(ctx context.Context, topic string) error

	// Acquire locks the key to ensure strict ordering.
	Acquire(ctx context.Context, msg *InternalMessage, originalTopic string) error

	// Release unlocks the key.
	Release(ctx context.Context, msg *InternalMessage) error

	// IsLocked checks if the key is currently locked.
	IsLocked(ctx context.Context, msg *InternalMessage) bool
}

// KafkaStateCoordinator implements StateCoordinator using a compacted Kafka topic and local memory.
type KafkaStateCoordinator struct {
	tracker         MessageChainTracker
	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	cfg             *Config
	logger          Logger
	errors          chan<- error
}

func NewKafkaStateCoordinator(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	tracker MessageChainTracker,
	errCh chan<- error,
) *KafkaStateCoordinator {
	if tracker == nil {
		tracker = NewKeyMemoryTracker()
	}

	return &KafkaStateCoordinator{
		tracker:         tracker,
		producer:        producer,
		consumerFactory: consumerFactory,
		admin:           admin,
		cfg:             cfg,
		logger:          logger,
		errors:          errCh,
	}
}

func (k *KafkaStateCoordinator) IsLocked(ctx context.Context, msg *InternalMessage) bool {
	return k.tracker.IsRelated(ctx, msg)
}

func (k *KafkaStateCoordinator) Start(ctx context.Context, topic string) error {
	if err := k.ensureRedirectTopic(ctx, topic); err != nil {
		return err
	}

	if err := k.restoreState(ctx, topic); err != nil {
		return err
	}

	// start live monitoring of redirect topic
	return k.startRedirectConsumer(ctx, topic)
}

func (k *KafkaStateCoordinator) Acquire(ctx context.Context, msg *InternalMessage, originalTopic string) error {
	id, ok := GetHeaderValue[string](&msg.Headers, headerID)
	if !ok {
		id = string(msg.Key)
	}

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
		topic:     k.redirectTopic(originalTopic),
		key:       []byte(id),
		value:     []byte(id),
		headers:   &headerListWrapper{headers: headers},
		timestamp: time.Now(),
	}

	return k.producer.Produce(ctx, redirectMsg.Topic(), redirectMsg)
}

func (k *KafkaStateCoordinator) Release(ctx context.Context, msg *InternalMessage) error {
	id, ok := GetHeaderValue[string](&msg.Headers, headerID)
	if !ok {
		id = string(msg.Key)
	}

	topic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		topic = msg.topic
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
		topic:     k.redirectTopic(topic),
		key:       []byte(id),
		value:     nil, // Tombstone
		headers:   &headerListWrapper{headers: headers},
		timestamp: time.Now(),
	}

	return k.producer.Produce(ctx, tombstoneMsg.Topic(), tombstoneMsg)
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
		"segment.ms":     "100",
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

	// Clean up ephemeral group
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
	if msg.Value() == nil {
		return k.ReleaseMessage(ctx, k.toInternalMessage(msg))
	}

	originalTopicBytes, ok := msg.Headers().Get(HeaderTopic)
	if !ok {
		return errors.New("redirect message missing topic header")
	}
	originalKeyBytes, ok := msg.Headers().Get(headerKey)
	if !ok {
		return errors.New("redirect message missing key header")
	}

	originalMsg := &InternalMessage{
		topic: string(originalTopicBytes),
		Key:   originalKeyBytes,
	}

	_, err := k.tracker.AddMessage(ctx, originalMsg)
	return err
}

func (k *KafkaStateCoordinator) ReleaseMessage(ctx context.Context, msg *InternalMessage) error {
	topic, ok := GetHeaderValue[string](&msg.Headers, HeaderTopic)
	if !ok {
		return errors.New("topic header not found")
	}
	key, ok := GetHeaderValue[string](&msg.Headers, headerKey)
	if !ok {
		return errors.New("key header not found")
	}

	return k.tracker.ReleaseMessage(ctx, &InternalMessage{
		Key:   []byte(key),
		topic: topic,
	})
}

func (k *KafkaStateCoordinator) toInternalMessage(msg Message) *InternalMessage {
	headers := make(HeaderList, 0)
	for key, value := range msg.Headers().All() {
		headers = append(headers, Header{Key: []byte(key), Value: value})
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

func (k *KafkaStateCoordinator) redirectTopic(topic string) string {
	return k.cfg.RedirectTopicPrefix + "_" + topic
}

// Internal Handlers

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
