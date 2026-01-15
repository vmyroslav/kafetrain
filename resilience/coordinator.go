package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	lm              lockMap
	mu              sync.RWMutex
	producer        Producer
	consumerFactory ConsumerFactory
	admin           Admin
	cfg             *Config
	logger          Logger
	errors          chan<- error
	instanceID      string
}

func NewKafkaStateCoordinator(
	cfg *Config,
	logger Logger,
	producer Producer,
	consumerFactory ConsumerFactory,
	admin Admin,
	errCh chan<- error,
) *KafkaStateCoordinator {
	return &KafkaStateCoordinator{
		lm:              make(lockMap),
		producer:        producer,
		consumerFactory: consumerFactory,
		admin:           admin,
		cfg:             cfg,
		logger:          logger,
		errors:          errCh,
		instanceID:      uuid.New().String(),
	}
}

func (k *KafkaStateCoordinator) IsLocked(_ context.Context, msg *InternalMessage) bool {
	k.mu.RLock()
	defer k.mu.RUnlock()

	// if key for this topic has a reference count > 0, then message is related
	count, exists := k.lm.getRefCount(msg.topic, string(msg.Key))

	return exists && count > 0
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
	id, ok := GetHeaderValue[string](&msg.Headers, headerID)
	if !ok {
		id = string(msg.Key)
	}

	// Optimistic locking: Update local state immediately
	key := string(msg.Key)
	k.acquireLocal(originalTopic, key)

	headers := HeaderList{
		{
			Key:   []byte(HeaderTopic),
			Value: []byte(originalTopic),
		},
		{
			Key:   []byte("key"),
			Value: msg.Key,
		},
		{
			Key:   []byte(HeaderCoordinatorID),
			Value: []byte(k.instanceID),
		},
	}

	redirectMsg := &messageWrapper{
		topic:     k.redirectTopic(originalTopic),
		key:       []byte(id),
		value:     []byte(id),
		headers:   &headerListWrapper{headers: headers},
		timestamp: time.Now(),
	}

	if err := k.producer.Produce(ctx, redirectMsg.Topic(), redirectMsg); err != nil {
		// rollback local state on failure
		k.releaseLocal(originalTopic, key)

		return err
	}

	return nil
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

	// Optimistic release: Update local state immediately
	key := string(msg.Key)
	k.releaseLocal(topic, key)

	headers := HeaderList{
		{
			Key:   []byte(HeaderTopic),
			Value: []byte(topic),
		},
		{
			Key:   []byte(headerKey),
			Value: msg.Key,
		},
		{
			Key:   []byte(HeaderCoordinatorID),
			Value: []byte(k.instanceID),
		},
	}

	tombstoneMsg := &messageWrapper{
		topic:     k.redirectTopic(topic),
		key:       []byte(id),
		value:     nil, // Tombstone
		headers:   &headerListWrapper{headers: headers},
		timestamp: time.Now(),
	}

	if err := k.producer.Produce(ctx, tombstoneMsg.Topic(), tombstoneMsg); err != nil {
		// Rollback local state on failure (re-acquire)
		k.acquireLocal(topic, key)
		return err
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
	// Ignore echo messages from self
	if originID, ok := msg.Headers().Get(HeaderCoordinatorID); ok {
		if string(originID) == k.instanceID {
			return nil
		}
	}

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

	_, err := k.addMessage(ctx, originalMsg)
	return err
}

func (k *KafkaStateCoordinator) addMessage(_ context.Context, msg *InternalMessage) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	key := string(msg.Key)

	// increment reference count for this key
	k.lm.incrementRef(msg.topic, key)

	return key, nil
}

func (k *KafkaStateCoordinator) acquireLocal(topic, key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.lm.incrementRef(topic, key)
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

	k.mu.Lock()
	defer k.mu.Unlock()

	// decrement reference count
	newCount, exists := k.lm.decrementRef(topic, key)

	// key wasn't tracked, this is idempotent
	if !exists {
		return nil
	}

	// remove key when count reaches 0
	if newCount <= 0 {
		k.lm.removeKey(topic, key)
	}

	// clean up topic map if empty
	if k.lm.isTopicEmpty(topic) {
		k.lm.removeTopic(topic)
	}

	return nil
}

func (k *KafkaStateCoordinator) releaseLocal(topic, key string) {
	k.mu.Lock()
	defer k.mu.Unlock()

	newCount, exists := k.lm.decrementRef(topic, key)
	if !exists {
		return
	}
	if newCount <= 0 {
		k.lm.removeKey(topic, key)
	}
	if k.lm.isTopicEmpty(topic) {
		k.lm.removeTopic(topic)
	}
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

// topic map ['topic-name' => ['topic-key' => refCount]].
// Reference counting ensures keys are unlocked only when all messages with that key complete.
type lockMap map[string]map[string]int

// getRefCount returns the reference count for a topic/key pair.
// Returns (count, exists) - exists is false if topic or key doesn't exist.
func (lm lockMap) getRefCount(topic, key string) (int, bool) {
	if lm[topic] == nil {
		return 0, false
	}

	count, ok := lm[topic][key]

	return count, ok
}

// incrementRef increments the reference count for a topic/key pair
func (lm lockMap) incrementRef(topic, key string) int {
	if lm[topic] == nil {
		lm[topic] = make(map[string]int)
	}

	lm[topic][key]++

	return lm[topic][key]
}

// decrementRef decrements the reference count for a topic/key pair.
// Returns the new reference count and whether the key existed.
func (lm lockMap) decrementRef(topic, key string) (int, bool) {
	if lm[topic] == nil {
		return 0, false
	}

	if _, ok := lm[topic][key]; !ok {
		return 0, false
	}

	lm[topic][key]--

	return lm[topic][key], true
}

// removeKey removes a key from a topic
func (lm lockMap) removeKey(topic, key string) {
	if lm[topic] == nil {
		return
	}

	delete(lm[topic], key)
}

// removeTopic removes an entire topic and all its keys.
func (lm lockMap) removeTopic(topic string) {
	delete(lm, topic)
}

// isTopicEmpty returns true if topic has no keys or doesn't exist.
func (lm lockMap) isTopicEmpty(topic string) bool {
	return len(lm[topic]) == 0
}

// hasKey returns true if the topic/key pair exists (regardless of count value).
func (lm lockMap) hasKey(topic, key string) bool {
	if lm[topic] == nil {
		return false
	}

	_, ok := lm[topic][key]

	return ok
}
