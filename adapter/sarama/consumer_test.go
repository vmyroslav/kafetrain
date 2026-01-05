package sarama

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vmyroslav/kafetrain/resilience"
)

// mockConsumerGroup mocks sarama.ConsumerGroup
type mockConsumerGroup struct {
	mock.Mock
}

func (m *mockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	args := m.Called(ctx, topics, handler)
	return args.Error(0)
}

func (m *mockConsumerGroup) Errors() <-chan error {
	return nil
}

func (m *mockConsumerGroup) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockConsumerGroup) Pause(topicPartitions map[string][]int32) {
	m.Called(topicPartitions)
}

func (m *mockConsumerGroup) Resume(topicPartitions map[string][]int32) {
	m.Called(topicPartitions)
}

func (m *mockConsumerGroup) PauseAll() {
	m.Called()
}

func (m *mockConsumerGroup) ResumeAll() {
	m.Called()
}

// mockResilienceHandler mocks resilience.ConsumerHandler
type mockResilienceHandler struct {
	mock.Mock
}

func (m *mockResilienceHandler) Handle(ctx context.Context, msg resilience.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

// mockSession mocks sarama.ConsumerGroupSession
type mockSession struct {
	mock.Mock
}

func (m *mockSession) Claims() map[string][]int32 {
	args := m.Called()
	return args.Get(0).(map[string][]int32)
}

func (m *mockSession) MemberID() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockSession) GenerationID() int32 {
	args := m.Called()
	return int32(args.Int(0))
}

func (m *mockSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	m.Called(topic, partition, offset, metadata)
}

func (m *mockSession) Commit() {
	m.Called()
}

func (m *mockSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	m.Called(topic, partition, offset, metadata)
}

func (m *mockSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.Called(msg, metadata)
}

func (m *mockSession) Context() context.Context {
	args := m.Called()
	return args.Get(0).(context.Context)
}

// mockClaim mocks sarama.ConsumerGroupClaim
type mockClaim struct {
	mock.Mock
	messages chan *sarama.ConsumerMessage
}

func (m *mockClaim) Topic() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockClaim) Partition() int32 {
	args := m.Called()
	return int32(args.Int(0))
}

func (m *mockClaim) InitialOffset() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *mockClaim) HighWaterMarkOffset() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *mockClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func TestConsumerAdapter_Consume(t *testing.T) {
	t.Parallel()

	t.Run("success loop", func(t *testing.T) {
		t.Parallel()

		m := new(mockConsumerGroup)
		adapter := NewConsumerAdapter(m)

		// Mock Consume to return nil (successful loop iteration)
		// We'll simulate 1 iteration then context cancellation to break loop
		ctx, cancel := context.WithCancel(context.Background())

		// First call succeeds
		m.On("Consume", mock.MatchedBy(func(c context.Context) bool {
			return c == ctx
		}), []string{"topic-1"}, mock.AnythingOfType("*sarama.consumerGroupHandler")).Return(nil).Run(func(args mock.Arguments) {
			cancel() // Cancel context to stop the loop
		})

		err := adapter.Consume(ctx, []string{"topic-1"}, &mockResilienceHandler{})

		// Should return context Canceled
		assert.ErrorIs(t, err, context.Canceled)
		m.AssertExpectations(t)
	})

	t.Run("failure propagation", func(t *testing.T) {
		t.Parallel()
		m := new(mockConsumerGroup)
		adapter := NewConsumerAdapter(m)

		expectedErr := errors.New("kafka error")
		m.On("Consume", mock.Anything, mock.Anything, mock.Anything).Return(expectedErr)

		err := adapter.Consume(context.Background(), []string{"topic-1"}, &mockResilienceHandler{})

		assert.ErrorIs(t, err, expectedErr)
		m.AssertExpectations(t)
	})
}

func TestConsumerGroupHandler_ConsumeClaim(t *testing.T) {
	t.Parallel()

	t.Run("process messages successfully", func(t *testing.T) {
		t.Parallel()

		retryHandler := new(mockResilienceHandler)
		handler := &consumerGroupHandler{retryHandler: retryHandler}

		session := new(mockSession)
		claim := &mockClaim{
			messages: make(chan *sarama.ConsumerMessage, 2),
		}

		ctx := context.Background()
		session.On("Context").Return(ctx)

		// Create test messages
		msg1 := &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    100,
			Key:       []byte("key-1"),
			Value:     []byte("value-1"),
		}
		msg2 := &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    101,
			Key:       []byte("key-2"),
			Value:     []byte("value-2"),
		}

		// Fill channel
		claim.messages <- msg1
		claim.messages <- msg2
		close(claim.messages)

		// Expectations
		// 1. Handler called for msg1
		retryHandler.On("Handle", ctx, mock.MatchedBy(func(m resilience.Message) bool {
			return string(m.Key()) == "key-1" && string(m.Value()) == "value-1"
		})).Return(nil)

		// 2. Session marked for msg1
		session.On("MarkMessage", msg1, "").Return()

		// 3. Handler called for msg2
		retryHandler.On("Handle", ctx, mock.MatchedBy(func(m resilience.Message) bool {
			return string(m.Key()) == "key-2" && string(m.Value()) == "value-2"
		})).Return(nil)

		// 4. Session marked for msg2
		session.On("MarkMessage", msg2, "").Return()

		// Run
		err := handler.ConsumeClaim(session, claim)

		assert.NoError(t, err)
		retryHandler.AssertExpectations(t)
		session.AssertExpectations(t)
	})

	t.Run("handler error stops consumption", func(t *testing.T) {
		t.Parallel()

		retryHandler := new(mockResilienceHandler)
		handler := &consumerGroupHandler{retryHandler: retryHandler}

		session := new(mockSession)
		claim := &mockClaim{
			messages: make(chan *sarama.ConsumerMessage, 2),
		}

		ctx := context.Background()
		session.On("Context").Return(ctx)

		msg1 := &sarama.ConsumerMessage{Value: []byte("fail-me")}
		msg2 := &sarama.ConsumerMessage{Value: []byte("should-not-process")}

		claim.messages <- msg1
		claim.messages <- msg2
		// channel not closed, but loop should exit on error

		expectedErr := errors.New("processing failed")
		retryHandler.On("Handle", ctx, mock.Anything).Return(expectedErr)

		// Should NOT call MarkMessage for msg1

		err := handler.ConsumeClaim(session, claim)

		assert.ErrorIs(t, err, expectedErr)
		retryHandler.AssertExpectations(t)
		// Verify msg2 was NOT consumed (mock would panic if called unexpectedly)
	})
}

func TestConsumerAdapter_Close(t *testing.T) {
	t.Parallel()
	m := new(mockConsumerGroup)
	adapter := NewConsumerAdapter(m)

	m.On("Close").Return(nil)

	err := adapter.Close()
	assert.NoError(t, err)
	m.AssertExpectations(t)
}
