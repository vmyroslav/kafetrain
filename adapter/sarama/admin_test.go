package sarama

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAdminAdapter_CreateTopic(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		m := new(mockClusterAdmin)
		adapter := &AdminAdapter{admin: m}

		// use a matcher for the TopicDetail because map iteration order is random
		m.On("CreateTopic", "test-topic", mock.MatchedBy(func(d *sarama.TopicDetail) bool {
			if d.NumPartitions != 3 || d.ReplicationFactor != 2 {
				return false
			}

			if len(d.ConfigEntries) != 1 {
				return false
			}
			val, ok := d.ConfigEntries["cleanup.policy"]

			return ok && *val == "delete"
		}), false).Return(nil)

		err := adapter.CreateTopic(context.Background(), "test-topic", 3, 2, map[string]string{
			"cleanup.policy": "delete",
		})
		require.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("idempotency - topic already exists", func(t *testing.T) {
		t.Parallel()
		m := new(mockClusterAdmin)
		adapter := &AdminAdapter{admin: m}

		topicErr := &sarama.TopicError{
			Err: sarama.ErrTopicAlreadyExists,
		}

		m.On("CreateTopic", "test-topic", mock.Anything, false).Return(topicErr)

		err := adapter.CreateTopic(context.Background(), "test-topic", 1, 1, nil)

		// should treat as success (nil error)
		assert.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()

		m := new(mockClusterAdmin)
		adapter := &AdminAdapter{admin: m}

		expectedErr := errors.New("network error")
		m.On("CreateTopic", "test-topic", mock.Anything, false).Return(expectedErr)

		err := adapter.CreateTopic(context.Background(), "test-topic", 1, 1, nil)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr) || err.Error() == expectedErr.Error())
		m.AssertExpectations(t)
	})
}

func TestAdminAdapter_DescribeTopics(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		m := new(mockClusterAdmin)
		client := new(mockClient)
		adapter := &AdminAdapter{admin: m, client: client}

		saramaMetadata := []*sarama.TopicMetadata{
			{
				Name: "topic-1",
				Err:  sarama.ErrNoError,
				Partitions: []*sarama.PartitionMetadata{
					{ID: 0, Replicas: []int32{1, 2, 3}},
					{ID: 1, Replicas: []int32{1, 2, 3}},
				},
			},
			{
				Name: "topic-with-error",
				Err:  sarama.ErrUnknownTopicOrPartition,
			},
		}

		m.On("DescribeTopics", []string{"topic-1", "topic-with-error"}).Return(saramaMetadata, nil)

		client.On("GetOffset", "topic-1", int32(0), sarama.OffsetNewest).Return(int64(100), nil)
		client.On("GetOffset", "topic-1", int32(1), sarama.OffsetNewest).Return(int64(200), nil)

		result, err := adapter.DescribeTopics(context.Background(), []string{"topic-1", "topic-with-error"})
		require.NoError(t, err)

		// should only return the valid topic
		require.Len(t, result, 1)
		assert.Equal(t, "topic-1", result[0].Name())
		assert.Equal(t, int32(2), result[0].Partitions())

		offsets := result[0].PartitionOffsets()
		assert.Equal(t, int64(100), offsets[0])
		assert.Equal(t, int64(200), offsets[1])

		m.AssertExpectations(t)
		client.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()

		m := new(mockClusterAdmin)
		adapter := &AdminAdapter{admin: m}

		m.On("DescribeTopics", []string{"topic-1"}).Return(nil, errors.New("fail"))

		_, err := adapter.DescribeTopics(context.Background(), []string{"topic-1"})
		assert.Error(t, err)
		m.AssertExpectations(t)
	})
}

func TestAdminAdapter_DeleteConsumerGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		m := new(mockClusterAdmin)
		adapter := &AdminAdapter{admin: m}

		m.On("DeleteConsumerGroup", "group-1").Return(nil)

		err := adapter.DeleteConsumerGroup(context.Background(), "group-1")
		assert.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()

		m := new(mockClusterAdmin)
		adapter := &AdminAdapter{admin: m}

		m.On("DeleteConsumerGroup", "group-1").Return(errors.New("fail"))

		err := adapter.DeleteConsumerGroup(context.Background(), "group-1")
		assert.Error(t, err)
		m.AssertExpectations(t)
	})
}

func TestAdminAdapter_Close(t *testing.T) {
	t.Parallel()

	m := new(mockClusterAdmin)
	adapter := &AdminAdapter{admin: m}

	m.On("Close").Return(nil)

	err := adapter.Close()
	assert.NoError(t, err)
	m.AssertExpectations(t)
}

// mockClusterAdmin mocks sarama.ClusterAdmin interface.
// We embed the interface to satisfy all methods, but only implement the ones we need.
type mockClusterAdmin struct {
	mock.Mock
	sarama.ClusterAdmin
}

func (m *mockClusterAdmin) CreateTopic(name string, detail *sarama.TopicDetail, validateOnly bool) error {
	args := m.Called(name, detail, validateOnly)
	return args.Error(0)
}

func (m *mockClusterAdmin) DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error) {
	args := m.Called(topics)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*sarama.TopicMetadata), args.Error(1)
}

func (m *mockClusterAdmin) DeleteConsumerGroup(group string) error {
	args := m.Called(group)
	return args.Error(0)
}

func (m *mockClusterAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}

// mockClient mocks sarama.Client interface.
type mockClient struct {
	mock.Mock
	sarama.Client
}

func (m *mockClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	args := m.Called(topic, partitionID, time)
	return args.Get(0).(int64), args.Error(1)
}
