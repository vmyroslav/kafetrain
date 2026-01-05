package sarama

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"github.com/vmyroslav/kafetrain/resilience"
)

// AdminAdapter wraps sarama.ClusterAdmin to implement retry.Admin interface.
type AdminAdapter struct {
	admin sarama.ClusterAdmin
}

// NewAdminAdapter creates a retry.Admin from a Sarama ClusterAdmin.
func NewAdminAdapter(admin sarama.ClusterAdmin) resilience.Admin {
	return &AdminAdapter{admin: admin}
}

// NewAdminAdapterFromClient creates a retry.Admin from a Sarama Client.
// Convenience constructor that creates ClusterAdmin internally.
func NewAdminAdapterFromClient(client sarama.Client) (resilience.Admin, error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return NewAdminAdapter(admin), nil
}

// CreateTopic implements retry.Admin interface.
func (a *AdminAdapter) CreateTopic(_ context.Context, name string, partitions int32, replicationFactor int16, config map[string]string) error {
	// convert config map[string]string to map[string]*string (Sarama format)
	saramaConfig := make(map[string]*string, len(config))
	for k, v := range config {
		val := v
		saramaConfig[k] = &val
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     saramaConfig,
	}

	err := a.admin.CreateTopic(name, topicDetail, false)
	if err != nil {
		// ignore "topic already exists" error
		if topicErr, ok := err.(*sarama.TopicError); ok {
			if topicErr.Err == sarama.ErrTopicAlreadyExists {
				return nil
			}
		}

		return fmt.Errorf("failed to create topic %s: %w", name, err)
	}

	return nil
}

// DescribeTopics implements retry.Admin interface.
func (a *AdminAdapter) DescribeTopics(_ context.Context, topics []string) ([]resilience.TopicMetadata, error) {
	metadata, err := a.admin.DescribeTopics(topics)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	result := make([]resilience.TopicMetadata, 0, len(metadata))
	for _, md := range metadata {
		if md.Err != sarama.ErrNoError {
			continue
		}

		// Get replication factor from first partition (all partitions have same RF)
		var replicationFactor int16 = 1
		if len(md.Partitions) > 0 {
			replicationFactor = int16(len(md.Partitions[0].Replicas))
		}

		result = append(result, &topicMetadata{
			name:              md.Name,
			partitions:        int32(len(md.Partitions)),
			replicationFactor: replicationFactor,
		})
	}

	return result, nil
}

// DeleteConsumerGroup implements retry.Admin interface.
func (a *AdminAdapter) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	err := a.admin.DeleteConsumerGroup(groupID)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Close implements retry.Admin interface.
func (a *AdminAdapter) Close() error {
	return a.admin.Close()
}

// topicMetadata implements retry.TopicMetadata interface.
type topicMetadata struct {
	name              string
	partitions        int32
	replicationFactor int16
}

func (t *topicMetadata) Name() string             { return t.name }
func (t *topicMetadata) Partitions() int32        { return t.partitions }
func (t *topicMetadata) ReplicationFactor() int16 { return t.replicationFactor }
