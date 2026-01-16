package sarama

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
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
		return nil, err
	}

	return NewAdminAdapter(admin), nil
}

// CreateTopic implements retry.Admin interface.
func (a *AdminAdapter) CreateTopic(
	_ context.Context,
	name string,
	partitions int32,
	replicationFactor int16,
	config map[string]string,
) error {
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
		var topicErr *sarama.TopicError
		if errors.As(err, &topicErr) {
			if errors.Is(topicErr.Err, sarama.ErrTopicAlreadyExists) {
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
		return nil, err
	}

	result := make([]resilience.TopicMetadata, 0, len(metadata))
	for _, md := range metadata {
		if !errors.Is(md.Err, sarama.ErrNoError) {
			continue
		}

		result = append(result, &topicMetadata{
			name:       md.Name,
			partitions: int32(len(md.Partitions)),
		})
	}

	return result, nil
}

// DeleteConsumerGroup implements retry.Admin interface.
func (a *AdminAdapter) DeleteConsumerGroup(_ context.Context, groupID string) error {
	err := a.admin.DeleteConsumerGroup(groupID)
	if err != nil {
		return err
	}

	return nil
}

// Close implements retry.Admin interface.
func (a *AdminAdapter) Close() error {
	return a.admin.Close()
}

// topicMetadata implements retry.TopicMetadata interface.
type topicMetadata struct {
	name       string
	partitions int32
}

func (t *topicMetadata) Name() string      { return t.name }
func (t *topicMetadata) Partitions() int32 { return t.partitions }
