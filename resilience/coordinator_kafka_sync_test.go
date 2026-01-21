package resilience

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKafkaStateCoordinator_Synchronize_TopicNotStarted(t *testing.T) {
	coordinator := NewKafkaStateCoordinator(
		&Config{},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		&AdminMock{},
		nil,
	)

	// Should fail because Start() wasn't called (topic is empty)
	err := coordinator.Synchronize(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "coordinator not started: topic is empty", err.Error())
}

func TestKafkaStateCoordinator_Synchronize_EmptyRedirectTopic(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(ctx context.Context, topics []string) ([]TopicMetadata, error) {
			// Return metadata indicating empty topic (or no partitions)
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{}, // No offsets
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	// Manually set topic as if Start() was called
	coordinator.mu.Lock()
	coordinator.topic = "orders"
	coordinator.mu.Unlock()

	// Should return immediately
	err := coordinator.Synchronize(context.Background())
	assert.NoError(t, err)
}

func TestKafkaStateCoordinator_Synchronize_AlreadyCaughtUp(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(ctx context.Context, topics []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10}, // HWM = 10
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = "orders"
	// Simulate that we have already consumed up to offset 9 (HWM 10 means next is 10, so 9 is the last one)
	coordinator.consumedOffsets[0] = 9
	coordinator.mu.Unlock()

	err := coordinator.Synchronize(context.Background())
	assert.NoError(t, err)
}

func TestKafkaStateCoordinator_Synchronize_BlocksUntilCaughtUp(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(ctx context.Context, topics []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 10}, // HWM = 10
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = "orders"
	coordinator.consumedOffsets[0] = 5 // Lagging behind (5 < 9)
	coordinator.mu.Unlock()

	// Use a channel to signal when Synchronize returns
	done := make(chan error)
	go func() {
		done <- coordinator.Synchronize(context.Background())
	}()

	// Ensure it blocks initially
	select {
	case <-done:
		t.Fatal("Synchronize should have blocked")
	case <-time.After(100 * time.Millisecond):
		// Expected behavior
	}

	// Simulate background consumer updating the offset
	coordinator.mu.Lock()
	coordinator.consumedOffsets[0] = 9 // Catch up
	coordinator.mu.Unlock()

	// Now it should complete
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Synchronize failed to return after catching up")
	}
}

func TestKafkaStateCoordinator_Synchronize_ContextCancellation(t *testing.T) {
	mockAdmin := &AdminMock{
		DescribeTopicsFunc: func(ctx context.Context, topics []string) ([]TopicMetadata, error) {
			return []TopicMetadata{
				&mockTopicMetadata{
					name:       "redirect_orders",
					partitions: 1,
					offsets:    map[int32]int64{0: 100}, // HWM = 100
				},
			}, nil
		},
	}

	coordinator := NewKafkaStateCoordinator(
		&Config{RedirectTopicPrefix: "redirect"},
		&LoggerMock{},
		&ProducerMock{},
		&ConsumerFactoryMock{},
		mockAdmin,
		nil,
	)

	coordinator.mu.Lock()
	coordinator.topic = "orders"
	coordinator.consumedOffsets[0] = 5 // Lagging
	coordinator.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := coordinator.Synchronize(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	// Let it block for a bit
	time.Sleep(50 * time.Millisecond)
	cancel()

	wg.Wait()
}
