package resilience

import (
	"context"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyTracker_AddMessage(t *testing.T) {
	msgs := []Message{
		{
			Key:       []byte("test-1"),
			Payload:   nil,
			Headers:   nil,
			topic:     "topic-test-1",
			offset:    0,
			partition: 0,
		},
		{
			Key:       []byte("test-2"),
			Payload:   nil,
			Headers:   nil,
			topic:     "topic-test-2",
			offset:    0,
			partition: 0,
		},
		{
			Key:       []byte("test-1"),
			Payload:   nil,
			Headers:   nil,
			topic:     "topic-test-1",
			offset:    0,
			partition: 0,
		},
		{
			Key:       []byte("test-1"),
			Payload:   nil,
			Headers:   nil,
			topic:     "topic-test-1",
			offset:    0,
			partition: 0,
		},
		{
			Key:       []byte("test-1"),
			Payload:   nil,
			Headers:   nil,
			topic:     "topic-test-3",
			offset:    0,
			partition: 0,
		},
	}

	var (
		kt  = NewKeyTracker()
		ctx = context.Background()
		err error
	)

	for _, msg := range msgs {
		_, err = kt.AddMessage(ctx, msg)
		require.NoError(t, err)
	}

	for _, msg := range msgs {
		assert.True(t, kt.IsRelated(ctx, msg))
	}

	assert.False(t, kt.IsRelated(ctx, Message{
		Key:       []byte("new-key"),
		Payload:   nil,
		Headers:   nil,
		topic:     "new-topic",
		offset:    0,
		partition: 0,
	}))
}

func TestKeyTracker_ReleaseMessage(t *testing.T) {

	var (
		kt  = NewKeyTracker()
		ctx = context.Background()
		err error
	)

	_, err = kt.AddMessage(ctx, Message{
		Key:       []byte("test-1"),
		Payload:   nil,
		Headers:   nil,
		topic:     "test-topic",
		offset:    1,
		partition: 1,
	})
	require.NoError(t, err)

	err = kt.ReleaseMessage(ctx, Message{
		Key:       []byte("test-1"),
		Payload:   nil,
		Headers:   nil,
		topic:     "test-topic",
		offset:    0,
		partition: 0,
	})
	require.NoError(t, err)

	assert.True(t, len(kt.lm) == 0)
}
