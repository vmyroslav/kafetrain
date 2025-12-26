package resilience

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyTracker_AddMessage(t *testing.T) {
	t.Parallel()

	msgs := []Message{
		{Key: []byte("test-1"), topic: "topic-test-1"},
		{Key: []byte("test-2"), topic: "topic-test-2"},
		{Key: []byte("test-1"), topic: "topic-test-1"}, // Same key
		{Key: []byte("test-1"), topic: "topic-test-1"}, // Same key again
		{Key: []byte("test-1"), topic: "topic-test-3"}, // Same key, different topic
	}

	kt := NewKeyTracker()
	ctx := t.Context()

	for _, msg := range msgs {
		_, err := kt.AddMessage(ctx, &msg)
		require.NoError(t, err)
	}

	// verify reference counts
	assert.Equal(t, 3, kt.lm["topic-test-1"]["test-1"], "should have ref count of 3")
	assert.Equal(t, 1, kt.lm["topic-test-2"]["test-2"], "should have ref count of 1")
	assert.Equal(t, 1, kt.lm["topic-test-3"]["test-1"], "should have ref count of 1")

	// all should be related
	for _, msg := range msgs {
		assert.True(t, kt.IsRelated(ctx, &msg))
	}

	// new key should not be related
	assert.False(t, kt.IsRelated(ctx, &Message{
		Key:   []byte("new-key"),
		topic: "new-topic",
	}))
}

func TestKeyTracker_ReleaseMessage(t *testing.T) {
	t.Parallel()

	kt := NewKeyTracker()
	ctx := t.Context()

	msg := &Message{Key: []byte("test"), topic: "topic"}

	// add 3 times (simulating 3 messages with same key)
	_, err := kt.AddMessage(ctx, msg)
	require.NoError(t, err)
	_, err = kt.AddMessage(ctx, msg)
	require.NoError(t, err)
	_, err = kt.AddMessage(ctx, msg)
	require.NoError(t, err)

	assert.Equal(t, 3, kt.lm["topic"]["test"], "ref count should be 3")
	assert.True(t, kt.IsRelated(ctx, msg), "should be related")

	// release once
	err = kt.ReleaseMessage(ctx, msg)
	require.NoError(t, err)
	assert.Equal(t, 2, kt.lm["topic"]["test"], "ref count should be 2")
	assert.True(t, kt.IsRelated(ctx, msg), "should still be related")

	// release again
	err = kt.ReleaseMessage(ctx, msg)
	require.NoError(t, err)
	assert.Equal(t, 1, kt.lm["topic"]["test"], "ref count should be 1")
	assert.True(t, kt.IsRelated(ctx, msg), "should still be related")

	// final release
	err = kt.ReleaseMessage(ctx, msg)
	require.NoError(t, err)
	assert.False(t, kt.IsRelated(ctx, msg), "should no longer be related")
	assert.Equal(t, 0, len(kt.lm), "map should be empty")
}
