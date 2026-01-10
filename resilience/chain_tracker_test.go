package resilience

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyTracker_AddMessage(t *testing.T) {
	t.Parallel()

	msgs := []InternalMessage{
		{Key: []byte("test-1"), topic: "topic-test-1"},
		{Key: []byte("test-2"), topic: "topic-test-2"},
		{Key: []byte("test-1"), topic: "topic-test-1"}, // Same key
		{Key: []byte("test-1"), topic: "topic-test-1"}, // Same key again
		{Key: []byte("test-1"), topic: "topic-test-3"}, // Same key, different topic
	}

	kt := NewKeyMemoryTracker()
	ctx := t.Context()

	for _, msg := range msgs {
		_, err := kt.AddMessage(ctx, &msg)
		require.NoError(t, err)
	}

	// all should be related
	for _, msg := range msgs {
		assert.True(t, kt.IsRelated(ctx, &msg), "message should be related after adding")
	}

	// new key should not be related
	assert.False(t, kt.IsRelated(ctx, &InternalMessage{
		Key:   []byte("new-key"),
		topic: "new-topic",
	}), "new message should not be related")
}

func TestKeyTracker_ReleaseMessage(t *testing.T) {
	t.Parallel()

	kt := NewKeyMemoryTracker()
	ctx := t.Context()

	msg := &InternalMessage{Key: []byte("test"), topic: "topic"}

	// add 3 times (simulating 3 messages with same key)
	_, err := kt.AddMessage(ctx, msg)
	require.NoError(t, err)
	_, err = kt.AddMessage(ctx, msg)
	require.NoError(t, err)
	_, err = kt.AddMessage(ctx, msg)
	require.NoError(t, err)

	assert.True(t, kt.IsRelated(ctx, msg), "should be related")

	// release once
	err = kt.ReleaseMessage(ctx, msg)
	require.NoError(t, err)
	assert.True(t, kt.IsRelated(ctx, msg), "should still be related after first release")

	// release again
	err = kt.ReleaseMessage(ctx, msg)
	require.NoError(t, err)
	assert.True(t, kt.IsRelated(ctx, msg), "should still be related after second release")

	// final release
	err = kt.ReleaseMessage(ctx, msg)
	require.NoError(t, err)
	assert.False(t, kt.IsRelated(ctx, msg), "should no longer be related after final release")
}

func TestKeyTracker_MultipleTopicsAndKeys(t *testing.T) {
	t.Parallel()

	kt := NewKeyMemoryTracker()
	ctx := t.Context()

	// Test multiple topics with different keys
	msg1 := &InternalMessage{Key: []byte("key1"), topic: "topic1"}
	msg2 := &InternalMessage{Key: []byte("key2"), topic: "topic1"}
	msg3 := &InternalMessage{Key: []byte("key1"), topic: "topic2"}

	// Add messages
	_, err := kt.AddMessage(ctx, msg1)
	require.NoError(t, err)
	_, err = kt.AddMessage(ctx, msg2)
	require.NoError(t, err)
	_, err = kt.AddMessage(ctx, msg3)
	require.NoError(t, err)

	// All should be related
	assert.True(t, kt.IsRelated(ctx, msg1), "msg1 should be related")
	assert.True(t, kt.IsRelated(ctx, msg2), "msg2 should be related")
	assert.True(t, kt.IsRelated(ctx, msg3), "msg3 should be related")

	// Release msg1 - msg2 and msg3 should still be related
	err = kt.ReleaseMessage(ctx, msg1)
	require.NoError(t, err)
	assert.False(t, kt.IsRelated(ctx, msg1), "msg1 should not be related after release")
	assert.True(t, kt.IsRelated(ctx, msg2), "msg2 should still be related")
	assert.True(t, kt.IsRelated(ctx, msg3), "msg3 should still be related")

	// Release msg2 - only msg3 should be related
	err = kt.ReleaseMessage(ctx, msg2)
	require.NoError(t, err)
	assert.False(t, kt.IsRelated(ctx, msg2), "msg2 should not be related after release")
	assert.True(t, kt.IsRelated(ctx, msg3), "msg3 should still be related")

	// Release msg3 - nothing should be related
	err = kt.ReleaseMessage(ctx, msg3)
	require.NoError(t, err)
	assert.False(t, kt.IsRelated(ctx, msg3), "msg3 should not be related after release")
}

func TestLockMap_GetRefCount(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// test empty map
	count, exists := lm.getRefCount("topic", "key")
	assert.False(t, exists, "should not exist in empty map")
	assert.Equal(t, 0, count, "count should be 0 for non-existent key")

	// add a key
	lm["topic"] = map[string]int{"key": 5}

	// test existing key
	count, exists = lm.getRefCount("topic", "key")
	assert.True(t, exists, "should exist")
	assert.Equal(t, 5, count, "count should be 5")

	// test non-existent key in existing topic
	count, exists = lm.getRefCount("topic", "other-key")
	assert.False(t, exists, "should not exist")
	assert.Equal(t, 0, count, "count should be 0")

	// test non-existent topic
	count, exists = lm.getRefCount("other-topic", "key")
	assert.False(t, exists, "should not exist for non-existent topic")
	assert.Equal(t, 0, count, "count should be 0")

	// test key with zero count
	lm["topic"]["zero-key"] = 0
	count, exists = lm.getRefCount("topic", "zero-key")
	assert.True(t, exists, "should exist even with zero count")
	assert.Equal(t, 0, count, "count should be 0")
}

func TestLockMap_IncrementRef(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// first increment on empty map
	count := lm.incrementRef("topic", "key")
	assert.Equal(t, 1, count, "first increment should return 1")
	assert.Equal(t, 1, lm["topic"]["key"], "map should contain count of 1")

	// second increment
	count = lm.incrementRef("topic", "key")
	assert.Equal(t, 2, count, "second increment should return 2")
	assert.Equal(t, 2, lm["topic"]["key"], "map should contain count of 2")

	// increment different key in same topic
	count = lm.incrementRef("topic", "other-key")
	assert.Equal(t, 1, count, "first increment of other key should return 1")
	assert.Equal(t, 1, lm["topic"]["other-key"], "map should contain count of 1 for other key")
	assert.Equal(t, 2, lm["topic"]["key"], "original key should still be 2")

	// increment key in different topic
	count = lm.incrementRef("other-topic", "key")
	assert.Equal(t, 1, count, "first increment in new topic should return 1")
	assert.Equal(t, 1, lm["other-topic"]["key"], "new topic should have count of 1")

	// verify topic isolation
	assert.Len(t, lm, 2, "should have 2 topics")
	assert.Len(t, lm["topic"], 2, "first topic should have 2 keys")
	assert.Len(t, lm["other-topic"], 1, "second topic should have 1 key")
}

func TestLockMap_DecrementRef(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// decrement on empty map
	count, exists := lm.decrementRef("topic", "key")
	assert.False(t, exists, "should not exist in empty map")
	assert.Equal(t, 0, count, "decrement on empty map should return 0")

	// setup: add a key with count 3
	lm["topic"] = map[string]int{"key": 3}

	// first decrement
	count, exists = lm.decrementRef("topic", "key")
	assert.True(t, exists, "should exist")
	assert.Equal(t, 2, count, "first decrement should return 2")
	assert.Equal(t, 2, lm["topic"]["key"], "map should contain count of 2")

	// second decrement
	count, exists = lm.decrementRef("topic", "key")
	assert.True(t, exists, "should exist")
	assert.Equal(t, 1, count, "second decrement should return 1")

	// decrement to zero
	count, exists = lm.decrementRef("topic", "key")
	assert.True(t, exists, "should exist")
	assert.Equal(t, 0, count, "decrement to zero should return 0")

	// decrement below zero (this is now prevented by logic in decrementRef, but if it existed...)
	// Note: decrementRef only decrements if key exists. Since we don't delete the key inside decrementRef itself,
	// it can technically go below zero if called repeatedly.
	count, exists = lm.decrementRef("topic", "key")
	assert.True(t, exists, "should exist")
	assert.Equal(t, -1, count, "decrement below zero should return -1")

	// decrement non-existent key in existing topic
	count, exists = lm.decrementRef("topic", "other-key")
	assert.False(t, exists, "should not exist")
	assert.Equal(t, 0, count, "decrement non-existent key should return 0")
}

func TestLockMap_RemoveKey(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// remove from empty map (should not panic)
	lm.removeKey("topic", "key")
	assert.Empty(t, lm, "map should still be empty")

	// setup
	lm["topic"] = map[string]int{"key1": 1, "key2": 2}

	// remove existing key
	lm.removeKey("topic", "key1")
	assert.NotContains(t, lm["topic"], "key1", "key1 should be removed")
	assert.Contains(t, lm["topic"], "key2", "key2 should still exist")

	// remove non-existent key (should not panic)
	lm.removeKey("topic", "non-existent")
	assert.Len(t, lm["topic"], 1, "topic should still have 1 key")

	// remove from non-existent topic (should not panic)
	lm.removeKey("other-topic", "key")
	assert.NotContains(t, lm, "other-topic", "other-topic should not be created")
}

func TestLockMap_RemoveTopic(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// remove from empty map (should not panic)
	lm.removeTopic("topic")
	assert.Empty(t, lm, "map should still be empty")

	// setup
	lm["topic1"] = map[string]int{"key": 1}
	lm["topic2"] = map[string]int{"key": 2}

	// remove existing topic
	lm.removeTopic("topic1")
	assert.NotContains(t, lm, "topic1", "topic1 should be removed")
	assert.Contains(t, lm, "topic2", "topic2 should still exist")

	// remove non-existent topic (should not panic)
	lm.removeTopic("non-existent")
	assert.Len(t, lm, 1, "should still have 1 topic")
}

func TestLockMap_IsTopicEmpty(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// empty map
	assert.True(t, lm.isTopicEmpty("topic"), "non-existent topic should be empty")

	// topic with keys
	lm["topic"] = map[string]int{"key": 1}
	assert.False(t, lm.isTopicEmpty("topic"), "topic with keys should not be empty")

	// empty topic
	lm["empty-topic"] = map[string]int{}
	assert.True(t, lm.isTopicEmpty("empty-topic"), "topic with no keys should be empty")

	// topic with zero-count key
	lm["zero-topic"] = map[string]int{"key": 0}
	assert.False(t, lm.isTopicEmpty("zero-topic"), "topic with zero-count key should not be empty")
}

func TestLockMap_HasKey(t *testing.T) {
	t.Parallel()

	lm := make(lockMap)

	// empty map
	assert.False(t, lm.hasKey("topic", "key"), "should not have key in empty map")

	// non-existent topic
	lm["other-topic"] = map[string]int{"key": 1}
	assert.False(t, lm.hasKey("topic", "key"), "should not have key in non-existent topic")

	// existing key
	assert.True(t, lm.hasKey("other-topic", "key"), "should have existing key")

	// non-existent key in existing topic
	assert.False(t, lm.hasKey("other-topic", "other-key"), "should not have non-existent key")

	// key with zero count
	lm["other-topic"]["zero-key"] = 0
	assert.True(t, lm.hasKey("other-topic", "zero-key"), "should have key even with zero count")

	// key with negative count
	lm["other-topic"]["neg-key"] = -1
	assert.True(t, lm.hasKey("other-topic", "neg-key"), "should have key even with negative count")
}
