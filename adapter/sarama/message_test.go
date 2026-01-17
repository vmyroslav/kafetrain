package sarama

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestMessage(t *testing.T) {
	t.Parallel()

	now := time.Now()
	saramaMsg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 1,
		Offset:    123,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Timestamp: now,
		Headers: []*sarama.RecordHeader{
			{Key: []byte("h1"), Value: []byte("v1")},
		},
	}

	msg := NewMessage(saramaMsg)

	t.Run("Getters", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "test-topic", msg.Topic())
		assert.Equal(t, []byte("test-key"), msg.Key())
		assert.Equal(t, []byte("test-value"), msg.Value())
		assert.Equal(t, now, msg.Timestamp())
	})

	t.Run("Headers", func(t *testing.T) {
		t.Parallel()

		h := msg.Headers()
		val, ok := h.Get("h1")
		assert.True(t, ok)
		assert.Equal(t, []byte("v1"), val)
	})
}

func TestHeaders(t *testing.T) {
	t.Parallel()

	t.Run("GetAndSet", func(t *testing.T) {
		t.Parallel()
		h := NewHeaders(nil)

		h.Set("key1", []byte("val1"))
		val, ok := h.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, []byte("val1"), val)

		// Update existing
		h.Set("key1", []byte("val1-updated"))
		val, ok = h.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, []byte("val1-updated"), val)

		// Get non-existent
		_, ok = h.Get("non-existent")
		assert.False(t, ok)
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()
		h := NewHeaders([]*sarama.RecordHeader{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		})

		h.Delete("k1")
		_, ok := h.Get("k1")
		assert.False(t, ok)

		_, ok = h.Get("k2")
		assert.True(t, ok)

		// Delete non-existent (should not panic)
		h.Delete("non-existent")
	})

	t.Run("All", func(t *testing.T) {
		t.Parallel()
		h := NewHeaders([]*sarama.RecordHeader{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		})

		all := h.All()
		assert.Len(t, all, 2)
		assert.Equal(t, []byte("v1"), all["k1"])
		assert.Equal(t, []byte("v2"), all["k2"])
	})

	t.Run("Clone", func(t *testing.T) {
		t.Parallel()
		h := NewHeaders([]*sarama.RecordHeader{
			{Key: []byte("k1"), Value: []byte("v1")},
		})

		cloned := h.Clone()
		cloned.Set("k1", []byte("v1-cloned"))
		cloned.Set("k2", []byte("v2"))

		// Original should be unchanged
		val, _ := h.Get("k1")
		assert.Equal(t, []byte("v1"), val)
		_, ok := h.Get("k2")
		assert.False(t, ok)

		// Cloned should have changes
		val, _ = cloned.Get("k1")
		assert.Equal(t, []byte("v1-cloned"), val)
		_, ok = cloned.Get("k2")
		assert.True(t, ok)
	})
}

func TestToSaramaHeaders(t *testing.T) {
	t.Parallel()

	t.Run("from Sarama headers", func(t *testing.T) {
		t.Parallel()

		h := NewHeaders([]*sarama.RecordHeader{
			{Key: []byte("k1"), Value: []byte("v1")},
		})

		sh := ToSaramaHeaders(h)

		assert.Len(t, sh, 1)

		assert.Equal(t, []byte("k1"), sh[0].Key)

		assert.Equal(t, []byte("v1"), sh[0].Value)
	})
}
