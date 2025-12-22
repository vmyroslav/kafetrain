package resilience

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandlerRegistry(t *testing.T) {
	t.Parallel()
	var topic = "test"
	hr := NewHandlerRegistry()
	handler := new(dummyHandler)

	hr.Add(topic, handler)
	h, ok := hr.Get(topic)

	assert.True(t, ok, "handler should be found")
	assert.Equal(t, h, handler, "handler should be nil")

	eh, ok := hr.Get("not-existing-topic")

	assert.False(t, ok, "handler should NOT be found")
	assert.Nil(t, eh, "handler should be nil")
}

func TestHandlerRegistryFunc(t *testing.T) {
	t.Parallel()
	var topic = "test"
	hr := NewHandlerRegistry()

	handler := MessageHandleFunc(func(ctx context.Context, message Message) error {
		return nil
	})

	hr.Add(topic, handler)

	h, ok := hr.Get(topic)

	assert.True(t, ok, "handler should be found")
	assert.NotNil(t, h, "handler should not be nil")

	eh, ok := hr.Get("not-existing-topic")

	assert.False(t, ok, "handler should NOT be found")
	assert.Nil(t, eh, "handler should be nil")
}

type dummyHandler struct {
}

func (d *dummyHandler) Handle(_ context.Context, _ Message) error {
	return nil
}
