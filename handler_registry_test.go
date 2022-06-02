package kafetrain

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
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

type dummyHandler struct {
}

func (d *dummyHandler) Handle(_ context.Context, _ Message) error {
	return nil
}
