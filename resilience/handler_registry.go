package resilience

import "sync"

type HandlerRegistry struct {
	handlers map[string]MessageHandler
	mu       sync.RWMutex
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]MessageHandler),
	}
}

func (r *HandlerRegistry) Add(topic string, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.handlers[topic] = handler
}

func (r *HandlerRegistry) Get(topic string) (MessageHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, ok := r.handlers[topic]

	return handler, ok
}
