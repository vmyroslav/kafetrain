package resilience

import "sync"

type HandlerRegistry struct {
	handlers map[string]MessageHandler
	sync.RWMutex
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]MessageHandler),
	}
}

func (r *HandlerRegistry) Add(topic string, handler MessageHandler) {
	r.Lock()
	defer r.Unlock()
	r.handlers[topic] = handler
}

func (r *HandlerRegistry) Get(topic string) (MessageHandler, bool) {
	r.RLock()
	defer r.RUnlock()
	handler, ok := r.handlers[topic]

	return handler, ok
}
