package kafetrain

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
	r.handlers[topic] = handler
	r.Unlock()
}

func (r *HandlerRegistry) Get(topic string) (MessageHandler, bool) {
	r.RLock()
	handler, ok := r.handlers[topic]
	r.RUnlock()

	return handler, ok
}
