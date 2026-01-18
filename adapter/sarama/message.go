package sarama

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/vmyroslav/kafetrain/resilience"
)

// Message wraps sarama.ConsumerMessage to implement retry.Message interface.
// This is a thin adapter that allows kafetrain core to work with Sarama messages.
type Message struct {
	*sarama.ConsumerMessage
}

// NewMessage creates a retry.Message from a Sarama ConsumerMessage.
func NewMessage(msg *sarama.ConsumerMessage) resilience.Message {
	return &Message{ConsumerMessage: msg}
}

// Partition implements retry.Message interface.
func (m *Message) Partition() int32 {
	return m.ConsumerMessage.Partition
}

// Offset implements retry.Message interface.
func (m *Message) Offset() int64 {
	return m.ConsumerMessage.Offset
}

// Topic implements retry.Message interface.
func (m *Message) Topic() string {
	return m.ConsumerMessage.Topic
}

// Key implements retry.Message interface.
func (m *Message) Key() []byte {
	return m.ConsumerMessage.Key
}

// Value implements retry.Message interface.
func (m *Message) Value() []byte {
	return m.ConsumerMessage.Value
}

// Timestamp implements retry.Message interface.
func (m *Message) Timestamp() time.Time {
	return m.ConsumerMessage.Timestamp
}

// Headers implements retry.Message interface.
func (m *Message) Headers() resilience.Headers {
	return &Headers{headers: m.ConsumerMessage.Headers}
}

// Headers wraps Sarama RecordHeaders to implement retry.Headers interface.
type Headers struct {
	headers []*sarama.RecordHeader
}

// NewHeaders creates retry.Headers from Sarama RecordHeaders.
func NewHeaders(headers []*sarama.RecordHeader) resilience.Headers {
	return &Headers{headers: headers}
}

// Get retrieves the value of a header by key.
func (h *Headers) Get(key string) ([]byte, bool) {
	for _, header := range h.headers {
		if string(header.Key) == key {
			return header.Value, true
		}
	}

	return nil, false
}

// Set sets the value of a header by key.
func (h *Headers) Set(key string, value []byte) {
	// check if header exists, update it
	for i, header := range h.headers {
		if string(header.Key) == key {
			h.headers[i].Value = value
			return
		}
	}

	// Header doesn't exist, add it
	h.headers = append(h.headers, &sarama.RecordHeader{
		Key:   []byte(key),
		Value: value,
	})
}

// Delete implements retry.Headers interface.
func (h *Headers) Delete(key string) {
	for i, header := range h.headers {
		if string(header.Key) == key {
			// remove by swapping with last element and truncating
			h.headers[i] = h.headers[len(h.headers)-1]
			h.headers = h.headers[:len(h.headers)-1]

			return
		}
	}
}

// All returns all headers as a map.
func (h *Headers) All() map[string][]byte {
	result := make(map[string][]byte, len(h.headers))
	for _, header := range h.headers {
		result[string(header.Key)] = header.Value
	}

	return result
}

// Clone returns a deep copy of the headers.
func (h *Headers) Clone() resilience.Headers {
	cloned := make([]*sarama.RecordHeader, len(h.headers))
	for i, header := range h.headers {
		cloned[i] = &sarama.RecordHeader{
			Key:   append([]byte(nil), header.Key...),
			Value: append([]byte(nil), header.Value...),
		}
	}

	return &Headers{headers: cloned}
}

// ToSaramaHeaders converts retry.Headers to Sarama RecordHeaders.
func ToSaramaHeaders(headers resilience.Headers) []sarama.RecordHeader {
	if h, ok := headers.(*Headers); ok {
		result := make([]sarama.RecordHeader, len(h.headers))
		for i, header := range h.headers {
			result[i] = *header
		}

		return result
	}

	// convert from generic Headers to Sarama format
	all := headers.All()
	result := make([]sarama.RecordHeader, 0, len(all))
	for k, v := range all {
		result = append(result, sarama.RecordHeader{
			Key:   []byte(k),
			Value: v,
		})
	}

	return result
}
