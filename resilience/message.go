package resilience

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

const (
	// HeaderRetryAttempt current retry count
	HeaderRetryAttempt = "x-retry-attempt"
	// HeaderRetryMax maximum retries allowed
	HeaderRetryMax = "x-retry-max"
	// HeaderRetryNextTime next retry timestamp (Unix)
	HeaderRetryNextTime = "x-retry-next-time"
	// HeaderRetryOriginalTime first failure timestamp (Unix)
	HeaderRetryOriginalTime = "x-retry-original-time"
	// HeaderRetryReason error message from last failure
	HeaderRetryReason = "x-retry-reason"
)

const (
	// HeaderCoordinatorID unique identifier for the coordinator instance
	HeaderCoordinatorID = "x-coordinator-id"
	// HeaderTopic stores the original topic name
	HeaderTopic = "topic"
	HeaderID    = "id"
	HeaderRetry = "retry"
	HeaderKey   = "key"
)

// Message represents a Kafka message
type Message interface {
	Topic() string

	Key() []byte
	Value() []byte
	Headers() Headers
	Timestamp() time.Time
}

// Headers represents message headers.
type Headers interface {
	// Get retrieves a header value by key
	Get(key string) ([]byte, bool)

	// Set adds or updates a header
	Set(key string, value []byte)

	// Delete removes a header
	Delete(key string)

	// All returns all headers as a map
	All() map[string][]byte

	// Clone creates a deep copy of headers
	Clone() Headers
}

// InternalMessage is the internal message representation.
type InternalMessage struct {
	TimestampData time.Time
	topic         string
	KeyData       []byte
	Payload       []byte
	HeaderData    HeaderList
}

func NewFromMessage(msg Message) *InternalMessage {
	headers := make(HeaderList, 0, len(msg.Headers().All()))
	for key, value := range msg.Headers().All() {
		headers = append(headers, Header{Key: []byte(key), Value: value})
	}

	return &InternalMessage{
		topic:         msg.Topic(),
		KeyData:       msg.Key(),
		Payload:       msg.Value(),
		HeaderData:    headers,
		TimestampData: msg.Timestamp(),
	}
}

// Topic returns the topic name of the message.
func (m *InternalMessage) Topic() string {
	return m.topic
}

// SetTopic sets the topic name of the message.
func (m *InternalMessage) SetTopic(topic string) {
	m.topic = topic
}

// Value returns the message payload.
func (m *InternalMessage) Value() []byte {
	return m.Payload
}

// Key returns the message key.
func (m *InternalMessage) Key() []byte {
	return m.KeyData
}

// Timestamp returns the message timestamp.
func (m *InternalMessage) Timestamp() time.Time {
	return m.TimestampData
}

// Headers returns the message headers.
func (m *InternalMessage) Headers() Headers {
	return &m.HeaderData
}

type Header struct {
	Key   []byte
	Value []byte
}

type HeaderList []Header

func (h *HeaderList) Get(key string) ([]byte, bool) {
	for _, header := range *h {
		if string(header.Key) == key {
			return header.Value, true
		}
	}

	return nil, false
}

func (h *HeaderList) Set(key string, value []byte) {
	// find and update existing header
	for i, header := range *h {
		if string(header.Key) == key {
			(*h)[i].Value = value
			return
		}
	}

	// add new header if not found
	*h = append(*h, Header{
		Key:   []byte(key),
		Value: value,
	})
}

func (h *HeaderList) All() map[string][]byte {
	result := make(map[string][]byte, len(*h))
	for _, header := range *h {
		result[string(header.Key)] = header.Value
	}

	return result
}

func (h *HeaderList) Delete(key string) {
	newHeaders := make(HeaderList, 0, len(*h))
	for _, header := range *h {
		if string(header.Key) != key {
			newHeaders = append(newHeaders, header)
		}
	}

	*h = newHeaders
}

func (h *HeaderList) Clone() Headers {
	// deep copy the headers
	cloned := make(HeaderList, len(*h))
	for i, header := range *h {
		cloned[i] = Header{
			Key:   append([]byte(nil), header.Key...),
			Value: append([]byte(nil), header.Value...),
		}
	}

	return &cloned
}

func GetHeaderValue[T any](h *HeaderList, key string) (T, bool) {
	var (
		zero     T
		val      string
		found    bool
		keyBytes = []byte(key)
	)

	for _, hdr := range *h {
		if bytes.Equal(hdr.Key, keyBytes) {
			val = string(hdr.Value)
			found = true

			break
		}
	}

	if !found {
		return zero, false
	}

	var anyVal any = zero
	switch anyVal.(type) {
	case string:
		v, ok := any(val).(T)

		return v, ok
	case int:
		i, err := strconv.Atoi(val)
		if err != nil {
			return zero, false
		}

		v, ok := any(i).(T)

		return v, ok
	case time.Time:
		unix, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return zero, false
		}

		v, ok := any(time.Unix(unix, 0)).(T)

		return v, ok
	default:
		return zero, false
	}
}

// SetHeader updates or adds a header with the given key and value.
// Supported types: string, int, time.Time
// Returns an error if an unsupported type is provided.
func SetHeader[T any](h *HeaderList, key string, value T) error {
	var valStr string

	// convert T to string based on its type
	switch v := any(value).(type) {
	case string:
		valStr = v
	case int:
		valStr = strconv.Itoa(v)
	case time.Time:
		valStr = strconv.FormatInt(v.Unix(), 10)
	default:
		return fmt.Errorf("SetHeader: unsupported type %T (supported: string, int, time.Time)", value)
	}

	keyBytes := []byte(key)

	// check if the key already exists to update it
	for i, hdr := range *h {
		if bytes.Equal(hdr.Key, keyBytes) {
			(*h)[i].Value = []byte(valStr)
			return nil
		}
	}

	// if not found, append a new header
	*h = append(*h, Header{
		Key:   keyBytes,
		Value: []byte(valStr),
	})

	return nil
}
