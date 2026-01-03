package retryold

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

// Message generic kafka message. TODO: add generic type for marshaling
type Message struct {
	Timestamp time.Time
	topic     string
	Key       []byte
	Payload   []byte
	Headers   HeaderList
	offset    int64
	partition int32
}

// Topic returns the topic name of the message.
func (m *Message) Topic() string {
	return m.topic
}

// Offset returns the offset of the message within its partition.
func (m *Message) Offset() int64 {
	return m.offset
}

// Partition returns the partition number of the message.
func (m *Message) Partition() int32 {
	return m.partition
}

type Header struct {
	Key   []byte
	Value []byte
}

type HeaderList []Header

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
// Panics if an unsupported type is provided.
func SetHeader[T any](h *HeaderList, key string, value T) {
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
		panic(fmt.Sprintf("SetHeader: unsupported type %T (use string, int, or time.Time)", value))
	}

	keyBytes := []byte(key)

	// check if the key already exists to update it
	for i, hdr := range *h {
		if bytes.Equal(hdr.Key, keyBytes) {
			(*h)[i].Value = []byte(valStr)
			return
		}
	}

	// if not found, append a new header
	*h = append(*h, Header{
		Key:   keyBytes,
		Value: []byte(valStr),
	})
}
