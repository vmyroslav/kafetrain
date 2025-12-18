package kafetrain

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

const (
	HeaderRetryAttempt      = "x-retry-attempt"       // Current retry count
	HeaderRetryMax          = "x-retry-max"           // Maximum retries allowed
	HeaderRetryNextTime     = "x-retry-next-time"     // Next retry timestamp (Unix)
	HeaderRetryOriginalTime = "x-retry-original-time" // First failure timestamp (Unix)
	HeaderRetryReason       = "x-retry-reason"        // Error message from last failure
)

// Message generic kafka message. TODO: add generic type for marshaling
type Message struct {
	Key     []byte
	Payload []byte
	Headers HeaderList

	transformedKey     any
	transformedPayload any

	topic     string
	offset    int64
	partition int32

	Timestamp time.Time
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
		return any(val).(T), true
	case int:
		i, err := strconv.Atoi(val)
		if err != nil {
			return zero, false
		}
		return any(i).(T), true
	case time.Time:
		unix, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return zero, false
		}
		return any(time.Unix(unix, 0)).(T), true
	default:
		return zero, false
	}
}

// SetHeader updates or adds a header with the given key and value.
// Supported types: string, int, time.Time
// Panics if an unsupported type is provided.
func SetHeader[T any](h *HeaderList, key string, value T) {
	var valStr string

	// Convert T to string based on its type
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

	// Check if the key already exists to update it
	for i, hdr := range *h {
		if bytes.Equal(hdr.Key, keyBytes) {
			(*h)[i].Value = []byte(valStr)
			return
		}
	}

	// If not found, append a new header
	*h = append(*h, Header{
		Key:   keyBytes,
		Value: []byte(valStr),
	})
}
