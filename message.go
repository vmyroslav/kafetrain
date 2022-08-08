package kafetrain

import "time"

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

type HeaderList []*Header

func (h *HeaderList) Set(key, val string) {
	*h = append(*h, &Header{Key: []byte(key), Value: []byte(val)})
}

func (h *HeaderList) Get(key string) (string, bool) {
	for _, hdr := range *h {
		if string(hdr.Key) == key {
			return string(hdr.Value), true
		}
	}

	return "", false
}
