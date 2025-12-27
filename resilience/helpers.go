package resilience

import (
	"context"
	"strconv"

	"github.com/IBM/sarama"
)

// IsRetriable checks if an error should trigger a retry.
// Returns true if the error is a RetriableError with Retry=true.
func IsRetriable(err error) bool {
	retErr, ok := err.(RetriableError)
	return ok && retErr.Retry
}

// GetRetryAttempt extracts the retry attempt count from a Sarama message's headers.
// Returns 0 if the header is not found or cannot be parsed.
func GetRetryAttempt(msg *sarama.ConsumerMessage) int {
	for _, h := range msg.Headers {
		if string(h.Key) == HeaderRetryAttempt {
			val, err := strconv.Atoi(string(h.Value))
			if err != nil {
				return 0
			}
			return val
		}
	}
	return 0
}

// IsRetryMessage checks if a Sarama message originated from a retry topic.
// Returns true if the message has the original topic header (HeaderTopic).
func IsRetryMessage(msg *sarama.ConsumerMessage) bool {
	for _, h := range msg.Headers {
		if string(h.Key) == HeaderTopic {
			return true
		}
	}
	return false
}

// GetOriginalTopic extracts the original topic name from a retry message.
// Returns empty string if the message is not a retry message.
func GetOriginalTopic(msg *sarama.ConsumerMessage) string {
	for _, h := range msg.Headers {
		if string(h.Key) == HeaderTopic {
			return string(h.Value)
		}
	}
	return ""
}

// GetMaxRetries extracts the maximum retry count from a message's headers.
// Returns 0 if the header is not found or cannot be parsed.
func GetMaxRetries(msg *sarama.ConsumerMessage) int {
	for _, h := range msg.Headers {
		if string(h.Key) == HeaderRetryMax {
			val, err := strconv.Atoi(string(h.Value))
			if err != nil {
				return 0
			}
			return val
		}
	}
	return 0
}

// GetRetryReason extracts the error reason from the last retry attempt.
// Returns empty string if the header is not found.
func GetRetryReason(msg *sarama.ConsumerMessage) string {
	for _, h := range msg.Headers {
		if string(h.Key) == HeaderRetryReason {
			return string(h.Value)
		}
	}
	return ""
}

// ToMessageHandler adapts a Sarama-based handler for use in the registry.
// This allows users to write their business logic once using *sarama.ConsumerMessage
// and reuse it for both main consumer and retry consumer (via registry).
//
// Example:
//
//	func processOrder(ctx context.Context, msg *sarama.ConsumerMessage) error {
//	    // Business logic here
//	}
//
//	registry.Add(topic, resilience.ToMessageHandler(processOrder))
func ToMessageHandler(saramaHandler func(context.Context, *sarama.ConsumerMessage) error) MessageHandler {
	return MessageHandleFunc(func(ctx context.Context, msg *Message) error {
		// Convert resilience.Message → sarama.ConsumerMessage
		saramaMsg := toSaramaMessage(msg)

		// Call the user's sarama handler
		return saramaHandler(ctx, saramaMsg)
	})
}

// toSaramaMessage converts internal Message to sarama.ConsumerMessage
func toSaramaMessage(msg *Message) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Headers:   convertHeadersToSarama(msg.Headers),
		Timestamp: msg.Timestamp,
		Topic:     msg.Topic(),
		Partition: msg.Partition(),
		Offset:    msg.Offset(),
		Key:       msg.Key,
		Value:     msg.Payload,
	}
}

// convertHeadersToSarama converts internal HeaderList to Sarama RecordHeaders
func convertHeadersToSarama(headers HeaderList) []*sarama.RecordHeader {
	result := make([]*sarama.RecordHeader, len(headers))

	for i, header := range headers {
		result[i] = &sarama.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		}
	}

	return result
}
