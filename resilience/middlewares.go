package resilience

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// NewLoggingMiddleware Middleware for logging.
func NewLoggingMiddleware(logger *zap.Logger) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message Message) error {
			start := time.Now()

			if err := next(ctx, message); err != nil {
				return err
			}

			logger.Info("message handled",
				zap.String("topic", message.topic),
				zap.Int32("partition", message.partition),
				zap.Int64("offset", message.offset),
				zap.Duration("duration", time.Since(start)),
			)

			return nil
		}
	}
}

// NewErrorHandlingMiddleware track message processing time.
func NewErrorHandlingMiddleware(t *ErrorTracker) Middleware {
	// TODO: could be done in interceptors?
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, msg Message) error {
			log.Println("ErrorHandlingMiddleware")

			if t.IsRelated(msg.topic, msg) {
				log.Println("related msg")

				if err := t.Redirect(ctx, msg); err != nil {
					return errors.Wrap(err, "failed to redirect msg")
				}

				return nil
			}

			if err := next(ctx, msg); err != nil {
				var e RetriableError
				if errors.As(err, &e) {
					// Use RedirectWithError to capture the error reason
					if err := t.RedirectWithError(ctx, msg, e.Origin); err != nil {
						return errors.Wrap(err, "failed to redirect msg")
					}

					return nil
				}

				return err
			}

			return nil
		}
	}
}

// NewRetryMiddleware implements retry logic with backoff delay mechanism and DLQ support.
// It checks the x-retry-next-time header and waits until that time before processing.
// If max retries are exceeded, sends the message to Dead Letter Queue instead of processing.
func NewRetryMiddleware(et *ErrorTracker) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message Message) error {
			// Check if max retries exceeded (MaxRetries of 0 means infinite retries)
			currentAttempt, _ := GetHeaderValue[int](&message.Headers, HeaderRetryAttempt)
			maxRetries := et.cfg.MaxRetries

			if maxRetries > 0 && currentAttempt > maxRetries {
				log.Printf("retry msg: max retries exceeded (%d > %d), sending to DLQ",
					currentAttempt, maxRetries)

				// Get the last error reason from headers
				lastErrorReason, _ := GetHeaderValue[string](&message.Headers, HeaderRetryReason)

				lastError := errors.New(lastErrorReason)
				if lastErrorReason == "" {
					lastError = errors.New("max retries exceeded")
				}

				// Send to DLQ
				if err := et.SendToDLQ(ctx, message, lastError); err != nil {
					log.Printf("retry msg: failed to send to DLQ: %v", err)
					return errors.Wrap(err, "failed to send message to DLQ")
				}

				// Still publish tombstone to remove from tracking
				if err := et.Free(ctx, message); err != nil {
					log.Printf("retry msg: failed to publish tombstone after DLQ: %v", err)
					return errors.Wrap(err, "failed to free message after DLQ")
				}

				log.Println("retry msg: successfully sent to DLQ and freed from tracking")

				return nil
			}

			// Check if this message has a scheduled retry time
			nextRetryTime, ok := GetHeaderValue[time.Time](&message.Headers, HeaderRetryNextTime)
			if ok {
				now := time.Now()
				if now.Before(nextRetryTime) {
					// Calculate remaining delay
					delay := nextRetryTime.Sub(now)

					log.Printf("retry msg: waiting %v before processing (scheduled for %v)",
						delay, nextRetryTime)

					// Wait until the scheduled time or context is canceled
					select {
					case <-time.After(delay):
						// Continue processing after delay
						log.Println("retry msg: delay complete, processing message")
					case <-ctx.Done():
						// Context canceled during delay
						return ctx.Err()
					}
				}
			}

			log.Printf("retry msg: processing (attempt %d/%d)", currentAttempt, maxRetries)

			// Process the message
			if err := next(ctx, message); err != nil {
				return err
			}

			// Success! Publish tombstone to remove from tracking
			if err := et.Free(ctx, message); err != nil {
				return err
			}

			log.Println("retry msg: successfully processed and freed from tracking")

			return nil
		}
	}
}

// NewFilterMiddleware filter messages based on provided filter function.
func NewFilterMiddleware(filterFunc func(msg Message) bool) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message Message) error {
			if ok := filterFunc(message); !ok {
				return nil
			}

			return next(ctx, message)
		}
	}
}
