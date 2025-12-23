package resilience

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// NewLoggingMiddleware Middleware for logging.
func NewLoggingMiddleware(logger *zap.Logger) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message *Message) error {
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
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, msg *Message) error {
			if t.IsRelated(msg.topic, msg) {
				t.logger.Debug("message is related to existing error chain, redirecting",
					zap.String("topic", msg.topic),
					zap.String("key", string(msg.Key)),
				)

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
		return func(ctx context.Context, message *Message) error {
			currentAttempt, _ := GetHeaderValue[int](&message.Headers, HeaderRetryAttempt)
			maxRetries := et.cfg.MaxRetries

			if maxRetries > 0 && currentAttempt > maxRetries {
				return et.HandleMaxRetriesExceeded(ctx, message, currentAttempt, maxRetries)
			}

			if err := et.WaitForRetryTime(ctx, message); err != nil {
				return err
			}

			et.logger.Debug("processing retry message",
				zap.String("topic", message.topic),
				zap.Int("attempt", currentAttempt),
				zap.Int("max_retries", maxRetries),
			)

			if err := next(ctx, message); err != nil {
				return err
			}

			if err := et.Free(ctx, message); err != nil {
				return err
			}

			et.logger.Info("retry message successfully processed and freed from tracking",
				zap.String("topic", message.topic),
				zap.Int("attempts", currentAttempt),
			)

			return nil
		}
	}
}

// NewFilterMiddleware filter messages based on provided filter function.
func NewFilterMiddleware(filterFunc func(msg *Message) bool) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message *Message) error {
			if ok := filterFunc(message); !ok {
				return nil
			}

			return next(ctx, message)
		}
	}
}
