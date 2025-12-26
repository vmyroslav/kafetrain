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

				// Add to tracking BEFORE redirect
				if _, err := t.comparator.AddMessage(ctx, msg); err != nil {
					return errors.Wrap(err, "failed to track related message")
				}

				if err := t.Redirect(ctx, msg); err != nil {
					return errors.Wrap(err, "failed to redirect msg")
				}

				return nil
			}

			if err := next(ctx, msg); err != nil {
				var e RetriableError
				if errors.As(err, &e) {
					// Add to tracking BEFORE redirect
					if _, err := t.comparator.AddMessage(ctx, msg); err != nil {
						return errors.Wrap(err, "failed to track failed message")
					}

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
				zap.Int("current_attempt", currentAttempt),
				zap.Int("max_retries", maxRetries),
				zap.Int64("offset", message.Offset()),
				zap.Int32("partition", message.Partition()),
			)

			if err := next(ctx, message); err != nil {
				// Check if it's a retriable error
				var retriableErr RetriableError
				if errors.As(err, &retriableErr) && retriableErr.ShouldRetry() {
					// Check if we've exceeded max retries
					if maxRetries > 0 && currentAttempt+1 > maxRetries {
						// Send to DLQ
						return et.HandleMaxRetriesExceeded(ctx, message, currentAttempt+1, maxRetries)
					}

					// Redirect with incremented attempt counter
					if redirectErr := et.RedirectWithError(ctx, message, retriableErr.Origin); redirectErr != nil {
						et.logger.Error("failed to redirect retry failure",
							zap.String("topic", message.topic),
							zap.Error(redirectErr),
						)

						return errors.Wrap(redirectErr, "failed to redirect retry failure")
					}

					et.logger.Debug("successfully redirected retry failure, committing offset",
						zap.String("topic", message.topic),
						zap.Int64("offset", message.Offset()),
						zap.Int("next_attempt", currentAttempt+1),
					)

					// Successfully redirected, commit offset
					return nil
				}

				// Non-retriable error, propagate it
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
