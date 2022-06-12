package kafetrain

import (
	"context"
	"go.uber.org/zap"
	"time"

	"github.com/pkg/errors"
)

// NewLoggingMiddleware Middleware for logging.
func NewLoggingMiddleware(logger *zap.Logger) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message Message) error {
			start := time.Now()

			logger.With(
				zap.String("topic", message.topic),
				zap.Int32("partition", message.partition),
				zap.Int64("offset", message.offset),
			)

			if err := next(ctx, message); err != nil {
				return err
			}

			logger.Info("message handled",
				zap.Duration("duration", time.Since(start)),
			)

			return nil
		}
	}
}

// NewErrorHandlingMiddleware track message processing time.
func NewErrorHandlingMiddleware(t *ErrorTracker) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message Message) error {
			if t.IsRelated(message.topic, message.Key) {
				if err := t.Redirect(ctx, message); err != nil {
					return errors.Wrap(err, "failed to redirect message")
				}

				return nil
			}

			if err := next(ctx, message); err != nil {
				var e *RetriableError
				if errors.As(err, &e) {
					if err := t.Redirect(ctx, message); err != nil {
						return errors.Wrap(err, "failed to redirect message")
					}

					return nil
				}

				return err
			}

			return nil
		}
	}
}

// NewRetryMiddleware track message processing time.
func NewRetryMiddleware(et *ErrorTracker) Middleware {
	return func(next MessageHandleFunc) MessageHandleFunc {
		return func(ctx context.Context, message Message) error {
			//TODO: add limiter and backoff
			if err := next(ctx, message); err != nil {
				return err
			}

			if err := et.Free(ctx, message); err != nil {
				return err
			}

			return nil
		}
	}
}