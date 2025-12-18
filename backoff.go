package kafetrain

import (
	"fmt"
	"math"
	"time"
)

// BackoffStrategy defines how to calculate delays between retry attempts
type BackoffStrategy interface {
	// NextDelay calculates the delay duration for the given attempt number (0-indexed)
	NextDelay(attempt int) time.Duration
}

// ExponentialBackoff implements exponential backoff with configurable parameters
type ExponentialBackoff struct {
	initialDelay time.Duration // Initial delay (e.g., 1s)
	maxDelay     time.Duration // Maximum delay cap (e.g., 5m)
	multiplier   float64       // Multiplier for each attempt (e.g., 2.0)
}

// NewExponentialBackoff creates a new ExponentialBackoff with defaults
func NewExponentialBackoff() *ExponentialBackoff {
	backoff, _ := NewExponentialBackoffWithConfig(1*time.Second, 5*time.Minute, 2.0)

	return backoff
}

// NewExponentialBackoffWithConfig creates a new ExponentialBackoff with custom parameters.
// Returns an error if parameters are invalid.
func NewExponentialBackoffWithConfig(
	initialDelay, maxDelay time.Duration,
	multiplier float64,
) (*ExponentialBackoff, error) {
	if initialDelay <= 0 {
		return nil, fmt.Errorf("initialDelay must be > 0, got %v", initialDelay)
	}

	if maxDelay < initialDelay {
		return nil, fmt.Errorf("maxDelay (%v) must be >= initialDelay (%v)", maxDelay, initialDelay)
	}

	if multiplier < 1.0 {
		return nil, fmt.Errorf("multiplier must be >= 1.0, got %v", multiplier)
	}

	return &ExponentialBackoff{
		initialDelay: initialDelay,
		maxDelay:     maxDelay,
		multiplier:   multiplier,
	}, nil
}

// NextDelay calculates the next delay using exponential backoff formula
func (e *ExponentialBackoff) NextDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := float64(e.initialDelay) * math.Pow(e.multiplier, float64(attempt))

	// cap at maxDelay
	if delay > float64(e.maxDelay) {
		return e.maxDelay
	}

	return time.Duration(delay)
}

// ConstantBackoff implements a constant delay between retries
type ConstantBackoff struct {
	delay time.Duration
}

// NewConstantBackoff creates a new ConstantBackoff with the specified delay.
func NewConstantBackoff(delay time.Duration) (*ConstantBackoff, error) {
	if delay <= 0 {
		return nil, fmt.Errorf("delay must be > 0, got %v", delay)
	}

	return &ConstantBackoff{delay: delay}, nil
}

// NextDelay returns a constant delay regardless of attempt number
func (c *ConstantBackoff) NextDelay(_ int) time.Duration {
	return c.delay
}

// LinearBackoff implements linear backoff
type LinearBackoff struct {
	initialDelay time.Duration // Initial delay
	increment    time.Duration // Amount to add per attempt
	maxDelay     time.Duration // Maximum delay cap
}

// NewLinearBackoff creates a new LinearBackoff with sensible defaults
func NewLinearBackoff() *LinearBackoff {
	backoff, _ := NewLinearBackoffWithConfig(1*time.Second, 1*time.Second, 1*time.Minute)

	return backoff
}

// NewLinearBackoffWithConfig creates a new LinearBackoff with custom parameters.
// Returns an error if parameters are invalid.
func NewLinearBackoffWithConfig(initialDelay, increment, maxDelay time.Duration) (*LinearBackoff, error) {
	if initialDelay <= 0 {
		return nil, fmt.Errorf("initialDelay must be > 0, got %v", initialDelay)
	}

	if increment < 0 {
		return nil, fmt.Errorf("increment must be >= 0, got %v", increment)
	}

	if maxDelay < initialDelay {
		return nil, fmt.Errorf("maxDelay (%v) must be >= initialDelay (%v)", maxDelay, initialDelay)
	}

	return &LinearBackoff{
		initialDelay: initialDelay,
		increment:    increment,
		maxDelay:     maxDelay,
	}, nil
}

// NextDelay calculates the next delay using linear formula
func (l *LinearBackoff) NextDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := l.initialDelay + (l.increment * time.Duration(attempt))

	if delay > l.maxDelay {
		return l.maxDelay
	}

	return delay
}
