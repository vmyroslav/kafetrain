package resilience

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExponentialBackoff_Default(t *testing.T) {
	t.Parallel()

	backoff := NewExponentialBackoff()

	// test default behavior
	assert.Equal(t, 1*time.Second, backoff.NextDelay(0), "first delay should be 1s")
	assert.Equal(t, 2*time.Second, backoff.NextDelay(1), "second delay should be 2s")
	assert.Equal(t, 4*time.Second, backoff.NextDelay(2), "third delay should be 4s")
}

func TestExponentialBackoff_NextDelay(t *testing.T) {
	t.Parallel()

	backoff, err := NewExponentialBackoffWithConfig(1*time.Second, 1*time.Minute, 2.0)
	require.NoError(t, err)

	testCases := []struct {
		attempt  int
		expected time.Duration
		name     string
	}{
		{0, 1 * time.Second, "attempt 0 (first failure)"},
		{1, 2 * time.Second, "attempt 1"},
		{2, 4 * time.Second, "attempt 2"},
		{3, 8 * time.Second, "attempt 3"},
		{4, 16 * time.Second, "attempt 4"},
		{5, 32 * time.Second, "attempt 5"},
		{6, 1 * time.Minute, "attempt 6 (capped at max)"},
		{10, 1 * time.Minute, "attempt 10 (still capped)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			delay := backoff.NextDelay(tc.attempt)
			assert.Equal(t, tc.expected, delay, "delay for attempt %d", tc.attempt)
		})
	}
}

func TestExponentialBackoff_NegativeAttempt(t *testing.T) {
	t.Parallel()

	backoff := NewExponentialBackoff()

	delay := backoff.NextDelay(-1)
	assert.Equal(t, 1*time.Second, delay, "negative attempt should use initial delay")

	delay = backoff.NextDelay(-10)
	assert.Equal(t, 1*time.Second, delay, "negative attempt should use initial delay")
}

func TestExponentialBackoff_MaxDelayCap(t *testing.T) {
	t.Parallel()

	backoff, err := NewExponentialBackoffWithConfig(1*time.Second, 10*time.Second, 2.0)
	require.NoError(t, err)

	// After 4 attempts: 1s, 2s, 4s, 8s, 16s (but capped at 10s)
	delay := backoff.NextDelay(4)
	assert.Equal(t, 10*time.Second, delay, "delay should be capped at MaxDelay")

	delay = backoff.NextDelay(100)
	assert.Equal(t, 10*time.Second, delay, "delay should still be capped at MaxDelay")
}

func TestExponentialBackoff_CustomMultiplier(t *testing.T) {
	t.Parallel()

	backoff, err := NewExponentialBackoffWithConfig(100*time.Millisecond, 1*time.Hour, 3.0) // Triple each time
	require.NoError(t, err)

	testCases := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},  // 100ms * 3^0 = 100ms
		{1, 300 * time.Millisecond},  // 100ms * 3^1 = 300ms
		{2, 900 * time.Millisecond},  // 100ms * 3^2 = 900ms
		{3, 2700 * time.Millisecond}, // 100ms * 3^3 = 2700ms
	}

	for _, tc := range testCases {
		delay := backoff.NextDelay(tc.attempt)
		assert.Equal(t, tc.expected, delay, "delay for attempt %d with multiplier 3.0", tc.attempt)
	}
}

func TestConstantBackoff(t *testing.T) {
	t.Parallel()

	delay := 5 * time.Second
	backoff, err := NewConstantBackoff(delay)
	require.NoError(t, err)

	// All attempts should return the same delay
	for i := 0; i < 10; i++ {
		result := backoff.NextDelay(i)
		assert.Equal(t, delay, result, "constant backoff should always return same delay")
	}
}

func TestConstantBackoff_NegativeAttempt(t *testing.T) {
	t.Parallel()

	backoff, err := NewConstantBackoff(3 * time.Second)
	require.NoError(t, err)

	delay := backoff.NextDelay(-1)
	assert.Equal(t, 3*time.Second, delay, "negative attempt should still return constant delay")
}

func TestLinearBackoff_Default(t *testing.T) {
	t.Parallel()

	backoff := NewLinearBackoff()

	// Test default behavior by checking actual delays
	assert.Equal(t, 1*time.Second, backoff.NextDelay(0), "first delay should be 1s")
	assert.Equal(t, 2*time.Second, backoff.NextDelay(1), "second delay should be 2s (1s initial + 1s increment)")
	assert.Equal(t, 3*time.Second, backoff.NextDelay(2), "third delay should be 3s")
}

func TestLinearBackoff_NextDelay(t *testing.T) {
	t.Parallel()

	backoff, err := NewLinearBackoffWithConfig(1*time.Second, 1*time.Second, 5*time.Second)
	require.NoError(t, err)

	testCases := []struct {
		attempt  int
		expected time.Duration
		name     string
	}{
		{0, 1 * time.Second, "attempt 0 (initial)"},
		{1, 2 * time.Second, "attempt 1 (initial + 1 * increment)"},
		{2, 3 * time.Second, "attempt 2 (initial + 2 * increment)"},
		{3, 4 * time.Second, "attempt 3 (initial + 3 * increment)"},
		{4, 5 * time.Second, "attempt 4 (capped at max)"},
		{5, 5 * time.Second, "attempt 5 (still capped)"},
		{10, 5 * time.Second, "attempt 10 (still capped)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			delay := backoff.NextDelay(tc.attempt)
			assert.Equal(t, tc.expected, delay, "delay for attempt %d", tc.attempt)
		})
	}
}

func TestLinearBackoff_NegativeAttempt(t *testing.T) {
	t.Parallel()

	backoff, err := NewLinearBackoffWithConfig(2*time.Second, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	delay := backoff.NextDelay(-1)
	assert.Equal(t, 2*time.Second, delay, "negative attempt should use initial delay")
}

func TestLinearBackoff_SmallIncrement(t *testing.T) {
	t.Parallel()

	backoff, err := NewLinearBackoffWithConfig(100*time.Millisecond, 50*time.Millisecond, 1*time.Second)
	require.NoError(t, err)

	testCases := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 150 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{5, 350 * time.Millisecond},
		{10, 600 * time.Millisecond},
		{20, 1 * time.Second}, // Capped
	}

	for _, tc := range testCases {
		delay := backoff.NextDelay(tc.attempt)
		assert.Equal(t, tc.expected, delay, "delay for attempt %d", tc.attempt)
	}
}

func TestBackoffStrategy_Interface(t *testing.T) {
	t.Parallel()

	// Verify all backoff types implement BackoffStrategy interface
	var (
		_ BackoffStrategy = (*ExponentialBackoff)(nil)
		_ BackoffStrategy = (*ConstantBackoff)(nil)
		_ BackoffStrategy = (*LinearBackoff)(nil)
	)
}

func TestExponentialBackoff_RealisticScenario(t *testing.T) {
	t.Parallel()
	// Simulate a realistic retry scenario with default settings
	backoff := NewExponentialBackoff()
	maxDelay := 5 * time.Minute // Default max delay

	var totalWaitTime time.Duration

	maxAttempts := 10

	for attempt := 0; attempt < maxAttempts; attempt++ {
		delay := backoff.NextDelay(attempt)
		totalWaitTime += delay
		t.Logf("Attempt %d: delay=%v, total wait=%v", attempt, delay, totalWaitTime)

		// Verify delay never exceeds max
		require.LessOrEqual(t, delay, maxDelay, "delay should never exceed MaxDelay")

		// Verify delay is reasonable (not negative or zero)
		require.Greater(t, delay, time.Duration(0), "delay should be positive")
	}

	// After 10 attempts with exponential backoff, we should have waited a significant time
	// Attempts: 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 300s (capped)
	// But attempts 7+ are capped at 5m = 300s
	// So: 1+2+4+8+16+32+64+128+256+300 = 811s
	t.Logf("Total wait time after %d attempts: %v", maxAttempts, totalWaitTime)
}

func TestConstantBackoff_ConsistentDelays(t *testing.T) {
	t.Parallel()

	backoff, err := NewConstantBackoff(2 * time.Second)
	require.NoError(t, err)

	// Verify that all delays are exactly the same
	previousDelay := backoff.NextDelay(0)
	for attempt := 1; attempt < 100; attempt++ {
		delay := backoff.NextDelay(attempt)
		assert.Equal(t, previousDelay, delay, "all delays should be equal")
		previousDelay = delay
	}
}

func TestLinearBackoff_GrowthRate(t *testing.T) {
	t.Parallel()

	backoff, err := NewLinearBackoffWithConfig(1*time.Second, 1*time.Second, 100*time.Second) // High max so we don't hit it
	require.NoError(t, err)

	// Verify linear growth
	for attempt := 0; attempt < 10; attempt++ {
		delay := backoff.NextDelay(attempt)
		expected := 1*time.Second + (1 * time.Second * time.Duration(attempt))
		assert.Equal(t, expected, delay, "delay should grow linearly")
	}
}

func TestBackoffStrategies_Comparison(t *testing.T) {
	t.Parallel()
	// Compare how different strategies behave over 5 attempts
	exponential := NewExponentialBackoff()
	constant, err := NewConstantBackoff(5 * time.Second)
	require.NoError(t, err)
	linear, err := NewLinearBackoffWithConfig(1*time.Second, 2*time.Second, 1*time.Hour)
	require.NoError(t, err)

	t.Log("\nComparison of backoff strategies:")
	t.Log("Attempt | Exponential | Constant | Linear")
	t.Log("--------|-------------|----------|-------")

	for attempt := 0; attempt < 6; attempt++ {
		expDelay := exponential.NextDelay(attempt)
		constDelay := constant.NextDelay(attempt)
		linDelay := linear.NextDelay(attempt)

		t.Logf("   %d    |   %6s    |  %6s  | %6s",
			attempt,
			expDelay.String(),
			constDelay.String(),
			linDelay.String(),
		)
	}
}

func TestExponentialBackoff_ZeroMultiplier(t *testing.T) {
	t.Parallel()
	// Edge case: multiplier of 1.0 means no growth
	backoff, err := NewExponentialBackoffWithConfig(1*time.Second, 10*time.Second, 1.0)
	require.NoError(t, err)

	// With multiplier 1.0, delay should always be InitialDelay
	for attempt := 0; attempt < 5; attempt++ {
		delay := backoff.NextDelay(attempt)
		assert.Equal(t, 1*time.Second, delay, "with multiplier 1.0, delay should not grow")
	}
}

func TestLinearBackoff_ZeroIncrement(t *testing.T) {
	t.Parallel()
	// Edge case: zero increment means constant delay
	backoff, err := NewLinearBackoffWithConfig(5*time.Second, 0, 10*time.Second)
	require.NoError(t, err)

	// With zero increment, delay should always be InitialDelay
	for attempt := 0; attempt < 5; attempt++ {
		delay := backoff.NextDelay(attempt)
		assert.Equal(t, 5*time.Second, delay, "with zero increment, delay should not grow")
	}
}

// Validation tests

func TestExponentialBackoff_InvalidInitialDelay(t *testing.T) {
	t.Parallel()

	_, err := NewExponentialBackoffWithConfig(0, 1*time.Second, 2.0)
	require.Error(t, err, "should return error when initialDelay is 0")
	assert.Contains(t, err.Error(), "initialDelay must be > 0")

	_, err = NewExponentialBackoffWithConfig(-1*time.Second, 1*time.Second, 2.0)
	require.Error(t, err, "should return error when initialDelay is negative")
	assert.Contains(t, err.Error(), "initialDelay must be > 0")
}

func TestExponentialBackoff_InvalidMaxDelay(t *testing.T) {
	t.Parallel()

	_, err := NewExponentialBackoffWithConfig(10*time.Second, 5*time.Second, 2.0)
	require.Error(t, err, "should return error when maxDelay < initialDelay")
	assert.Contains(t, err.Error(), "maxDelay")
	assert.Contains(t, err.Error(), "initialDelay")
}

func TestExponentialBackoff_InvalidMultiplier(t *testing.T) {
	t.Parallel()

	_, err := NewExponentialBackoffWithConfig(1*time.Second, 5*time.Second, 0.5)
	require.Error(t, err, "should return error when multiplier < 1.0")
	assert.Contains(t, err.Error(), "multiplier must be >= 1.0")

	_, err = NewExponentialBackoffWithConfig(1*time.Second, 5*time.Second, 0)
	require.Error(t, err, "should return error when multiplier is 0")
	assert.Contains(t, err.Error(), "multiplier must be >= 1.0")
}

func TestConstantBackoff_InvalidDelay(t *testing.T) {
	t.Parallel()

	_, err := NewConstantBackoff(0)
	require.Error(t, err, "should return error when delay is 0")
	assert.Contains(t, err.Error(), "delay must be > 0")

	_, err = NewConstantBackoff(-1 * time.Second)
	require.Error(t, err, "should return error when delay is negative")
	assert.Contains(t, err.Error(), "delay must be > 0")
}

func TestLinearBackoff_InvalidInitialDelay(t *testing.T) {
	t.Parallel()

	_, err := NewLinearBackoffWithConfig(0, 1*time.Second, 5*time.Second)
	require.Error(t, err, "should return error when initialDelay is 0")
	assert.Contains(t, err.Error(), "initialDelay must be > 0")

	_, err = NewLinearBackoffWithConfig(-1*time.Second, 1*time.Second, 5*time.Second)
	require.Error(t, err, "should return error when initialDelay is negative")
	assert.Contains(t, err.Error(), "initialDelay must be > 0")
}

func TestLinearBackoff_InvalidIncrement(t *testing.T) {
	t.Parallel()

	_, err := NewLinearBackoffWithConfig(1*time.Second, -1*time.Second, 5*time.Second)
	require.Error(t, err, "should return error when increment is negative")
	assert.Contains(t, err.Error(), "increment must be >= 0")
}

func TestLinearBackoff_InvalidMaxDelay(t *testing.T) {
	t.Parallel()

	_, err := NewLinearBackoffWithConfig(10*time.Second, 1*time.Second, 5*time.Second)
	require.Error(t, err, "should return error when maxDelay < initialDelay")
	assert.Contains(t, err.Error(), "maxDelay")
	assert.Contains(t, err.Error(), "initialDelay")
}
