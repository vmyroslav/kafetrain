package resilience

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const meterName = "github.com/vmyroslav/kafka-resilience"

// trackerInstruments holds OTel metric instruments used by ErrorTracker.
type trackerInstruments struct {
	retryRedirected metric.Int64Counter
	retryProcessed  metric.Int64Counter
	dlqEnqueued     metric.Int64Counter
	backoffWait     metric.Float64Histogram
}

func newTrackerInstruments(meter metric.Meter) *trackerInstruments {
	ins := &trackerInstruments{}

	ins.retryRedirected, _ = meter.Int64Counter(
		"kafka.resilience.retry.redirected",
		metric.WithDescription("Messages sent to the retry topic"),
		metric.WithUnit("{message}"),
	)

	ins.retryProcessed, _ = meter.Int64Counter(
		"kafka.resilience.retry.processed",
		metric.WithDescription("Retried messages that were processed successfully"),
		metric.WithUnit("{message}"),
	)

	ins.dlqEnqueued, _ = meter.Int64Counter(
		"kafka.resilience.dlq.enqueued",
		metric.WithDescription("Messages sent to the dead letter queue"),
		metric.WithUnit("{message}"),
	)

	ins.backoffWait, _ = meter.Float64Histogram(
		"kafka.resilience.backoff.wait",
		metric.WithDescription("Time spent waiting during retry backoff"),
		metric.WithUnit("s"),
	)

	return ins
}

// coordinatorInstruments holds OTel metric instruments used by KafkaStateCoordinator.
type coordinatorInstruments struct {
	lockAcquired    metric.Int64Counter
	lockReleased    metric.Int64Counter
	stateRestoreDur metric.Float64Histogram
}

func newCoordinatorInstruments(meter metric.Meter) *coordinatorInstruments {
	ins := &coordinatorInstruments{}

	ins.lockAcquired, _ = meter.Int64Counter(
		"kafka.resilience.lock.acquired",
		metric.WithDescription("Distributed lock acquisitions"),
		metric.WithUnit("{operation}"),
	)

	ins.lockReleased, _ = meter.Int64Counter(
		"kafka.resilience.lock.released",
		metric.WithDescription("Distributed lock releases"),
		metric.WithUnit("{operation}"),
	)

	ins.stateRestoreDur, _ = meter.Float64Histogram(
		"kafka.resilience.state_restore.duration",
		metric.WithDescription("Time taken to restore coordinator state on startup"),
		metric.WithUnit("s"),
	)

	return ins
}

func resolveMeter(meter metric.Meter) metric.Meter {
	if meter == nil {
		return otel.Meter(meterName)
	}

	return meter
}

// NoopMeter returns a no-op meter that discards all metrics.
// Use this to explicitly disable metrics even when a global OTel SDK is registered.
func NoopMeter() metric.Meter {
	return noop.Meter{}
}
