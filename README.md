# kafetrain

**Ordered retry mechanism for Kafka consumers** that works with your existing code.

Kafetrain provides a sophisticated 3-topic retry mechanism that maintains **FIFO ordering guarantees** during retries - the hard part of Kafka error handling. The retry logic is completely decoupled from message consumption, so you can integrate it into any consumer pattern.

## Why Kafetrain?

**The Problem:** Kafka doesn't natively support message retries while maintaining ordering. If a message fails, you can't just "retry it later" without blocking the partition or losing FIFO guarantees.

**The Solution:** Kafetrain implements a chain-based retry mechanism:
- Failed messages are redirected to retry topics with backoff delays
- Message keys are tracked to prevent duplicate processing
- Original partition ordering is maintained through key-based routing
- Automatic DLQ (Dead Letter Queue) handling after max retries
- Works with **any** Sarama consumer implementation

## Features

- ✅ **Ordered retries** - Maintains FIFO ordering during retry chains
- ✅ **Configurable backoff** - Exponential backoff with jitter
- ✅ **DLQ support** - Automatic dead letter queue after max retries
- ✅ **Chain tracking** - Prevents processing subsequent messages until retries complete
- ✅ **Standalone architecture** - Works with your existing consumer code
- ✅ **Zero vendor lock-in** - Optional Consumer wrapper, not required
- ✅ **Pure Sarama API** - No forced abstractions or custom types

## Quick Start

Kafetrain provides **three layers** - choose based on your needs:

### Layer 1: Minimal Standalone (Zero Abstractions)

**Best for:** Maximum control, existing Sarama infrastructure, large codebases

You run BOTH main and retry consumers with the SAME handler:

```go
package main

import (
    "context"
    "github.com/IBM/sarama"
    "github.com/vmyroslav/kafetrain/resilience"
)

func main() {
    // Create ErrorTracker - only manages tracking
    tracker, _ := resilience.NewErrorTracker(cfg, logger, resilience.NewKeyTracker())
    tracker.StartTracking(ctx, "orders")  // Only starts redirect consumer

    // YOU create and manage BOTH consumers
    mainConsumer, _ := sarama.NewConsumerGroup(brokers, groupID, config)
    retryConsumer, _ := sarama.NewConsumerGroup(brokers, groupID+"-retry", config)

    handler := &YourHandler{tracker: tracker}

    // Same handler for BOTH consumers (DRY!)
    go mainConsumer.Consume(ctx, []string{"orders"}, handler)
    go retryConsumer.Consume(ctx, []string{tracker.GetRetryTopic("orders")}, handler)
}

type YourHandler struct {
    tracker *resilience.ErrorTracker
}

func (h *YourHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        if h.tracker.IsInRetryChain(msg) {
            // Side-line: Immediately redirect to retry topic to maintain order without blocking partition
            // We pass nil as error since this is just a re-routing, not a processing failure
            h.tracker.Redirect(ctx, msg, nil)
            session.MarkMessage(msg, "")
            continue
        }

        if err := processMessage(msg); err != nil {
            if resilience.IsRetriable(err) {
                h.tracker.Redirect(ctx, msg, err)
                session.MarkMessage(msg, "")
                continue
            }
            return err
        }

        if resilience.IsRetryMessage(msg) {
            h.tracker.Free(ctx, msg)  // Release from tracking
        }

        session.MarkMessage(msg, "")
    }
    return nil
}
```

See [examples/standalone](examples/standalone/) for complete example.

### Layer 2: Managed Retry (Convenience)

**Best for:** Want retry managed but keep control of main consumer

```go
package main

import (
    "context"
    "github.com/IBM/sarama"
    "github.com/vmyroslav/kafetrain/resilience"
)

func processOrder(ctx context.Context, msg *sarama.ConsumerMessage) error {
    // Your business logic
    var order Order
    json.Unmarshal(msg.Value, &order)

    if err := chargePayment(order); err != nil {
        return resilience.RetriableError{Retry: true, Origin: err}
    }
    return nil
}

func main() {
    // Layer 1: Create tracker
    tracker, _ := resilience.NewErrorTracker(cfg, logger, resilience.NewKeyTracker())
    tracker.StartTracking(ctx, "orders")

    // Layer 2: Retry manager handles retry consumer
    retryMgr := resilience.NewRetryManagerWithSaramaHandler(tracker, processOrder)
    retryMgr.StartRetryConsumer(ctx, "orders")

    // You manage main consumer with YOUR Sarama code
    mainConsumer, _ := sarama.NewConsumerGroup(brokers, groupID, config)
    handler := &YourHandler{tracker: tracker, processOrder: processOrder}
    mainConsumer.Consume(ctx, []string{"orders"}, handler)
}
```

### Layer 3: Full Wrapper (Maximum Convenience)

**Best for:** New projects, simple use cases, prefer abstraction over control

```go
package main

import (
    "context"
    "github.com/vmyroslav/kafetrain/resilience"
)

func main() {
    // Layer 1: Create tracker
    tracker, _ := resilience.NewErrorTracker(cfg, logger, resilience.NewKeyTracker())
    tracker.StartTracking(ctx, "orders")

    // Layer 2: Managed retry
    handler := NewOrderHandler(logger)
    retryMgr := resilience.NewRetryManager(tracker, handler)
    retryMgr.StartRetryConsumer(ctx, "orders")

    // Layer 3: Wrapper with middlewares
    consumer, _ := resilience.NewKafkaConsumer(cfg, logger)
    consumer.WithMiddlewares(
        resilience.NewLoggingMiddleware(logger),
        resilience.NewErrorHandlingMiddleware(tracker),
    )

    consumer.Consume(ctx, "orders", handler)
}
```

See [examples/error_tracker](examples/error_tracker/) for complete example.

## How It Works

### 3-Topic Retry Mechanism

```
Primary Topic (e.g., "orders")
    ↓ [Consumer processes message]
    ↓ [Handler returns RetriableError]
    ↓
ErrorTracker.Redirect() publishes to TWO topics:
    ├→ Retry Topic ("retry-orders"): Full message for reprocessing
    └→ Redirect Topic ("redirect-orders"): Tracking record (key → message ID)
         Uses compact cleanup policy for state management
    ↓
[Retry Consumer] processes retry topic
    ↓ [Success] → Free() publishes tombstone to redirect topic
    ↓ [Max retries] → SendToDLQ()
    ↓
[Redirect Consumer] processes tombstone → removes from lock map
```

### Chain Tracking

- Messages with the same key are tracked together
- If message A fails and is retried, message B with the same key is blocked
- When A succeeds or goes to DLQ, B is unblocked
- Maintains strict FIFO ordering for each key

## Configuration

```go
cfg := &resilience.Config{
    Brokers:             []string{"localhost:9092"},
    Version:             "4.1.0",
    GroupID:             "my-consumer",
    MaxRetries:          5,                // Max retry attempts
    RetryTopicPrefix:    "retry",          // Retry topic naming
    RedirectTopicPrefix: "redirect",       // Tracking topic naming
    DLQTopicPrefix:      "dlq",            // Dead letter queue naming
    FreeOnDLQ:           false,            // Free from tracking after DLQ
}
```

### DLQ Behavior

- `FreeOnDLQ: false` (default): Messages remain in tracking after DLQ, blocking subsequent messages
  - Use for strict FIFO ordering guarantees
  - Trade-off: Keys with failed messages may be permanently blocked

- `FreeOnDLQ: true`: Messages are freed from tracking after DLQ
  - Use when you want to continue processing despite failures
  - Trade-off: May violate FIFO ordering if order is critical

## API Reference

### Layer 1: ErrorTracker (Core Tracking API)

```go
// Create tracker (pure tracking - no retry consumer)
tracker, err := resilience.NewErrorTracker(cfg, logger, keyTracker)
// With custom backoff:
tracker, err := resilience.NewErrorTrackerWithBackoff(cfg, logger, keyTracker, backoff)

// Start tracking (only starts redirect consumer)
err := tracker.StartTracking(ctx, "topic-name")

// Get retry topic name (for manual consumption)
retryTopic := tracker.GetRetryTopic("topic-name")

// Redirect failed message to retry topic
err := tracker.Redirect(ctx, *sarama.ConsumerMessage, error)

// Free successfully processed message from tracking
err := tracker.Free(ctx, *sarama.ConsumerMessage)

// Check if message is in retry chain
inChain := tracker.IsInRetryChain(*sarama.ConsumerMessage)
```

### Layer 2: RetryManager (Managed Retry API)

```go
// Create retry manager with MessageHandler
retryMgr := resilience.NewRetryManager(tracker, handler)

// Create retry manager with Sarama handler function (recommended)
retryMgr := resilience.NewRetryManagerWithSaramaHandler(tracker,
    func(ctx context.Context, msg *sarama.ConsumerMessage) error {
        // Your business logic
        return processMessage(ctx, msg)
    })

// Start retry consumer (manages retry consumer lifecycle)
err := retryMgr.StartRetryConsumer(ctx, "topic-name")
```

### Helper Functions

```go
// Check if error is retriable
if resilience.IsRetriable(err) { ... }

// Get retry attempt count
attempt := resilience.GetRetryAttempt(msg)

// Check if message is from retry topic
if resilience.IsRetryMessage(msg) { ... }

// Get original topic name
originalTopic := resilience.GetOriginalTopic(msg)

// Get retry reason
reason := resilience.GetRetryReason(msg)
```

## When to Use Each Layer

### Use Layer 1 (Minimal Standalone) when:
- ✅ You have existing Sarama consumer infrastructure
- ✅ You need maximum control and transparency
- ✅ You're integrating into a large existing codebase
- ✅ You prefer explicit over implicit
- ✅ You're using other libraries/frameworks (Watermill, franz-go, etc.)
- ✅ You want zero forced abstractions

### Use Layer 2 (Managed Retry) when:
- ✅ You want retry handling but keep control of main consumer
- ✅ You have custom consumption patterns
- ✅ You need flexibility with some convenience
- ✅ You're gradually adopting kafetrain

### Use Layer 3 (Full Wrapper) when:
- ✅ Starting a new project
- ✅ Simple, straightforward use cases
- ✅ You prefer convenience over control
- ✅ You like middleware composition patterns

## Examples

- [standalone/](examples/standalone/) - ErrorTracker with raw Sarama consumer (recommended)
- [simple_consumer/](examples/simple_consumer/) - Using Consumer wrapper
- [error_tracker/](examples/error_tracker/) - Full error tracking setup
- [stream/](examples/stream/) - Channel-based streaming consumption

## Architecture

See [decoupled-architecture.md](decoupled-architecture.md) for detailed architecture documentation.

Key design principles:
1. **ErrorTracker is standalone** - Works with any consumer
2. **Consumer is optional** - Convenience wrapper, not required
3. **Zero forced abstractions** - Users work with Sarama types
4. **Single responsibility** - Library does retries, not consumption

## Installation

```bash
go get github.com/vmyroslav/kafetrain
```

## Requirements

- Go 1.21+
- Kafka 2.0+
- IBM Sarama v1.33+

## Contributing

See [CLAUDE.md](CLAUDE.md) for development guidelines and architecture details.

## License

MIT
