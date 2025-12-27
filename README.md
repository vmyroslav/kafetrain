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

### Standalone Usage (Recommended)

Use ErrorTracker with your existing Sarama consumer:

```go
package main

import (
    "context"
    "github.com/IBM/sarama"
    "github.com/vmyroslav/kafetrain/resilience"
)

func main() {
    // 1. Create ErrorTracker
    tracker, _ := resilience.NewTracker(cfg, logger, resilience.NewKeyTracker(), nil)

    // 2. Start retry consumers
    go tracker.StartRetryConsumers(context.Background(), "orders")

    // 3. Use your own Sarama consumer
    consumerGroup, _ := sarama.NewConsumerGroup(brokers, groupID, config)
    handler := &YourHandler{tracker: tracker}
    consumerGroup.Consume(ctx, []string{"orders"}, handler)
}

// Your existing consumer handler
type YourHandler struct {
    tracker *resilience.ErrorTracker
}

func (h *YourHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        // Skip if in retry chain
        if h.tracker.IsInRetryChain(msg) {
            continue
        }

        // Process message
        if err := processMessage(msg); err != nil {
            if resilience.IsRetriable(err) {
                h.tracker.Redirect(ctx, msg, err)  // Send to retry topic
                continue
            }
            return err  // Non-retriable error
        }

        // Success - free from tracking if retry message
        if resilience.IsRetryMessage(msg) {
            h.tracker.Free(ctx, msg)
        }

        session.MarkMessage(msg, "")
    }
    return nil
}
```

See [examples/standalone](examples/standalone/) for complete example.

### Consumer Wrapper Usage (Optional)

For simpler cases, use the optional Consumer wrapper:

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
        return resilience.RetriableError{
            Retry: true,
            Origin: err,
        }
    }

    return nil
}

func main() {
    tracker, _ := resilience.NewTracker(cfg, logger, resilience.NewKeyTracker(), nil)
    go tracker.StartRetryConsumers(context.Background(), "orders")

    // Optional Consumer wrapper handles Redirect/Free automatically
    consumer, _ := resilience.NewKafkaConsumer(cfg, logger)
    consumer.WithMiddlewares(
        resilience.NewLoggingMiddleware(logger),
        resilience.NewErrorHandlingMiddleware(tracker),
    )

    consumer.Consume(ctx, "orders", resilience.MessageHandleFunc(processOrder))
}
```

See [examples/simple_consumer](examples/simple_consumer/) for complete example.

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

### ErrorTracker (Core API)

```go
// Create tracker
tracker, err := resilience.NewTracker(cfg, logger, keyTracker, backoffStrategy)

// Start retry consumers (independent of main consumption)
err := tracker.StartRetryConsumers(ctx, "topic-name")

// Redirect failed message to retry topic
err := tracker.Redirect(ctx, *sarama.ConsumerMessage, error)

// Free successfully processed message from tracking
err := tracker.Free(ctx, *sarama.ConsumerMessage)

// Check if message is in retry chain
inChain := tracker.IsInRetryChain(*sarama.ConsumerMessage)
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

## When to Use Each Approach

### Use Standalone ErrorTracker when:
- ✅ You have existing Sarama consumer code
- ✅ You want full control over consumption logic
- ✅ You need to integrate into large existing codebase
- ✅ Your team prefers explicit control over abstraction
- ✅ You're using other libraries/frameworks (Watermill, etc.)

### Use Consumer Wrapper when:
- ✅ Starting a new project
- ✅ You want convenience over control
- ✅ Simple, straightforward use cases
- ✅ You prefer middleware composition

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
