# Basic Example

This is the **default** way to use `kafka-resilience`. The library automatically manages the retry worker and handles all Redirect/Free operations internally.

## When to Use

Use this pattern when:
- You want minimal setup and configuration
- Standard retry behavior with automatic ordering is sufficient
- You don't need fine-grained control over retry/free operations

## Prerequisites

- Go 1.25+
- Kafka running at `localhost:9092` (or set `KAFKA_BROKERS`)

Start Kafka locally:
```bash
docker-compose -f ../docker-compose.yml up -d
```

## Running

```bash
cd examples/sarama/basic
go run .

# Or with custom broker:
KAFKA_BROKERS="localhost:9094" go run .
```

## What Happens

1. Connects to Kafka and creates topics automatically
2. Starts the resilience tracker with automatic retry worker
3. Produces 3 test messages:
   - `order-1`: Succeeds immediately
   - `order-2`: Fails repeatedly until max retries (goes to DLQ)
   - `order-3`: Succeeds immediately
4. Shows retry delays and state changes in logs

## Key Code

```go
// One-line setup
tracker, _ := saramaadapter.NewResilienceTracker(cfg, client)

// Start tracker (coordinator + retry worker)
tracker.Start(ctx, topic, handler)

// Wrap your handler for automatic ordering
resilientHandler := tracker.NewResilientHandler(handler)
consumer.Consume(ctx, []string{topic}, resilientHandler)
```

See also: [manual](../manual/) for more granular control, [strict](../strict/) for strict consistency.
