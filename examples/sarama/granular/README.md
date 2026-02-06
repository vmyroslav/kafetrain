# Granular Example

Demonstrates **granular control** over retry operations. You manage both main and retry consumers yourself, calling `Redirect()` and `Free()` explicitly.

## When to Use

- Fine-grained control over the retry flow
- Custom consumer group management
- Custom retry logic beyond the standard pattern

## Prerequisites

- Go 1.25+
- Kafka running at `localhost:9092` (or set `KAFKA_BROKERS`)

```bash
docker-compose -f ../docker-compose.yml up -d
```

## Running

```bash
cd examples/sarama/granular
go run .
```

## Key Concepts

### StartCoordinator vs Start

```go
// Only starts the coordinator (no retry worker)
tracker.StartCoordinator(ctx, topic)

// Starts coordinator AND retry worker (see basic example)
tracker.Start(ctx, topic, handler)
```

### Explicit Redirect/Free

```go
// Redirect failed messages to retry topic
if err := tracker.Redirect(ctx, msg, err); err != nil {
    // handle redirect failure
}

// Release the lock after successful retry
if err := tracker.Free(ctx, msg); err != nil {
    // handle free failure
}
```

### IsInRetryChain

Check if a key is currently locked:

```go
if tracker.IsInRetryChain(ctx, msg) {
    // This key has pending retries - redirect to maintain order
    tracker.Redirect(ctx, msg, errors.New("predecessor in retry"))
}
```

See also: [basic](../basic/), [strict](../strict/).
