# Strict Example

Demonstrates **strict ordering guarantees** during consumer group rebalancing using `Synchronize()`.

## When to Use

- Absolute ordering guarantees during rebalancing
- Cannot tolerate any race conditions
- Willing to trade latency during rebalancing for stronger consistency

## The Problem

By default, `kafka-resilience` is eventually consistent during rebalancing. There's a tiny race window where a consumer might process a message before fully syncing lock state from the redirect topic.

## The Solution

Call `Synchronize()` in your consumer's `Setup` handler:

```go
func (h *Handler) Setup(session sarama.ConsumerGroupSession) error {
    return h.tracker.Synchronize(session.Context())
}
```

## Prerequisites

- Go 1.25+
- Kafka running at `localhost:9092` (or set `KAFKA_BROKERS`)

```bash
docker-compose -f ../docker-compose.yml up -d
```

## Running

```bash
cd examples/sarama/strict
go run .
```

## Trade-offs

| Aspect | Default | With Synchronize |
|--------|---------|------------------|
| Consistency | Eventually consistent | Strictly consistent |
| Rebalance latency | Minimal | Higher (waits for sync) |
| Race conditions | Rare, small window | None |

See also: [basic](../basic/), [granular](../granular/).
