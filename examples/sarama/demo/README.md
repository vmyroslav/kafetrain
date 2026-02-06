# Interactive Demo

An HTTP-based demo to verify `kafka-resilience` features interactively.

## Features

- Send messages via HTTP
- Control which messages fail
- See locked keys in real-time
- Verify ordering guarantees

## Prerequisites

- Go 1.25+
- Docker (for Kafka)

## Quick Start

```bash
# Start Kafka
docker-compose -f ../docker-compose.yml up -d

# Run demo
cd examples/sarama/demo
go run .

# Server starts at http://localhost:8080
```

## Endpoints

### POST /produce

Send a message to Kafka.

```bash
# Send a message that will succeed
curl -X POST localhost:8080/produce \
  -H "Content-Type: application/json" \
  -d '{"key": "order-1", "value": "data"}'

# Send a message that will fail (triggers retry)
curl -X POST localhost:8080/produce \
  -H "Content-Type: application/json" \
  -d '{"key": "order-2", "value": "data", "fail": true}'
```

### GET /locks

See which keys are currently locked (in retry chain).

```bash
curl localhost:8080/locks
# {"locked_keys": ["order-2"], "pending_fails": []}
```

### GET /stats

Get processing statistics.

```bash
curl localhost:8080/stats
# {"total_processed": 5, "succeeded": 3, "failed": 2, "retried": 1, "last_values": {...}}
```

### POST /fail

Control which keys will fail on next attempt.

```bash
# Make order-3 fail on next processing
curl -X POST localhost:8080/fail \
  -H "Content-Type: application/json" \
  -d '{"key": "order-3", "fail": true}'

# Clear fail flag
curl -X POST localhost:8080/fail \
  -H "Content-Type: application/json" \
  -d '{"key": "order-3", "fail": false}'
```

## Verifying Ordering Guarantees

The key feature of `kafka-resilience` is that messages for a locked key are queued until the retry completes. Here's how to verify:

```bash
# 1. Send a message that fails (key gets locked)
curl -X POST localhost:8080/produce -d '{"key":"A","fail":true}'

# 2. Check that key A is now locked
curl localhost:8080/locks
# {"locked_keys": ["A"], ...}

# 3. Send another message for the same key
curl -X POST localhost:8080/produce -d '{"key":"A","value":"second"}'

# 4. Watch logs - you'll see:
#    "Key is in retry chain, redirecting to maintain order"
#    The second message is queued, not processed immediately

# 5. After retry completes (success or DLQ), both messages process in order
```

## Verifying DLQ

Messages that fail 3 times go to the DLQ:

```bash
# Send a message
curl -X POST localhost:8080/produce -d '{"key":"B","fail":true}'

# Set it to keep failing
curl -X POST localhost:8080/fail -d '{"key":"B","fail":true}'
curl -X POST localhost:8080/fail -d '{"key":"B","fail":true}'

# Watch logs - after 3 retries, message goes to DLQ
# "max retries exceeded, sending to DLQ"
```

See also: [basic](../basic/), [granular](../granular/), [strict](../strict/).
