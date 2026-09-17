# RabbitMQ

A small, production-oriented Go client for [RabbitMQ](https://www.rabbitmq.com/), built on top of
[`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) — the official, maintained
successor of the archived `streadway/amqp`.

![Go](https://img.shields.io/badge/go-1.23+-blue)
![CI](https://github.com/kazarus/RabbitMQ/actions/workflows/ci.yml/badge.svg)

## Why this rewrite

This repository started as a tutorial-style wrapper (`TRabbitMQ`) that worked for demos but was
unsafe to ship. The rewrite fixes every issue found in the evaluation:

| Legacy problem | Fix in this version |
| --- | --- |
| `log.Fatalf` + `panic` on connection errors — a library can kill your process | Every failure is **returned as an error**; the caller decides |
| `NewRabbitMQSimple` passed an empty URL and crashed instead of reporting it | `NewSimple` validates the URL and fails with a descriptive error |
| Depended on archived `streadway/amqp` | Uses maintained `rabbitmq/amqp091-go` (same API) |
| Imported `KzPack4Go` only for a string ternary | Dependency removed; zero non-AMQP dependencies |
| Errors swallowed with `fmt.Println(err)` and execution continued | All errors propagate; publish failures are never silent |
| `ConsumeMQ(queue)` declared one queue but consumed from another | Single `Consume(ctx, queue, handler)` with correct semantics |
| Consumers blocked forever on `<-forever`; could not be stopped | `Consume` is context-aware and stops cleanly; `Close()` wakes consumers |
| No reconnection — any broker blip killed the client | Automatic reconnect with exponential backoff (default on) |
| No `go.mod`, no tests, no CI, empty README | Proper module, unit + integration tests, GitHub Actions, this README |
| `Destory` typo, `TRabbitMQ` naming, channel use unsynchronized | Idiomatic `Client` API, safe for concurrent use |

## Features

- **Never kills your process** — all errors are returned, `New` fails fast instead of `log.Fatal`-ing.
- **Context-aware** — publishes and consumers are cancellable; pass a `context` with a deadline.
- **Automatic reconnection** with exponential backoff (100 ms → 5 s, `WithReconnect`).
- **Concurrency-safe** — channel operations are serialized internally; one client per process is fine.
- **Simple mode and exchange routing** — publish straight to a queue, or to any exchange.
- **At-least-once delivery** — optional manual ack/nack with requeue-on-error.
- **Durable queues/messages, prefetch/QoS, heartbeats, dial timeouts** — all configurable.
- **Zero non-AMQP dependencies.**

## Requirements

- Go 1.23+
- RabbitMQ 3.x (any modern version)

## Installation

```sh
go get github.com/kazarus/RabbitMQ
```

## Quick start (simple mode)

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kazarus/RabbitMQ"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// A failed connection is an error, not a crash.
	c, err := RabbitMQ.NewSimple("amqp://guest:guest@localhost:5672/", "tasks")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	// Producer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.PublishText(ctx, "", "hello world"); err != nil {
		log.Fatalf("publish: %v", err)
	}

	// Consumer — blocks until ctx is cancelled or Close() is called.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	err = c.Consume(ctx, "", func(_ context.Context, d amqp.Delivery) error {
		fmt.Printf("received: %s\n", d.Body)
		return nil // ack
	})
	if err != nil && err != context.Canceled {
		log.Fatalf("consume: %v", err)
	}
}
```

## Configuration

`New(url, opts...)` / `NewSimple(url, queue, opts...)` accept any combination of options:

| Option | Default | Purpose |
| --- | --- | --- |
| `WithQueue(name)` | `""` | Default queue used when the queue argument is empty |
| `WithExchange(name)` | `""` | Default exchange (empty = AMQP default exchange) |
| `WithRouteKey(key)` | `""` | Legacy compatibility; `Publish` routes by queue name |
| `WithReconnect(bool)` | `true` | Retry with backoff while the broker is unreachable |
| `WithAutoAck(bool)` | `true` | `false` → handler acks/nacks manually |
| `WithDurableQueues(bool)` | `false` | Persistent queues and messages |
| `WithRequeueOnError(bool)` | `false` | Manual-ack mode: requeue messages whose handler failed |
| `WithDialTimeout(d)` | `5s` | Per-attempt TCP/AMQP handshake timeout |
| `WithHeartbeat(d)` | `10s` | AMQP heartbeat interval |
| `WithPrefetch(n)` | `0` | Consumer prefetch count (0 = unlimited) |
| `WithLazyConnect(bool)` | `false` | Defer the initial connection to first use |
| `WithLogger(l)` | discard | Internal diagnostics (e.g. handler errors in auto-ack mode) |

## Reconnection

With `WithReconnect(true)` (default), operations **block** (bounded by their context) and retry with
backoff while the broker is unreachable. Consumers that were running automatically reconnect and
resubscribe; in manual-ack mode, unacknowledged messages are redelivered by the broker (at-least-once).
Set `WithReconnect(false)` to fail fast instead.

## Delivery semantics

- **Auto-ack (default):** the broker acknowledges on delivery. Handler errors cannot nack — they are
  logged. Use this for idempotent, loss-tolerant handlers.
- **Manual ack (`WithAutoAck(false)`):** return `nil` to `Ack`, return an error to `Nack`.
  With `WithRequeueOnError(true)` a failed message is requeued and redelivered; otherwise it is dropped.

## Advanced use

Use `WithChannel` for operations the wrapper doesn't expose (exchange declares, bindings, QoS):

```go
err := c.WithChannel(ctx, func(ch *amqp.Channel) error {
	_, err := ch.ExchangeDeclare("logs", "fanout", true, false, false, false, nil)
	return err
})
```

Then publish with `PublishWithExchange(ctx, "logs", "", body)`.

## Testing

Unit tests need no broker:

```sh
go test ./...
```

Integration tests need a live broker — start one with Docker, then run with the `integration` tag:

```sh
docker compose up -d            # RabbitMQ on localhost:5672 (+ management UI on :15672)
go test -tags=integration ./...
```

Override the broker URL with `RABBITMQ_URL` (default `amqp://guest:guest@localhost:5672/`).

## Migrating from the legacy `TRabbitMQ` API

| Legacy | New |
| --- | --- |
| `NewRabbitMQ(queue, exchange, key, url)` | `New(url, WithQueue(q), WithExchange(e), WithRouteKey(k))` |
| `NewRabbitMQSimple(queue)` | `NewSimple(url, queue)` |
| `PublishSimple(msg)` | `PublishText(ctx, "", msg)` |
| `ConsumeSimple()` | `Consume(ctx, "", handler)` |
| `PublishMQ(queue, msg)` | `PublishText(ctx, queue, msg)` |
| `ConsumeMQ(queue, listener)` | `Consume(ctx, queue, handler)` |
| `Destory()` | `Close()` |

Behavioral notes:

- The URL is now **required** and validated; a dead broker is an error, not a crash.
- Consumers no longer block forever — cancel their `context` or call `Close()`.
- Handler errors in manual-ack mode nack the message instead of being ignored.

## License

No license is specified yet — see the repository owner before using this package in a commercial
product.
