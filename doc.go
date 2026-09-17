// Package RabbitMQ is a small, production-oriented Go client for RabbitMQ
// built on top of github.com/rabbitmq/amqp091-go (the maintained successor of
// the archived streadway/amqp).
//
// Unlike many tutorial-style wrappers it:
//
//   - never terminates the host process: every failure is returned as an error;
//   - is context-aware, so publishes and consumers can be cancelled;
//   - reconnects automatically with exponential backoff (WithReconnect is
//     enabled by default);
//   - is safe for concurrent use by multiple goroutines;
//   - supports both the classic "simple mode" (publishing straight to a queue
//     through the default exchange) and explicit exchange routing.
//
// # Quick start (simple mode)
//
//	c, err := RabbitMQ.NewSimple("amqp://guest:guest@localhost:5672/", "tasks")
//	if err != nil {
//		// The error is returned instead of log.Fatal — the caller decides
//		// how to handle a broker that is down at startup.
//		return err
//	}
//	defer c.Close()
//
//	if err := c.PublishText(ctx, "", "hello"); err != nil { ... }
//
//	if err := c.Consume(ctx, "", func(ctx context.Context, d amqp.Delivery) error {
//		log.Printf("got %q", d.Body)
//		return nil // ack
//	}); err != nil { ... }
//
// See the README for full documentation, configuration options, and the
// migration guide for the legacy TRabbitMQ API.
package RabbitMQ
