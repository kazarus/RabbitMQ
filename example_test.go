//go:build integration

// Runnable godoc examples (run with: go test -tags=integration ./...).
package rabbitmq_test

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/kazarus/RabbitMQ"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ExampleNewSimple() {
	c, err := rabbitmq.NewSimple("amqp://guest:guest@localhost:5672/", "tasks",
		rabbitmq.WithDialTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err) // in real code: return the error to your caller
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.PublishText(ctx, "", "hello"); err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_Consume() {
	c, err := rabbitmq.NewSimple("amqp://guest:guest@localhost:5672/", "tasks",
		rabbitmq.WithDialTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consume blocks until ctx is cancelled or the client is closed.
	err = c.Consume(ctx, "", func(_ context.Context, d amqp.Delivery) error {
		log.Printf("received: %s", d.Body)
		return nil // ack
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
