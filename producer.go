package RabbitMQ

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publish sends body to queue, declaring the queue first if it does not
// exist. When queue is empty the client's default queue (WithQueue or
// NewSimple) is used. Messages are routed through the client's default
// exchange (the AMQP default exchange routes straight to the queue).
//
// Pass a context with a timeout: Publish blocks until the message is written
// or ctx is done, and with Reconnect enabled it also blocks (retrying) while
// the broker is unreachable.
func (c *Client) Publish(ctx context.Context, queue string, body []byte) error {
	queue = c.resolveQueue(queue)
	if queue == "" {
		return errors.New("rabbitmq: queue name is required (pass it to Publish or set it via WithQueue/NewSimple)")
	}
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		if err := declareQueue(ch, queue, c.cfg.Durable); err != nil {
			return err
		}
		return ch.PublishWithContext(ctx, c.cfg.Exchange, queue, false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			DeliveryMode: deliveryMode(c.cfg.Durable),
		})
	})
}

// PublishText is Publish for string payloads.
func (c *Client) PublishText(ctx context.Context, queue, message string) error {
	return c.Publish(ctx, queue, []byte(message))
}

// PublishWithExchange publishes body to exchange with routing key key,
// bypassing queue declaration. Use it for fanout/topic/direct exchanges that
// are declared out-of-band (e.g. via WithChannel).
func (c *Client) PublishWithExchange(ctx context.Context, exchange, key string, body []byte) error {
	if exchange == "" && key == "" {
		return errors.New("rabbitmq: exchange and routing key are both empty")
	}
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		return ch.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			DeliveryMode: deliveryMode(c.cfg.Durable),
		})
	})
}

// EnsureQueue declares queue (creating it if missing) and returns an error if
// the declaration is refused. An empty queue falls back to the client's
// default queue.
func (c *Client) EnsureQueue(ctx context.Context, queue string) error {
	queue = c.resolveQueue(queue)
	if queue == "" {
		return errors.New("rabbitmq: queue name is required")
	}
	return c.withChannel(ctx, func(ch *amqp.Channel) error {
		return declareQueue(ch, queue, c.cfg.Durable)
	})
}

// declareQueue creates queue if it does not exist (declaring an existing
// queue with identical arguments is a no-op).
func declareQueue(ch *amqp.Channel, name string, durable bool) error {
	if _, err := ch.QueueDeclare(name, durable, false, false, false, nil); err != nil {
		return fmt.Errorf("rabbitmq: declare queue %q: %w", name, err)
	}
	return nil
}

func deliveryMode(durable bool) uint8 {
	if durable {
		return amqp.Persistent
	}
	return amqp.Transient
}
