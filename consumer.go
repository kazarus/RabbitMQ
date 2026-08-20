package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler processes a single delivered message. It is invoked sequentially in
// a single goroutine per consumer, so handlers need not be thread-safe.
//
// In manual-ack mode (WithAutoAck(false)) a nil return acknowledges the
// message; a non-nil return nacks it, requeueing it only if
// WithRequeueOnError(true) was set.
type Handler func(ctx context.Context, d amqp.Delivery) error

// errResubscribe is an internal sentinel: the consumer's channel died and the
// delivery stream must be re-established (which transparently reconnects).
var errResubscribe = errors.New("rabbitmq: resubscribe")

// Consume delivers messages from queue to handler, declaring the queue first
// if it does not exist. An empty queue falls back to the client's default
// queue.
//
// Consume blocks until ctx is cancelled (it returns ctx.Err()), until the
// client is closed (ErrClosed), or until a non-transient error occurs. When
// the broker connection drops, the consumer automatically reconnects and
// resubscribes: in manual-ack mode, messages that were not acknowledged are
// redelivered by the broker.
func (c *Client) Consume(ctx context.Context, queue string, handler Handler) error {
	if handler == nil {
		return errors.New("rabbitmq: nil handler")
	}
	queue = c.resolveQueue(queue)
	if queue == "" {
		return errors.New("rabbitmq: queue name is required (pass it to Consume or set it via WithQueue/NewSimple)")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		deliveries, err := c.subscribe(ctx, queue)
		if err != nil {
			return err
		}
		err = c.consumeLoop(ctx, handler, deliveries)
		if errors.Is(err, errResubscribe) {
			continue // channel died; reconnect and resubscribe
		}
		return err
	}
}

// subscribe opens a dedicated consumer channel and starts consuming queue.
// Consumers use their own channel so acks never race with publishes. It
// retries transient failures (a connection that dies between reconnect
// attempts) until ctx is done.
func (c *Client) subscribe(ctx context.Context, queue string) (<-chan amqp.Delivery, error) {
	for {
		conn, err := c.ensureConn(ctx)
		if err != nil {
			return nil, err
		}
		ch, err := conn.Channel()
		if err != nil {
			if isTransient(err) {
				if !pauseRetry(ctx) {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, fmt.Errorf("rabbitmq: open consumer channel: %w", err)
		}
		if c.cfg.Prefetch > 0 {
			if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
				_ = ch.Close()
				return nil, fmt.Errorf("rabbitmq: set prefetch: %w", err)
			}
		}
		if err := declareQueue(ch, queue, c.cfg.Durable); err != nil {
			_ = ch.Close()
			if isTransient(err) {
				if !pauseRetry(ctx) {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, err
		}
		deliveries, err := ch.ConsumeWithContext(ctx, queue, "", c.cfg.AutoAck, false, false, false, nil)
		if err != nil {
			_ = ch.Close()
			if isTransient(err) {
				if !pauseRetry(ctx) {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, fmt.Errorf("rabbitmq: consume %q: %w", queue, err)
		}
		return deliveries, nil
	}
}

// pauseRetry waits a short moment between transient retries so a flapping
// connection cannot hot-loop. It reports false when ctx is done.
func pauseRetry(ctx context.Context) bool {
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// consumeLoop drains deliveries until ctx is done, the client is closed, or
// the delivery stream dies (connection loss) — the latter signals
// errResubscribe so the caller re-establishes the consumer.
func (c *Client) consumeLoop(ctx context.Context, handler Handler, deliveries <-chan amqp.Delivery) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return ErrClosed
		case d, ok := <-deliveries:
			if !ok {
				return errResubscribe
			}
			c.dispatch(ctx, handler, d)
		}
	}
}

// dispatch routes a delivery to the handler and performs the ack/nack that
// follows from its result. A panicking handler is recovered and treated as a
// handler error, so a single bad message cannot kill the consumer.
func (c *Client) dispatch(ctx context.Context, handler Handler, d amqp.Delivery) {
	if c.cfg.AutoAck {
		// The broker already acknowledged delivery; a handler error cannot be
		// nacked, so log it for observability.
		if err := c.safeHandle(ctx, handler, d); err != nil {
			c.cfg.Logger.Printf("rabbitmq: handler error (auto-ack, message lost): %v", err)
		}
		return
	}
	if err := c.safeHandle(ctx, handler, d); err != nil {
		_ = d.Nack(false, c.cfg.RequeueOnError)
		return
	}
	_ = d.Ack(false)
}

// safeHandle runs the handler, converting a panic into an error.
func (c *Client) safeHandle(ctx context.Context, handler Handler, d amqp.Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handler(ctx, d)
}

// isTransient reports whether err is a connection/channel-level failure that
// a reconnect can reasonably fix, as opposed to a permanent protocol or
// configuration error.
func isTransient(err error) bool {
	return errors.Is(err, amqp.ErrClosed) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}
