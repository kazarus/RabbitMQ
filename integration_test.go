//go:build integration

// Integration tests require a live RabbitMQ broker. Run them with:
//
//	docker compose up -d          # starts rabbitmq on localhost:5672
//	go test -tags=integration ./...
//
// The broker URL can be overridden with the RABBITMQ_URL environment variable.
package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kazarus/RabbitMQ"
	amqp "github.com/rabbitmq/amqp091-go"
)

var testURL = func() string {
	if u := os.Getenv("RABBITMQ_URL"); u != "" {
		return u
	}
	return "amqp://guest:guest@localhost:5672/"
}()

func newTestClient(t *testing.T, opts ...rabbitmq.Option) *rabbitmq.Client {
	t.Helper()
	opts = append([]rabbitmq.Option{
		rabbitmq.WithDialTimeout(5 * time.Second),
		rabbitmq.WithReconnect(false), // fail fast in tests
	}, opts...)
	c, err := rabbitmq.New(testURL, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func uniqueQueue(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

func deleteQueue(t *testing.T, c *rabbitmq.Client, queue string) {
	t.Helper()
	_ = c.WithChannel(context.Background(), func(ch *amqp.Channel) error {
		_, err := ch.QueueDelete(queue, false, false, false)
		return err
	})
}

// TestPublishConsumeRoundTrip publishes before consuming — messages persist in
// the queue until a consumer picks them up.
func TestPublishConsumeRoundTrip(t *testing.T) {
	c := newTestClient(t)
	queue := uniqueQueue("roundtrip")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	const n = 5
	for i := 0; i < n; i++ {
		if err := c.PublishText(context.Background(), queue, fmt.Sprintf("msg-%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, n)
	go func() {
		_ = c.Consume(ctx, queue, func(context.Context, amqp.Delivery) error {
			got <- "x"
			if len(got) == n {
				cancel()
			}
			return nil
		})
	}()

	for i := 0; i < n; i++ {
		select {
		case <-got:
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

// TestDefaultQueueFallback exercises NewSimple: an empty queue argument falls
// back to the queue configured at construction.
func TestDefaultQueueFallback(t *testing.T) {
	queue := uniqueQueue("simple")
	c, err := rabbitmq.NewSimple(testURL, queue,
		rabbitmq.WithDialTimeout(5*time.Second), rabbitmq.WithReconnect(false))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	if err := c.PublishText(context.Background(), "", "via-default"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 1)
	go func() {
		_ = c.Consume(ctx, "", func(_ context.Context, d amqp.Delivery) error {
			got <- string(d.Body)
			cancel()
			return nil
		})
	}()
	select {
	case m := <-got:
		if m != "via-default" {
			t.Fatalf("unexpected message %q", m)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

// TestManualAckNackRequeue verifies at-least-once handling: a handler error
// nacks and requeues, so the message is redelivered.
func TestManualAckNackRequeue(t *testing.T) {
	c := newTestClient(t, rabbitmq.WithAutoAck(false), rabbitmq.WithRequeueOnError(true))
	queue := uniqueQueue("nack")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	if err := c.PublishText(context.Background(), queue, "once"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	attempts, redelivered := 0, false
	go func() {
		_ = c.Consume(ctx, queue, func(_ context.Context, d amqp.Delivery) error {
			mu.Lock()
			attempts++
			redelivered = d.Redelivered
			mu.Unlock()
			if d.Redelivered {
				cancel() // ack via nil return
				return nil
			}
			return errors.New("simulated failure") // nack + requeue
		})
	}()

	select {
	case <-ctx.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for redelivery")
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts < 2 {
		t.Fatalf("expected at least 2 delivery attempts, got %d", attempts)
	}
	if !redelivered {
		t.Fatal("expected a redelivered message")
	}
}

// TestPublishWithExchangeDefault routes through the AMQP default exchange,
// where the routing key is the queue name.
func TestPublishWithExchangeDefault(t *testing.T) {
	c := newTestClient(t)
	queue := uniqueQueue("exchange")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	if err := c.EnsureQueue(context.Background(), queue); err != nil {
		t.Fatalf("ensure queue: %v", err)
	}
	if err := c.PublishWithExchange(context.Background(), "", queue, []byte("via-exchange")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 1)
	go func() {
		_ = c.Consume(ctx, queue, func(_ context.Context, d amqp.Delivery) error {
			got <- string(d.Body)
			cancel()
			return nil
		})
	}()
	select {
	case m := <-got:
		if m != "via-exchange" {
			t.Fatalf("unexpected message %q", m)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestDurableQueues(t *testing.T) {
	c := newTestClient(t, rabbitmq.WithDurableQueues(true))
	queue := uniqueQueue("durable")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	if err := c.EnsureQueue(context.Background(), queue); err != nil {
		t.Fatalf("ensure durable queue: %v", err)
	}
	// Redeclaring with identical arguments must be a no-op.
	if err := c.EnsureQueue(context.Background(), queue); err != nil {
		t.Fatalf("redeclare durable queue: %v", err)
	}
}

func TestPrefetch(t *testing.T) {
	c := newTestClient(t, rabbitmq.WithPrefetch(1))
	queue := uniqueQueue("prefetch")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	const n = 3
	for i := 0; i < n; i++ {
		if err := c.PublishText(context.Background(), queue, fmt.Sprintf("p%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan struct{}, n)
	go func() {
		_ = c.Consume(ctx, queue, func(context.Context, amqp.Delivery) error {
			got <- struct{}{}
			if len(got) == n {
				cancel()
			}
			return nil
		})
	}()
	for i := 0; i < n; i++ {
		select {
		case <-got:
		case <-time.After(15 * time.Second):
			t.Fatal("timed out waiting for message")
		}
	}
}

// TestConcurrentPublishers hammers one client from many goroutines to prove
// the internal channel serialization is race-free.
func TestConcurrentPublishers(t *testing.T) {
	c := newTestClient(t)
	queue := uniqueQueue("concurrent")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	const producers, per = 4, 50
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := c.PublishText(context.Background(), queue, fmt.Sprintf("%d-%d", p, i)); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	total := producers * per
	got := make(chan struct{}, total)
	go func() {
		_ = c.Consume(ctx, queue, func(context.Context, amqp.Delivery) error {
			got <- struct{}{}
			if len(got) == total {
				cancel()
			}
			return nil
		})
	}()
	for i := 0; i < total; i++ {
		select {
		case <-got:
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out after %d/%d messages", i, total)
		}
	}
}

// TestConsumeCancel verifies consumers stop (and unregister) on cancellation.
func TestConsumeCancel(t *testing.T) {
	c := newTestClient(t)
	queue := uniqueQueue("cancel")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Consume(ctx, queue, func(context.Context, amqp.Delivery) error { return nil }) }()
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop on cancellation")
	}
}

// TestCloseStopsConsumer verifies Close() wakes up running consumers.
func TestCloseStopsConsumer(t *testing.T) {
	c := newTestClient(t)
	queue := uniqueQueue("close")
	t.Cleanup(func() { deleteQueue(t, c, queue) })

	done := make(chan error, 1)
	go func() {
		done <- c.Consume(context.Background(), queue, func(context.Context, amqp.Delivery) error { return nil })
	}()
	time.Sleep(300 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, rabbitmq.ErrClosed) {
			t.Fatalf("got %v, want ErrClosed", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop on close")
	}
}
