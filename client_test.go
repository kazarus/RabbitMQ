package RabbitMQ

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// A closed port that refuses connections instantly, used to exercise dial
// failures without a broker.
const deadBrokerURL = "amqp://guest:guest@127.0.0.1:1/"

func TestConfigDefaultsAndOptions(t *testing.T) {
	cfg := defaultConfig("amqp://localhost:5672/")
	if cfg.DialTimeout != defaultDialTimeout || cfg.Heartbeat != defaultHeartbeat {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if !cfg.Reconnect || !cfg.AutoAck {
		t.Fatalf("Reconnect and AutoAck should default to true: %+v", cfg)
	}
	if cfg.Logger == nil {
		t.Fatal("default logger must not be nil")
	}

	WithQueue("q1")(&cfg)
	WithExchange("ex")(&cfg)
	WithRouteKey("rk")(&cfg)
	WithReconnect(false)(&cfg)
	WithAutoAck(false)(&cfg)
	WithDurableQueues(true)(&cfg)
	WithRequeueOnError(true)(&cfg)
	WithDialTimeout(3 * time.Second)(&cfg)
	WithHeartbeat(30 * time.Second)(&cfg)
	WithPrefetch(10)(&cfg)
	WithLazyConnect(true)(&cfg)

	if cfg.Queue != "q1" || cfg.Exchange != "ex" || cfg.RouteKey != "rk" {
		t.Fatalf("names not applied: %+v", cfg)
	}
	if cfg.Reconnect || cfg.AutoAck {
		t.Fatalf("boolean options not applied: %+v", cfg)
	}
	if !cfg.Durable || !cfg.RequeueOnError || !cfg.LazyConnect {
		t.Fatalf("enabling options not applied: %+v", cfg)
	}
	if cfg.DialTimeout != 3*time.Second || cfg.Heartbeat != 30*time.Second || cfg.Prefetch != 10 {
		t.Fatalf("numeric options not applied: %+v", cfg)
	}
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"valid", "amqp://guest:guest@localhost:5672/", false},
		{"valid amqps", "amqps://user:pass@example.com:5671/vhost", false},
		{"empty", "", true},
		{"whitespace", "   ", true},
		{"no scheme", "localhost:5672", true},
		{"garbage", "://not a url", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := defaultConfig(tc.url).validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validate(%q) error = %v, wantErr %v", tc.url, err, tc.wantErr)
			}
		})
	}
}

func TestValidateBadNumericOptions(t *testing.T) {
	cfg := defaultConfig("amqp://localhost:5672/")
	WithDialTimeout(-time.Second)(&cfg)
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error for non-positive DialTimeout")
	}
	cfg = defaultConfig("amqp://localhost:5672/")
	WithPrefetch(-1)(&cfg)
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error for negative Prefetch")
	}
}

func TestNewValidationErrors(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("expected error for empty URL")
	}
	if _, err := New("not-a-url"); err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

// TestNewDialFailureIsErrorNotFatal guards the most important behavioral fix:
// a dead broker must surface as an error, never as log.Fatal/panic.
func TestNewDialFailureIsErrorNotFatal(t *testing.T) {
	_, err := New(deadBrokerURL, WithDialTimeout(200*time.Millisecond))
	if err == nil {
		t.Fatal("expected dial error")
	}
}

func TestLazyConnectDefersDial(t *testing.T) {
	c, err := New(deadBrokerURL, WithLazyConnect(true), WithReconnect(false),
		WithDialTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatalf("lazy connect must not dial at construction: %v", err)
	}
	defer c.Close()
	if err := c.Ping(context.Background()); err == nil {
		t.Fatal("expected dial failure on first use")
	}
}

func TestNewSimpleConfig(t *testing.T) {
	c, err := NewSimple(deadBrokerURL, "tasks", WithLazyConnect(true), WithReconnect(false))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Config().Queue != "tasks" {
		t.Fatalf("NewSimple did not set the queue: %+v", c.Config())
	}
	if c.URL() != deadBrokerURL {
		t.Fatalf("URL() = %q", c.URL())
	}
}

// TestReconnectRetriesUntilContextExpires proves the retry loop engages: with
// Reconnect on and the broker down, an operation blocks until its context is
// done instead of failing instantly.
func TestReconnectRetriesUntilContextExpires(t *testing.T) {
	c, err := New(deadBrokerURL, WithLazyConnect(true), WithDialTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	err = c.Publish(ctx, "q", []byte("x"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded from retry loop, got: %v", err)
	}
}

func TestNoReconnectFailsFast(t *testing.T) {
	c, err := New(deadBrokerURL, WithLazyConnect(true), WithReconnect(false),
		WithDialTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	start := time.Now()
	err = c.Publish(context.Background(), "q", []byte("x"))
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("with Reconnect disabled the call must fail fast, not retry")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("fail-fast took too long: %v", time.Since(start))
	}
}

func TestOperationsAfterClose(t *testing.T) {
	c, err := New(deadBrokerURL, WithLazyConnect(true), WithReconnect(false),
		WithDialTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close must be a no-op: %v", err)
	}

	if err := c.Publish(context.Background(), "q", []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: got %v, want ErrClosed", err)
	}
	if err := c.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ping after close: got %v, want ErrClosed", err)
	}
	if err := c.WithChannel(context.Background(), func(*amqp.Channel) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("withChannel after close: got %v, want ErrClosed", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Consume(context.Background(), "q", func(context.Context, amqp.Delivery) error { return nil })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("consume after close: got %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consume after close did not return")
	}
}

func TestPublishRequiresQueue(t *testing.T) {
	c, err := New(deadBrokerURL, WithLazyConnect(true), WithReconnect(false))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Publish(context.Background(), "", []byte("x")); err == nil {
		t.Fatal("expected error for empty queue name")
	}
}

func TestConsumeValidation(t *testing.T) {
	c, err := New(deadBrokerURL, WithLazyConnect(true), WithReconnect(false))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Consume(context.Background(), "", func(context.Context, amqp.Delivery) error { return nil }); err == nil {
		t.Fatal("expected error for empty queue name")
	}
	if err := c.Consume(context.Background(), "q", nil); err == nil {
		t.Fatal("expected error for nil handler")
	}
}

func TestReconnectBackoff(t *testing.T) {
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		maxReconnectWait, // 100ms<<6 overflows the cap
		maxReconnectWait,
	}
	for i, w := range want {
		if got := reconnectBackoff(i); got != w {
			t.Fatalf("reconnectBackoff(%d) = %v, want %v", i, got, w)
		}
	}
}
