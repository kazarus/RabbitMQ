package RabbitMQ

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrClosed is returned when an operation is attempted on a client that has
// been closed with Client.Close.
var ErrClosed = errors.New("rabbitmq: client is closed")

// Default tunables. These can be overridden with the With* options.
const (
	defaultDialTimeout   = 5 * time.Second
	defaultHeartbeat     = 10 * time.Second
	initialReconnectWait = 100 * time.Millisecond
	maxReconnectWait     = 5 * time.Second
)

// Config holds every tunable setting of a Client. Create it with the With*
// option functions; the zero value is never used directly.
type Config struct {
	// URL is the AMQP connection URL, e.g. "amqp://guest:guest@localhost:5672/".
	URL string

	// Queue is the default queue name used by Publish/Consume when the queue
	// argument passed to them is empty. Equivalent to the legacy QueueName.
	Queue string

	// Exchange is the default exchange used by Publish when routing to a
	// queue. Leave empty to use the AMQP default exchange, which routes
	// messages straight to the queue named by the routing key.
	Exchange string

	// RouteKey is kept for API compatibility with the legacy wrapper; Publish
	// routes by queue name, which is what the legacy code effectively did.
	RouteKey string

	// DialTimeout bounds each TCP/AMQP handshake attempt.
	DialTimeout time.Duration

	// Heartbeat is the AMQP heartbeat interval. Values below 1s defer to the
	// server's negotiated interval.
	Heartbeat time.Duration

	// Reconnect enables automatic reconnection with exponential backoff while
	// the broker is unreachable. Operations block (bounded by their context)
	// until a connection is available again. Disabled: operations fail fast.
	Reconnect bool

	// AutoAck lets the broker acknowledge messages immediately on delivery.
	// With AutoAck false, the handler must Ack/Nack the delivery itself
	// (returning a non-nil error nacks it). Defaults to true to match the
	// legacy behavior.
	AutoAck bool

	// Durable makes declared queues and published messages persistent.
	Durable bool

	// RequeueOnError, in manual-ack mode, requeues a message whose handler
	// returned an error instead of dropping it.
	RequeueOnError bool

	// Prefetch sets the consumer prefetch count (0 = unlimited).
	Prefetch int

	// LazyConnect defers the initial connection to the first operation. New
	// otherwise dials eagerly so misconfiguration surfaces immediately.
	LazyConnect bool

	// Logger receives internal diagnostics (e.g. handler errors in auto-ack
	// mode). Discards by default; attach one with WithLogger.
	Logger *log.Logger
}

// Option configures a Client. Options are applied in order, so later options
// win.
type Option func(*Config)

// WithQueue sets the default queue name used when Publish/Consume are called
// with an empty queue argument.
func WithQueue(name string) Option { return func(c *Config) { c.Queue = name } }

// WithExchange sets the default exchange used by Publish.
func WithExchange(name string) Option { return func(c *Config) { c.Exchange = name } }

// WithRouteKey sets the default routing key. Kept for API compatibility with
// the legacy wrapper; Publish routes by queue name.
func WithRouteKey(key string) Option { return func(c *Config) { c.RouteKey = key } }

// WithReconnect enables or disables automatic reconnection with exponential
// backoff (enabled by default).
func WithReconnect(enabled bool) Option {
	return func(c *Config) { c.Reconnect = enabled }
}

// WithAutoAck controls whether the broker acknowledges messages immediately on
// delivery (enabled by default). Disable it to ack/nack from the handler.
func WithAutoAck(enabled bool) Option { return func(c *Config) { c.AutoAck = enabled } }

// WithDurableQueues makes queue declarations and message delivery persistent.
func WithDurableQueues(enabled bool) Option {
	return func(c *Config) { c.Durable = enabled }
}

// WithRequeueOnError requeues messages whose handler returned an error (in
// manual-ack mode) instead of dropping them.
func WithRequeueOnError(enabled bool) Option {
	return func(c *Config) { c.RequeueOnError = enabled }
}

// WithDialTimeout bounds each connection attempt.
func WithDialTimeout(d time.Duration) Option {
	return func(c *Config) { c.DialTimeout = d }
}

// WithHeartbeat sets the AMQP heartbeat interval.
func WithHeartbeat(d time.Duration) Option { return func(c *Config) { c.Heartbeat = d } }

// WithPrefetch sets the consumer prefetch count (0 = unlimited).
func WithPrefetch(n int) Option { return func(c *Config) { c.Prefetch = n } }

// WithLazyConnect defers the initial connection to the first operation.
func WithLazyConnect(enabled bool) Option {
	return func(c *Config) { c.LazyConnect = enabled }
}

// WithLogger attaches a logger for internal diagnostics.
func WithLogger(l *log.Logger) Option { return func(c *Config) { c.Logger = l } }

func defaultConfig(url string) Config {
	return Config{
		URL:         url,
		DialTimeout: defaultDialTimeout,
		Heartbeat:   defaultHeartbeat,
		Reconnect:   true,
		AutoAck:     true,
		Logger:      log.New(io.Discard, "", 0),
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return errors.New("rabbitmq: connection URL is required")
	}
	if _, err := amqp.ParseURI(c.URL); err != nil {
		return fmt.Errorf("rabbitmq: invalid connection URL %q: %w", c.URL, err)
	}
	if c.DialTimeout <= 0 {
		return errors.New("rabbitmq: DialTimeout must be positive")
	}
	if c.Prefetch < 0 {
		return errors.New("rabbitmq: Prefetch must not be negative")
	}
	return nil
}

// Client is a RabbitMQ connection wrapper. A Client is safe for concurrent
// use; all channel operations are serialized internally.
//
// The zero value is not usable — construct one with New or NewSimple.
type Client struct {
	cfg Config

	mu      sync.Mutex // guards conn, channel, and (re)connection state
	conn    *amqp.Connection
	channel *amqp.Channel

	closed    chan struct{}
	closeOnce sync.Once
}

// New creates a Client and, unless WithLazyConnect(true) is used, dials the
// broker immediately. A failed initial dial is returned as an error — the
// caller decides how to react, instead of the process being killed by
// log.Fatal.
func New(url string, opts ...Option) (*Client, error) {
	cfg := defaultConfig(url)
	for _, o := range opts {
		o(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, closed: make(chan struct{})}
	if !cfg.LazyConnect {
		// Startup: a single attempt so a broken configuration fails fast.
		ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
		defer cancel()
		if _, err := c.ensureConnRetry(ctx, false); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// NewSimple creates a Client pre-configured for the classic "simple mode":
// messages are published to (and consumed from) a single queue through the
// default exchange.
func NewSimple(url, queueName string, opts ...Option) (*Client, error) {
	return New(url, append([]Option{WithQueue(queueName)}, opts...)...)
}

// Config returns a copy of the client's configuration.
func (c *Client) Config() Config { return c.cfg }

// URL returns the AMQP connection URL the client dials.
func (c *Client) URL() string { return c.cfg.URL }

// Close idempotently closes the underlying channel and connection. Consumers
// started with Consume return ErrClosed; in-flight publishes fail.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.channel != nil {
			_ = c.channel.Close()
			c.channel = nil
		}
		if c.conn != nil {
			err = c.conn.Close()
			c.conn = nil
		}
	})
	return err
}

// Ping verifies that the connection is live, re-establishing it if it died.
// It is suitable for health checks.
func (c *Client) Ping(ctx context.Context) error {
	return c.withChannel(ctx, func(*amqp.Channel) error { return nil })
}

// WithChannel runs fn with exclusive access to the client's producer channel,
// opening a new one if necessary (and reconnecting first if the connection
// died). Advanced users can use it for operations the wrapper does not expose,
// such as declaring exchanges or bindings.
func (c *Client) WithChannel(ctx context.Context, fn func(*amqp.Channel) error) error {
	return c.withChannel(ctx, fn)
}

// withChannel acquires the client lock, ensures a live connection and producer
// channel, and runs fn under the lock so channel use stays serialized. If the
// connection dies while opening the channel, it retries on a fresh connection
// (bounded, so a broken broker still surfaces as an error).
func (c *Client) withChannel(ctx context.Context, fn func(*amqp.Channel) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for attempt := 0; ; attempt++ {
		conn, err := c.ensureConnLocked(ctx, c.cfg.Reconnect)
		if err != nil {
			return err
		}
		if c.channel != nil && !c.channel.IsClosed() {
			return fn(c.channel)
		}
		ch, err := conn.Channel()
		if err == nil {
			c.channel = ch
			return fn(ch)
		}
		if isTransient(err) && attempt < 3 && !c.isClosed() {
			c.conn = nil // force a fresh connection on the next attempt
			continue
		}
		return fmt.Errorf("rabbitmq: open channel: %w", err)
	}
}

// ensureConn returns a live connection, re-establishing it (with backoff when
// cfg.Reconnect is set) if the previous one died.
func (c *Client) ensureConn(ctx context.Context) (*amqp.Connection, error) {
	return c.ensureConnRetry(ctx, c.cfg.Reconnect)
}

func (c *Client) ensureConnRetry(ctx context.Context, retry bool) (*amqp.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ensureConnLocked(ctx, retry)
}

// ensureConnLocked must be called with c.mu held.
func (c *Client) ensureConnLocked(ctx context.Context, retry bool) (*amqp.Connection, error) {
	if c.isClosed() {
		return nil, ErrClosed
	}
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if c.isClosed() {
			return nil, ErrClosed
		}
		conn, err := dial(c.cfg)
		if err == nil {
			c.conn = conn
			c.channel = nil // producer channel must be reopened on the new conn
			return conn, nil
		}
		lastErr = err
		if !retry {
			return nil, fmt.Errorf("rabbitmq: dial %q: %w", c.cfg.URL, err)
		}
		timer := time.NewTimer(reconnectBackoff(attempt))
		select {
		case <-c.closed:
			timer.Stop()
			return nil, ErrClosed
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("rabbitmq: dial %q: %w (last error: %v)", c.cfg.URL, ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *Client) resolveQueue(queue string) string {
	if queue == "" {
		return c.cfg.Queue
	}
	return queue
}

// dial opens a single AMQP connection with the configured timeouts.
func dial(cfg Config) (*amqp.Connection, error) {
	return amqp.DialConfig(cfg.URL, amqp.Config{
		Heartbeat: cfg.Heartbeat,
		Dial:      (&net.Dialer{Timeout: cfg.DialTimeout}).Dial,
	})
}

// reconnectBackoff returns the wait before retry attempt n, capped at
// maxReconnectWait.
func reconnectBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := initialReconnectWait << attempt
	if d > maxReconnectWait || d < initialReconnectWait { // d < initial guards against overflow
		return maxReconnectWait
	}
	return d
}
