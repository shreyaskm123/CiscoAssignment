package mqclient

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "streamer/proto"
)

// groupStreamer is the fleet namespace this publisher belongs to (see
// JoinPartitionRequest.group). It keeps streamer shard assignments and offsets
// independent from the collector fleet that consumes the same event log.
const groupStreamer = "streamer"

// Client is a thin gRPC client for the MessageQueue service.
type Client struct {
	addr string
	conn *grpc.ClientConn
	stub mqpb.MessageQueueClient
}

// Option customises Dial.
type Option func(*dialConfig)

type dialConfig struct {
	token     string
	tokenFile string
}

// WithToken authenticates this client to the MQ with a bearer token.
func WithToken(token string) Option {
	return func(c *dialConfig) { c.token = token }
}

// WithTokenFile authenticates with the token in path, re-read on every RPC so a
// rotated Secret takes effect without reconnecting.
func WithTokenFile(path string) Option {
	return func(c *dialConfig) { c.tokenFile = path }
}

// Dial connects to the MQ. Insecure creds are used because the assignment runs
// inside a private cluster network without TLS; swap for a real credential
// bundle in production.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	var cfg dialConfig
	for _, o := range opts {
		o(&cfg)
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	if cfg.token != "" || cfg.tokenFile != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(tokenCredentials{
			token:     cfg.token,
			tokenFile: cfg.tokenFile,
		}))
	}
	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial mq %s: %w", addr, err)
	}
	return &Client{addr: addr, conn: conn, stub: mqpb.NewMessageQueueClient(conn)}, nil
}

// tokenCredentials attaches the bearer token to every RPC. It is consulted per
// call by gRPC, so re-reading the file is what gives the client hot rotation.
//
// RequireTransportSecurity is false because the transport is plaintext in this
// cluster; that is exactly why the MQ is protected with NetworkPolicy and why
// this must flip to true when the listener moves to TLS.
type tokenCredentials struct {
	token     string
	tokenFile string
}

var _ credentials.PerRPCCredentials = tokenCredentials{}

func (t tokenCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	token := t.token
	if t.tokenFile != "" {
		if b, err := os.ReadFile(t.tokenFile); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				token = s
			}
		}
	}
	if token == "" {
		return nil, fmt.Errorf("mq auth: no token available for %s", t.tokenFile)
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (t tokenCredentials) RequireTransportSecurity() bool { return false }

// Close tears down the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// GetOffset returns the last acknowledged offset stored by the MQ for
// consumerID, and whether any offset is known yet. Critical for crash
// recovery: the streamer resumes processing from this position.
func (c *Client) GetOffset(ctx context.Context, consumerID string) (int64, bool, error) {
	resp, err := c.stub.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: consumerID, Group: groupStreamer})
	if err != nil {
		return 0, false, fmt.Errorf("get offset for %s: %w", consumerID, err)
	}
	return resp.Offset, resp.Exists, nil
}

// JoinPartition registers/heartbeats this consumer with the MQ's partition
// registry and returns the replica's assigned (index, total). The join polls on
// a fixed cadence elsewhere, which both keeps the consumer alive (heartbeat)
// and detects scale up/down (the returned total/index change).
func (c *Client) JoinPartition(ctx context.Context, consumerID string, ttlSeconds int32) (index, total int, err error) {
	resp, err := c.stub.JoinPartition(ctx, &mqpb.JoinPartitionRequest{
		ConsumerId: consumerID, TtlSeconds: ttlSeconds, Group: groupStreamer,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("join partition for %s: %w", consumerID, err)
	}
	return int(resp.Index), int(resp.Total), nil
}

// LeavePartition deregisters this consumer from the partition registry. Called
// after a graceful drain so surviving replicas rebalance immediately.
func (c *Client) LeavePartition(ctx context.Context, consumerID string) error {
	_, err := c.stub.LeavePartition(ctx, &mqpb.LeavePartitionRequest{ConsumerId: consumerID, Group: groupStreamer})
	if err != nil {
		return fmt.Errorf("leave partition for %s: %w", consumerID, err)
	}
	return nil
}

// Stream maintains a connected PublishEvents bidirectional stream. It
// transparently reconnects when the underlying stream fails, satisfying the
// "resend after reconnect" requirement.
type Stream struct {
	client *Client
	onAck  func(*mqpb.Ack)

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	send   grpc.BidiStreamingClient[mqpb.Event, mqpb.Ack]

	sent  atomic.Int64 // events written to the stream
	acked atomic.Int64 // acks received back from the MQ
}

// NewStream opens the publish stream. onAck is invoked serially for every Ack
// received from the MQ (offset advancement happens there).
//
// The stream intentionally owns its own lifecycle context (independent of ctx)
// so a drain on graceful shutdown can still deliver already-buffered events
// after the run loop has been cancelled. It is torn down via Close.
func (c *Client) NewStream(ctx context.Context, onAck func(*mqpb.Ack)) (*Stream, error) {
	s := &Stream{client: c, onAck: onAck}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if err := s.ensure(); err != nil {
		return nil, err
	}
	return s, nil
}

// ensure creates the underlying stream if it is not active and starts its ack
// reader goroutine. Callers must hold s.mu.
func (s *Stream) ensure() error {
	stream, err := s.client.stub.PublishEvents(s.ctx)
	if err != nil {
		return fmt.Errorf("open publish stream: %w", err)
	}
	s.send = stream
	go s.readAcks(stream)
	return nil
}

// Send publishes one event. If the current stream is dead it is recreated and
// the event is resent, so an event is never silently dropped. Caller passes a
// deadline-preserving context via the Stream's base context.
func (s *Stream) Send(e *mqpb.Event) error {
	return s.sendUntil(e, time.Time{})
}

// SendUntil publishes an event but stops retrying once deadline has passed. It
// is used by the graceful-shutdown drain so the process never hangs forever on
// an unreachable MQ.
func (s *Stream) SendUntil(e *mqpb.Event, deadline time.Time) error {
	return s.sendUntil(e, deadline)
}

func (s *Stream) sendUntil(e *mqpb.Event, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return fmt.Errorf("drain deadline exceeded")
		}
		if s.send != nil {
			if err := s.send.Send(e); err == nil {
				s.sent.Add(1)
				return nil
			}
		}
		s.send = nil // force reconnect
		if err := s.ensure(); err != nil {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
	}
}

func (s *Stream) readAcks(stream grpc.BidiStreamingClient[mqpb.Event, mqpb.Ack]) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			return // stream ended; next Send reconnects
		}
		s.acked.Add(1)
		s.onAck(ack)
	}
}

// Sent returns how many events have been written to the stream.
func (s *Stream) Sent() int64 { return s.sent.Load() }

// Acked returns how many acks have been received from the MQ. Comparing Acked
// to Sent tells the drain whether every delivered event was confirmed.
func (s *Stream) Acked() int64 { return s.acked.Load() }

// CloseSend flushes pending writes and signals hangup.
func (s *Stream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.send == nil {
		return nil
	}
	return s.send.CloseSend()
}

// Close cancels the stream context, closing both directions.
func (s *Stream) Close() {
	s.cancel()
}
