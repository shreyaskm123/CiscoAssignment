// Package mqclient is a thin gRPC client for the MessageQueue consumer contract
// (Consume/CommitOffset) plus the registry APIs the collector shares with the
// streamer fleet (GetOffset, JoinPartition, LeavePartition).
package mqclient

import (
	"context"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "streamer/proto"
)

// groupCollector is the fleet namespace this consumer belongs to (see
// JoinPartitionRequest.group). Collector shard assignments, committed cursors
// and the MQ retention barrier are scoped to it, independent of the streamer
// publishers sharing the same event log.
const groupCollector = "collector"

// Consumer is a client-side view of a Consume server stream.
type Consumer interface {
	// Recv returns the next event, or io.EOF when the MQ's current log tail has
	// been reached (the caller re-invokes Consume from its committed cursor).
	Recv() (*mqpb.ConsumedEvent, error)
}

// Client is a thin gRPC client for the MessageQueue service.
type Client struct {
	conn *grpc.ClientConn
	stub mqpb.MessageQueueClient
}

// New wraps an existing connection.
func New(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, stub: mqpb.NewMessageQueueClient(conn)}
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

// Dial connects to the MQ with insecure credentials (private cluster network).
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
	return New(conn), nil
}

// tokenCredentials attaches the bearer token to every RPC. gRPC consults it per
// call, so re-reading the file is what gives the client hot rotation.
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

// GetOffset returns the committed cursor for a consumer (exists=false when the
// consumer has never committed, i.e. a fresh consumer that must start at -1).
func (c *Client) GetOffset(ctx context.Context, consumerID string) (int64, bool, error) {
	resp, err := c.stub.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: consumerID, Group: groupCollector})
	if err != nil {
		return 0, false, fmt.Errorf("get offset for %s: %w", consumerID, err)
	}
	return resp.Offset, resp.Exists, nil
}

// JoinPartition registers/heartbeats the consumer with the MQ partition registry
// and returns its assigned (index, total). Polled on a cadence by the watcher.
func (c *Client) JoinPartition(ctx context.Context, consumerID string, ttlSeconds int32) (index, total int, err error) {
	resp, err := c.stub.JoinPartition(ctx, &mqpb.JoinPartitionRequest{
		ConsumerId: consumerID, TtlSeconds: ttlSeconds, Group: groupCollector,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("join partition for %s: %w", consumerID, err)
	}
	return int(resp.Index), int(resp.Total), nil
}

// LeavePartition deregisters the consumer after a graceful drain.
func (c *Client) LeavePartition(ctx context.Context, consumerID string) error {
	_, err := c.stub.LeavePartition(ctx, &mqpb.LeavePartitionRequest{ConsumerId: consumerID, Group: groupCollector})
	if err != nil {
		return fmt.Errorf("leave partition for %s: %w", consumerID, err)
	}
	return nil
}

// CommitOffset records the consumer's processed position. The collector only
// advances this after the batch is durably written to ClickHouse.
func (c *Client) CommitOffset(ctx context.Context, consumerID string, offset int64) error {
	_, err := c.stub.CommitOffset(ctx, &mqpb.CommitOffsetRequest{
		ConsumerId: consumerID, Offset: offset, Group: groupCollector,
	})
	if err != nil {
		return fmt.Errorf("commit offset %d for %s: %w", offset, consumerID, err)
	}
	return nil
}

// Consume opens a server stream of events with offset > start (inclusive of the
// events published after start, exclusive of the cursor itself).
func (c *Client) Consume(ctx context.Context, consumerID string, start int64) (Consumer, error) {
	stream, err := c.stub.Consume(ctx, &mqpb.ConsumeRequest{
		ConsumerId: consumerID, StartOffset: start, Group: groupCollector,
	})
	if err != nil {
		return nil, fmt.Errorf("consume for %s@%d: %w", consumerID, start, err)
	}
	return streamConsumer{MessageQueue_ConsumeClient: stream}, nil
}

type streamConsumer struct {
	mqpb.MessageQueue_ConsumeClient
}

func (s streamConsumer) Recv() (*mqpb.ConsumedEvent, error) {
	return s.MessageQueue_ConsumeClient.Recv()
}
