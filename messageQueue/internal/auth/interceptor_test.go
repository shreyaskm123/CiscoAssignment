package auth_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mqpb "streamer/proto"

	"messagequeue/internal/auth"
	"messagequeue/internal/mq"
)

// startMQ runs a real messagequeue with the auth interceptors attached and
// returns its address.
func startMQ(t *testing.T, tokens string) string {
	t.Helper()
	a, err := auth.New("", tokens)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(a.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(a.StreamServerInterceptor()),
	)
	mqpb.RegisterMessageQueueServer(srv, mq.New(15*time.Second))
	mqpb.RegisterReplicationServer(srv, mq.New(15*time.Second))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func dial(t *testing.T, addr, token string) mqpb.MessageQueueClient {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(auth.TokenCredentials{Token: token}))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return mqpb.NewMessageQueueClient(conn)
}

// PublishEvents is the write path to the whole pipeline. If the stream
// interceptor did not cover it, anyone who can reach the port could inject
// telemetry - so this asserts the bidi stream itself is refused.
func TestPublishStreamRequiresToken(t *testing.T) {
	addr := startMQ(t, "streamer=tok-streamer,collector=tok-collector")

	// No credentials at all.
	anon := dial(t, addr, "")
	assertPublishRefused(t, anon)
	assertUnaryRefused(t, anon)

	// Wrong token.
	wrong := dial(t, addr, "not-the-token")
	assertPublishRefused(t, wrong)
	assertUnaryRefused(t, wrong)

	// Correct tokens: both client identities are served.
	for _, token := range []string{"tok-streamer", "tok-collector"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := dial(t, addr, token).PublishEvents(ctx)
		if err != nil {
			cancel()
			t.Fatalf("PublishEvents with %q: %v", token, err)
		}
		err = stream.Send(&mqpb.Event{EventId: "e-" + token, CsvLineOffset: 1, MetricName: "utilization.gpu", Value: 42})
		if err != nil {
			cancel()
			t.Fatalf("publish with %q: %v", token, err)
		}
		ack, err := stream.Recv()
		if err != nil {
			cancel()
			t.Fatalf("ack with %q: %v", token, err)
		}
		if ack.GetOffset() < 0 {
			t.Errorf("ack offset = %d want >= 0", ack.GetOffset())
		}
		_ = stream.CloseSend()
		cancel()
	}
}

// A disabled Authenticator must keep the local/test workflow working.
func TestNoTokensLeavesQueueOpen(t *testing.T) {
	addr := startMQ(t, "")
	stream, err := dial(t, addr, "").PublishEvents(context.Background())
	if err != nil {
		t.Fatalf("PublishEvents: %v", err)
	}
	if err := stream.Send(&mqpb.Event{EventId: "open", CsvLineOffset: 1, MetricName: "m", Value: 1}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func assertPublishRefused(t *testing.T, c mqpb.MessageQueueClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.PublishEvents(ctx)
	if err == nil {
		// The stream is created lazily, so the refusal only surfaces on I/O.
		// gRPC reports io.EOF from Send and defers the real status to Recv, so
		// both must be attempted to see the Unauthenticated code.
		sendErr := stream.Send(&mqpb.Event{EventId: "anon", CsvLineOffset: 1, MetricName: "m", Value: 1})
		_, recvErr := stream.Recv()
		err = sendErr
		if err == nil || err == io.EOF {
			err = recvErr
		}
	}
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Errorf("publish code = %v (%v) want Unauthenticated", code, err)
	}
}

func assertUnaryRefused(t *testing.T, c mqpb.MessageQueueClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "c1", Group: "collector"})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Errorf("GetOffset code = %v (%v) want Unauthenticated", code, err)
	}
}
