package mq

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mqpb "streamer/proto"
)

func startServer(t *testing.T, s *Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	mqpb.RegisterMessageQueueServer(srv, s)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func dial(t *testing.T, addr string) mqpb.MessageQueueClient {
	t.Helper()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return mqpb.NewMessageQueueClient(conn)
}

func TestRegistryJoinLeave(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	idx, err := c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "b", TtlSeconds: 60})
	if err != nil || idx.Index != 0 || idx.Total != 1 {
		t.Fatalf("join b: resp=%v err=%v, want 0/1", idx, err)
	}
	idx, err = c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "a", TtlSeconds: 60})
	if err != nil || idx.Index != 0 || idx.Total != 2 {
		t.Fatalf("join a: resp=%v err=%v, want 0/2", idx, err)
	}
	idx, err = c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "b", TtlSeconds: 60})
	if err != nil || idx.Index != 1 || idx.Total != 2 {
		t.Fatalf("heartbeat b: resp=%v err=%v, want 1/2", idx, err)
	}
	if _, err := c.LeavePartition(ctx, &mqpb.LeavePartitionRequest{ConsumerId: "a"}); err != nil {
		t.Fatalf("leave a: %v", err)
	}
	idx, _ = c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "b", TtlSeconds: 60})
	if idx.Index != 0 || idx.Total != 1 {
		t.Fatalf("after leave: b=%v, want 0/1", idx)
	}
}

func TestPublishAckAndOffsets(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	client := dial(t, addr)
	ctx := context.Background()

	stream, err := client.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	ev := func(id string, off int64) *mqpb.Event {
		return &mqpb.Event{EventId: id, CsvLineOffset: off, Tags: map[string]string{"pod_name": "pod-0"}}
	}

	for i, e := range []*mqpb.Event{ev("e_1", 0), ev("e_2", 1), ev("e_3", 2)} {
		if err := stream.Send(e); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	for i, want := range []int64{0, 1, 2} {
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if ack.Offset != want || !ack.Success {
			t.Fatalf("ack %d = offset=%d success=%v, want offset=%d success=true", i, ack.Offset, ack.Success, want)
		}
	}
	stream.CloseSend()
	for {
		if _, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("stream close: %v", err)
		}
	}

	off, err := client.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "pod-0", Group: streamerGroup})
	if err != nil || !off.Exists || off.Offset != 2 {
		t.Fatalf("get offset: resp=%v err=%v, want exists=true offset=2", off, err)
	}
}

func TestDedupOnEventID(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	client := dial(t, addr)
	ctx := context.Background()

	stream, err := client.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	send := func(id string, off int64) *mqpb.Ack {
		t.Helper()
		if err := stream.Send(&mqpb.Event{EventId: id, CsvLineOffset: off, Tags: map[string]string{"pod_name": "pod-0"}}); err != nil {
			t.Fatalf("send: %v", err)
		}
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		return ack
	}

	if ack := send("e_1", 0); !ack.Success || ack.Error != "" {
		t.Fatalf("first send ack = %+v, want success with no error", ack)
	}
	if ack := send("e_1", 0); !ack.Success || ack.Error == "" {
		t.Fatalf("duplicate ack = %+v, want success flagged as duplicate", ack)
	}
	stream.CloseSend()
}
