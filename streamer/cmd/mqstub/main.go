// Command mqstub is a minimal in-memory implementation of the MessageQueue
// service used for local testing: it prints every published event as JSON to
// stdout, acks it with the event's offset, and honours GetOffset so the
// streamer's crash-recovery path can be exercised too.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	mqpb "streamer/proto"
)

var (
	addr = flag.String("addr", ":50051", "gRPC listen address")
	// maxEvents exits cleanly after acknowledging this many events (0 = run forever).
	maxEvents = flag.Int("max-events", 0, "exit after this many acknowledged events (0 = forever)")
)

type stubMQ struct {
	mqpb.UnimplementedMessageQueueServer

	mu        sync.Mutex
	consumers map[string]int64
	acked     int

	reMu    sync.Mutex
	replica map[string]struct{}
}

func newStubMQ() *stubMQ {
	return &stubMQ{consumers: map[string]int64{}, replica: map[string]struct{}{}}
}

func (s *stubMQ) GetOffset(_ context.Context, req *mqpb.GetOffsetRequest) (*mqpb.GetOffsetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if off, ok := s.consumers[req.ConsumerId]; ok {
		return &mqpb.GetOffsetResponse{Offset: off, Exists: true}, nil
	}
	return &mqpb.GetOffsetResponse{Exists: false}, nil
}

// JoinPartition implements the registry: index = rank in sorted active set.
func (s *stubMQ) JoinPartition(_ context.Context, req *mqpb.JoinPartitionRequest) (*mqpb.JoinPartitionResponse, error) {
	s.reMu.Lock()
	defer s.reMu.Unlock()
	s.replica[req.ConsumerId] = struct{}{}
	ids := make([]string, 0, len(s.replica))
	for id := range s.replica {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	idx := 0
	for i, id := range ids {
		if id == req.ConsumerId {
			idx = i
			break
		}
	}
	log.Printf("registry join consumer=%s index=%d/%d", req.ConsumerId, idx, len(ids))
	return &mqpb.JoinPartitionResponse{Index: int32(idx), Total: int32(len(ids))}, nil
}

func (s *stubMQ) LeavePartition(_ context.Context, req *mqpb.LeavePartitionRequest) (*mqpb.LeavePartitionResponse, error) {
	s.reMu.Lock()
	defer s.reMu.Unlock()
	delete(s.replica, req.ConsumerId)
	log.Printf("registry leave consumer=%s (active=%d)", req.ConsumerId, len(s.replica))
	return &mqpb.LeavePartitionResponse{}, nil
}

// eventJSON is the canonical snake_case JSON representation of an event,
// matching the assignment's example payload exactly (zeros included).
type eventJSON struct {
	EventID         string            `json:"event_id"`
	SourceTimestamp string            `json:"source_timestamp"` // RFC3339 nanoseconds
	MetricName      string            `json:"metric_name"`
	GPUIndex        int32             `json:"gpu_index"`
	DeviceName      string            `json:"device_name"`
	DeviceID        string            `json:"device_id"`
	ModelName       string            `json:"model_name"`
	Hostname        string            `json:"hostname"`
	Value           float64           `json:"value"`
	CSVLineOffset   int64             `json:"csv_line_offset"`
	LoopCount       int64             `json:"loop_count"`
	Tags            map[string]string `json:"tags"`
}

func (s *stubMQ) PublishEvents(stream grpc.BidiStreamingServer[mqpb.Event, mqpb.Ack]) error {
	for {
		evt, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		out := &eventJSON{
			EventID:         evt.EventId,
			SourceTimestamp: evt.SourceTimestamp,
			MetricName:      evt.MetricName,
			GPUIndex:        evt.GpuIndex,
			DeviceName:      evt.DeviceName,
			DeviceID:        evt.DeviceId,
			ModelName:       evt.ModelName,
			Hostname:        evt.Hostname,
			Value:           evt.Value,
			CSVLineOffset:   evt.CsvLineOffset,
			LoopCount:       evt.LoopCount,
			Tags:            evt.Tags,
		}
		b, _ := json.Marshal(out)
		log.Printf("RECEIVED event_id=%s -> %s", evt.EventId, b)

		s.mu.Lock()
		s.consumers[evt.Tags["pod_name"]] = evt.CsvLineOffset
		s.acked++
		s.mu.Unlock()

		if err := stream.Send(&mqpb.Ack{EventId: evt.EventId, Offset: evt.CsvLineOffset, Success: true}); err != nil {
			return err
		}
	}
}

// waitQuota blocks until maxEvents acks have been reached (if set), then
// returns so the caller can stop the gRPC server.
func (s *stubMQ) waitQuota() {
	if *maxEvents == 0 {
		return
	}
	for {
		s.mu.Lock()
		n := s.acked
		s.mu.Unlock()
		if n >= *maxEvents {
			log.Printf("acked %d events (limit reached) - shutting down", n)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("mqstub listening on %s", lis.Addr())

	srv := grpc.NewServer()
	server := newStubMQ()
	mqpb.RegisterMessageQueueServer(srv, server)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigs
		srv.GracefulStop()
	}()
	if *maxEvents > 0 {
		// self-terminate once the ack quota has been reached
		go func() {
			server.waitQuota()
			srv.Stop()
		}()
	}

	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	log.Printf("mqstub stopped")
}
