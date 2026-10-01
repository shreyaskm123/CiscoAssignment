// Command authcheck is an operational diagnostic for a RUNNING messagequeue: it
// proves that an unauthenticated client is refused while an authenticated one is
// served, on both the unary and the bidirectional-stream path.
//
//	go run ./cmd/authcheck 127.0.0.1:50061 /path/to/streamer-token
//
// Every RPC should report Unauthenticated for the first two probes and OK for
// the token files you pass. It is safe to run against production: the probes it
// publishes are ordinary events on a throwaway consumer id.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mqpb "streamer/proto"

	"messagequeue/internal/auth"
)

func try(label, addr, token string) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(auth.TokenCredentials{Token: token}))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		fmt.Printf("%-22s dial error: %v\n", label, err)
		return
	}
	defer conn.Close()
	c := mqpb.NewMessageQueueClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Unary: GetOffset
	_, uerr := c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "probe", Group: "collector"})

	// Stream: PublishEvents (the write path into the whole pipeline)
	stream, serr := c.PublishEvents(ctx)
	if serr == nil {
		serr = stream.Send(&mqpb.Event{EventId: "probe-" + label, CsvLineOffset: 1, MetricName: "m", Value: 1})
		if serr == nil || serr == io.EOF {
			_, serr = stream.Recv()
		}
	}
	fmt.Printf("%-22s unary=%-18s stream=%s\n", label, status.Code(uerr), status.Code(serr))
}

func main() {
	addr := os.Args[1]
	try("no-token", addr, "")
	try("wrong-token", addr, "definitely-not-valid")
	for _, f := range os.Args[2:] {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Printf("read %s: %v\n", f, err)
			continue
		}
		try("valid:"+f, addr, string(b))
	}
}
