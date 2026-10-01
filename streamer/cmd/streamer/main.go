// Command streamer reads the configMap-mounted DCGM metrics CSV, derives its
// replica partition (index, total) from the MQ partition registry, and
// publishes only the rows it owns (currentLineNum % total == index) as
// real-time events to the message queue in the looped iteration model
// described by the assignment.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"streamer/internal/app"
	"streamer/internal/config"
	"streamer/internal/csvsrc"
	"streamer/internal/health"
	"streamer/internal/mqclient"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	log.Printf("streamer starting pod=%s consumer=%s", cfg.PodName, cfg.ConsumerID)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reader, err := csvsrc.Open(cfg.CSVPath)
	if err != nil {
		log.Printf("csv open failed (will retry in background): %v", err)
	}

	client, err := mqclient.Dial(ctx, cfg.MQAddr, mqAuthOptions(cfg.MQTokenFile, cfg.MQToken)...)
	if err != nil {
		log.Fatalf("dial mq %s: %v", cfg.MQAddr, err)
	}
	defer client.Close()

	if reader == nil {
		reader, err = openWithRetry(ctx, cfg.CSVPath)
		if err != nil {
			log.Fatalf("open csv %s: %v", cfg.CSVPath, err)
		}
	}

	streamer, err := app.New(ctx, cfg, reader, client)
	if err != nil {
		log.Fatalf("streamer init: %v", err)
	}

	// Probes. Started before Run so the pod reports NotReady until it has a
	// partition assignment and has had a publish acknowledged.
	hs := health.New(cfg.HealthAddr, streamer.Status)
	go func() {
		if err := hs.Start(ctx); err != nil {
			log.Printf("health server: %v", err)
		}
	}()

	if err := streamer.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			// Shutdown was triggered by SIGTERM/SIGINT after a successful
			// drain: that is a normal, graceful stop, not a failure.
			log.Printf("streamer exited: %v", err)
			return
		}
		log.Fatal(err)
	}
}

// openWithRetry keeps trying to open the CSV so the pod tolerates a
// configMap that is not yet mounted at container start.
func openWithRetry(ctx context.Context, path string) (*csvsrc.Reader, error) {
	for {
		r, err := csvsrc.Open(path)
		if err == nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			log.Printf("retrying csv open in 1s: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// mqAuthOptions builds the per-RPC credentials for the MQ. A file-based token is
// preferred because gRPC re-reads it on every call, which is what makes rotation
// seamless; an inline token is the local-dev fallback.
func mqAuthOptions(tokenFile, token string) []mqclient.Option {
	var opts []mqclient.Option
	if tokenFile != "" {
		opts = append(opts, mqclient.WithTokenFile(tokenFile))
	}
	if token != "" {
		opts = append(opts, mqclient.WithToken(token))
	}
	return opts
}
