// Command collector consumes the MQ event log shard owned by this replica
// (offset % total == index) and writes it to ClickHouse (or, with SINK=stdout,
// prints a receipt per event for lab runs). It coexists with the streamer: the
// MQ registry gives each fleet its own group's indices, so any number of
// collector replicas can share the same message queue with the publishers.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"collector/internal/app"
	"collector/internal/config"
	"collector/internal/health"
	"collector/internal/mqclient"
	"collector/internal/sink"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mq, err := mqclient.Dial(ctx, cfg.MQAddr, mqAuthOptions(cfg.MQTokenFile, cfg.MQToken)...)
	if err != nil {
		log.Fatalf("mq: %v", err)
	}
	defer mq.Close()

	var wr sink.Writer
	switch cfg.Sink {
	case config.SinkStdout:
		wr = sink.NewStdout()
	default:
		ch, err := sink.OpenClickHouse(ctx, cfg.ClickHouse.Host, cfg.ClickHouse.Port,
			cfg.ClickHouse.User, cfg.ClickHouse.Password, cfg.ClickHouse.DB, cfg.ClickHouse.Table, cfg.ClickHouse.Cluster,
			cfg.DedupPreCheck)
		if err != nil {
			log.Fatalf("clickhouse: %v", err)
		}
		defer ch.Close()
		wr = ch
	}

	col := app.New(cfg, mq, wr)

	// Probes. Started before Run so the pod reports NotReady (no shard
	// assignment yet) rather than looking healthy while it is still waiting for
	// the registry.
	hs := health.New(cfg.HealthAddr, col.Status)
	go func() {
		if err := hs.Start(ctx); err != nil {
			log.Printf("health server: %v", err)
		}
	}()

	if err := col.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("collector: %v", err)
	}
	log.Printf("collector exited cleanly")
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
