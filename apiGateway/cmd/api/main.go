// Command api exposes the telemetry query API (read-only over the ClickHouse
// events table). Run with a collector already pointed at the same
// CLICKHOUSE_DB/CLICKHOUSE_TABLE so the schema exists.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"apigateway/internal/auth"
	"apigateway/internal/httpapi"
	"apigateway/internal/store"
)

// setupAuth builds the authenticator and the server options from the
// environment. It is a separate function, and takes getenv as a parameter, so
// that the wiring can be tested: a mis-wired authenticator produces an API
// that issues tokens and then rejects them, which no unit test of the auth
// package alone would catch.
func setupAuth(getenv func(string) string) (*auth.Authenticator, []httpapi.Option, error) {
	// A malformed spec is an error: silently starting with no credentials would
	// expose the API to anyone who can reach it.
	authn, err := auth.New(getenv("API_TOKENS_FILE"), getenv("API_TOKENS"))
	if err != nil {
		return nil, nil, fmt.Errorf("api auth: %w", err)
	}
	if !authn.Enabled() {
		log.Printf("WARNING: API auth is DISABLED (set API_TOKENS_FILE or API_TOKENS); " +
			"every /api/v1 request is accepted without a token")
	}

	// Signed access tokens are optional. With no signing key the exchange
	// endpoint is not registered at all, and static tokens are the only way in.
	// A key that is set but malformed IS an error: ignoring it would look like
	// the feature was never enabled.
	var signer *auth.Signer
	if key := getenv("API_TOKEN_SIGNING_KEY"); key != "" {
		ttlRaw := getenv("API_TOKEN_TTL")
		if ttlRaw == "" {
			ttlRaw = auth.TokenTTL.String()
		}
		ttl, err := time.ParseDuration(ttlRaw)
		if err != nil {
			return nil, nil, fmt.Errorf("API_TOKEN_TTL: %w", err)
		}
		signer, err = auth.NewSigner(key, ttl)
		if err != nil {
			return nil, nil, fmt.Errorf("API_TOKEN_SIGNING_KEY: %w", err)
		}
		// The same signer must both issue and verify, so give the Authenticator
		// a copy wired to it.
		authn = authn.WithSigner(signer)
		log.Printf("token exchange enabled: POST /api/v1/token, access tokens live %s", signer.TTL())
	}

	// Build the options only now. Capturing authn into the slice before the
	// reassignment above would silently hand the server the pre-signer
	// Authenticator: the exchange endpoint would work and then reject every
	// token it had just issued.
	opts := []httpapi.Option{httpapi.WithAuth(authn)}
	if signer != nil {
		opts = append(opts, httpapi.WithTokenSigner(signer))
	}
	return authn, opts, nil
}

func main() {
	addr := envOr("HTTP_ADDR", ":8080")
	host := envOr("CLICKHOUSE_HOST", "localhost")
	port := envInt("CLICKHOUSE_PORT", 9000)
	user := envOr("CLICKHOUSE_USER", "default")
	pass := os.Getenv("CLICKHOUSE_PASSWORD")
	db := envOr("CLICKHOUSE_DB", "telemetry")
	table := envOr("CLICKHOUSE_TABLE", "events")

	_, opts, err := setupAuth(os.Getenv)
	if err != nil {
		log.Fatalf("%v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ch, err := store.Open(ctx, host, port, user, pass, db, table)
	if err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	defer ch.Close()
	log.Printf("serving %s (db=%s table=%s)", addr, db, table)

	if err := httpapi.ServeBlocking(ctx, addr, ch, opts...); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("http: %v", err)
	}
	log.Printf("api exited cleanly")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
