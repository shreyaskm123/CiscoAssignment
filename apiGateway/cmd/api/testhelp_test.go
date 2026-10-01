package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"apigateway/internal/httpapi"
	"apigateway/internal/store"
)

// stubStore satisfies store.Store with empty results. These tests are about auth
// wiring, so the handlers must reach the store and get nothing back - which is
// exactly what a real empty telemetry database looks like.
type stubStore struct{}

func (stubStore) ListGPUs(context.Context, store.Page) ([]store.GPU, int64, bool, error) {
	return nil, 0, false, nil
}
func (stubStore) Telemetry(context.Context, string, *time.Time, *time.Time, store.Page) ([]store.Telemetry, int64, bool, error) {
	return nil, 0, false, nil
}
func (stubStore) Ping(context.Context) error { return nil }

// newServerForTest builds a Server wired exactly as main() does, so a test can
// catch a mis-wiring that unit tests of the auth package cannot see.
func newServerForTest(t *testing.T, opts ...httpapi.Option) http.Handler {
	t.Helper()
	return httpapi.NewServer(stubStore{}, opts...).Handler()
}
