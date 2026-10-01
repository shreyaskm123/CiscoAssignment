package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"apigateway/internal/auth"
)

// The OpenAPI generator builds its paths from the route table (routes.go), so a
// route that is in the table but not actually served would still produce a valid
// document. These tests close that gap from the other side: the mux is probed
// with real requests, so a route registered wrongly, or served with the wrong
// method, fails here rather than in a client's hands.

// wantPatterns is the API as it is documented, written out independently of the
// table. If someone edits routes.go, this fails: either the server changed (so
// this list is stale) or the spec is about to be regenerated from the new table
// (so the edit needs a deliberate decision about the published contract).
var wantPatterns = []string{
	"GET /api/v1/gpus",
	"GET /api/v1/gpus/{id}/telemetry",
	"POST /api/v1/token",
	"GET /healthz",
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	signer, err := auth.NewSigner(strings.Repeat("ab", 32), time.Minute)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return NewServer(&fakeStore{}, WithAuth(mustAuth(t)), WithTokenSigner(signer))
}

// decode unmarshals a recorded response body, failing the test on malformed
// JSON so a handler cannot satisfy an assertion by returning something else.
func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
}

func mustAuth(t *testing.T) *auth.Authenticator {
	t.Helper()
	a, err := auth.New("", "prom=tok-a")
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	return a
}

// TestTableMatchesDocumentedPatterns keeps the route table and the documented
// API surface identical.
func TestTableMatchesDocumentedPatterns(t *testing.T) {
	got := make([]string, 0, len(wantPatterns))
	for _, r := range Routes() {
		got = append(got, r.Method+" "+r.Path)
	}
	if len(got) != len(wantPatterns) {
		t.Fatalf("route table has %d routes %v, documented API has %d %v",
			len(got), got, len(wantPatterns), wantPatterns)
	}
	for _, want := range wantPatterns {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Errorf("route %q is gone from the table; it was part of the published API", want)
		}
	}
}

// TestEveryTableRouteIsServed: each route must be matched by the live mux, with
// the right method.
func TestEveryTableRouteIsServed(t *testing.T) {
	srv := newTestServer(t)
	for _, r := range Routes() {
		probe := strings.ReplaceAll(r.Path, "{id}", "GPU-test")
		req := httptest.NewRequest(r.Method, probe, nil)
		// Authenticated routes live on the inner mux (the outer one only has the
		// /api/v1/ subtree that mounts the auth wrapper); unauthenticated ones
		// are on the outer mux directly.
		mux := srv.mux
		if r.Auth {
			mux = srv.api
		}
		_, pattern := mux.Handler(req)
		if pattern == "" {
			t.Errorf("%s %s is in the route table but the mux does not serve it", r.Method, probe)
			continue
		}
		if pattern != r.Method+" "+r.Path {
			t.Errorf("%s %s is served by pattern %q, which is a different route", r.Method, probe, pattern)
		}
	}
}

// TestUndocumentedRequestsAreRejected pins the negative cases, which is what
// keeps the spec honest in the other direction: if a stray route existed, it
// would be served but undocumented.
func TestUndocumentedRequestsAreRejected(t *testing.T) {
	srv := newTestServer(t)
	cases := []struct {
		method, path string
		want         int
		why          string
	}{
		{http.MethodGet, "/api/v1/gpus/GPU-test", http.StatusNotFound,
			"there is no per-GPU metadata endpoint; that is by design"},
		{http.MethodGet, "/api/v1/nope", http.StatusNotFound, "unknown path"},
		{http.MethodPost, "/api/v1/gpus", http.StatusMethodNotAllowed, "wrong method on a known path"},
		{http.MethodDelete, "/api/v1/gpus/GPU-test/telemetry", http.StatusMethodNotAllowed, "wrong method on a known path"},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.path, nil)
		req.Header.Set("Authorization", "Bearer tok-a")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s %s = %d, want %d (%s)", c.method, c.path, rec.Code, c.want, c.why)
		}
	}
}

// TestUnauthenticatedRoutesAreNotBehindTheWrapper: /healthz must be reachable
// with no Authorization header, because the kubelet probe sends none and
// authenticating it would restart a healthy pod.
func TestUnauthenticatedRoutesAreNotBehindTheWrapper(t *testing.T) {
	srv := newTestServer(t)
	for _, r := range Routes() {
		if r.Auth {
			continue
		}
		req := httptest.NewRequest(r.Method, r.Path, nil)
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusUnauthorized {
			t.Errorf("%s %s is registered on the auth-wrapped mux, but is meant to be unauthenticated", r.Method, r.Path)
		}
		if _, pattern := srv.mux.Handler(req); pattern == "" {
			t.Errorf("%s %s must be registered on the outer mux to be reachable without a token", r.Method, r.Path)
		}
	}
}

// TestTokenRouteAbsentWithoutSigningKey: the exchange endpoint must not exist
// when there is no key, rather than existing and failing.
func TestTokenRouteAbsentWithoutSigningKey(t *testing.T) {
	srv := NewServer(&fakeStore{}, WithAuth(mustAuth(t)))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/token", nil)
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/v1/token without a signing key = %d, want 404 (the route must not exist)", rec.Code)
	}
	for _, r := range Routes() {
		if r.Op == OpIssueToken && (r.Enabled == nil || r.Enabled(srv)) {
			t.Error("the token route claims to be enabled on a server with no signer")
		}
	}
}

// TestResponseBodiesMatchTheirTypes: the bodies the handlers write are the ones
// the spec describes, and the nullable/omitted fields behave as declared.
func TestResponseBodiesMatchTheirTypes(t *testing.T) {
	t.Run("gpus", func(t *testing.T) {
		rec := doReq(t, &fakeStore{gpus: nil}, "GET", "/api/v1/gpus")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body map[string]any
		decode(t, rec, &body)
		for _, k := range []string{"gpus", "count", "total", "limit", "offset", "order", "next"} {
			if _, ok := body[k]; !ok {
				t.Errorf("missing documented field %q", k)
			}
		}
		if body["next"] != nil {
			t.Errorf("next = %v, want null on the last page", body["next"])
		}
	})

	t.Run("telemetry window echoed", func(t *testing.T) {
		rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET",
			"/api/v1/gpus/GPU-a/telemetry?start_time=2025-07-18T20:42:34Z&end_time=2025-07-18T20:43:00Z")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", rec.Code, rec.Body)
		}
		var body map[string]any
		decode(t, rec, &body)
		if body["start_time"] == nil || body["end_time"] == nil {
			t.Errorf("window bounds must be echoed when supplied, got %v", body)
		}
		if body["gpu_id"] != "GPU-a" {
			t.Errorf("gpu_id = %v, want GPU-a", body["gpu_id"])
		}
	})

	t.Run("telemetry window absent when unbounded", func(t *testing.T) {
		rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET", "/api/v1/gpus/GPU-a/telemetry")
		var body map[string]any
		decode(t, rec, &body)
		for _, k := range []string{"start_time", "end_time"} {
			if _, ok := body[k]; ok {
				t.Errorf("%s must be omitted when the query had no bound, got %v", k, body[k])
			}
		}
	})

	t.Run("error body", func(t *testing.T) {
		rec := doReq(t, &fakeStore{}, "GET", "/api/v1/gpus/GPU-a/telemetry?limit=abc")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d", rec.Code)
		}
		var body ErrorResponse
		decode(t, rec, &body)
		if body.Error == "" {
			t.Error("error body has no message")
		}
	})

	t.Run("health body", func(t *testing.T) {
		rec := doReq(t, &fakeStore{}, "GET", "/healthz")
		var body HealthResponse
		decode(t, rec, &body)
		if body.Status != "ok" {
			t.Errorf("status = %q", body.Status)
		}
	})
}
