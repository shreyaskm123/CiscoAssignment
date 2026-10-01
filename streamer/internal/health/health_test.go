package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The endpoints must be distinguishable: /healthz says the process is alive,
// /readyz says it can do its job. Collapsing them would reintroduce the bug this
// replaced, where one probe answered for both questions.
func TestHealthzIsAlwaysOK(t *testing.T) {
	srv := New("", func() Check { return Check{Reason: "not ready, but that must not affect liveness"} })
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	code, body := rec.Code, rec.Body.String()
	if code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200 (a dependency must never fail liveness)", code)
	}
	var v map[string]string
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v["status"] != "ok" {
		t.Errorf("/healthz body = %v", v)
	}
}

func TestReadyzReportsNotReadyWithReason(t *testing.T) {
	s := New("",
		func() Check {
			return Check{Reason: "no shard assignment from the MQ registry yet", Fields: map[string]string{"shard": "0/0"}}
		})
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d, want 503", rec.Code)
	}
	var c Check
	if err := json.Unmarshal(rec.Body.Bytes(), &c); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if c.Ready {
		t.Error("body claims ready while the check says otherwise")
	}
	if c.Reason == "" {
		t.Error("a 503 body must carry a reason")
	}
	if c.Fields["shard"] != "0/0" {
		t.Errorf("diagnostics missing from the body: %v", c.Fields)
	}
}

func TestReadyzReportsReady(t *testing.T) {
	s := New("", func() Check { return Check{Ready: true, Fields: map[string]string{"shard": "1/3"}} })
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", rec.Code)
	}
}

func TestNilCheckIsReady(t *testing.T) {
	s := New("", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/readyz with a nil check = %d, want 200", rec.Code)
	}
}

func TestOnlyGetIsRouted(t *testing.T) {
	s := New("", func() Check { return Check{Ready: true} })
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /readyz = %d, want 405", rec.Code)
	}
}

func TestEmptyAddrDisablesServing(t *testing.T) {
	s := New("", func() Check { return Check{Ready: true} })
	// Start must return promptly rather than binding anything.
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	go func() { done <- s.Start(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start with no address returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start blocked with no address configured")
	}
}
