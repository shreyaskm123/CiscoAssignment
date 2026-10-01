package auth

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIdentify(t *testing.T) {
	a, err := New("", "prom=tok-a,shreyas=tok-b")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, tc := range []struct{ token, want string }{
		{"tok-a", "prom"},
		{"tok-b", "shreyas"},
		{"", ""},
		{"tok", ""},
		{"tok-a-extra", ""},
		{"TOK-A", ""},
	} {
		got, ok := a.Identify(tc.token)
		if want := tc.want != ""; ok != want {
			t.Errorf("Identify(%q) ok = %v want %v", tc.token, ok, want)
		}
		if ok && got != tc.want {
			t.Errorf("Identify(%q) = %q want %q", tc.token, got, tc.want)
		}
	}
}

func TestNewRejectsMalformedSpec(t *testing.T) {
	for _, spec := range []string{"prom", "=tok", "prom=", "ok=1,bad"} {
		if _, err := New("", spec); err == nil {
			t.Errorf("New(%q) = nil error, want error", spec)
		}
	}
}

func TestNilAuthenticatorIsPassthrough(t *testing.T) {
	var a *Authenticator
	if a.Enabled() {
		t.Error("nil authenticator should report disabled")
	}
	if _, ok := a.Identify("anything"); !ok {
		t.Error("nil authenticator should allow")
	}
	rec := httptest.NewRecorder()
	a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/gpus", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d want 200 (auth disabled)", rec.Code)
	}
}

func TestMiddleware(t *testing.T) {
	a, err := New("", "prom=tok-a")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var gotID string
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = IdentityFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	for _, tc := range []struct {
		name, header string
		want         int
	}{
		{"valid", "Bearer tok-a", http.StatusOK},
		{"lowercase scheme", "bearer tok-a", http.StatusOK},
		{"wrong token", "Bearer nope", http.StatusUnauthorized},
		{"missing header", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic tok-a", http.StatusUnauthorized},
		{"empty bearer", "Bearer ", http.StatusUnauthorized},
		{"identity in header", "Bearer prom", http.StatusUnauthorized},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d want %d", tc.name, rec.Code, tc.want)
		}
		if tc.want == http.StatusOK && gotID != "prom" {
			t.Errorf("%s: identity = %q want prom", tc.name, gotID)
		}
		if rec.Code == http.StatusUnauthorized {
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("%s: missing WWW-Authenticate", tc.name)
			}
			if !strings.Contains(rec.Body.String(), "unauthorized") {
				t.Errorf("%s: body = %q", tc.name, rec.Body.String())
			}
		}
	}
}

// A token file is re-read on every request so a rotated Secret takes effect
// without a restart, and a transient read failure keeps the last good set
// serving instead of rejecting everyone.
// Enabled must reflect the token file, not just the inline spec: a false
// "disabled" report would log that an enforced API is open (and vice versa).
func TestEnabledTracksFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens")
	a, err := New(path, "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.Enabled() {
		t.Error("Enabled = true before any token is available")
	}
	if err := os.WriteFile(path, []byte("prom=tok-a"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !a.Enabled() {
		t.Error("Enabled = false with a populated token file")
	}
}

func TestFileReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(path, []byte("prom=old-token\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	a, err := New(path, "prom=bootstrap-token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := a.Identify("old-token"); !ok {
		t.Error("file token should be active after reload")
	}
	if err := os.WriteFile(path, []byte("prom=new-token,shreyas=second\n"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if _, ok := a.Identify("old-token"); ok {
		t.Error("old token should stop working after rotation")
	}
	if id, ok := a.Identify("second"); !ok || id != "shreyas" {
		t.Errorf("Identify(second) = %q,%v want shreyas,true", id, ok)
	}
	if _, ok := a.Identify("bootstrap-token"); ok {
		t.Error("bootstrap token should be superseded by the file")
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, ok := a.Identify("new-token"); !ok {
		t.Error("last good set should survive a transient read failure")
	}
}
