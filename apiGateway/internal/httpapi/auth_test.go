package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"apigateway/internal/auth"
)

func doAuthReq(t *testing.T, s *Server, path, header string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", path, nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// The API routes must reject unauthenticated callers while /healthz stays open,
// because the kubelet probe sends no Authorization header.
func TestAuthProtectsAPIButNotHealthz(t *testing.T) {
	a, err := auth.New("", "prom=tok-a")
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	srv := NewServer(&fakeStore{}, WithAuth(a))

	for _, path := range []string{"/api/v1/gpus", "/api/v1/gpus/GPU-a/telemetry"} {
		if rec := doAuthReq(t, srv, path, ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s without token: status = %d want 401", path, rec.Code)
		}
		if rec := doAuthReq(t, srv, path, "Bearer wrong"); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s wrong token: status = %d want 401", path, rec.Code)
		}
		if rec := doAuthReq(t, srv, path, "Bearer tok-a"); rec.Code != http.StatusOK {
			t.Errorf("%s valid token: status = %d want 200", path, rec.Code)
		}
	}

	if rec := doAuthReq(t, srv, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("/healthz without token: status = %d want 200 (kubelet sends no header)", rec.Code)
	}
	if rec := doAuthReq(t, srv, "/healthz", "Bearer garbage"); rec.Code != http.StatusOK {
		t.Errorf("/healthz with junk token: status = %d want 200", rec.Code)
	}
}

// Without WithAuth the API must stay open - that is the dev default.
func TestNoAuthLeavesAPIOpen(t *testing.T) {
	srv := NewServer(&fakeStore{})
	if rec := doAuthReq(t, srv, "/api/v1/gpus", ""); rec.Code != http.StatusOK {
		t.Errorf("status = %d want 200 (auth disabled)", rec.Code)
	}
}
