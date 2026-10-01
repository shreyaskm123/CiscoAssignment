package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const goodKey = "abababababababababababababababababababababababababababababababab"

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// Regression test for a real deployment bug: the server options were built
// before the authenticator was re-wired with the signer, so the API served
// POST /api/v1/token and then returned 401 for every token it had just issued.
// The auth and httpapi packages both had passing tests throughout - only the
// wiring in main was wrong, so it is the wiring that has to be tested.
func TestSetupAuthWiresSignerIntoTheServer(t *testing.T) {
	_, opts, err := setupAuth(envFrom(map[string]string{
		"API_TOKENS":            "shreyas=static-tok",
		"API_TOKEN_SIGNING_KEY": goodKey,
		"API_TOKEN_TTL":         "10m",
	}))
	if err != nil {
		t.Fatalf("setupAuth: %v", err)
	}
	srv := newServerForTest(t, opts...)

	// exchange
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/token", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated exchange = %d, want 401", rec.Code)
	}
	req := httptest.NewRequest("POST", "/api/v1/token", nil)
	req.Header.Set("Authorization", "Bearer static-tok")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("exchange = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// the issued token must actually authenticate - this is the assertion that
	// would have caught the bug
	use := httptest.NewRequest("GET", "/api/v1/gpus", nil)
	use.Header.Set("Authorization", "Bearer "+body.AccessToken)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, use)
	if rec.Code != http.StatusOK {
		t.Fatalf("access token was rejected with %d - signer not wired into the Authenticator", rec.Code)
	}
}

func TestSetupAuthWithoutSigningKeyLeavesExchangeOff(t *testing.T) {
	_, opts, err := setupAuth(envFrom(map[string]string{"API_TOKENS": "shreyas=static-tok"}))
	if err != nil {
		t.Fatalf("setupAuth: %v", err)
	}
	srv := newServerForTest(t, opts...)
	req := httptest.NewRequest("POST", "/api/v1/token", nil)
	req.Header.Set("Authorization", "Bearer static-tok")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatal("exchange endpoint is served with no signing key configured")
	}
}

func TestSetupAuthRejectsBadConfig(t *testing.T) {
	cases := map[string]map[string]string{
		"malformed tokens": {"API_TOKENS": "no-equals-sign"},
		"malformed key":    {"API_TOKENS": "a=b", "API_TOKEN_SIGNING_KEY": "nothex"},
		"short key":        {"API_TOKENS": "a=b", "API_TOKEN_SIGNING_KEY": strings.Repeat("ab", 8)},
		"malformed ttl":    {"API_TOKENS": "a=b", "API_TOKEN_SIGNING_KEY": goodKey, "API_TOKEN_TTL": "fifteen minutes"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := setupAuth(envFrom(env)); err == nil {
				t.Fatalf("setupAuth(%v) accepted a bad config", env)
			}
		})
	}
}

func TestSetupAuthDefaultsTTLWhenUnset(t *testing.T) {
	_, opts, err := setupAuth(envFrom(map[string]string{
		"API_TOKENS":            "shreyas=static-tok",
		"API_TOKEN_SIGNING_KEY": goodKey,
	}))
	if err != nil {
		t.Fatalf("setupAuth: %v", err)
	}
	srv := newServerForTest(t, opts...)
	req := httptest.NewRequest("POST", "/api/v1/token", nil)
	req.Header.Set("Authorization", "Bearer static-tok")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	var body struct {
		ExpiresIn int64 `json:"expires_in"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ExpiresIn != int64((15 * time.Minute).Seconds()) {
		t.Fatalf("expires_in = %d, want the 15m default (%d)", body.ExpiresIn, int64((15 * time.Minute).Seconds()))
	}
}
