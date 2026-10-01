package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"apigateway/internal/auth"
)

const testSigningKey = "abababababababababababababababababababababababababababababababab"

// signedServer builds a Server with static auth plus signed-token support,
// which is how main.go wires it in production.
func signedServer(t *testing.T, spec string) (*Server, *auth.Signer) {
	t.Helper()
	base, err := auth.New("", spec)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	signer, err := auth.NewSigner(testSigningKey, 10*time.Minute)
	if err != nil {
		t.Fatalf("auth.NewSigner: %v", err)
	}
	return NewServer(&fakeStore{}, WithAuth(base.WithSigner(signer)), WithTokenSigner(signer)), signer
}

func postToken(t *testing.T, s *Server, header string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/token", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// The whole point of the endpoint: exchange the long-lived credential for a
// short-lived one, then use the short-lived one against a normal API route.
func TestTokenExchangeThenUseAccessToken(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")

	rec := postToken(t, srv, "Bearer static-tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/token = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got TokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode token response: %v", err)
	}
	if got.AccessToken == "" {
		t.Fatal("response carried an empty access_token")
	}
	if got.TokenType != "Bearer" {
		t.Errorf("token_type = %q, want Bearer", got.TokenType)
	}
	if got.Identity != "shreyas" {
		t.Errorf("identity = %q, want shreyas", got.Identity)
	}
	if got.ExpiresIn != int64((10 * time.Minute).Seconds()) {
		t.Errorf("expires_in = %d, want %d", got.ExpiresIn, int64((10 * time.Minute).Seconds()))
	}
	if _, err := time.Parse(time.RFC3339, got.ExpiresAt); err != nil {
		t.Errorf("expires_at %q is not RFC3339: %v", got.ExpiresAt, err)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}

	// The issued token must authenticate a normal API route...
	if r := doAuthReq(t, srv, "/api/v1/gpus", "Bearer "+got.AccessToken); r.Code != http.StatusOK {
		t.Errorf("GET /api/v1/gpus with access token = %d, want 200", r.Code)
	}
	// ...and the static credential must keep working, so no existing caller
	// has to migrate.
	if r := doAuthReq(t, srv, "/api/v1/gpus", "Bearer static-tok"); r.Code != http.StatusOK {
		t.Errorf("GET /api/v1/gpus with static token = %d, want 200", r.Code)
	}
}

// Regression test. The exchange used to sit behind the ordinary auth
// middleware, which accepts signed access tokens on every route - so an access
// token could POST /api/v1/token and receive a fresh one. A leaked 15-minute
// token could then renew itself forever, making the expiry meaningless. Only
// the long-lived static credential may exchange.
func TestAccessTokenCannotRefreshItself(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")

	rec := postToken(t, srv, "Bearer static-tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("static exchange = %d, want 200", rec.Code)
	}
	var first TokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The access token works on a normal route...
	if r := doAuthReq(t, srv, "/api/v1/gpus", "Bearer "+first.AccessToken); r.Code != http.StatusOK {
		t.Fatalf("access token on /api/v1/gpus = %d, want 200", r.Code)
	}
	// ...but must NOT be able to exchange itself for another.
	again := postToken(t, srv, "Bearer "+first.AccessToken)
	if again.Code != http.StatusUnauthorized {
		t.Fatalf("exchange with an access token = %d, want 401: %s",
			again.Code, again.Body.String())
	}
	if strings.Contains(again.Body.String(), "access_token") {
		t.Error("a rejected self-refresh response still contained a token")
	}

	// The static credential can still refresh, which is the whole point.
	if rec := postToken(t, srv, "Bearer static-tok"); rec.Code != http.StatusOK {
		t.Errorf("static exchange after a rejected self-refresh = %d, want 200", rec.Code)
	}
}

func TestTokenExchangeRequiresAuth(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")
	for name, hdr := range map[string]string{
		"no header":   "",
		"wrong token": "Bearer nope",
		"wrong ident": "Bearer other=tok",
	} {
		t.Run(name, func(t *testing.T) {
			if rec := postToken(t, srv, hdr); rec.Code != http.StatusUnauthorized {
				t.Fatalf("POST /api/v1/token = %d, want 401: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// A tampered access token must not authenticate, even though it is
// well-formed and unexpired.
func TestTamperedAccessTokenRejected(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")
	rec := postToken(t, srv, "Bearer static-tok")
	var got TokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	payload, sig, _ := strings.Cut(got.AccessToken, ".")
	tampered := payload + "." + flipLast(sig)
	if r := doAuthReq(t, srv, "/api/v1/gpus", "Bearer "+tampered); r.Code != http.StatusUnauthorized {
		t.Fatalf("tampered token = %d, want 401", r.Code)
	}
}

// flipLast mutates the signature so it must fail verification.
//
// It flips a bit in the middle of the decoded bytes rather than editing the
// last base64 character. A 43-character base64 string carries 258 bits for 256
// bits of data, so its final character only has 4 meaningful bits: roughly 8%
// of signatures have an aliased last character, and editing it yields a string
// that decodes to byte-identical data. The test then failed intermittently while
// the auth code was in fact correct.
func flipLast(s string) string {
	if s == "" {
		return "x"
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) == 0 {
		return s + "x"
	}
	// Flip the low bit of a middle byte: guaranteed to change the decoded
	// signature, and still a well-formed token string.
	raw[len(raw)/2] ^= 0x01
	return base64.RawURLEncoding.EncodeToString(raw)
}

// Without a signing key the route must not exist at all, rather than exist and
// fail: a 404 tells a client the feature is unavailable, a 500 would look like
// a server fault.
func TestTokenRouteAbsentWithoutSigner(t *testing.T) {
	base, err := auth.New("", "shreyas=static-tok")
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	srv := NewServer(&fakeStore{}, WithAuth(base))
	rec := postToken(t, srv, "Bearer static-tok")
	if rec.Code == http.StatusOK {
		t.Fatalf("POST /api/v1/token succeeded with no signer configured: %s", rec.Body.String())
	}
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/v1/token = %d, want 404/405", rec.Code)
	}
}

// Each exchange must yield a distinct token, so a caller cannot be identified
// by token equality and one leaked token does not imply a stable one.
func TestTokenExchangeIssuesDistinctTokens(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		rec := postToken(t, srv, "Bearer static-tok")
		var got TokenResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if seen[got.AccessToken] {
			t.Fatal("the exchange returned a duplicate access token")
		}
		seen[got.AccessToken] = true
	}
}

// The response must not leak the signing key in any form.
func TestTokenResponseHasNoKeyMaterial(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")
	rec := postToken(t, srv, "Bearer static-tok")
	if strings.Contains(rec.Body.String(), testSigningKey) {
		t.Fatal("token response contains the signing key")
	}
	// Nor should the static credential appear in the body.
	if strings.Contains(rec.Body.String(), "static-tok") {
		t.Fatal("token response echoes the static credential")
	}
}

// Health must stay unauthenticated: the exchange endpoint must not change that.
func TestHealthzStillOpenWithSigner(t *testing.T) {
	srv, _ := signedServer(t, "shreyas=static-tok")
	if rec := doAuthReq(t, srv, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rec.Code)
	}
}
