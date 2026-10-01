// Package auth implements shared-secret bearer-token authentication for the
// telemetry API.
//
// Credentials are a comma-separated list of identity=token pairs, for example
//
//	API_TOKENS="prom=9f2c...,shreyas=4b81..."
//
// One token per client (never one global token) so that access can be audited
// per identity and revoked per client without breaking the other callers.
//
// Tokens may instead be loaded from a file (API_TOKENS_FILE), which is what
// makes rotation possible without a pod restart: Kubernetes refreshes a
// mounted Secret volume in place, so re-reading the file picks up a new Secret
// with no downtime and no redeploy. Because the format is a *list*, a rotation
// can overlap the old and new token for the same identity - write both, let
// clients switch, then drop the old one - so no caller ever sees a 401.
package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
)

// credential is one caller's identity and its bearer token.
type credential struct {
	identity string
	token    string
}

// Authenticator validates bearer tokens against a set of credentials. A nil
// *Authenticator is valid and means "auth disabled", so callers can pass the
// result of a build step straight through without nil checks.
type Authenticator struct {
	path string
	mu   sync.Mutex
	// creds is the credential set currently in force. It starts as whatever was
	// parsed from the inline spec and is replaced by each successful read of
	// path, so a transient read/parse failure leaves the last good set serving
	// rather than locking every caller out.
	creds []credential
	// signer, when non-nil, additionally accepts short-lived signed access
	// tokens. Static shared secrets keep working unchanged, so adding signed
	// tokens does not force existing callers to migrate.
	signer *Signer
}

// WithSigner returns an Authenticator that also accepts signed access tokens.
// It builds a fresh Authenticator rather than copying the receiver: the
// receiver holds a sync.Mutex, and copying one is a bug (go vet reports
// "assignment copies lock value"). The original is left usable.
func (a *Authenticator) WithSigner(s *Signer) *Authenticator {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return &Authenticator{path: a.path, creds: a.creds, signer: s}
}

// New builds an Authenticator. Exactly one of path (a file to re-read on every
// request) or spec (an inline list) is normally used; spec acts as the
// bootstrap value when path has not been read successfully yet. A malformed
// spec is an error so that a typo fails the process at startup instead of
// silently disabling or weakening auth.
func New(path, spec string) (*Authenticator, error) {
	creds, err := parseSpec(spec)
	if err != nil {
		return nil, err
	}
	return &Authenticator{path: path, creds: creds}, nil
}

// Enabled reports whether any credential is configured. Callers log a warning
// when it is false: an empty token set means every request is allowed. It
// consults the effective credential set, so a token file counts as enabled -
// the startup log must never claim auth is off while the middleware is
// enforcing it (or the reverse).
func (a *Authenticator) Enabled() bool {
	return a != nil && len(a.tokens()) > 0
}

// tokens returns the credentials currently in force, re-reading the token file
// when one is configured.
func (a *Authenticator) tokens() []credential {
	if a.path == "" {
		return a.creds
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	creds, err := parseSpec(readFile(a.path))
	if err == nil && len(creds) > 0 {
		a.creds = creds
	}
	return a.creds
}

// Credential records which kind of secret authenticated a request. It matters
// for the token exchange: a short-lived access token must not be able to mint
// its own replacement, or "expires in 15m" means nothing - a leaked token
// renews itself forever.
type Credential int

const (
	// CredentialNone means the request was not authenticated.
	CredentialNone Credential = iota
	// CredentialStatic is a long-lived shared secret from the token file.
	CredentialStatic
	// CredentialSigned is a short-lived HMAC access token.
	CredentialSigned
)

func (c Credential) String() string {
	switch c {
	case CredentialStatic:
		return "static"
	case CredentialSigned:
		return "signed"
	default:
		return "none"
	}
}

// Identify matches a presented token and returns the owning identity.
func (a *Authenticator) Identify(presented string) (string, bool) {
	identity, _, ok := a.IdentifyKind(presented)
	return identity, ok
}

// IdentifyKind is Identify plus the kind of credential that matched, so that a
// handler can treat static and signed tokens differently.
//
// Every credential is compared on every call - no early return - so response
// time does not reveal which token matched or how many are configured. The
// static set is checked first so the common in-cluster path costs nothing
// extra; only a token shaped like a signed token reaches the signer.
func (a *Authenticator) IdentifyKind(presented string) (string, Credential, bool) {
	if a == nil {
		return "", CredentialNone, true
	}
	identity, ok := "", false
	for _, c := range a.tokens() {
		if subtle.ConstantTimeCompare([]byte(presented), []byte(c.token)) == 1 {
			identity, ok = c.identity, true
		}
	}
	if ok {
		return identity, CredentialStatic, true
	}
	if a.signer != nil && LooksSigned(presented) {
		if sub, err := a.signer.Verify(presented); err == nil {
			return sub, CredentialSigned, true
		}
	}
	return "", CredentialNone, false
}

// Middleware rejects requests that do not carry a valid bearer token. A nil
// Authenticator passes traffic through unchanged (auth disabled).
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	if a == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r.Header.Get("Authorization"))
		identity, kind, valid := a.IdentifyKind(presented)
		if !ok || !valid {
			w.Header().Set("WWW-Authenticate", `Bearer realm="telemetry"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "unauthorized"})
			return
		}
		ctx := withIdentity(r.Context(), identity)
		ctx = withCredential(ctx, kind)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken extracts the token from an `Authorization: Bearer <token>`
// header. The scheme match is case-insensitive per RFC 7235.
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(strings.TrimSpace(header), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

type identityKey struct{}
type credentialKey struct{}

func withIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

func withCredential(ctx context.Context, kind Credential) context.Context {
	return context.WithValue(ctx, credentialKey{}, kind)
}

// IdentityFrom returns the authenticated caller's identity for a request that
// passed through Middleware, or "" when the request was not authenticated.
func IdentityFrom(ctx context.Context) string {
	id, _ := ctx.Value(identityKey{}).(string)
	return id
}

// CredentialFrom returns which kind of secret authenticated the request. It
// defaults to CredentialStatic for a request that did not come through
// Middleware, so a handler that forgets to check cannot accidentally reject
// every caller.
func CredentialFrom(ctx context.Context) Credential {
	c, ok := ctx.Value(credentialKey{}).(Credential)
	if !ok {
		return CredentialStatic
	}
	return c
}

// parseSpec parses "identity=token[,identity=token]..." entries. Entries
// without a token are rejected: a silently ignored entry is a credential the
// operator believes is in force but is not.
func parseSpec(spec string) ([]credential, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	var creds []credential
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		identity, token, found := strings.Cut(entry, "=")
		identity, token = strings.TrimSpace(identity), strings.TrimSpace(token)
		if !found || identity == "" || token == "" {
			return nil, fmt.Errorf("malformed credential %q: want identity=token", entry)
		}
		creds = append(creds, credential{identity: identity, token: token})
	}
	return creds, nil
}

// readFile reads the token file, tolerating a trailing newline written by
// editors and by `kubectl create secret --from-file`.
func readFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
