// Package auth implements shared-secret bearer-token authentication for the
// MessageQueue gRPC surface.
//
// It guards BOTH registered services - MessageQueue (streamer publishes,
// collector consumes) and Replication (follower syncs from the leader). That
// second one matters: an unguarded Replication service would let anything that
// can reach the port inject fabricated WAL frames, which is a strictly worse
// failure than an open publish path.
//
// Credentials are a comma-separated list of identity=token pairs:
//
//	MQ_AUTH_TOKENS="streamer=a1b2...,collector=c3d4..."
//
// One token per client so access is attributable and revocable per client.
// Tokens may be loaded from a file (MQ_AUTH_TOKENS_FILE) so a rotated
// Kubernetes Secret is picked up without a restart.
//
// Tokens are read from the environment rather than a command-line flag on
// purpose: command-line arguments are visible to every process in the
// container via /proc and `ps`.
package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type credential struct {
	identity string
	token    string
}

// Authenticator validates bearer tokens on incoming gRPC calls. A nil
// *Authenticator is valid and means "auth disabled".
type Authenticator struct {
	path  string
	mu    sync.Mutex
	creds []credential
}

// New builds an Authenticator. A malformed spec is an error so a typo fails
// startup instead of silently leaving the queue open.
func New(path, spec string) (*Authenticator, error) {
	creds, err := parseSpec(spec)
	if err != nil {
		return nil, err
	}
	return &Authenticator{path: path, creds: creds}, nil
}

// Enabled reports whether any credential is in force. It consults the token
// file when one is configured, so the startup log never claims auth is off
// while the interceptors are enforcing it.
func (a *Authenticator) Enabled() bool {
	return a != nil && len(a.active()) > 0
}

func (a *Authenticator) active() []credential {
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

// identify matches a presented token. Every credential is compared on every
// call - no early return - so timing does not reveal which token matched.
func (a *Authenticator) identify(presented string) (string, bool) {
	if a == nil || !a.Enabled() {
		return "", true
	}
	identity, ok := "", false
	for _, c := range a.active() {
		if subtle.ConstantTimeCompare([]byte(presented), []byte(c.token)) == 1 {
			identity, ok = c.identity, true
		}
	}
	return identity, ok
}

type identityKey struct{}

// IdentityFrom returns the authenticated caller for a request that passed the
// interceptors, or "" if auth is disabled.
func IdentityFrom(ctx context.Context) string {
	id, _ := ctx.Value(identityKey{}).(string)
	return id
}

// authorize returns a context carrying the caller's identity, or
// codes.Unauthenticated. A disabled Authenticator allows the call through.
func (a *Authenticator) authorize(ctx context.Context) (context.Context, error) {
	if a == nil || !a.Enabled() {
		return ctx, nil
	}
	presented := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			presented = bearer(v[0])
		}
	}
	identity, ok := a.identify(presented)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "a valid bearer token is required")
	}
	return context.WithValue(ctx, identityKey{}, identity), nil
}

// UnaryServerInterceptor rejects unauthenticated unary calls.
func (a *Authenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authed, err := a.authorize(ctx)
		if err != nil {
			return nil, err
		}
		return handler(authed, req)
	}
}

// StreamServerInterceptor rejects unauthenticated streams. This is the critical
// one: PublishEvents and Consume are both bidirectional streams, so a unary-only
// interceptor would leave the entire data path open.
func (a *Authenticator) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		authed, err := a.authorize(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &contextStream{ServerStream: ss, ctx: authed})
	}
}

// contextStream overrides Context so the handler sees the authenticated
// context rather than the raw incoming one.
type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

// TokenCredentials presents a bearer token on every RPC of a connection. It
// implements credentials.PerRPCCredentials.
//
// When TokenFile is set the file is re-read per RPC, so a client picks up a
// rotated Secret without reconnecting. A read failure keeps the last value
// that worked (Token, or the last good file contents) rather than dropping the
// call, so a transient kubelet Secret refresh cannot stall a publisher.
//
// RequireTransportSecurity returns false because the cluster runs gRPC in
// plaintext, so refusing to send the token would break every client. That makes
// the token readable by anything on the pod network - the NetworkPolicies and
// TLS in transit are what close that gap. Flip this to true the moment the MQ
// listener is moved to TLS, otherwise the credentials are silently dropped.
type TokenCredentials struct {
	Token     string
	TokenFile string
}

var _ credentials.PerRPCCredentials = TokenCredentials{}

// GetRequestMetadata returns the authorization header for one RPC.
func (t TokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token := t.Token
	if t.TokenFile != "" {
		// Per-RPC: prefer the current file contents, fall back to whatever
		// still works if the file is momentarily unreadable.
		if b, err := os.ReadFile(t.TokenFile); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				token = s
			}
		}
	}
	if token == "" {
		return nil, fmt.Errorf("mq auth: no token available")
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

// RequireTransportSecurity reports whether a TLS transport is mandatory; see
// the type comment.
func (t TokenCredentials) RequireTransportSecurity() bool { return false }

// bearer extracts the token from an `authorization: Bearer <token>` header,
// matching the scheme case-insensitively per RFC 7235.
func bearer(header string) string {
	scheme, token, found := strings.Cut(strings.TrimSpace(header), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

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

func readFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
