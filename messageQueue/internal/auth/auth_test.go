package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func incomingCtx(token string) context.Context {
	if token == "" {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{})
	}
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestParseSpec(t *testing.T) {
	for _, bad := range []string{"streamer", "=tok", "streamer=", "ok=1,bad"} {
		if _, err := parseSpec(bad); err == nil {
			t.Errorf("parseSpec(%q) = nil error, want error", bad)
		}
	}
	creds, err := parseSpec(" streamer=a1 , collector=b2 ")
	if err != nil {
		t.Fatalf("parseSpec: %v", err)
	}
	if len(creds) != 2 || creds[0].identity != "streamer" || creds[1].token != "b2" {
		t.Fatalf("parseSpec = %+v", creds)
	}
}

// The identity must survive both interceptor paths, otherwise audit-by-identity
// silently degrades to "authenticated but anonymous".
func TestAuthorizeCarriesIdentity(t *testing.T) {
	a, err := New("", "streamer=tok-a,collector=tok-b")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, tc := range []struct{ token, want string }{
		{"tok-a", "streamer"},
		{"tok-b", "collector"},
		{"wrong", ""},
		{"", ""},
	} {
		ctx, err := a.authorize(incomingCtx(tc.token))
		if tc.want == "" {
			if code := status.Code(err); code != codes.Unauthenticated {
				t.Errorf("authorize(%q) code = %v want Unauthenticated", tc.token, code)
			}
			continue
		}
		if err != nil {
			t.Errorf("authorize(%q) = %v want nil", tc.token, err)
			continue
		}
		if got := IdentityFrom(ctx); got != tc.want {
			t.Errorf("authorize(%q) identity = %q want %q", tc.token, got, tc.want)
		}
	}

	// Unary path: the handler must see the identity.
	var seen string
	if _, err := a.UnaryServerInterceptor()(incomingCtx("tok-a"), nil,
		&grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			seen = IdentityFrom(ctx)
			return nil, nil
		}); err != nil {
		t.Fatalf("unary interceptor: %v", err)
	}
	if seen != "streamer" {
		t.Errorf("unary handler identity = %q want streamer", seen)
	}
}

// contextStream is the load-bearing bit of StreamServerInterceptor: without the
// Context override the handler would re-derive an unauthenticated context.
func TestContextStreamOverridesContext(t *testing.T) {
	a, err := New("", "streamer=tok-a")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	authed, err := a.authorize(incomingCtx("tok-a"))
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	raw := &fakeServerStream{ctx: incomingCtx("")}
	wrapped := &contextStream{ServerStream: raw, ctx: authed}
	if got := IdentityFrom(wrapped.Context()); got != "streamer" {
		t.Errorf("stream identity = %q want streamer", got)
	}
	if got := IdentityFrom(raw.ctx); got != "" {
		t.Errorf("raw context unexpectedly authenticated as %q", got)
	}
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

// A disabled Authenticator must pass traffic through (dev default) while a
// malformed one is rejected at startup.
func TestDisabledPassesThrough(t *testing.T) {
	var a *Authenticator
	if a.Enabled() {
		t.Error("nil should report disabled")
	}
	if _, err := a.authorize(incomingCtx("")); err != nil {
		t.Errorf("nil authorize = %v want nil", err)
	}
	empty, err := New("", "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := empty.authorize(incomingCtx("")); err != nil {
		t.Errorf("empty authorize = %v want nil (auth disabled)", err)
	}
	if _, err := New("", "oops"); err == nil {
		t.Error("New with malformed spec = nil error, want error")
	}
}

func TestEnabledTracksFileAndRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens")
	a, err := New(path, "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.Enabled() {
		t.Error("Enabled = true with no token file yet")
	}
	if err := os.WriteFile(path, []byte("streamer=old\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !a.Enabled() {
		t.Error("Enabled = false with a populated token file")
	}
	if err := os.WriteFile(path, []byte("streamer=new,follower=f2\n"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if _, err := a.authorize(incomingCtx("old")); status.Code(err) != codes.Unauthenticated {
		t.Error("old token still accepted after rotation")
	}
	if _, err := a.authorize(incomingCtx("new")); err != nil {
		t.Errorf("new token rejected: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := a.authorize(incomingCtx("new")); err != nil {
		t.Errorf("last good set should survive a read failure: %v", err)
	}
}

// TokenCredentials is consulted per RPC, which is what lets clients rotate
// without reconnecting.
func TestTokenCredentialsReloadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	c := TokenCredentials{TokenFile: path}
	md, err := c.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md["authorization"] != "Bearer first" {
		t.Errorf("metadata = %v", md)
	}
	if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	md, err = c.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md["authorization"] != "Bearer second" {
		t.Errorf("metadata after rotation = %v", md)
	}
	if _, err := (TokenCredentials{}).GetRequestMetadata(context.Background()); err == nil {
		t.Error("empty credentials returned no error, want one")
	}
}
