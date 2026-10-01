package election

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// The API server parses Lease timestamps with metav1.Time, which is RFC3339 with
// MICROSECOND precision. Emitting second precision instead is a 400 on every
// write, and the symptom is deeply misleading: the log says the election failed,
// every replica stays a read-only follower, and nothing is ever published - with
// no hint that the timestamp format is the cause. This test pins the wire
// format against the exact layout the API server expects.
func TestLeaseTimestampsUseMicrosecondPrecision(t *testing.T) {
	got := fmtTime(time.Date(2026, 10, 1, 5, 43, 34, 123456789, time.UTC))
	want := "2026-10-01T05:43:34.123456Z"
	if got != want {
		t.Fatalf("fmtTime()=%q, want %q\nsecond-precision RFC3339 is rejected by the API server with a 400", got, want)
	}
	if !strings.HasSuffix(got, "Z") || len(got) != len(want) {
		t.Fatalf("timestamp %q is not in metav1.Time format", got)
	}
}

func TestParseTimeRoundTripsBothLayouts(t *testing.T) {
	orig := time.Date(2026, 10, 1, 5, 43, 34, 123456000, time.UTC)
	if got := parseTime(fmtTime(orig)); !got.Equal(orig) {
		t.Fatalf("round trip lost precision: got %v want %v", got, orig)
	}
	// A Lease written by kubectl or another client may use plain RFC3339.
	if got := parseTime("2026-10-01T05:43:34Z"); got.IsZero() {
		t.Fatal("plain RFC3339 must parse too, or a Lease written elsewhere reads as expired")
	}
	if !parseTime("").IsZero() {
		t.Fatal("empty timestamp must parse as the zero time")
	}
	if !parseTime("not-a-time").IsZero() {
		t.Fatal("garbage must parse as the zero time, not panic")
	}
}

// TestKubeBackendCreateAndRoundTrip drives the REST backend against a fake API
// server so the request shape, auth header and CAS semantics are covered without
// a cluster.
func TestKubeBackendCreateAndRoundTrip(t *testing.T) {
	var created k8sLease
	// currentRV models the API server's stored resourceVersion: it only advances
	// on a successful write, which is exactly what makes a second write carrying
	// the old token a 409.
	currentRV := "7"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization=%q, want bearer token", got)
		}
		switch r.Method {
		case http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			created.Metadata.ResourceVersion = currentRV
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(&created)
		case http.MethodPut:
			var in k8sLease
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				t.Fatalf("decode update body: %v", err)
			}
			// Emulate the API server's CAS: a stale resourceVersion is a 409.
			if in.Metadata.ResourceVersion != currentRV {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"kind":"Status","message":"the object has been modified","reason":"Conflict"}`))
				return
			}
			created = in
			currentRV = "8"
			_ = json.NewEncoder(w).Encode(&created)
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&created)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	b := &KubeBackend{
		BaseURL:        srv.URL,
		TokenPath:      writeTemp(t, "test-token"),
		HTTPClient:     srv.Client(),
		RequestTimeout: 5 * time.Second,
	}
	ctx := context.Background()

	l := &Lease{
		Namespace:         "telemetry",
		Name:              "mq-leader",
		HolderIdentity:    "telemetry-messagequeue-0",
		LeaseDurationSecs: 15,
		AcquireTime:       time.Now(),
		RenewTime:         time.Now(),
	}
	if err := b.Create(ctx, l); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := b.Get(ctx, "telemetry", "mq-leader")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.HolderIdentity != "telemetry-messagequeue-0" {
		t.Fatalf("holder round trip failed: %q", got.HolderIdentity)
	}
	if got.ResourceVersion != "7" {
		t.Fatalf("resourceVersion round trip failed: %q", got.ResourceVersion)
	}
	if got.RenewTime.IsZero() {
		t.Fatal("renewTime did not survive the round trip")
	}

	// A correct CAS token succeeds...
	got.RenewTime = time.Now()
	if err := b.Update(ctx, got); err != nil {
		t.Fatalf("Update with fresh resourceVersion: %v", err)
	}
	// Replaying that same (now stale) token is rejected. This is the guarantee
	// that two candidates racing for an expired lease cannot both win: only the
	// one whose Get observed the current version gets a 200.
	if err := b.Update(ctx, got); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Update must be ErrConflict, got %v", err)
	}
}

func TestKubeBackendGetMissingIsErrNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","reason":"NotFound"}`))
	}))
	defer srv.Close()
	b := &KubeBackend{BaseURL: srv.URL, TokenPath: writeTemp(t, "t"), HTTPClient: srv.Client(), RequestTimeout: time.Second}
	if _, err := b.Get(context.Background(), "telemetry", "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestNewKubeBackendFailsOutsideCluster: a pod that cannot reach the API server
// must refuse to start rather than silently running without an election, which
// would make every replica a writer at once.
func TestNewKubeBackendFailsOutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := NewKubeBackend("", ""); err == nil {
		t.Fatal("NewKubeBackend must fail when the in-cluster environment is absent")
	}
}

func writeTemp(t *testing.T, contents string) string {
	t.Helper()
	p := t.TempDir() + "/token"
	if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	return p
}
