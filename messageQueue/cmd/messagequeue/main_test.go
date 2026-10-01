package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"messagequeue/internal/mq"
)

// startHealth builds the mux on *healthAddr and serves it in a goroutine, so
// these tests drive the handlers through the real mux rather than a copy.
// That matters because the bug this guards against was a nil dereference and a
// wrong-role report in exactly this code path.

// TestReadyzNilHANeverPanics is the regression test for the nil-`ha` crash.
//
// The chart renders readiness unconditionally, and -leader / no-flag modes run
// without an mq.HA. If /readyz dereferenced a nil HA, the handler would panic,
// net/http would close the connection with no response, and the kubelet probe
// would fail with EOF. That looks exactly like a dead process: the pod would be
// restarted forever while the gRPC server was perfectly healthy.
func TestReadyzNilHANeverPanics(t *testing.T) {
	mux := withRole(t, nil, buildHealthMux())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	// A panic inside the handler would propagate here and fail the test.
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz with no HA: got status %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "role=follower") {
		t.Errorf("readyz with no HA: body %q, want it to report role=follower", rec.Body.String())
	}
}

// TestHealthzNeverDependsOnRole guards the split-brain-adjacent rule that
// liveness must stay role-independent. If /healthz ever consulted the lease, a
// leader that steps down would look unhealthy to kubelet and get restarted,
// destroying the quorum during a perfectly normal failover.
func TestHealthzNeverDependsOnRole(t *testing.T) {
	mux := withRole(t, nil, buildHealthMux())

	for _, role := range []string{"leader", "follower"} {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("healthz (%s): got %d, want 200", role, rec.Code)
		}
	}
}

// TestReadyzPromotedLeaderReportsLeaderRole is the regression test for the
// second bug found in this path: startHealth was called with a nil HA even
// under election, so the live leader reported role=follower and the
// stale-leader fence could never fire.
// withRole publishes ha to the probe handlers for the duration of one test and
// restores the previous value afterwards, since roleProbe is process-global.
func withRole(t *testing.T, ha *mq.HA, mux *http.ServeMux) *http.ServeMux {
	t.Helper()
	prev := roleProbe.Swap(ha)
	t.Cleanup(func() { roleProbe.Store(prev) })
	return mux
}

// newDurableHA builds an HA over a real WAL store. Promote() refuses to run on
// an in-memory store, which is the correct production rule (an acked event must
// survive a restart) but means the test has to provide the durability the
// cluster always has.
func newDurableHA(t *testing.T) *mq.HA {
	t.Helper()
	dir := t.TempDir()
	store, err := mq.NewWalStore(dir + "/wal.log")
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return mq.NewHA(mq.NewWithStore(0, store))
}

func TestReadyzPromotedLeaderReportsLeaderRole(t *testing.T) {
	ha := newDurableHA(t)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}

	mux := withRole(t, ha, buildHealthMux())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz for a promoted leader: got %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "role=leader") {
		t.Fatalf("readyz for a promoted leader: body %q, want role=leader "+
			"(reporting follower here means the stale-leader fence can never fire)", rec.Body.String())
	}
}

// TestReadyzDemotedLeaderIsReady proves a stepped-down node stays routable for
// reads. If demotion made /readyz fail, kubelet would pull the surviving peer
// out of the endpoints at the exact moment it must serve.
func TestReadyzDemotedLeaderIsReady(t *testing.T) {
	ha := newDurableHA(t)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	ha.Demote()

	mux := withRole(t, ha, buildHealthMux())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after demotion: got %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "role=follower") {
		t.Errorf("readyz after demotion: body %q, want role=follower", rec.Body.String())
	}
}

// staleLease is a leaseFreshness that always reports a stale renewal, standing in
// for a leader partitioned from the API server.
type staleLease struct{}

func (staleLease) LeaseFresh(time.Time) bool { return false }

// withLease publishes l to the probe handlers for one test, restoring the
// previous value afterwards since leaseProbe is process-global.
func withLease(t *testing.T, l leaseFreshness) {
	t.Helper()
	prev := leaseProbe.Swap(&leaseHolder{f: l})
	t.Cleanup(func() { leaseProbe.Store(prev) })
}

// TestReadyzStaleLeaderIsNotReady is the split-brain guard, tested at the only
// layer that can be reached without an API server.
//
// A leader that cannot renew its lease must leave the leader Service's
// endpoints, or it keeps receiving writes that a survivor is also accepting.
// The component=leader label cannot be cleared in that state because patching it
// needs the very API server that was lost, so readiness is the only local
// signal kubelet acts on. The fence is the 503 here.
//
// Every earlier test passed while this behaviour was broken, because all of them
// reported role=follower and a follower is unconditionally ready.
func TestReadyzStaleLeaderIsNotReady(t *testing.T) {
	ha := newDurableHA(t)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	withLease(t, staleLease{})

	mux := withRole(t, ha, buildHealthMux())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz for a leader with a stale lease: got %d, want 503; "+
			"body=%q (a 200 here leaves a partitioned leader in the endpoints)",
			rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "stale") {
		t.Errorf("readyz 503 body %q, want it to say the lease is stale: "+
			"an operator seeing this is looking at a leader that lost the API server",
			rec.Body.String())
	}
}

// TestHealthzStaysUpWhileStaleLeader is the paired guarantee: fencing readiness
// must not fence liveness. A stale leader that answers 503 on /readyz but stays
// 200 on /healthz is removed from routing while staying alive to be demoted or
// fenced properly; if /healthz also failed, kubelet would restart it, and a
// restarted leader discards its quorum state.
func TestHealthzStaysUpWhileStaleLeader(t *testing.T) {
	ha := newDurableHA(t)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	withLease(t, staleLease{})

	mux := withRole(t, ha, buildHealthMux())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("healthz for a stale leader: got %d, want 200 "+
			"(liveness must never depend on the lease)", rec.Code)
	}
}

// TestNewHealthServerDisabledByEmptyAddr pins the documented
// "-health-addr= (empty) = disabled" behaviour. http.Server treats an empty Addr
// as ":http", so without the guard an operator who set an empty value to turn
// probes off would instead get a listener bound to port 80.
//
// This asserts the decision rather than probing port 80, because binding a
// privileged port fails outright for a non-root process: a dial check would pass
// whether or not the guard exists.
func TestNewHealthServerDisabledByEmptyAddr(t *testing.T) {
	prev := *healthAddr
	t.Cleanup(func() { *healthAddr = prev })

	*healthAddr = ""
	if srv := newHealthServer(); srv != nil {
		t.Fatalf("newHealthServer() with an empty -health-addr returned a server on %q; "+
			"it must be nil so no listener binds :http", srv.Addr)
	}

	// And the enabled path must still build a server on the requested address.
	*healthAddr = "127.0.0.1:0"
	srv := newHealthServer()
	if srv == nil {
		t.Fatal("newHealthServer() with a non-empty -health-addr returned nil")
	}
	if srv.Addr != "127.0.0.1:0" {
		t.Errorf("health server Addr = %q, want %q", srv.Addr, "127.0.0.1:0")
	}
	if srv.Handler == nil {
		t.Error("health server has no handler, so probes would 404")
	}
}
