package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"messagequeue/internal/mq"
)

// The exposition format is the contract with Prometheus and with `curl`, so it is
// asserted on directly rather than only through a live scrape.

func TestRenderMetricsFormat(t *testing.T) {
	st := mq.Stats{
		Role: "leader", IsLeader: true,
		Head: 10908719, Base: 10784177,
		Consumers: []mq.ConsumerCursor{
			{Group: "collector", Consumer: "telemetry-collector-abc", Committed: 10784177, Kind: mq.CursorLogOffset},
			{Group: "streamer", Consumer: "telemetry-streamer-xyz", Committed: 221, Kind: mq.CursorExternal},
		},
	}
	got := renderMetrics(st, "telemetry-messagequeue-0", testProc())

	want := []string{
		"# HELP mq_role 1 on the leader, 0 on a follower.",
		"# TYPE mq_role gauge",
		`mq_role{node="telemetry-messagequeue-0"} 1`,
		"# TYPE mq_log_head gauge",
		`mq_log_head{node="telemetry-messagequeue-0"} 10908719`,
		`mq_log_base{node="telemetry-messagequeue-0"} 10784177`,
		`mq_retained_entries{node="telemetry-messagequeue-0"} 124543`,
		`mq_consumer_committed_offset{node="telemetry-messagequeue-0",group="collector",consumer="telemetry-collector-abc",kind="log-offset"} 10784177`,
		`mq_consumer_lag{node="telemetry-messagequeue-0",group="collector",consumer="telemetry-collector-abc",kind="log-offset"} 124542`,
		// The streamer's committed value is a CSV row. Its lag is withheld
		// rather than reported as 10908498.
		`mq_consumer_committed_offset{node="telemetry-messagequeue-0",group="streamer",consumer="telemetry-streamer-xyz",kind="external-position"} 221`,
	}
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("missing %q in:\n%s", w, got)
		}
	}

	// The streamer's lag must be absent entirely.
	if strings.Contains(got, `mq_consumer_lag{node="telemetry-messagequeue-0",group="streamer"`) {
		t.Errorf("external cursor was given a lag:\n%s", got)
	}
	// HELP and TYPE may appear once per metric name, never once per sample.
	for _, name := range []string{"mq_consumer_committed_offset", "mq_consumer_lag", "mq_role", "mq_retained_entries"} {
		if n := strings.Count(got, "# HELP "+name+" "); n != 1 {
			t.Errorf("# HELP %s appears %d times, want 1", name, n)
		}
		if n := strings.Count(got, "# TYPE "+name+" "); n != 1 {
			t.Errorf("# TYPE %s appears %d times, want 1", name, n)
		}
	}
	// Every sample line must carry a node label, so a scrape of several pods can
	// be told apart.
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, `node="`) {
			t.Errorf("sample without a node label: %q", line)
		}
	}
}

// A follower must report 0, otherwise a dashboard reads two leaders.
func TestRenderMetricsFollowerRole(t *testing.T) {
	got := renderMetrics(mq.Stats{Role: "follower", Head: 5, Base: 0}, "mq-1", testProc())
	if !strings.Contains(got, `mq_role{node="mq-1"} 0`) {
		t.Errorf("follower should report mq_role 0:\n%s", got)
	}
}

func TestRenderMetricsEmptyLogIsNegativeHeadNotGarbage(t *testing.T) {
	got := renderMetrics(mq.Stats{Head: -1}, "mq-0", testProc())
	if !strings.Contains(got, `mq_log_head{node="mq-0"} -1`) {
		t.Errorf("empty log should report head -1:\n%s", got)
	}
	if !strings.Contains(got, `mq_retained_entries{node="mq-0"} 0`) {
		t.Errorf("empty log should report 0 retained:\n%s", got)
	}
}

func TestEscapeLabelKeepsExpositionWellFormed(t *testing.T) {
	st := mq.Stats{
		Head: 1,
		Consumers: []mq.ConsumerCursor{
			{Group: `grp"x`, Consumer: "c", Committed: 0, Kind: mq.CursorLogOffset},
		},
	}
	got := renderMetrics(st, "n", testProc())
	if strings.Count(got, "\n") != strings.Count(strings.ReplaceAll(got, "\n", "\n"), "\n") {
		t.Fatal("unreachable")
	}
	// The quote must not terminate the label early.
	if !strings.Contains(got, `group="grp\"x"`) {
		t.Errorf("quote not escaped:\n%s", got)
	}
}

// The handler must answer 503 rather than empty output when no server has been
// published yet, so a scraper does not record a successful scrape of nothing.
func TestMetricsWithoutServerIsUnavailable(t *testing.T) {
	setStatsProbe(nil)
	rec := httptest.NewRecorder()
	buildHealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when no server is registered", rec.Code)
	}
}

func TestMetricsWithServerIsServed(t *testing.T) {
	setStatsProbe(mq.New(0))
	rec := httptest.NewRecorder()
	buildHealthMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if !strings.Contains(rec.Body.String(), "mq_retained_entries") {
		t.Errorf("body missing the depth metric:\n%s", rec.Body.String())
	}
	// The probe routes must keep working alongside it.
	for _, path := range []string{"/healthz", "/readyz"} {
		r := httptest.NewRecorder()
		buildHealthMux().ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		if r.Code != http.StatusOK {
			t.Errorf("%s = %d, want 200", path, r.Code)
		}
	}
}

// testProc is a fixed process snapshot so the renderer can be asserted on
// without depending on the machine running the tests.
func testProc() procStats {
	return procStats{
		RSSBytes: 55 * 1024 * 1024, VMSizeBytes: 1272 * 1024 * 1024,
		HeapInuseBytes: 44 << 20, HeapAllocBytes: 47 << 20, SysBytes: 61 << 20,
		Goroutines: 214, WALBytes: 5068 * 1024,
	}
}

// Memory and disk are what an operator polls for, so their presence and units
// are pinned here rather than left to a live scrape to reveal.
func TestRenderMetricsIncludesProcessAndDiskStats(t *testing.T) {
	got := renderMetrics(mq.Stats{Head: 3}, "mq-0", testProc())
	mib := int64(1024 * 1024)
	for _, want := range []string{
		fmt.Sprintf("mq_process_resident_memory_bytes{node=%q} %d", "mq-0", 55*mib),
		fmt.Sprintf("mq_process_virtual_memory_bytes{node=%q} %d", "mq-0", 1272*mib),
		fmt.Sprintf("mq_go_memstats_heap_inuse_bytes{node=%q} %d", "mq-0", 44*mib),
		fmt.Sprintf("mq_go_memstats_heap_alloc_bytes{node=%q} %d", "mq-0", 47*mib),
		fmt.Sprintf("mq_go_memstats_sys_bytes{node=%q} %d", "mq-0", 61*mib),
		fmt.Sprintf("mq_go_goroutines{node=%q} %d", "mq-0", 214),
		fmt.Sprintf("mq_wal_bytes{node=%q} %d", "mq-0", int64(5068*1024)),
		fmt.Sprintf("mq_wal_stat_failed{node=%q} %d", "mq-0", 0),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

// An unstattable WAL must be visible as a failure. Reporting 0 bytes would read
// as "no log on disk", which is the opposite of what happened.
func TestRenderMetricsWALFailureIsNotZeroBytes(t *testing.T) {
	p := testProc()
	p.WALErr = errors.New("no such file")
	got := renderMetrics(mq.Stats{Head: 1}, "mq-0", p)
	if !strings.Contains(got, "mq_wal_stat_failed{node=\"mq-0\"} 1") {
		t.Errorf("WAL failure not reported:\n%s", got)
	}
	if strings.Contains(got, "mq_wal_bytes") {
		t.Errorf("WAL size exported despite the stat failing:\n%s", got)
	}
}

func TestReadProcStatsReportsRealMemory(t *testing.T) {
	p := readProcStats("")
	// The Go runtime numbers come from the runtime itself and are always
	// available, including on macOS where there is no /proc.
	if p.HeapInuseBytes == 0 {
		t.Error("heap inuse = 0, want a positive value")
	}
	if p.Goroutines <= 0 {
		t.Errorf("goroutines = %d, want a positive value", p.Goroutines)
	}
	if _, err := os.Stat("/proc/self/statm"); err != nil {
		// RSS and VmSize can only be read on Linux. Asserting them here would
		// make the suite fail on a developer machine for no real reason.
		if p.RSSBytes != 0 || p.VMSizeBytes != 0 {
			t.Errorf("RSS/virtual = %d/%d, want 0 without /proc", p.RSSBytes, p.VMSizeBytes)
		}
		t.Skip("no /proc/self/statm on this platform; RSS assertions skipped")
	}
	if p.RSSBytes <= 0 {
		t.Errorf("RSS = %d, want a positive value", p.RSSBytes)
	}
	// VmSize is deliberately far larger than RSS for a Go process; equal values
	// would mean the wrong field was parsed.
	if p.VMSizeBytes <= p.RSSBytes {
		t.Errorf("virtual = %d, want more than RSS = %d", p.VMSizeBytes, p.RSSBytes)
	}
}

// The WAL size must reflect the file on disk, and a missing path must surface as
// an error rather than a silent zero.
func TestReadProcStatsWAL(t *testing.T) {
	dir := t.TempDir()
	wal := filepath.Join(dir, "wal.log")
	if err := os.WriteFile(wal, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	p := readProcStats(wal)
	if p.WALErr != nil {
		t.Fatalf("WALErr = %v, want nil", p.WALErr)
	}
	if p.WALBytes != 4096 {
		t.Errorf("WALBytes = %d, want 4096", p.WALBytes)
	}
	if bad := readProcStats(filepath.Join(dir, "missing")); bad.WALErr == nil {
		t.Error("WALErr = nil for a missing file, want an error")
	}
}
