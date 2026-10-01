package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"apigateway/internal/store"
)

type fakeStore struct {
	gpus    []store.GPU
	events  []store.Telemetry
	err     error
	gotID   string
	gotS    *time.Time
	gotE    *time.Time
	gotPage store.Page
}

// ListGPUs and Telemetry apply the requested page themselves, so these tests
// exercise the same slicing a real query would do rather than returning
// everything and pretending the parameters were ignored.
func (f *fakeStore) ListGPUs(_ context.Context, p store.Page) ([]store.GPU, int64, bool, error) {
	f.gotPage = p
	// Mirror the store: the real query sorts in SQL, so the fake sorts here
	// rather than returning rows in whatever order the test happened to build.
	rows := append([]store.GPU(nil), f.gpus...)
	sort.SliceStable(rows, func(i, j int) bool {
		if p.Desc {
			return rows[i].ID > rows[j].ID
		}
		return rows[i].ID < rows[j].ID
	})
	if p.After != "" {
		after, cursorDesc, err := store.DecodeGPUCursor(p.After)
		if err != nil {
			return nil, 0, false, err
		}
		if cursorDesc != p.Desc {
			return nil, 0, false, store.ErrInvalidCursor
		}
		out := []store.GPU{}
		for _, g := range rows {
			if (p.Desc && g.ID < after) || (!p.Desc && g.ID > after) {
				out = append(out, g)
			}
		}
		rows = out
	}
	return pagedSlice(len(rows), sliceGPUs(rows, p), int64(len(f.gpus)), f.err)
}

// pagedSlice reproduces the store's fetch-N+1 contract: hasMore is true only
// when rows remain beyond the page, so a client never has to make a wasted
// trailing request to discover it has reached the end.
func pagedSlice[T any](available int, page []T, total int64, err error) ([]T, int64, bool, error) {
	if err != nil {
		return nil, 0, false, err
	}
	return page, total, len(page) < available, nil
}

func (f *fakeStore) Telemetry(_ context.Context, id string, start, end *time.Time, p store.Page) ([]store.Telemetry, int64, bool, error) {
	f.gotID, f.gotS, f.gotE, f.gotPage = id, start, end, p
	// Mirror the store: resume on the same side of (source_ts, event_id) that
	// the sort runs, so a descending walk cannot resume "forwards" and re-serve
	// the page the client just read.
	rows := f.events
	if p.After != "" {
		lastTS, lastID, cursorDesc, err := store.DecodeTelemetryCursor(p.After)
		if err != nil {
			return nil, 0, false, err
		}
		if cursorDesc != p.Desc {
			return nil, 0, false, store.ErrInvalidCursor
		}
		// Mirror the real store: the timestamp half must actually parse, so the
		// fake rejects exactly what ClickHouse would.
		if _, err := time.Parse(time.RFC3339Nano, lastTS); err != nil {
			return nil, 0, false, store.ErrInvalidCursor
		}
		out := []store.Telemetry{}
		for _, e := range rows {
			after := e.SourceTS > lastTS || (e.SourceTS == lastTS && e.EventID > lastID)
			if p.Desc {
				after = e.SourceTS < lastTS || (e.SourceTS == lastTS && e.EventID < lastID)
			}
			if after {
				out = append(out, e)
			}
		}
		rows = out
	}
	// The real store sorts in SQL; sort here so the fake cannot pass by
	// returning rows in whatever order the test happened to build them.
	rows = append([]store.Telemetry(nil), rows...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].SourceTS != rows[j].SourceTS {
			if p.Desc {
				return rows[i].SourceTS > rows[j].SourceTS
			}
			return rows[i].SourceTS < rows[j].SourceTS
		}
		if p.Desc {
			return rows[i].EventID > rows[j].EventID
		}
		return rows[i].EventID < rows[j].EventID
	})
	return pagedSlice(len(rows), sliceTelemetry(rows, p), int64(len(f.events)), f.err)
}

func sliceGPUs(in []store.GPU, p store.Page) []store.GPU {
	if p.Offset >= len(in) {
		return nil
	}
	end := p.Offset + p.Limit
	if end > len(in) {
		end = len(in)
	}
	return in[p.Offset:end]
}

func sliceTelemetry(in []store.Telemetry, p store.Page) []store.Telemetry {
	if p.Offset >= len(in) {
		return nil
	}
	end := p.Offset + p.Limit
	if end > len(in) {
		end = len(in)
	}
	return in[p.Offset:end]
}

func (f *fakeStore) Ping(context.Context) error { return f.err }

func doReq(t *testing.T, s store.Store, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	NewServer(s).Handler().ServeHTTP(rec, req)
	return rec
}

func TestListGPUs(t *testing.T) {
	rec := doReq(t, &fakeStore{gpus: []store.GPU{
		{ID: "GPU-a", Device: "nvidia0", Index: 0, Hostname: "h1", Model: "H100"},
	}}, "GET", "/api/v1/gpus")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200", rec.Code)
	}
	var body struct {
		Count int         `json:"count"`
		GPUs  []store.GPU `json:"gpus"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v\n%s", err, rec.Body)
	}
	if body.Count != 1 || body.GPUs[0].ID != "GPU-a" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestTelemetryWithoutWindow(t *testing.T) {
	f := &fakeStore{events: []store.Telemetry{{EventID: "e1", SourceTS: "2025-07-18T20:42:34Z"}}}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-a/telemetry")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200", rec.Code)
	}
	if f.gotID != "GPU-a" || f.gotS != nil || f.gotE != nil {
		t.Fatalf("store called with id=%q start=%v end=%v", f.gotID, f.gotS, f.gotE)
	}
}

func TestTelemetryWindowParsed(t *testing.T) {
	f := &fakeStore{}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-a/telemetry?start_time=2025-07-18T20:42:34Z&end_time=2025-07-18T21:00:00Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200", rec.Code)
	}
	if f.gotS == nil || f.gotE == nil {
		t.Fatalf("window not forwarded: %v %v", f.gotS, f.gotE)
	}
	if !f.gotS.Equal(time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC)) {
		t.Fatalf("start = %v", *f.gotS)
	}
	if !f.gotE.Equal(time.Date(2025, 7, 18, 21, 0, 0, 0, time.UTC)) {
		t.Fatalf("end = %v", *f.gotE)
	}
}

func TestTelemetryBadRequest(t *testing.T) {
	cases := []string{
		"/api/v1/gpus/GPU-a/telemetry?start_time=garbage",
		"/api/v1/gpus/GPU-a/telemetry?end_time=garbage",
		"/api/v1/gpus/GPU-a/telemetry?start_time=2025-07-18T21:00:00Z&end_time=2025-07-18T20:00:00Z",
		"/api/v1/gpus/bad%20id/telemetry",
	}
	for _, p := range cases {
		if rec := doReq(t, &fakeStore{}, "GET", p); rec.Code != http.StatusBadRequest {
			t.Errorf("%s -> status %d want 400", p, rec.Code)
		}
	}
}

func TestTelemetryStartOnly(t *testing.T) {
	f := &fakeStore{}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-a/telemetry?start_time=2025-07-18T20:42:34.123Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200", rec.Code)
	}
	if f.gotS == nil || f.gotE != nil {
		t.Fatalf("start=%v end=%v want start only", f.gotS, f.gotE)
	}
	if f.gotS.Nanosecond() != 123000000 {
		t.Fatalf("nanosecond precision lost: %v", *f.gotS)
	}
}

func TestStoreErrorMapped(t *testing.T) {
	rec := doReq(t, &fakeStore{err: errors.New("boom")}, "GET", "/api/v1/gpus")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d want 500", rec.Code)
	}
}

func TestHealthz(t *testing.T) {
	rec := doReq(t, &fakeStore{}, "GET", "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200", rec.Code)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v\n%s", err, rec.Body)
	}
	if body.Status != "ok" {
		t.Fatalf("status = %q want ok", body.Status)
	}
}

func TestHealthzPingFailure(t *testing.T) {
	rec := doReq(t, &fakeStore{err: errors.New("db down")}, "GET", "/healthz")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d want 500", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	rec := doReq(t, &fakeStore{}, "POST", "/api/v1/gpus")
	if rec.Code == http.StatusOK {
		t.Fatal("POST /api/v1/gpus should not succeed")
	}
}
