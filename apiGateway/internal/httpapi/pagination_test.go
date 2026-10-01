package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"apigateway/internal/store"
)

// manyEvents gives every third row the same source_ts, so the event_id
// tiebreaker in the resume key is actually exercised. A cursor scheme that
// keyed on source_ts alone would re-read or skip rows here.
func manyEvents(n int) []store.Telemetry {
	out := make([]store.Telemetry, n)
	base := time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC)
	for i := range out {
		out[i] = store.Telemetry{
			EventID:  fmt.Sprintf("e%04d", i),
			SourceTS: base.Add(time.Duration(i/3) * time.Second).Format(time.RFC3339Nano),
			Value:    float64(i),
		}
	}
	return out
}

// walkAll follows next links exactly as a client would and returns the event
// ids in the order they arrived, plus the number of pages walked.
func walkAll(t *testing.T, h http.Handler, start string) (seen []string, pages int) {
	t.Helper()
	url := start
	for url != "" {
		body := get(t, h, url)
		for _, e := range eventsOf(t, body) {
			seen = append(seen, e.EventID)
		}
		pages++
		if pages > 50 {
			t.Fatal("next link never terminated - infinite paging")
		}
		url, _ = body["next"].(string)
		if url != "" {
			if i := indexOf(url, "/api/v1/"); i >= 0 {
				url = url[i:]
			}
		}
	}
	return seen, pages
}

// paged is the paginated envelope. Declared here rather than reusing the
// handler's map so the test asserts the contract clients depend on, not the
// handler's internal shape.
type paged struct {
	Count  int    `json:"count"`
	Total  int64  `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
	Next   string `json:"next"`
}

func TestDefaultPageIsTen(t *testing.T) {
	f := &fakeStore{events: manyEvents(6509)}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry")
	if rec.Code != http.StatusOK {
		t.Fatalf("= %d: %s", rec.Code, rec.Body.String())
	}
	var p paged
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Count != DefaultLimit {
		t.Errorf("count = %d, want the default page of %d", p.Count, DefaultLimit)
	}
	if p.Total != 6509 {
		t.Errorf("total = %d, want 6509", p.Total)
	}
	if p.Limit != 10 || p.Offset != 0 {
		t.Errorf("limit/offset = %d/%d, want 10/0", p.Limit, p.Offset)
	}
	if p.Next == "" {
		t.Error("next link is empty on the first of many pages")
	}
}

func TestNextLinkIsFollowableAndWalksTheWholeSet(t *testing.T) {
	const total = 25
	f := &fakeStore{events: manyEvents(total)}
	h := NewServer(f).Handler()

	// Follow next until it runs out, exactly as a client would.
	var (
		seen  []string
		pages int
		url   = "/api/v1/gpus/GPU-abc/telemetry"
	)
	for url != "" {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("page %d: %d %s", pages+1, rec.Code, rec.Body.String())
		}
		var body struct {
			paged
			Events []store.Telemetry `json:"events"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, e := range body.Events {
			seen = append(seen, e.EventID)
		}
		pages++
		if pages > 10 {
			t.Fatal("next link never terminated - infinite paging")
		}
		// The link is absolute; keep only the path+query for the next request.
		url = body.Next
		if i := indexOf(url, "/api/v1/"); i >= 0 {
			url = url[i:]
		}
	}

	if pages != 3 {
		t.Errorf("walked %d pages, want 3 (10+10+5 of %d)", pages, total)
	}
	if len(seen) != total {
		t.Errorf("saw %d events across pages, want %d", len(seen), total)
	}
	seenSet := map[string]int{}
	for _, id := range seen {
		seenSet[id]++
	}
	for id, n := range seenSet {
		if n != 1 {
			t.Errorf("event %s appeared %d times - pages overlap or skip", id, n)
		}
	}
	if len(seenSet) != total {
		t.Errorf("saw %d distinct events, want %d - a page was skipped", len(seenSet), total)
	}
}

func TestNextLinkPreservesFilters(t *testing.T) {
	f := &fakeStore{events: manyEvents(100)}
	rec := doReq(t, f, "GET",
		"/api/v1/gpus/GPU-abc/telemetry?start_time=2025-07-18T20:40:00Z&end_time=2025-07-18T20:45:00Z&limit=7")
	var p paged
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Limit != 7 {
		t.Fatalf("limit = %d, want 7", p.Limit)
	}
	// Following the link must not drop the time window, or the caller silently
	// pages through a different result set.
	for _, want := range []string{"start_time=", "end_time=", "limit=7", "cursor="} {
		if !contains(p.Next, want) {
			t.Errorf("next link %q is missing %q", p.Next, want)
		}
	}
	if contains(p.Next, "offset=") {
		t.Errorf("next link %q still carries an offset; it must resume by cursor", p.Next)
	}
}

func TestLastPageHasNullNext(t *testing.T) {
	f := &fakeStore{events: manyEvents(10)}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry")
	var p paged
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Total != 10 || p.Count != 10 {
		t.Errorf("count/total = %d/%d, want 10/10", p.Count, p.Total)
	}
	if p.Next != "" {
		t.Errorf("next = %q, want null on the final page", p.Next)
	}
	if !contains(rec.Body.String(), `"next":null`) {
		t.Errorf("final page should serialise next as null, got: %s", rec.Body.String())
	}
}

func TestOffsetPastEndReportsTotalAndNoNext(t *testing.T) {
	f := &fakeStore{events: manyEvents(15)}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry?offset=500")
	var p paged
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Count != 0 {
		t.Errorf("count = %d, want 0", p.Count)
	}
	if p.Total != 15 {
		t.Errorf("total = %d, want 15 - the caller needs it to correct their offset", p.Total)
	}
	if p.Next != "" {
		t.Errorf("next = %q, want null", p.Next)
	}
}

func TestListGPUsIsPaginated(t *testing.T) {
	f := &fakeStore{gpus: []store.GPU{
		{ID: "GPU-1"}, {ID: "GPU-2"}, {ID: "GPU-3"},
	}}
	rec := doReq(t, f, "GET", "/api/v1/gpus?limit=2")
	var p struct {
		paged
		GPUs []store.GPU `json:"gpus"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Count != 2 || p.Total != 3 {
		t.Errorf("count/total = %d/%d, want 2/3", p.Count, p.Total)
	}
	if len(p.GPUs) != 2 || p.GPUs[0].ID != "GPU-1" {
		t.Errorf("gpus = %+v, want the first two", p.GPUs)
	}
	if !contains(p.Next, "cursor=") {
		t.Errorf("next = %q, want a cursor", p.Next)
	}
}

func TestBadPaginationIsRejected(t *testing.T) {
	for name, q := range map[string]string{
		"limit=0":    "limit=0",
		"limit=-1":   "limit=-1",
		"limit=abc":  "limit=abc",
		"limit=1.5":  "limit=1.5",
		"limit=1001": "limit=1001",
		"offset=-1":  "offset=-1",
		"offset=abc": "offset=abc",
	} {
		t.Run(name, func(t *testing.T) {
			rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET",
				"/api/v1/gpus/GPU-abc/telemetry?"+q)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s = %d, want 400: %s", q, rec.Code, rec.Body.String())
			}
			if !contains(rec.Body.String(), "limit") && !contains(rec.Body.String(), "offset") {
				t.Errorf("%s: error should name the bad parameter, got %s", q, rec.Body.String())
			}
		})
	}
}

// An empty or whitespace-padded value is treated as "not supplied" rather than
// rejected. A client that templates ?limit={{pageSize}} and renders an empty
// string should get the default page, not a 400 it has no way to interpret.
// Genuinely malformed values (abc, -1, 0) are still errors.
func TestEmptyAndPaddedPaginationFallsBackToDefault(t *testing.T) {
	for _, q := range []string{"limit=", "limit=%20", "limit=%2010", "offset=", "offset=%20"} {
		t.Run(q, func(t *testing.T) {
			rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET",
				"/api/v1/gpus/GPU-abc/telemetry?"+q)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s = %d, want 200: %s", q, rec.Code, rec.Body.String())
			}
			var p paged
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if p.Limit != DefaultLimit || p.Offset != 0 {
				t.Errorf("%s: limit/offset = %d/%d, want the default %d/0",
					q, p.Limit, p.Offset, DefaultLimit)
			}
		})
	}
}

func TestLimitAtMaxIsAccepted(t *testing.T) {
	rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET",
		"/api/v1/gpus/GPU-abc/telemetry?limit="+strconv.Itoa(MaxLimit))
	if rec.Code != http.StatusOK {
		t.Errorf("limit=MaxLimit = %d, want 200: %s", rec.Code, rec.Body.String())
	}
}

func TestParsePageDefaults(t *testing.T) {
	p, err := parsePage(nil)
	if err != nil {
		t.Fatalf("parsePage(nil): %v", err)
	}
	if p.Limit != DefaultLimit || p.Offset != 0 {
		t.Errorf("parsePage(nil) = %+v, want limit %d offset 0", p, DefaultLimit)
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func contains(s, sub string) bool { return indexOf(s, sub) >= 0 }

// --- ordering -------------------------------------------------------------

// The default has to stay oldest-first: callers who never heard of `order` were
// built against that, and silently flipping the default would reorder every
// existing paging loop and integration test in the wild.
func TestOrderDefaultsToAscending(t *testing.T) {
	rec := doReq(t, &fakeStore{events: manyEvents(25)}, "GET", "/api/v1/gpus/GPU-abc/telemetry")
	var body struct {
		paged
		Order  string            `json:"order"`
		Events []store.Telemetry `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Order != "asc" {
		t.Errorf("order = %q, want asc", body.Order)
	}
	if body.Events[0].EventID != "e0000" {
		t.Errorf("first event = %s, want e0000 (oldest first)", body.Events[0].EventID)
	}
}

// order=desc is the useful default for exploration: "what is this GPU doing
// right now" should not be buried under the device's whole history.
func TestOrderDescReturnsNewestFirst(t *testing.T) {
	rec := doReq(t, &fakeStore{events: manyEvents(25)}, "GET",
		"/api/v1/gpus/GPU-abc/telemetry?order=desc&limit=5")
	var body struct {
		paged
		Order  string            `json:"order"`
		Events []store.Telemetry `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Order != "desc" {
		t.Errorf("order = %q, want desc", body.Order)
	}
	if body.Events[0].EventID != "e0024" {
		t.Errorf("first event = %s, want e0024 (newest first)", body.Events[0].EventID)
	}
	// The whole page must be descending, not just the first row.
	for i := 1; i < len(body.Events); i++ {
		if body.Events[i-1].SourceTS < body.Events[i].SourceTS {
			t.Errorf("page is not descending at %d: %s then %s",
				i, body.Events[i-1].SourceTS, body.Events[i].SourceTS)
		}
	}
}

func TestOrderIsCaseInsensitiveAndTrimmed(t *testing.T) {
	for _, q := range []string{"order=desc", "order=DESC", "order=Desc", "order=%20desc%20", "order=asc", "order=ASC"} {
		rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET",
			"/api/v1/gpus/GPU-abc/telemetry?"+q)
		if rec.Code != http.StatusOK {
			t.Errorf("%s = %d, want 200: %s", q, rec.Code, rec.Body.String())
		}
	}
	// Blank means "not supplied", matching how limit/offset behave, so a
	// templated ?order={{dir}} degrades to the default instead of a 400.
	rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET", "/api/v1/gpus/GPU-abc/telemetry?order=")
	if rec.Code != http.StatusOK {
		t.Errorf("order= (blank) = %d, want 200: %s", rec.Code, rec.Body.String())
	}
}

// A typo must not silently fall back to ascending: the caller would believe
// they asked for the newest rows and be handed the oldest, with nothing in the
// response to tell them.
func TestBadOrderIsRejected(t *testing.T) {
	for _, q := range []string{"order=dsc", "order=descending", "order=down", "order=1"} {
		rec := doReq(t, &fakeStore{events: manyEvents(3)}, "GET",
			"/api/v1/gpus/GPU-abc/telemetry?"+q)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400: %s", q, rec.Code, rec.Body.String())
		}
		if !contains(rec.Body.String(), "order") {
			t.Errorf("%s: error should name the bad parameter, got %s", q, rec.Body.String())
		}
	}
}

// The resume key only means "before this row" while the sort also runs
// newest-first. If next dropped order=desc, page two would re-sort ascending
// and hand back the oldest rows - the opposite end of the data set.
func TestNextLinkPreservesOrder(t *testing.T) {
	rec := doReq(t, &fakeStore{events: manyEvents(100)}, "GET",
		"/api/v1/gpus/GPU-abc/telemetry?order=desc&limit=7")
	var p paged
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !contains(p.Next, "order=desc") {
		t.Errorf("next link %q dropped order=desc; the walk would flip direction mid-pagination", p.Next)
	}
	if !contains(p.Next, "limit=7") {
		t.Errorf("next link %q dropped the limit", p.Next)
	}
}

// The whole point of the change: a descending walk must still see every row
// exactly once, newest to oldest, with no page overlapping the last.
func TestDescWalkVisitsEveryRowExactlyOnceNewestFirst(t *testing.T) {
	const total = 25
	f := &fakeStore{events: manyEvents(total)}
	seen, pages := walkAll(t, NewServer(f).Handler(),
		"/api/v1/gpus/GPU-abc/telemetry?order=desc&limit=10")

	if pages != 3 {
		t.Errorf("walked %d pages, want 3 (10+10+5 of %d)", pages, total)
	}
	if len(seen) != total {
		t.Fatalf("saw %d events, want %d - a page was skipped", len(seen), total)
	}
	if seen[0] != "e0024" {
		t.Errorf("walk started at %s, want e0024 (newest)", seen[0])
	}
	if seen[len(seen)-1] != "e0000" {
		t.Errorf("walk ended at %s, want e0000 (oldest)", seen[len(seen)-1])
	}
	counts := map[string]int{}
	for _, id := range seen {
		counts[id]++
	}
	for id, n := range counts {
		if n != 1 {
			t.Errorf("event %s appeared %d times - descending pages overlap or skip", id, n)
		}
	}
}

// The dangerous case: a key issued by an ascending walk, replayed against a
// descending request. The comparison operator is inverted, so the same key
// means the opposite thing - without an explicit check this returns a
// confidently wrong page instead of an error, and a client summing the pages
// computes a wrong total without any sign of trouble.
func TestCursorFromTheOppositeDirectionIsRejected(t *testing.T) {
	f := &fakeStore{events: manyEvents(25)}

	ascPage := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry?limit=5")
	var asc paged
	if err := json.Unmarshal(ascPage.Body.Bytes(), &asc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	ascCursor := asc.Next
	if i := indexOf(ascCursor, "cursor="); i >= 0 {
		ascCursor = ascCursor[i+len("cursor="):]
		if j := indexOf(ascCursor, "&"); j >= 0 {
			ascCursor = ascCursor[:j]
		}
	}

	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry?order=desc&cursor="+ascCursor)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("ascending cursor on a descending walk = %d, want 400: %s",
			rec.Code, rec.Body.String())
	}

	// And the same key used with its own direction still works, so the check
	// rejects the mismatch rather than the cursor.
	if ok := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry?cursor="+ascCursor); ok.Code != http.StatusOK {
		t.Errorf("ascending cursor on an ascending walk = %d, want 200: %s", ok.Code, ok.Body.String())
	}
}

// Descending paging must not double-count either. In a descending walk a row
// that arrives mid-walk with an *older* source_ts sorts behind the resume key
// and is picked up by this same walk, while a newer one sorts ahead and is not.
// Neither may be delivered twice.
func TestDescPagingIsNotDisturbedByRowsArrivingMidWalk(t *testing.T) {
	f := &fakeStore{events: manyEvents(20)}
	h := NewServer(f).Handler()

	first := get(t, h, "/api/v1/gpus/GPU-abc/telemetry?order=desc&limit=5")
	if first["next"] == nil {
		t.Fatal("expected a next link")
	}
	// An older row lands mid-walk: in a descending walk it sorts *after*
	// everything read so far, so this walk will see it.
	late := store.Telemetry{EventID: "late-older", SourceTS: "2020-01-01T00:00:00Z"}
	f.events = append(f.events, late)

	seen := map[string]int{}
	for _, e := range eventsOf(t, first) {
		seen[e.EventID]++
	}
	pages, url := 1, first["next"]
	for url != nil && pages < 20 {
		body := get(t, h, url.(string))
		for _, e := range eventsOf(t, body) {
			seen[e.EventID]++
		}
		pages++
		url = body["next"]
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s returned %d times in a descending walk", id, n)
		}
	}
	if seen["late-older"] != 1 {
		t.Errorf("late-older seen %d times, want 1 - a descending walk should pick up a row that sorts behind its cursor", seen["late-older"])
	}
}

// The GPU list is sorted by device_id rather than a timestamp, but it takes the
// same param so the API is consistent and no caller is quietly ignored.
func TestListGPUsOrderDesc(t *testing.T) {
	f := &fakeStore{gpus: []store.GPU{{ID: "GPU-1"}, {ID: "GPU-2"}, {ID: "GPU-3"}}}
	rec := doReq(t, f, "GET", "/api/v1/gpus?order=desc")
	var body struct {
		paged
		Order string      `json:"order"`
		GPUs  []store.GPU `json:"gpus"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Order != "desc" {
		t.Errorf("order = %q, want desc", body.Order)
	}
	if body.GPUs[0].ID != "GPU-3" {
		t.Errorf("first gpu = %s, want GPU-3 (descending)", body.GPUs[0].ID)
	}
	// A descending walk of the list must also cover every id exactly once.
	seen, _ := walkAllGPUs(t, NewServer(f).Handler(), "/api/v1/gpus?order=desc&limit=1")
	if len(seen) != 3 || seen[0] != "GPU-3" || seen[2] != "GPU-1" {
		t.Errorf("descending gpu walk = %v, want [GPU-3 GPU-2 GPU-1]", seen)
	}
}

func walkAllGPUs(t *testing.T, h http.Handler, start string) (seen []string, pages int) {
	t.Helper()
	url := start
	for url != "" {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", url, rec.Code, rec.Body.String())
		}
		var body struct {
			Next string      `json:"next"`
			GPUs []store.GPU `json:"gpus"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, g := range body.GPUs {
			seen = append(seen, g.ID)
		}
		pages++
		if pages > 20 {
			t.Fatal("next link never terminated")
		}
		url = body.Next
		if i := indexOf(url, "/api/v1/"); i >= 0 {
			url = url[i:]
		}
	}
	return seen, pages
}

// This is the bug that made offset paging unusable on this table: a row that
// arrives *between* two page requests but sorts *before* the rows already read.
// With OFFSET, that insert shifts everything along and the boundary row is
// returned twice, so a client that walks the next links double-counts it.
// A resume-key cursor is immune: it asks for "greater than the last key I saw",
// and a row behind that key simply never appears in a later page.
func TestPagingIsNotDisturbedByRowsArrivingMidWalk(t *testing.T) {
	f := &fakeStore{events: manyEvents(20)}
	h := NewServer(f).Handler()

	first := get(t, h, "/api/v1/gpus/GPU-abc/telemetry?limit=5")
	if first["next"] == nil {
		t.Fatal("expected a next link")
	}
	// A late row lands with a source_ts earlier than anything read so far, which
	// is exactly the case that breaks OFFSET.
	late := store.Telemetry{
		EventID:  "e0000-late",
		SourceTS: f.events[0].SourceTS,
		Value:    -1,
	}
	f.events = append([]store.Telemetry{late}, f.events...)

	seen := map[string]int{}
	for _, e := range eventsOf(t, first) {
		seen[e.EventID]++
	}
	pages, url := 1, first["next"]
	for url != nil && pages < 20 {
		body := get(t, h, url.(string))
		for _, e := range eventsOf(t, body) {
			seen[e.EventID]++
		}
		pages++
		url = body["next"]
	}

	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s was returned %d times; a client summing the pages would double-count it", id, n)
		}
	}
	// Every row that already existed when the walk began is still returned
	// exactly once - that is the guarantee that matters, because it is what
	// stops a client double-counting a metric.
	if len(seen) != 20 {
		t.Errorf("saw %d distinct events, want the 20 that existed at the start: a walk must not lose rows", len(seen))
	}

	// The late row is deliberately not returned. It sorted *behind* the resume
	// key, and a forward-only walk cannot go back. That is the trade: the walk
	// is consistent, but data that arrives mid-walk is picked up by the next
	// walk rather than being stitched into this one. The alternative - going
	// back - is what makes a row get counted twice.
	if seen["e0000-late"] != 0 {
		t.Error("the late row should not appear in this walk; it sorted behind the cursor")
	}

	// A fresh walk does see it, so nothing is lost from the table's point of view.
	fresh := get(t, h, "/api/v1/gpus/GPU-abc/telemetry?limit=100")
	found := false
	for _, e := range eventsOf(t, fresh) {
		if e.EventID == "e0000-late" {
			found = true
		}
	}
	if !found {
		t.Error("a fresh walk should see the late row")
	}
}

func get(t *testing.T, h http.Handler, url string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", url, rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return body
}

func eventsOf(t *testing.T, body map[string]any) []store.Telemetry {
	t.Helper()
	raw, err := json.Marshal(body["events"])
	if err != nil {
		t.Fatalf("re-marshal events: %v", err)
	}
	var out []store.Telemetry
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	return out
}

// A cursor is opaque, not signed, so one that is well-formed base64 of the right
// shape but was never issued is accepted and simply matches nothing. The response
// still carries the true total, so a client can tell "I am past the end" from
// "there is no data" and restart without the cursor.
func TestWellFormedButMeaninglessCursorYieldsEmptyPageWithRealTotal(t *testing.T) {
	f := &fakeStore{events: manyEvents(5)}
	rec := doReq(t, f, "GET",
		"/api/v1/gpus/GPU-abc/telemetry?cursor="+store.EncodeCursor("2999-01-01T00:00:00Z", "zzz"))
	var p paged
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Count != 0 {
		t.Errorf("count = %d, want 0", p.Count)
	}
	if p.Total != 5 {
		t.Errorf("total = %d, want 5 - a client needs this to notice the cursor was wrong", p.Total)
	}
	if p.Next != "" {
		t.Errorf("next = %q, want null", p.Next)
	}
}

// offset is still supported for callers that want random access (jumping to a
// known position), it is simply not what the next link uses.
func TestOffsetStillWorksForRandomAccess(t *testing.T) {
	f := &fakeStore{events: manyEvents(25)}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry?limit=5&offset=10")
	var p struct {
		paged
		Events []store.Telemetry `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Count != 5 || p.Offset != 10 {
		t.Fatalf("count/offset = %d/%d, want 5/10", p.Count, p.Offset)
	}
	if p.Events[0].EventID != "e0010" {
		t.Errorf("offset=10 gave %s, want e0010", p.Events[0].EventID)
	}
}

func TestBadCursorIsRejected(t *testing.T) {
	for _, c := range []string{"not-base64!!", "YWJj", "aGVsbG8"} {
		rec := doReq(t, &fakeStore{events: manyEvents(5)}, "GET",
			"/api/v1/gpus/GPU-abc/telemetry?cursor="+c)
		if rec.Code == http.StatusOK {
			t.Errorf("cursor=%s was accepted; a client would page from a bogus position", c)
		}
	}
}

// A one-part cursor is a valid GPU key but the wrong shape for telemetry, which
// needs (source_ts, event_id). Carrying a cursor from one endpoint to the other
// must be rejected rather than silently returning an empty page.
func TestCursorFromTheWrongEndpointIsRejected(t *testing.T) {
	f := &fakeStore{events: manyEvents(5), gpus: []store.GPU{{ID: "GPU-abc"}}}
	// EncodeCursor(id) is what /api/v1/gpus hands out.
	gpuCursor := store.EncodeCursor("GPU-abc")
	if rec := doReq(t, f, "GET", "/api/v1/gpus?cursor="+gpuCursor); rec.Code != http.StatusOK {
		t.Errorf("gpu cursor on /api/v1/gpus = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	rec := doReq(t, f, "GET", "/api/v1/gpus/GPU-abc/telemetry?cursor="+gpuCursor)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("gpu cursor on telemetry = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

// A mangled cursor is the client's mistake, so it must be a 400 with an
// actionable message - not a 500, which tells an operator the service is broken
// when nothing is.
func TestBadCursorIs400Not500(t *testing.T) {
	for _, path := range []string{"/api/v1/gpus", "/api/v1/gpus/GPU-abc/telemetry"} {
		// "zzz!!" is not base64; "Hw" decodes to two empty parts, which is the
		// wrong shape for /gpus and an unparseable timestamp for telemetry.
		// A token that happens to be well-formed base64 of the right shape is
		// indistinguishable from a real one - cursors are opaque, not signed -
		// so it is not in this list.
		for _, c := range []string{"zzz!!", "Hw"} {
			rec := doReq(t, &fakeStore{events: manyEvents(5), gpus: []store.GPU{{ID: "GPU-abc"}}},
				"GET", path+"?cursor="+c)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s cursor=%s = %d, want 400: %s", path, c, rec.Code, rec.Body.String())
			}
			if !contains(rec.Body.String(), "invalid cursor") {
				t.Errorf("%s cursor=%s: want an 'invalid cursor' message, got %s", path, c, rec.Body.String())
			}
		}
	}
}
