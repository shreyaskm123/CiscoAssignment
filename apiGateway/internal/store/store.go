// Package store is the read side of the telemetry pipeline: it answers the
// three public query APIs directly from the ClickHouse events table. The table
// is created (idempotently) by the collector from collector/schema.TableDDL;
// this package is strictly read-only and never changes the schema.
//
// The table's sort key (device_id, source_ts, event_id) is what makes every
// query a prefix range scan rather than a scan + sort:
//
//	ListGPUs            -> GROUP BY device_id, ...            (first key prefix)
//	Telemetry           -> WHERE device_id=? ORDER BY source_ts
//	Telemetry + window  -> WHERE device_id=? AND source_ts BETWEEN ..
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// GPU is one physical GPU (device_id = DCGM UUID) for which telemetry exists.
type GPU struct {
	ID       string `json:"id" jsonschema:"description=Stable device identifier (the DCGM GPU UUID); use it in the telemetry path"`
	Device   string `json:"device" jsonschema:"description=NVIDIA device name, e.g. nvidia0"`
	Index    uint16 `json:"index" jsonschema:"description=GPU index on the host. NOT an identifier: use id in the telemetry path"`
	Hostname string `json:"hostname" jsonschema:"description=Host the GPU is installed in"`
	Model    string `json:"model" jsonschema:"description=GPU model, e.g. NVIDIA H100 80GB HBM3"`
}

// Telemetry is one metric observation for a GPU, ordered by source_ts (and, for
// identical timestamps, by event_id to stay deterministic).
type Telemetry struct {
	EventID  string  `json:"event_id" jsonschema:"description=Unique id of this event: evt_<pod>_L<loop>_O<csv offset>"`
	SourceTS string  `json:"source_ts" jsonschema:"format=date-time,description=When the streamer produced the event (UTC). start_time and end_time filter on this field"`
	Metric   string  `json:"metric_name" jsonschema:"description=DCGM metric name, e.g. DCGM_FI_DEV_GPU_UTIL"`
	GPUIndex uint16  `json:"gpu_index" jsonschema:"description=GPU index on the host"`
	Device   string  `json:"device" jsonschema:"description=NVIDIA device name, e.g. nvidia0"`
	DeviceID string  `json:"device_id" jsonschema:"description=Device id of the GPU this observation belongs to"`
	Model    string  `json:"model_name" jsonschema:"description=GPU model"`
	Hostname string  `json:"hostname" jsonschema:"description=Host the GPU is installed in"`
	Value    float64 `json:"value" jsonschema:"description=The metric value, in the metric's own unit"`
	Cluster  string  `json:"cluster" jsonschema:"description=Cluster tag of the emitting streamer"`
	Pod      string  `json:"pod_name" jsonschema:"description=Streamer pod that emitted the event"`
	CSVLine  uint64  `json:"csv_line_offset" jsonschema:"description=Row in the source metrics CSV this event came from"`
	Loop     uint64  `json:"loop_count" jsonschema:"description=How many times the source CSV had been replayed when this was emitted"`
	MQOffset int64   `json:"mq_offset" jsonschema:"description=Position of the event in the message queue log"`
}

// Store is the query surface the API handlers depend on.
//
// ListGPUs and Telemetry are paginated: each returns one page of rows, the
// total number of rows matching the filter (so the caller can render
// "showing 10 of 6509"), and whether another page exists.
//
// hasMore is reported exactly, by asking the database for one row more than the
// caller asked for and keeping it secret. Guessing instead - "assume there is
// more until a page comes back short" - would make every client walk end with a
// wasted round trip, and guessing the other way would silently truncate results.
type Store interface {
	ListGPUs(ctx context.Context, page Page) ([]GPU, int64, bool, error)
	Telemetry(ctx context.Context, gpuID string, start, end *time.Time, page Page) ([]Telemetry, int64, bool, error)
	Ping(ctx context.Context) error
}

// Page is a requested slice of a result set.
//
// After is an opaque resume key produced by a previous page (see EncodeCursor).
// It is what makes the next-page link safe: the query resumes with
// "key > last key seen" instead of OFFSET n, so rows that are ingested *while*
// a caller is paging cannot shift earlier rows onto a later page. Offset is
// still supported for callers that genuinely want random access, but it is not
// what the next link uses.
//
// Desc flips the sort direction. It defaults to false (oldest first) so a
// caller that never heard of ordering keeps exactly the rows it always got.
// The direction is part of the sort *key*, not a cosmetic flag: it decides both
// the ORDER BY and which side of the resume key to continue from, and it is
// recorded inside the cursor so a key cannot be replayed against the opposite
// direction.
type Page struct {
	Limit  int
	Offset int
	After  string
	Desc   bool
}

// ErrInvalidCursor marks a cursor that is not one this endpoint issued - it was
// truncated, edited, or carried over from a different endpoint. It is a client
// error (400), not a server failure, so callers can tell the two apart.
var ErrInvalidCursor = errors.New("invalid cursor")

// EncodeCursor packs a resume key into an opaque, URL-safe token. It is shared
// by the ClickHouse store and the test fakes so that both agree on the format.
func EncodeCursor(parts ...string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, cursorSep)))
}

// DecodeCursor reverses EncodeCursor.
func DecodeCursor(s string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	return strings.Split(string(raw), cursorSep), nil
}

const cursorSep = "\x1f"

// EncodeTelemetryCursor packs a telemetry resume key - (source_ts, event_id) -
// together with the direction it was produced in.
//
// The direction travels inside the cursor on purpose. The same pair means
// "everything after this row" in an ascending walk and "everything before this
// row" in a descending one, so a cursor carried across an order change would
// not return an error - it would silently return the wrong half of the data set
// and a client summing the pages would quietly compute a wrong number. Failing
// loudly is the only safe outcome.
func EncodeTelemetryCursor(sourceTS, eventID string, desc bool) string {
	if desc {
		return EncodeCursor(sourceTS, eventID, "desc")
	}
	return EncodeCursor(sourceTS, eventID, "asc")
}

// DecodeTelemetryCursor is the inverse of EncodeTelemetryCursor. A two-part
// cursor is accepted as ascending: those are the keys this endpoint issued
// before ordering was configurable, and treating them as descending would break
// a paging loop that was already in flight across a deploy.
func DecodeTelemetryCursor(s string) (sourceTS, eventID string, desc bool, err error) {
	parts, err := DecodeCursor(s)
	if err != nil {
		return "", "", false, ErrInvalidCursor
	}
	switch {
	case len(parts) == 2:
		return parts[0], parts[1], false, nil
	case len(parts) == 3 && parts[2] == "asc":
		return parts[0], parts[1], false, nil
	case len(parts) == 3 && parts[2] == "desc":
		return parts[0], parts[1], true, nil
	default:
		return "", "", false, ErrInvalidCursor
	}
}

// EncodeGPUCursor packs a GPU-list resume key with its direction. See
// EncodeTelemetryCursor for why the direction is part of the key.
func EncodeGPUCursor(id string, desc bool) string {
	if desc {
		return EncodeCursor(id, "desc")
	}
	return EncodeCursor(id, "asc")
}

// DecodeGPUCursor is the inverse of EncodeGPUCursor. A one-part cursor is
// accepted as ascending for the same backwards-compatibility reason as in
// DecodeTelemetryCursor.
func DecodeGPUCursor(s string) (id string, desc bool, err error) {
	parts, err := DecodeCursor(s)
	if err != nil {
		return "", false, ErrInvalidCursor
	}
	switch {
	case len(parts) == 1:
		return parts[0], false, nil
	case len(parts) == 2 && parts[1] == "asc":
		return parts[0], false, nil
	case len(parts) == 2 && parts[1] == "desc":
		return parts[0], true, nil
	default:
		return "", false, ErrInvalidCursor
	}
}

// normalised guards the query layer against a caller that skipped the HTTP
// validation. Without it a zero Limit would quietly become "return nothing",
// which reads as "no such data" rather than "bad request".
func (p Page) normalised() Page {
	if p.Limit <= 0 {
		p.Limit = 10
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
	return p
}

// ClickHouseStore implements Store against the events table.
type ClickHouseStore struct {
	conn  driver.Conn
	table string
}

var gpuIDRE = regexp.MustCompile(`^[A-Za-z0-9_.:+-]{1,256}$`)

// ValidGPU reports whether a path {id} is safe to use as a bound query value.
// All ids in practice match DCGM UUIDs ("GPU-xxxx-...."); the whitelist only
// guards logging/params, it is not a security boundary (values are bound).
func ValidGPU(id string) bool { return gpuIDRE.MatchString(id) }

// Open pings ClickHouse and returns a read-only store for db.table. The table
// must already exist (the collector creates it); Open verifies that with a
// bounds-checking query and fails fast otherwise.
func Open(ctx context.Context, host string, port int, user, password, db, table string) (*ClickHouseStore, error) {
	if !validIdent(db) || !validIdent(table) {
		return nil, fmt.Errorf("invalid table identifiers db=%q table=%q", db, table)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{net.JoinHostPort(host, strconv.Itoa(port))},
		Auth:        clickhouse.Auth{Username: user, Password: password},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	full := db + "." + table
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+full).Scan(new(uint64)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("schema not ready (run the collector first): %w", err)
	}
	return &ClickHouseStore{conn: conn, table: full}, nil
}

func (s *ClickHouseStore) Close() error { return s.conn.Close() }

// Ping is the liveness/readiness signal: a bare round-trip to the server, with
// no table access, so it stays fast regardless of how large the events table
// grows (unlike ListGPUs, which must scan it).
func (s *ClickHouseStore) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

// ListGPUs returns one page of distinct GPUs plus the total number of distinct
// GPUs.
//
// Paging is by resume key on device_id, not OFFSET. The events table is being
// written to continuously, and a GPU that appears mid-walk would otherwise shift
// every later row by one and make the client read one GPU twice. The total is
// computed by a window function over the whole set *inside* the query, before
// the resume filter is applied, so it stays the same on every page.
func (s *ClickHouseStore) ListGPUs(ctx context.Context, page Page) ([]GPU, int64, bool, error) {
	page = page.normalised()
	// limit/offset are ints that have already been parsed from digits, so they
	// are interpolated rather than bound. clickhouse.Parameters only carries
	// strings, and the driver quotes those, which makes LIMIT '10' unparseable.
	q := `SELECT device_id, device_name, gpu_index, hostname, model_name, total FROM (
  SELECT device_id, device_name, gpu_index, hostname, model_name,
         toInt64(count() OVER ()) AS total
  FROM %s
  GROUP BY device_id, device_name, gpu_index, hostname, model_name
)`
	var (
		ctxWith  = ctx
		afterTxt string
		offset   = 0
	)
	// The resume predicate has to point the same way as the sort. With DESC the
	// continuation is "ids before the last one seen", not "ids after it".
	afterCmp, orderBy := ">", "device_id"
	if page.Desc {
		afterCmp, orderBy = "<", "device_id DESC"
	}
	if page.After != "" {
		afterID, cursorDesc, err := DecodeGPUCursor(page.After)
		if err != nil {
			return nil, 0, false, err
		}
		if cursorDesc != page.Desc {
			// The key is from a walk in the other direction; following it would
			// walk the list backwards from here.
			return nil, 0, false, ErrInvalidCursor
		}
		afterTxt = afterID
		q += " WHERE device_id " + afterCmp + " {after:String}"
		ctxWith = clickhouse.Context(ctxWith, clickhouse.WithParameters(clickhouse.Parameters{
			"after": afterTxt,
		}))
	} else {
		offset = page.Offset
	}
	// Ask for one row more than the caller wants; the surplus row is how we
	// know a next page exists without a second query.
	q += fmt.Sprintf(" ORDER BY %s LIMIT %d", orderBy, page.Limit+1)
	if offset > 0 {
		q += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := s.conn.Query(ctxWith, fmt.Sprintf(q, s.table))
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()

	var (
		gpus  []GPU
		total int64
	)
	for rows.Next() {
		var (
			g        GPU
			rowTotal int64
		)
		if err := rows.Scan(&g.ID, &g.Device, &g.Index, &g.Hostname, &g.Model, &rowTotal); err != nil {
			return nil, 0, false, err
		}
		total = rowTotal
		if len(gpus) == page.Limit {
			// The surplus row: a next page exists, but the caller must not see it.
			return gpus, total, true, nil
		}
		gpus = append(gpus, g)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}
	// The window function can only report a total from a row it actually
	// produced, so the last page comes back with total 0. Count directly so the
	// caller learns it has reached the end instead of concluding there is no
	// data at all.
	if total == 0 {
		if total, err = s.countGPUs(ctx); err != nil {
			return nil, 0, false, err
		}
	}
	return gpus, total, false, nil
}

func (s *ClickHouseStore) countGPUs(ctx context.Context) (int64, error) {
	q := fmt.Sprintf(`SELECT toInt64(count()) FROM (SELECT 1 FROM %s
GROUP BY device_id, device_name, gpu_index, hostname, model_name)`, s.table)
	var n int64
	if err := s.conn.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Telemetry returns one page of observations for a physical GPU (device_id),
// ordered by source_ts (the streamer's event time) and then event_id, so the
// order is total. start/end are inclusive window bounds on source_ts and nil
// means unbounded, so a late-arriving event lands in the window where it happened,
// not where it was written. Events are read with FINAL so a residual
// at-least-once replay never surfaces as duplicate rows.
//
// page.Desc reverses that order, which matters most here: an exploratory
// "what is this GPU doing" call almost always wants the newest samples first,
// and ascending order buries them under the entire history of the device.
//
// Paging is by resume key on (source_ts, event_id) rather than OFFSET. This
// table is written to continuously, and every row in a batch can share one
// source_ts, so a row ingested between two page requests would land *before*
// the current position and push everything along: an OFFSET walk would then
// re-read that boundary row and double-count it. Resuming on "greater than the
// last key I saw" cannot skip or repeat a row no matter what arrives meanwhile.
// In descending mode the comparison becomes "less than the last key I saw",
// which is the same guarantee read backwards.
func (s *ClickHouseStore) Telemetry(ctx context.Context, gpuID string, start, end *time.Time, page Page) ([]Telemetry, int64, bool, error) {
	// The window function must see the whole filtered set, not just the rows
	// after the resume key, or total would shrink on every page. So the total is
	// computed in an inner query and the resume filter is applied outside it.
	const inner = `SELECT event_id, source_ts, metric_name, gpu_index, device_name, device_id,
       model_name, hostname, value, csv_line_offset, loop_count, cluster, pod_name, mq_offset,
       toInt64(count() OVER ()) AS total
FROM %s FINAL
WHERE device_id = {gpu:String}`

	params := clickhouse.Parameters{"gpu": gpuID}
	extra := ""
	if start != nil {
		extra += " AND source_ts >= parseDateTime64BestEffort({start:String}, 9)"
		params["start"] = start.Format(time.RFC3339Nano)
	}
	if end != nil {
		extra += " AND source_ts <= parseDateTime64BestEffort({end:String}, 9)"
		params["end"] = end.Format(time.RFC3339Nano)
	}
	page = page.normalised()
	q := fmt.Sprintf("SELECT * FROM ("+inner+extra+")", s.table)

	offset := 0
	// The resume predicate must point the same way as the sort, otherwise a
	// descending walk would continue with "greater than the last row" and jump
	// straight back to the newest data, re-serving the page the client just
	// read. Both halves of the key flip together, so a whole-tuple comparison
	// stays correct.
	afterCmp, orderBy := ">", "source_ts, event_id"
	if page.Desc {
		afterCmp, orderBy = "<", "source_ts DESC, event_id DESC"
	}
	if page.After != "" {
		lastTS, lastID, cursorDesc, err := DecodeTelemetryCursor(page.After)
		if err != nil {
			return nil, 0, false, err
		}
		if cursorDesc != page.Desc {
			// A key issued for the opposite direction would walk the data set
			// the wrong way and return rows the client has already seen.
			return nil, 0, false, ErrInvalidCursor
		}
		parsedTS, err := time.Parse(time.RFC3339Nano, lastTS)
		if err != nil {
			return nil, 0, false, ErrInvalidCursor
		}
		params["cursorTS"] = parsedTS.Format(time.RFC3339Nano)
		params["cursorID"] = lastID
		q += fmt.Sprintf(
			` WHERE (source_ts, event_id) %s (parseDateTime64BestEffort({cursorTS:String}, 9), {cursorID:String})`,
			afterCmp)
	} else {
		offset = page.Offset
	}
	// limit/offset are parsed ints, so they are interpolated: Parameters only
	// carries strings and the driver quotes them, making LIMIT '10' invalid.
	// Ask for one row more than the caller wants; the surplus row is how we
	// know a next page exists without a second query.
	q += fmt.Sprintf(` ORDER BY %s LIMIT %d`, orderBy, page.Limit+1)
	if offset > 0 {
		q += fmt.Sprintf(" OFFSET %d", offset)
	}

	chCtx := clickhouse.Context(ctx, clickhouse.WithParameters(params))
	rows, err := s.conn.Query(chCtx, q)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()

	var (
		out   []Telemetry
		total int64
	)
	for rows.Next() {
		var (
			t        Telemetry
			sourceTS time.Time
			rowTotal int64
		)
		if err := rows.Scan(
			&t.EventID, &sourceTS, &t.Metric, &t.GPUIndex, &t.Device, &t.DeviceID,
			&t.Model, &t.Hostname, &t.Value, &t.CSVLine, &t.Loop,
			&t.Cluster, &t.Pod, &t.MQOffset, &rowTotal,
		); err != nil {
			return nil, 0, false, err
		}
		total = rowTotal
		t.SourceTS = sourceTS.UTC().Format(time.RFC3339Nano)
		if len(out) == page.Limit {
			// The surplus row: a next page exists, but the caller must not see it.
			return out, total, true, nil
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}
	// Reaching the end produces no rows, so the window function never got to
	// report a total. Count directly so the caller is told the true size of the
	// filtered set rather than concluding it is empty. This must use FINAL like
	// the paged query does, otherwise an un-merged replay would make the total
	// disagree with the rows the earlier pages returned.
	if total == 0 {
		total, err = s.countTelemetry(ctx, gpuID, start, end, params)
		if err != nil {
			return nil, 0, false, err
		}
	}
	return out, total, false, nil
}

func (s *ClickHouseStore) countTelemetry(ctx context.Context, gpuID string, start, end *time.Time, params clickhouse.Parameters) (int64, error) {
	extra := ""
	if start != nil {
		extra += " AND source_ts >= parseDateTime64BestEffort({start:String}, 9)"
	}
	if end != nil {
		extra += " AND source_ts <= parseDateTime64BestEffort({end:String}, 9)"
	}
	q := fmt.Sprintf(`SELECT toInt64(count()) FROM %s FINAL WHERE device_id = {gpu:String}`, s.table) + extra
	var n int64
	chCtx := clickhouse.Context(ctx, clickhouse.WithParameters(params))
	if err := s.conn.QueryRow(chCtx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

var identRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validIdent(s string) bool { return identRE.MatchString(s) }
