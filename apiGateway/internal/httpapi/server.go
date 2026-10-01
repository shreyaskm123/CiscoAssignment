// Package httpapi exposes the telemetry query API over HTTP.
//
// Endpoints:
//
//	GET /api/v1/gpus                          -> all GPUs that have telemetry
//	GET /api/v1/gpus/{id}/telemetry           -> all telemetry for one GPU, by time
//	GET /api/v1/gpus/{id}/telemetry?start_time=..&end_time=.. (inclusive window)
//	GET /api/v1/gpus/{id}/telemetry?order=desc (newest first; default asc)
//	GET /healthz                               -> DB liveness/readiness (backed by store.Ping)
//
// List endpoints page with a cursor and sort in the requested direction:
// telemetry by (source_ts, event_id), the GPU list by device_id.
//
// Times are RFC3339 / RFC3339Nano; responses are JSON, timestamps in
// RFC3339Nano UTC.
//
// Authentication: every /api/v1 route requires `Authorization: Bearer <token>`.
// /healthz is intentionally left unauthenticated - see NewServer.
//
// The route table in routes.go is the API's definition; the OpenAPI 3.1
// document at ../../openapi.json is generated from it with `go generate ./...`.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"apigateway/internal/auth"
	"apigateway/internal/store"
)

// Server wires a store to the HTTP routes.
type Server struct {
	store  store.Store
	auth   *auth.Authenticator
	signer *auth.Signer
	mux    *http.ServeMux
	api    *http.ServeMux
}

// Option customises a Server.
type Option func(*Server)

// WithAuth requires a valid bearer token on every /api/v1 route. A nil
// Authenticator leaves the API unauthenticated (development default).
func WithAuth(a *auth.Authenticator) Option { return func(s *Server) { s.auth = a } }

// WithTokenSigner enables the token exchange endpoint. The Signer must be the
// same one the Authenticator verifies with, otherwise a token issued here
// would be rejected by the middleware.
func WithTokenSigner(sg *auth.Signer) Option { return func(s *Server) { s.signer = sg } }

func NewServer(s store.Store, opts ...Option) *Server {
	srv := &Server{store: s, mux: http.NewServeMux(), api: http.NewServeMux()}
	for _, o := range opts {
		o(srv)
	}
	// The route table is the single source of truth for this API: it is both
	// what gets registered here and what the OpenAPI generator walks, so a route
	// cannot exist in one and be missing from the other.
	//
	// Authenticated routes go on the inner mux, which is mounted behind the
	// bearer-token middleware. /healthz is registered OUTSIDE that wrapper on
	// purpose: the kubelet probes send no Authorization header, so
	// authenticating /healthz would turn every probe into a 401 and restart a
	// perfectly healthy pod - the exact failure mode /healthz exists to prevent.
	for _, rt := range Routes() {
		if rt.Enabled != nil && !rt.Enabled(srv) {
			continue
		}
		if rt.Auth {
			srv.api.Handle(rt.Method+" "+rt.Path, rt.Handle(srv))
			continue
		}
		srv.mux.Handle(rt.Method+" "+rt.Path, rt.Handle(srv))
	}
	srv.mux.Handle(APIPrefix, srv.auth.Middleware(srv.api))
	return srv
}

// handleToken exchanges the caller's long-lived static credential (the same
// bearer token used for ordinary API calls) for a short-lived signed access
// token.
//
// Only a static credential may exchange. A signed access token is rejected
// here, even though the middleware accepts it on every other route: if an
// access token could mint a replacement, a leaked token would refresh itself
// indefinitely and the 15-minute lifetime would be decorative. The only way to
// renew is to present the long-lived secret, which is what makes the leak
// detectable and rotatable.
//
// The response deliberately does NOT include the signing key or any hint of it.
func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	identity := auth.IdentityFrom(r.Context())
	if identity == "" {
		// Unreachable via the mux: the middleware runs first. Guarded anyway so
		// the handler is safe to call directly in tests or from a future route
		// that forgets the wrapper.
		writeError(w, http.StatusUnauthorized, "unauthorized", nil)
		return
	}
	if kind := auth.CredentialFrom(r.Context()); kind != auth.CredentialStatic {
		w.Header().Set("WWW-Authenticate", `Bearer realm="telemetry"`)
		writeError(w, http.StatusUnauthorized,
			"the token exchange requires the long-lived API token, not an access token", nil)
		return
	}
	tok, exp, err := s.signer.Issue(identity)
	if err != nil {
		// The only realistic cause is a misconfigured signer, which NewServer
		// already prevents by not registering the route.
		writeError(w, http.StatusInternalServerError, "could not issue token", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, TokenResponse{
		AccessToken: tok,
		TokenType:   "Bearer",
		ExpiresIn:   int64(s.signer.TTL().Seconds()),
		ExpiresAt:   exp.UTC().Format(time.RFC3339),
		Identity:    identity,
	})
}

func (s *Server) Handler() http.Handler { return s.mux }

// parseTime parses an optional query time as RFC3339 (with optional nanos).
func parseTime(v string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errors.New("times must be RFC3339, e.g. 2025-07-18T20:42:34Z")
}

// Pagination defaults. A page of 10 keeps the common exploratory call small
// while the "next" link makes walking the whole set a loop rather than a
// single enormous response.
const (
	// DefaultLimit and MaxLimit are exported because the OpenAPI generator
	// publishes them: a spec that advertised a different bound from the one
	// parsePage enforces would be a lie clients act on.
	DefaultLimit = 10
	MaxLimit     = 1000
)

// parsePage reads limit/offset. A bad value is a 400 rather than a silent
// fallback: a client asking for limit=abc and quietly receiving 10 rows would
// have no way to tell its paging was not doing what it asked.
// parsePage reads limit/offset. A blank or whitespace-only value is treated as
// "not supplied" rather than an error, so a client templating ?limit={{pageSize}}
// gets the default page instead of a 400 it has no way to interpret. A genuinely
// malformed value is an error, and the message says which mistake was made:
// "abc" is not a number, whereas "0" is a number that is out of range.
func parsePage(q url.Values) (store.Page, error) {
	page := store.Page{Limit: DefaultLimit}
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return page, fmt.Errorf("limit must be an integer")
		}
		if n < 1 {
			return page, fmt.Errorf("limit must be a positive integer")
		}
		if n > MaxLimit {
			return page, fmt.Errorf("limit must not exceed %d", MaxLimit)
		}
		page.Limit = n
	}
	if v := strings.TrimSpace(q.Get("offset")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return page, fmt.Errorf("offset must be an integer")
		}
		if n < 0 {
			return page, fmt.Errorf("offset must be a non-negative integer")
		}
		page.Offset = n
	}
	// A cursor and an offset are alternative ways to say "start here". If both
	// are present the cursor wins, because it is the position the previous page
	// actually reached, and silently ignoring either would page wrongly.
	page.After = strings.TrimSpace(q.Get("cursor"))
	// Blank means "not supplied" and keeps the historical oldest-first default,
	// so an existing client that never passes order sees no behaviour change.
	switch v := strings.ToLower(strings.TrimSpace(q.Get("order"))); v {
	case "", "asc":
		page.Desc = false
	case "desc":
		page.Desc = true
	default:
		// Rejecting rather than defaulting matters: a typo'd order=dsc that
		// silently fell back to ascending would hand the caller the oldest
		// rows while they believe they asked for the newest, and they would have
		// no way to tell.
		return page, fmt.Errorf("order must be asc or desc")
	}
	return page, nil
}

// nextLink builds an absolute URL for the following page, preserving the
// caller's filters (start_time/end_time, order and any limit they chose) so
// following it cannot silently widen, narrow or re-sort the result set. It
// returns "" when the current page is the last one.
//
// Preserving order is what keeps a descending walk descending: the resume key
// only means "before this row" while the sort also runs newest-first. Dropping
// it would make page two re-sort ascending and hand the caller the oldest rows
// it had already seen.
//
// X-Forwarded-* are honoured because the API is normally reached through a
// port-forward or an ingress, and a link pointing at the wrong host is worse
// than no link.
//
// cursor is the resume key for the last row of the page that was just returned.
// The next link advances by that key rather than by an offset, so a client that
// follows the links sees every row exactly once even while the table is being
// written to. hasMore comes from the store, which knows exactly rather than
// guessing from the page size.
func nextLink(r *http.Request, page store.Page, hasMore bool, cursor string) string {
	if !hasMore || cursor == "" {
		return ""
	}
	u := *r.URL
	q := u.Query()
	// The resume key replaces any offset: resuming from a key and from a
	// position are different mechanisms and must not be mixed in one request.
	q.Set("cursor", cursor)
	q.Del("offset")
	if page.Limit != DefaultLimit {
		q.Set("limit", strconv.Itoa(page.Limit))
	}
	u.RawQuery = q.Encode()

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = strings.TrimSpace(strings.Split(p, ",")[0])
	}
	host := r.Host
	if h := r.Header.Get("X-Forwarded-Host"); h != "" {
		host = strings.TrimSpace(strings.Split(h, ",")[0])
	}
	return scheme + "://" + host + u.RequestURI()
}

func (s *Server) handleListGPUs(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	gpus, total, hasMore, err := s.store.ListGPUs(r.Context(), page)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	var cursor string
	if n := len(gpus); n > 0 {
		cursor = store.EncodeGPUCursor(gpus[n-1].ID, page.Desc)
	}
	writeJSON(w, http.StatusOK, GPUListResponse{
		GPUs:   gpus,
		Count:  len(gpus),
		Total:  total,
		Limit:  page.Limit,
		Offset: page.Offset,
		Order:  orderName(page.Desc),
		Next:   optString(nextLink(r, page, hasMore, cursor)),
	})
}

// orderName renders the direction for the response body, so a client can confirm
// what it got without having to remember what it asked for.
func orderName(desc bool) string {
	if desc {
		return "desc"
	}
	return "asc"
}

// writeStoreErr separates a bad request from a real failure. A cursor the client
// mangled is their mistake and deserves a 400 with an actionable message, not a
// 500 that looks like the service is broken.
func writeStoreErr(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrInvalidCursor) {
		writeError(w, http.StatusBadRequest, "invalid cursor", nil)
		return
	}
	writeError(w, http.StatusInternalServerError, "query failed", err)
}

// optString turns an empty string into nil, which the response types render as
// JSON null: "next": null means "this is the last page".
func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" || !store.ValidGPU(id) {
		writeError(w, http.StatusBadRequest, "invalid GPU id", nil)
		return
	}

	q := r.URL.Query()
	page, err := parsePage(q)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	var start, end *time.Time
	if v := q.Get("start_time"); v != "" {
		t, err := parseTime(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid start_time", err)
			return
		}
		start = &t
	}
	if v := q.Get("end_time"); v != "" {
		t, err := parseTime(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid end_time", err)
			return
		}
		end = &t
	}
	if start != nil && end != nil && start.After(*end) {
		writeError(w, http.StatusBadRequest, "start_time must not be after end_time", nil)
		return
	}

	evs, total, hasMore, err := s.store.Telemetry(r.Context(), id, start, end, page)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	var cursor string
	if n := len(evs); n > 0 {
		// The resume key must be exactly the ORDER BY key: source_ts then
		// event_id, in the direction the page was sorted, matching the store's
		// ORDER BY and resume predicate exactly.
		cursor = store.EncodeTelemetryCursor(evs[n-1].SourceTS, evs[n-1].EventID, page.Desc)
	}

	resp := TelemetryResponse{
		Events: evs,
		GPUID:  id,
		Count:  len(evs),
		Total:  total,
		Limit:  page.Limit,
		Offset: page.Offset,
		Order:  orderName(page.Desc),
		Next:   optString(nextLink(r, page, hasMore, cursor)),
	}
	// Echoed only when the caller sent a bound, so an unbounded query is
	// distinguishable from one over the whole range of time.
	if start != nil {
		s := start.UTC().Format(time.RFC3339Nano)
		resp.StartTime = &s
	}
	if end != nil {
		s := end.UTC().Format(time.RFC3339Nano)
		resp.EndTime = &s
	}
	writeJSON(w, http.StatusOK, resp)
}

// healthTimeout bounds the DB round-trip so the probe fails fast and
// deterministically instead of hanging on an unresponsive server.
const healthTimeout = 2 * time.Second

// handleHealth reports API liveness/readiness. It pings ClickHouse directly
// (no table scan), so it does not degrade as the events table grows — the
// health check must not depend on how much telemetry exists.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, "ping failed", err)
		return
	}
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode: %v", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string, err error) {
	if err != nil {
		log.Printf("%s: %v", msg, err)
	}
	writeJSON(w, code, ErrorResponse{Error: msg})
}

// ServeBlocking runs the API until ctx is cancelled, then shuts down.
func ServeBlocking(ctx context.Context, addr string, store store.Store, opts ...Option) error {
	srv := &http.Server{Addr: addr, Handler: NewServer(store, opts...).Handler(), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shut)
	}
}
