package httpapi

import (
	"net/http"

	"apigateway/internal/store"
)

// This file is the API's shape, in one place: which routes exist, and what
// bytes come back from each. The OpenAPI document is generated from it (see
// internal/openapi and `go generate ./...`), so a route added here, or a field
// changed on one of the response types, changes the published spec in the same
// commit. Two tests keep that honest: the generated spec must match the
// committed openapi.json, and every operation in the spec must be routable on
// the real mux.

// Operation IDs. They appear in the spec and in server logs, so they are
// treated as public API: renaming one breaks client generators.
const (
	OpListGPUs        = "listGPUs"
	OpGPUTelemetry    = "getGPUTelemetry"
	OpIssueToken      = "issueToken"
	OpHealth          = "getHealth"
	APIPrefix         = "/api/v1/"
	DefaultBearerAuth = "bearerAuth"
)

// Route is one HTTP endpoint of the API.
type Route struct {
	// Method and Path are the ServeMux pattern ("GET /api/v1/gpus"), split so
	// the spec can read them without re-parsing.
	Method string
	Path   string
	// Op is the operationId published in the spec.
	Op string
	// Auth marks a route as served behind the bearer-token middleware. Routes
	// without it are registered outside that wrapper, which is how /healthz
	// stays reachable by kubelet probes.
	Auth bool
	// Enabled reports whether the route exists on this server. Nil means
	// always. The token exchange exists only when a signing key is configured,
	// because without one there is no way to issue a token.
	Enabled func(s *Server) bool
	// Handle returns the handler for the route.
	Handle func(s *Server) http.HandlerFunc
}

// routes is the single source of truth for the API's paths. NewServer registers
// exactly this table, and the OpenAPI generator walks exactly this table, so the
// two cannot drift apart.
var routes = []Route{
	{
		Method: http.MethodGet,
		Path:   "/api/v1/gpus",
		Op:     OpListGPUs,
		Auth:   true,
		Handle: func(s *Server) http.HandlerFunc { return s.handleListGPUs },
	},
	{
		Method: http.MethodGet,
		Path:   "/api/v1/gpus/{id}/telemetry",
		Op:     OpGPUTelemetry,
		Auth:   true,
		Handle: func(s *Server) http.HandlerFunc { return s.handleTelemetry },
	},
	{
		Method:  http.MethodPost,
		Path:    "/api/v1/token",
		Op:      OpIssueToken,
		Auth:    true,
		Enabled: func(s *Server) bool { return s.signer != nil },
		Handle:  func(s *Server) http.HandlerFunc { return s.handleToken },
	},
	{
		Method: http.MethodGet,
		Path:   "/healthz",
		Op:     OpHealth,
		// Not Auth: kubelet probes carry no Authorization header, and
		// authenticating this route would restart a healthy pod.
		Handle: func(s *Server) http.HandlerFunc { return s.handleHealth },
	},
}

// Routes returns the API's routes. The slice is a copy, so a caller (the spec
// generator) cannot disturb the server's registration.
func Routes() []Route {
	out := make([]Route, len(routes))
	copy(out, routes)
	return out
}

// ---- response bodies ----
//
// These are exported (and tagged for the spec) because they are the contract:
// what a client receives. They used to be built as map[string]any at each call
// site, which no generator could describe and no compiler could check.

// GPUListResponse is the body of GET /api/v1/gpus.
//
// Count is the size of this page, Total is every matching row, and Next is the
// absolute URL of the following page (null on the last page). Follow Next rather
// than incrementing Offset: Next resumes on a key, so rows ingested while you
// page cannot shift onto a later page.
type GPUListResponse struct {
	GPUs   []store.GPU `json:"gpus"`
	Count  int         `json:"count" jsonschema:"description=GPUs in this page (equals limit unless this is the last page)"`
	Total  int64       `json:"total" jsonschema:"description=GPUs that have telemetry, across all pages"`
	Limit  int         `json:"limit" jsonschema:"description=Page size requested"`
	Offset int         `json:"offset" jsonschema:"description=Zero-based offset this page started at"`
	Order  string      `json:"order" jsonschema:"enum=asc,enum=desc,description=Sort direction by device_id"`
	// Next is always present but nullable: "next": null is how a client knows
	// it has reached the last page.
	Next *string `json:"next" jsonschema:"oneof_type=string;null" jsonschema_description:"Absolute URL of the next page; null on the last page"`
}

// TelemetryResponse is the body of GET /api/v1/gpus/{id}/telemetry.
//
// Events holds one page of metric observations, ordered by (source_ts,
// event_id) in the requested direction. StartTime and EndTime echo the window
// the server actually applied and are absent when the request had none.
type TelemetryResponse struct {
	Events []store.Telemetry `json:"events"`
	GPUID  string            `json:"gpu_id" jsonschema:"description=The GPU whose telemetry this is"`
	Count  int               `json:"count" jsonschema:"description=Events in this page (equals limit unless this is the last page)"`
	Total  int64             `json:"total" jsonschema:"description=Events matching the window, across all pages"`
	Limit  int               `json:"limit" jsonschema:"description=Page size requested"`
	Offset int               `json:"offset" jsonschema:"description=Zero-based offset this page started at"`
	Order  string            `json:"order" jsonschema:"enum=asc,enum=desc,description=Sort direction by (source_ts, event_id)"`
	// Next is always present but nullable: "next": null is how a client knows
	// it has reached the last page.
	Next      *string `json:"next" jsonschema:"oneof_type=string;null" jsonschema_description:"Absolute URL of the next page; null on the last page"`
	StartTime *string `json:"start_time,omitempty" jsonschema:"format=date-time,description=Inclusive start of the applied window; absent if unbounded"`
	EndTime   *string `json:"end_time,omitempty" jsonschema:"format=date-time,description=Inclusive end of the applied window; absent if unbounded"`
}

// TokenResponse is the body of POST /api/v1/token.
type TokenResponse struct {
	AccessToken string `json:"access_token" jsonschema:"description=Short-lived signed access token; send it as the bearer credential"`
	TokenType   string `json:"token_type" jsonschema:"enum=Bearer,description=Always Bearer"`
	ExpiresIn   int64  `json:"expires_in" jsonschema:"description=Access-token lifetime in seconds"`
	ExpiresAt   string `json:"expires_at" jsonschema:"format=date-time,description=When the access token stops being accepted"`
	Identity    string `json:"identity" jsonschema:"description=The identity the token was issued for"`
}

// HealthResponse is the body of GET /healthz.
type HealthResponse struct {
	Status string `json:"status" jsonschema:"enum=ok"`
}

// ErrorResponse is the body of every non-2xx response.
type ErrorResponse struct {
	Error string `json:"error" jsonschema:"description=Human-readable reason; the message names the offending field where one applies"`
}
