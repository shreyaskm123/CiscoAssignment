// Package openapi generates the OpenAPI 3.1 description of the telemetry API.
//
// The document is derived from the code, not written by hand:
//
//   - the paths, methods, security and error responses come from the route table
//     in internal/httpapi (httpapi.Routes), which is the same table NewServer
//     registers on the mux, so a route cannot be in one and missing from the
//     other;
//   - the schemas come from reflecting over the Go types the handlers actually
//     return, so a renamed or retyped field changes the spec in the same commit.
//
// Regenerate with:
//
//	go generate ./...          # writes ../../openapi.json
//
// The committed openapi.json is the artefact clients use (Postman, generators,
// documentation). A test fails if it is stale, so a changed API cannot ship
// with an out-of-date spec.
package openapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/invopop/jsonschema"

	"apigateway/internal/httpapi"
)

// Title and Version describe the API, not the deployment. Keep them in step
// with the Helm chart's appVersion if the spec is versioned per release.
const (
	Title       = "Telemetry API"
	Version     = "1.0.0"
	Description = "Read access to GPU telemetry collected by the DCGM-style pipeline " +
		"(streamer -> message queue -> collector -> ClickHouse)."
)

//go:generate go run ../../cmd/genopenapi -out ../../openapi.json

// Document is the root of an OpenAPI 3.1 document.
//
// The spec objects are plain structs rather than a vendor's types: the document
// is small and entirely built here, and this keeps the generator readable and
// the output free of that vendor's defaults. JSON object key order is
// deterministic (Go sorts map keys), so regenerating an unchanged API produces a
// byte-identical file and the diff stays empty.
type Document struct {
	OpenAPI    string                `json:"openapi"`
	Info       Info                  `json:"info"`
	Servers    []Server              `json:"servers,omitempty"`
	Tags       []Tag                 `json:"tags,omitempty"`
	Paths      map[string]PathItem   `json:"paths"`
	Components Components            `json:"components"`
	Security   []map[string][]string `json:"security,omitempty"`
}

type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type Server struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// PathItem maps an HTTP method to its operation. Keys are lowercase method
// names, as the specification requires.
type PathItem map[string]*Operation

// Operation describes one endpoint.
type Operation struct {
	OperationID string               `json:"operationId"`
	Summary     string               `json:"summary"`
	Description string               `json:"description,omitempty"`
	Tags        []string             `json:"tags,omitempty"`
	Parameters  []Parameter          `json:"parameters,omitempty"`
	RequestBody *RequestBody         `json:"requestBody,omitempty"`
	Responses   map[string]*Response `json:"responses"`
	// Security has no omitempty on purpose: OpenAPI inherits the document-level
	// requirement when the key is absent, and an unauthenticated route has to say
	// so with an explicit empty list.
	Security   []map[string][]string `json:"security"`
	Deprecated bool                  `json:"deprecated,omitempty"`
}

// Parameter is one query or path parameter.
type Parameter struct {
	Name        string  `json:"name"`
	In          string  `json:"in"`
	Description string  `json:"description,omitempty"`
	Required    bool    `json:"required,omitempty"`
	Schema      *Schema `json:"schema,omitempty"`
}

type RequestBody struct {
	Description string  `json:"description,omitempty"`
	Required    bool    `json:"required,omitempty"`
	Content     Content `json:"content"`
}

// Response is one possible outcome of an operation.
type Response struct {
	Description string  `json:"description"`
	Content     Content `json:"content,omitempty"`
}

// Content maps a media type to its schema.
type Content map[string]MediaType

type MediaType struct {
	Schema *Schema `json:"schema"`
}

type Components struct {
	Schemas         map[string]*Schema         `json:"schemas,omitempty"`
	SecuritySchemes map[string]*SecurityScheme `json:"securitySchemes,omitempty"`
}

type SecurityScheme struct {
	Type         string `json:"type"`
	Scheme       string `json:"scheme,omitempty"`
	BearerFormat string `json:"bearerFormat,omitempty"`
	Description  string `json:"description,omitempty"`
}

// Schema is a JSON Schema 2020-12 object, which is exactly what OpenAPI 3.1
// uses. The library's Schema is embedded so the generated field names, order and
// $defs handling are the library's, not a re-implementation.
type Schema = jsonschema.Schema

// bearer is the scheme name used in every operation's security requirement.
const bearer = httpapi.DefaultBearerAuth

// num is a numeric schema bound; the library models them as json.Number so no
// float rounding can creep into the document.
func num(v string) json.Number { return json.Number(v) }

// Build returns the OpenAPI document for the API as it is implemented.
func Build() *Document {
	doc := &Document{
		OpenAPI: "3.1.0",
		Info: Info{
			Title:       Title,
			Version:     Version,
			Description: Description,
		},
		Paths:    map[string]PathItem{},
		Security: []map[string][]string{{bearer: {}}},
		Components: Components{
			Schemas:         map[string]*Schema{},
			SecuritySchemes: map[string]*SecurityScheme{},
		},
	}
	doc.Components.SecuritySchemes[bearer] = &SecurityScheme{
		Type:         "http",
		Scheme:       "bearer",
		BearerFormat: "opaque signed token",
		Description: "Send `Authorization: Bearer <token>`. A long-lived static token works on every route. " +
			"POST /api/v1/token exchanges one for a short-lived access token; that access token is accepted on " +
			"the data routes but cannot be exchanged again.",
	}
	for _, op := range operations() {
		if doc.Paths[op.path] == nil {
			doc.Paths[op.path] = PathItem{}
		}
		doc.Paths[op.path][strings.ToLower(op.route.Method)] = op.operation(doc)
	}
	return doc
}

// operation is one entry of the route table plus its hand-written documentation.
// The per-operation prose cannot be derived from code; the paths, parameters that
// mirror Go parsing, response codes and schemas are checked against the code by
// the tests.
type operation struct {
	route  httpapi.Route
	path   string // the OpenAPI path, with {id} kept as a path template
	params []Parameter
	ok     string // schema name for the 200 response
	sum    string // summary
	desc   string // description
	tags   []string
	errors []int
}

// operations is the documented API. Every route in httpapi.Routes must appear
// here (enforced by a test), and every operation here is registered on the mux
// (also enforced).
func operations() []operation {
	limit := Parameter{
		Name:        "limit",
		In:          "query",
		Description: fmt.Sprintf("Maximum rows in the page (1..%d).", httpapi.MaxLimit),
		Schema: &Schema{
			Type:    "integer",
			Format:  "int32",
			Minimum: num("1"),
			Maximum: num(strconv.Itoa(httpapi.MaxLimit)),
			Default: httpapi.DefaultLimit,
		},
	}
	offset := Parameter{
		Name:        "offset",
		In:          "query",
		Description: "Zero-based offset to start at. An alternative to cursor for random access; ignored when cursor is present.",
		Schema:      &Schema{Type: "integer", Format: "int32", Minimum: num("0")},
	}
	cursor := Parameter{
		Name:        "cursor",
		In:          "query",
		Description: "Resume key taken from a previous page's `next` link. Follow `next` rather than building cursors.",
		Schema:      &Schema{Type: "string"},
	}
	order := Parameter{
		Name:        "order",
		In:          "query",
		Description: "Sort direction. asc (default) is oldest first; desc is newest first. It also selects which side of the cursor to resume from, so keep it the same while paging.",
		Schema: &Schema{
			Type: "string",
			Enum: []any{"asc", "desc"},
		},
	}
	window := func(name, desc string) Parameter {
		return Parameter{
			Name:        name,
			In:          "query",
			Description: desc,
			Schema:      &Schema{Type: "string", Format: "date-time"},
		}
	}
	id := Parameter{
		Name:        "id",
		In:          "path",
		Required:    true,
		Description: "GPU id, as returned by GET /api/v1/gpus (the `GPU-…` value, not the numeric index).",
		Schema:      &Schema{Type: "string"},
	}

	all := []operation{
		{
			route: mustRoute(httpapi.OpListGPUs),
			path:  "/api/v1/gpus",
			ok:    "GPUListResponse",
			sum:   "List GPUs with telemetry",
			desc: "One page of the GPUs that have telemetry, sorted by device id. " +
				"Follow `next` to walk the rest.",
			tags:   []string{"GPUs"},
			params: []Parameter{limit, offset, cursor, order},
			errors: []int{400, 401, 500},
		},
		{
			route: mustRoute(httpapi.OpGPUTelemetry),
			path:  "/api/v1/gpus/{id}/telemetry",
			ok:    "TelemetryResponse",
			sum:   "Get telemetry for one GPU",
			desc: "Metric observations for a single GPU, ordered by (source_ts, event_id) in the requested direction. " +
				"`start_time` and `end_time` bound the window inclusively and are compared against `source_ts`, " +
				"the time the streamer produced the event. There is no endpoint that returns a GPU's metadata on " +
				"its own; that is what GET /api/v1/gpus is for.",
			tags: []string{"Telemetry"},
			params: []Parameter{
				id, limit, offset, cursor, order,
				window("start_time", "Inclusive start of the window, RFC3339. Must not be after end_time."),
				window("end_time", "Inclusive end of the window, RFC3339."),
			},
			errors: []int{400, 401, 500},
		},
		{
			route: mustRoute(httpapi.OpIssueToken),
			path:  "/api/v1/token",
			ok:    "TokenResponse",
			sum:   "Exchange the static token for a short-lived access token",
			desc: "Presents the long-lived static token and receives a short-lived signed access token, so the " +
				"long-lived credential does not have to be pasted into a client. " +
				"Only a static token may be exchanged: an access token is rejected, so a leaked one expires instead " +
				"of renewing itself. The route exists only when the server is configured with a signing key.",
			tags:   []string{"Auth"},
			errors: []int{401, 500},
		},
		{
			route: mustRoute(httpapi.OpHealth),
			path:  "/healthz",
			ok:    "HealthResponse",
			sum:   "Liveness/readiness of the API and its database",
			desc: "Pings ClickHouse without scanning the events table. Unauthenticated, because kubelet probes " +
				"carry no Authorization header. Not a readiness gate for traffic: use the data routes for that.",
			tags:   []string{"Ops"},
			errors: []int{500},
		},
	}
	return all
}

// mustRoute looks a route up by operation id, panicking on a typo. A panic here
// is a build-time-style failure in a generator, and the tests would fail anyway;
// panicking beats publishing a spec with a silently missing operation.
func mustRoute(op string) httpapi.Route {
	for _, r := range httpapi.Routes() {
		if r.Op == op {
			return r
		}
	}
	panic("openapi: no route registered for operation " + op)
}

// operation turns an entry of the table into a spec operation, pulling the
// schemas out of doc so each response type appears once in components.
func (o operation) operation(doc *Document) *Operation {
	op := &Operation{
		OperationID: o.route.Op,
		Summary:     o.sum,
		Description: o.desc,
		Tags:        o.tags,
		Parameters:  o.params,
		Responses:   map[string]*Response{},
	}
	op.Responses["200"] = &Response{
		Description: http.StatusText(200),
		Content:     Content{"application/json": MediaType{Schema: ref(doc, o.ok)}},
	}
	for _, code := range o.errors {
		desc := http.StatusText(code)
		switch code {
		case 400:
			desc = "Bad request: invalid path parameter, window or paging argument. The message names the offending field."
		case 401:
			desc = "Missing, unknown or expired bearer token."
		case 500:
			desc = "The query failed."
		}
		op.Responses[strconv.Itoa(code)] = &Response{
			Description: desc,
			Content:     Content{"application/json": MediaType{Schema: ref(doc, "ErrorResponse")}},
		}
	}
	if o.route.Auth {
		op.Security = []map[string][]string{{bearer: {}}}
	} else {
		// An explicit empty list means "no security", overriding the document
		// level requirement, rather than inheriting it.
		op.Security = []map[string][]string{}
	}
	return op
}

// ref registers the named component schema (reflecting the Go type on first use)
// and returns a $ref to it.
func ref(doc *Document, name string) *Schema {
	if doc.Components.Schemas[name] == nil {
		doc.Components.Schemas[name] = reflectSchema(name)
	}
	return &Schema{Ref: "#/components/schemas/" + name}
}

// reflectSchema builds the JSON Schema for one of the API's response types.
// Nested types are inlined rather than referenced, so every component here is
// self-contained and no $ref inside a component can dangle.
func reflectSchema(name string) *Schema {
	v, ok := schemaTypes[name]
	if !ok {
		panic("openapi: no Go type registered for schema " + name)
	}
	r := &jsonschema.Reflector{
		ExpandedStruct:            true,
		Anonymous:                 true,
		DoNotReference:            true,
		AllowAdditionalProperties: false,
	}
	s := r.Reflect(v)
	// The dialect belongs to the document, not to each component.
	s.Version = ""
	if s.Title == "" {
		s.Title = name
	}
	return s
}

// schemaTypes maps a component name to the Go type the handler returns for it.
// This is the only hand-maintained link between the spec's names and the code:
// adding a response type means adding it here, and the tests fail until its
// fields are described.
var schemaTypes = map[string]any{
	"GPUListResponse":   &httpapi.GPUListResponse{},
	"TelemetryResponse": &httpapi.TelemetryResponse{},
	"TokenResponse":     &httpapi.TokenResponse{},
	"HealthResponse":    &httpapi.HealthResponse{},
	"ErrorResponse":     &httpapi.ErrorResponse{},
}

// Generate returns the document as the exact bytes to write to openapi.json:
// indented, newline-terminated, and stable across runs.
func Generate() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	// HTML escaping would turn &, < and > into < etc., which is noise in a
	// document meant to be read and diffed by humans.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(Build()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
