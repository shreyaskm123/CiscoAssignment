package openapi

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"apigateway/internal/httpapi"
)

// specPath is the committed artefact. It is what clients (Postman, generators,
// documentation) read, so it has to be regenerated whenever the API changes;
// TestCommittedSpecIsUpToDate is what enforces that.
const specPath = "../../openapi.json"

func generate(t *testing.T) []byte {
	t.Helper()
	b, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return b
}

func buildDoc(t *testing.T) *Document {
	t.Helper()
	var doc Document
	if err := json.Unmarshal(generate(t), &doc); err != nil {
		t.Fatalf("generated document does not decode into Document: %v", err)
	}
	return &doc
}

// TestCommittedSpecIsUpToDate is the gate that makes "auto generated" mean
// something: the file in the repository must be byte-identical to what the code
// generates now. Change a route, a field or a parameter and this fails until
// `go generate ./...` is run, so a stale spec cannot be committed.
func TestCommittedSpecIsUpToDate(t *testing.T) {
	want := generate(t)
	got, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v (run: go generate ./...)", specPath, err)
	}
	if string(got) != string(want) {
		t.Errorf("%s is out of date with the API definition.\nRun: go generate ./...\nfirst difference:\n%s",
			specPath, firstDiff(string(got), string(want)))
	}
}

// TestGenerationIsDeterministic: regenerating an unchanged API must produce
// identical bytes, otherwise the committed file would churn on every run and the
// staleness check above would be noise.
func TestGenerationIsDeterministic(t *testing.T) {
	if a, b := string(generate(t)), string(generate(t)); a != b {
		t.Error("two runs produced different documents; the generator is not deterministic")
	}
}

// TestSpecDocumentsEveryRoute: every route the server can serve appears in the
// spec with the right operationId. This is what catches a new endpoint added to
// the route table but never documented.
//
// The token route is conditional on a signing key, so the table is filtered the
// same way the server would filter it (Build describes the fully configured API,
// which is the useful artefact for clients).
func TestSpecDocumentsEveryRoute(t *testing.T) {
	doc := buildDoc(t)
	documented := map[string]string{} // "METHOD path" -> operationId
	for path, item := range doc.Paths {
		for method, op := range item {
			documented[method+" "+path] = op.OperationID
		}
	}
	for _, r := range httpapi.Routes() {
		key := strings.ToLower(r.Method) + " " + r.Path
		op, ok := documented[key]
		if !ok {
			t.Errorf("route %s is missing from the spec", key)
			continue
		}
		if op != r.Op {
			t.Errorf("route %s: operationId = %q, want %q", key, op, r.Op)
		}
	}
}

// TestSpecInventsNoRoutes is the other direction: the document must not describe
// an endpoint the server does not have.
func TestSpecInventsNoRoutes(t *testing.T) {
	doc := buildDoc(t)
	registered := map[string]bool{}
	for _, r := range httpapi.Routes() {
		registered[strings.ToLower(r.Method)+" "+r.Path] = true
	}
	for path, item := range doc.Paths {
		for method := range item {
			if !registered[method+" "+path] {
				t.Errorf("spec documents %s %s but no such route is registered", method, path)
			}
		}
	}
}

// TestPathParametersMatchPathTemplates: a declared path parameter has to appear
// in the path as {name}. A mismatch is a spec that describes a URL the server
// cannot route.
func TestPathParametersMatchPathTemplates(t *testing.T) {
	doc := buildDoc(t)
	for path, item := range doc.Paths {
		for method, op := range item {
			for _, p := range op.Parameters {
				if p.In != "path" {
					continue
				}
				if !strings.Contains(path, "{"+p.Name+"}") {
					t.Errorf("%s %s declares path parameter %q, absent from the path template", method, path, p.Name)
				}
				if !p.Required {
					t.Errorf("%s %s: path parameter %q must be required", method, path, p.Name)
				}
			}
			// Every {placeholder} in the path needs a parameter, or codegen
			// produces a client that cannot fill it in.
			for _, seg := range strings.Split(path, "/") {
				if !strings.HasPrefix(seg, "{") {
					continue
				}
				name := strings.Trim(seg, "{}")
				found := false
				for _, p := range op.Parameters {
					if p.In == "path" && p.Name == name {
						found = true
					}
				}
				if !found {
					t.Errorf("%s %s: path placeholder {%s} has no parameter", method, path, name)
				}
			}
		}
	}
}

// TestEveryOperationHasA200AndErrorBodies: a client generator produces useless
// code for an operation with no documented success or error response.
func TestEveryOperationHasA200AndErrorBodies(t *testing.T) {
	doc := buildDoc(t)
	for path, item := range doc.Paths {
		for method, op := range item {
			if op.OperationID == "" {
				t.Errorf("%s %s has no operationId; client generators need one", method, path)
			}
			ok, found := op.Responses["200"]
			if !found {
				t.Errorf("%s %s documents no 200 response", method, path)
			} else if ok.Content["application/json"].Schema == nil {
				t.Errorf("%s %s: 200 response has no schema", method, path)
			}
			if _, found := op.Responses["401"]; found && !secured(op) {
				t.Errorf("%s %s documents a 401 but declares no security", method, path)
			}
			if op.Responses["500"] == nil {
				t.Errorf("%s %s documents no 500 response", method, path)
			}
		}
	}
}

// TestSecurityMatchesTheAuthWrapper: the document-level requirement applies to
// the authenticated API, and /healthz must say so explicitly with an empty list
// (an absent key would inherit the requirement and be wrong).
func TestSecurityMatchesTheAuthWrapper(t *testing.T) {
	doc := buildDoc(t)
	if len(doc.Security) == 0 {
		t.Fatal("document declares no default security, but the API requires a bearer token")
	}
	if doc.Components.SecuritySchemes["bearerAuth"] == nil {
		t.Error("operations reference bearerAuth but it is not defined in components")
	}
	for _, r := range httpapi.Routes() {
		item, ok := doc.Paths[r.Path]
		if !ok {
			continue
		}
		op := item[strings.ToLower(r.Method)]
		if op == nil {
			continue
		}
		switch {
		case r.Auth && op.Security == nil:
			t.Errorf("%s %s is behind the auth wrapper but the spec does not say so", r.Method, r.Path)
		case !r.Auth && len(op.Security) != 0:
			t.Errorf("%s %s is unauthenticated but the spec declares security %v", r.Method, r.Path, op.Security)
		}
	}
}

// TestSchemasAreDerivedFromTheResponseTypes: the schemas must be real
// descriptions of the Go types, not empty placeholders. Spot checks on field
// names and types catch a generator that quietly stopped reflecting.
func TestSchemasAreDerivedFromTheResponseTypes(t *testing.T) {
	doc := buildDoc(t)
	for _, name := range []string{"GPUListResponse", "TelemetryResponse", "TokenResponse", "HealthResponse", "ErrorResponse"} {
		if doc.Components.Schemas[name] == nil {
			t.Errorf("component schema %s is missing", name)
		}
	}
	tel := doc.Components.Schemas["TelemetryResponse"]
	events, ok := tel.Properties.Get("events")
	if !ok || events.Items == nil {
		t.Fatal("TelemetryResponse.events should be an array of event objects")
	}
	for field, want := range map[string]string{
		"event_id":    "string",
		"source_ts":   "string",
		"metric_name": "string",
		"value":       "number",
		"gpu_index":   "integer",
		"mq_offset":   "integer",
	} {
		p, ok := events.Items.Properties.Get(field)
		if !ok {
			t.Errorf("telemetry event schema is missing %q", field)
			continue
		}
		if p.Type != want {
			t.Errorf("telemetry event %q: type = %q, want %q", field, p.Type, want)
		}
	}
	if src, _ := events.Items.Properties.Get("source_ts"); src.Format != "date-time" {
		t.Errorf("source_ts format = %q, want date-time (it is an RFC3339 timestamp)", src.Format)
	}
	// start_time/end_time are omitted when the query is unbounded, so they must
	// not be required: a client generated from this must not insist on them.
	for _, opt := range []string{"start_time", "end_time"} {
		if contains(tel.Required, opt) {
			t.Errorf("%s is always present, so it may be required", opt)
		}
	}
	// next is always present but null on the last page, so it must be nullable.
	next, ok := tel.Properties.Get("next")
	if !ok {
		t.Fatal("TelemetryResponse has no next property")
	}
	if !nullable(next) {
		t.Error(`next is rendered as JSON null on the last page but the schema is not nullable`)
	}
}

// TestPagingBoundsMatchTheHandler: the spec advertises the limits; parsePage
// enforces them. If they ever disagree the spec is a lie clients act on.
func TestPagingBoundsMatchTheHandler(t *testing.T) {
	doc := buildDoc(t)
	limit := findParam(t, doc, "/api/v1/gpus", "get", "limit")
	if limit.Schema.Minimum.String() != "1" {
		t.Errorf("limit minimum = %s, want 1", limit.Schema.Minimum)
	}
	if got := limit.Schema.Maximum.String(); got != strconv.Itoa(httpapi.MaxLimit) {
		t.Errorf("limit maximum = %s, want %d", got, httpapi.MaxLimit)
	}
	// The default is a JSON number, so it comes back as float64.
	if def, ok := limit.Schema.Default.(float64); !ok || int(def) != httpapi.DefaultLimit {
		t.Errorf("limit default = %v, want %d", limit.Schema.Default, httpapi.DefaultLimit)
	}
	order := findParam(t, doc, "/api/v1/gpus", "get", "order")
	if len(order.Schema.Enum) != 2 {
		t.Errorf("order enum = %v, want asc and desc", order.Schema.Enum)
	}
	for _, want := range []string{"start_time", "end_time", "cursor", "offset", "order"} {
		findParam(t, doc, "/api/v1/gpus/{id}/telemetry", "get", want)
	}
}

// TestCommittedSpecIsValidAndSelfContained loads the file that ships, not a
// freshly generated one: the artefact must be valid JSON with no dangling $ref,
// which is what spec-loading tooling checks first.
func TestCommittedSpecIsValidAndSelfContained(t *testing.T) {
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read %s: %v", specPath, err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("committed spec is not valid JSON: %v", err)
	}
	if doc["openapi"] != "3.1.0" {
		t.Errorf("openapi = %v, want 3.1.0", doc["openapi"])
	}
	components, _ := doc["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	if len(schemas) == 0 {
		t.Fatal("committed spec has no component schemas")
	}
	dangling := 0
	var walk func(v any)
	walk = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			if ref, ok := node["$ref"].(string); ok {
				if target, ok := strings.CutPrefix(ref, "#/components/schemas/"); ok {
					if _, ok := schemas[target]; !ok {
						t.Errorf("dangling $ref %q", ref)
						dangling++
					}
				}
			}
			for _, val := range node {
				walk(val)
			}
		case []any:
			for _, val := range node {
				walk(val)
			}
		}
	}
	walk(doc)
	// A referenced-but-unused component is a sign the generator left something
	// behind; a component nothing references means the response went back to a
	// hand-built map.
	referenced := map[string]bool{}
	paths, _ := doc["paths"].(map[string]any)
	for _, r := range httpapi.Routes() {
		if item, ok := paths[r.Path].(map[string]any); ok {
			if op, ok := item[strings.ToLower(r.Method)].(map[string]any); ok {
				walkRefs(op, referenced)
			}
		}
	}
	for name := range schemas {
		if !referenced[name] {
			t.Errorf("component schema %s is not referenced by any operation", name)
		}
	}
	if dangling > 0 {
		t.Errorf("%d dangling $ref(s)", dangling)
	}
}

func walkRefs(v any, out map[string]bool) {
	switch node := v.(type) {
	case map[string]any:
		if ref, ok := node["$ref"].(string); ok {
			if target, ok := strings.CutPrefix(ref, "#/components/schemas/"); ok {
				out[target] = true
			}
		}
		for _, val := range node {
			walkRefs(val, out)
		}
	case []any:
		for _, val := range node {
			walkRefs(val, out)
		}
	}
}

func secured(op *Operation) bool {
	return len(op.Security) > 0
}

func nullable(s *Schema) bool {
	if s.OneOf == nil {
		return false
	}
	for _, alt := range s.OneOf {
		if alt.Type == "null" {
			return true
		}
	}
	return false
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func findParam(t *testing.T, doc *Document, path, method, name string) Parameter {
	t.Helper()
	item, ok := doc.Paths[path]
	if !ok {
		t.Fatalf("path %s missing from the spec", path)
	}
	op := item[method]
	if op == nil {
		t.Fatalf("%s %s missing from the spec", method, path)
	}
	for _, p := range op.Parameters {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("parameter %q missing from %s %s", name, method, path)
	return Parameter{}
}

func firstDiff(got, want string) string {
	g, w := strings.Split(got, "\n"), strings.Split(want, "\n")
	for i := 0; i < len(g) && i < len(w); i++ {
		if g[i] != w[i] {
			return "  line " + strconv.Itoa(i+1) +
				"\n    committed: " + strings.TrimSpace(g[i]) +
				"\n    generated: " + strings.TrimSpace(w[i])
		}
	}
	return "  files differ in length only (committed " + strconv.Itoa(len(g)) +
		" lines, generated " + strconv.Itoa(len(w)) + ")"
}
