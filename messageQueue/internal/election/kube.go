// KubeBackend is the production Backend: it reads and writes a
// coordination.k8s.io/v1 Lease through the Kubernetes API server using the pod's
// own ServiceAccount credentials.
//
// It speaks plain REST rather than pulling in client-go. client-go's
// leaderelection package is the obvious alternative, but it drags roughly forty
// transitive modules into a module whose entire dependency set is currently
// google.golang.org/grpc. The Lease API is a small, stable, well-documented
// surface (GET/POST/PUT one object, CAS on resourceVersion), so talking to it
// directly keeps the supply chain of this service unchanged and makes the
// mutual-exclusion logic - the part that has to be right - unit-testable
// against an in-memory fake instead of only against a live cluster.
//
// The ServiceAccount token is re-read from disk on every request rather than
// cached, so a rotated projected token takes effect without a restart, matching
// how the MQ's auth token file is handled.
package election

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// In-cluster ServiceAccount paths and the in-cluster API endpoint. The
// KUBERNETES_SERVICE_HOST/PORT pair is injected into every pod.
const (
	defaultTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	defaultCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// KubeBackend implements Backend against the Kubernetes API server.
type KubeBackend struct {
	// BaseURL is the API server root, e.g. https://10.96.0.1:443.
	BaseURL string
	// TokenPath and CAPath point at the projected ServiceAccount credentials.
	TokenPath string
	CAPath    string
	// HTTPClient is used for all requests; nil builds one from the CA.
	HTTPClient *http.Client
	// RequestTimeout bounds a single API call.
	RequestTimeout time.Duration
}

// NewKubeBackend returns a Backend configured for the pod's in-cluster
// credentials. It returns an error when the credentials are absent so a
// misconfigured pod fails loudly instead of silently never winning an election.
func NewKubeBackend(tokenPath, caPath string) (*KubeBackend, error) {
	if tokenPath == "" {
		tokenPath = defaultTokenPath
	}
	if caPath == "" {
		caPath = defaultCAPath
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST/PORT unset (pass -election-backend=memory for local runs)")
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read cluster CA %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("cluster CA %s contained no usable certificates", caPath)
	}
	if _, err := os.Stat(tokenPath); err != nil {
		return nil, fmt.Errorf("read service account token %s: %w", tokenPath, err)
	}
	return &KubeBackend{
		BaseURL:        "https://" + host + ":" + port,
		TokenPath:      tokenPath,
		CAPath:         caPath,
		RequestTimeout: 10 * time.Second,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			},
		},
	}, nil
}

// leaseURL is the collection URL for Leases in a namespace.
func (b *KubeBackend) leaseURL(ns, name string) string {
	base := fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/%s/leases", strings.TrimSuffix(b.BaseURL, "/"), ns)
	if name == "" {
		return base
	}
	return base + "/" + name
}

// do performs one authenticated request and returns the status code and body.
func (b *KubeBackend) do(ctx context.Context, method, u string, body []byte) (int, []byte, error) {
	token, err := os.ReadFile(b.TokenPath)
	if err != nil {
		return 0, nil, fmt.Errorf("read service account token: %w", err)
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	timeout := b.RequestTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := b.HTTPClient.Do(req.WithContext(rctx))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, out, nil
}

// k8sLease is the wire shape of a coordination.k8s.io/v1 Lease.
type k8sLease struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   k8sMeta    `json:"metadata"`
	Spec       k8sLeaseSp `json:"spec"`
}

type k8sMeta struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type k8sLeaseSp struct {
	HolderIdentity       *string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds *int32  `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          *string `json:"acquireTime,omitempty"`
	RenewTime            *string `json:"renewTime,omitempty"`
	LeaseTransitions     *int32  `json:"leaseTransitions,omitempty"`
}

// metav1.Time serialises as RFC3339 with MICROSECOND precision, and it always
// carries an explicit offset ("2006-01-02T15:04:05.000000Z07:00"). Emitting
// second-precision RFC3339 instead makes the API server reject the whole Lease
// with a 400 parse error, which looks exactly like a broken election: every node
// stays a follower forever. The layout is therefore fixed, not a preference.
const metaTimeLayout = "2006-01-02T15:04:05.000000Z07:00"

func fmtTime(t time.Time) string { return t.UTC().Format(metaTimeLayout) }

// parseTime accepts the canonical layout and plain RFC3339, so a Lease written
// by kubectl or another client still round-trips.
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{metaTimeLayout, time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func toWire(l *Lease) *k8sLease {
	spec := k8sLeaseSp{}
	if l.HolderIdentity != "" {
		h := l.HolderIdentity
		spec.HolderIdentity = &h
	}
	if l.LeaseDurationSecs > 0 {
		d := l.LeaseDurationSecs
		spec.LeaseDurationSeconds = &d
	}
	if !l.AcquireTime.IsZero() {
		a := fmtTime(l.AcquireTime)
		spec.AcquireTime = &a
	}
	if !l.RenewTime.IsZero() {
		r := fmtTime(l.RenewTime)
		spec.RenewTime = &r
	}
	t := l.LeaseTransitions
	spec.LeaseTransitions = &t
	return &k8sLease{
		APIVersion: "coordination.k8s.io/v1",
		Kind:       "Lease",
		Metadata:   k8sMeta{Name: l.Name, Namespace: l.Namespace, ResourceVersion: l.ResourceVersion},
		Spec:       spec,
	}
}

func fromWire(w *k8sLease) *Lease {
	l := &Lease{
		Namespace:        w.Metadata.Namespace,
		Name:             w.Metadata.Name,
		ResourceVersion:  w.Metadata.ResourceVersion,
		LeaseTransitions: 0,
	}
	if w.Spec.HolderIdentity != nil {
		l.HolderIdentity = *w.Spec.HolderIdentity
	}
	if w.Spec.LeaseDurationSeconds != nil {
		l.LeaseDurationSecs = *w.Spec.LeaseDurationSeconds
	}
	if w.Spec.AcquireTime != nil {
		l.AcquireTime = parseTime(*w.Spec.AcquireTime)
	}
	if w.Spec.RenewTime != nil {
		l.RenewTime = parseTime(*w.Spec.RenewTime)
	}
	if w.Spec.LeaseTransitions != nil {
		l.LeaseTransitions = *w.Spec.LeaseTransitions
	}
	return l
}

// statusError maps an API status code onto the sentinel errors the elector
// branches on.
func statusError(code int, body []byte) error {
	switch code {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict:
		return ErrConflict
	default:
		var st struct {
			Message string `json:"message"`
			Reason  string `json:"reason"`
		}
		_ = json.Unmarshal(body, &st)
		if st.Message != "" {
			return fmt.Errorf("kubernetes api %d: %s (%s)", code, st.Message, st.Reason)
		}
		return fmt.Errorf("kubernetes api %d: %s", code, strings.TrimSpace(string(body)))
	}
}

// Get returns the Lease, or ErrNotFound.
func (b *KubeBackend) Get(ctx context.Context, ns, name string) (*Lease, error) {
	u := b.leaseURL(ns, url.PathEscape(name))
	code, body, err := b.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, statusError(code, body)
	}
	var w k8sLease
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("decode lease %s/%s: %w", ns, name, err)
	}
	return fromWire(&w), nil
}

// Create creates the Lease. A 409 (already exists) is ErrConflict, which the
// elector treats as losing the create race and retries.
func (b *KubeBackend) Create(ctx context.Context, l *Lease) error {
	u := b.leaseURL(l.Namespace, "")
	body, err := json.Marshal(toWire(l))
	if err != nil {
		return err
	}
	code, resp, err := b.do(ctx, http.MethodPost, u, body)
	if err != nil {
		return err
	}
	if code != http.StatusCreated && code != http.StatusOK {
		return statusError(code, resp)
	}
	return nil
}

// Update replaces the Lease, relying on the API server to reject a stale
// resourceVersion with 409. That rejection is the mutual-exclusion guarantee:
// only the candidate whose Get produced this resourceVersion can win.
func (b *KubeBackend) Update(ctx context.Context, l *Lease) error {
	u := b.leaseURL(l.Namespace, url.PathEscape(l.Name))
	body, err := json.Marshal(toWire(l))
	if err != nil {
		return err
	}
	code, resp, err := b.do(ctx, http.MethodPut, u, body)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return statusError(code, resp)
	}
	return nil
}
