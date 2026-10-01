// PodLabeler reflects a node's election role onto its own Pod, which is how
// clients get routed to the current leader.
//
// The clients (streamers, the collector) hold a single address: the leader
// Service. That Service's endpoints are selected by the pod label
// component=leader. So the lease decides who leads, and the label is what makes
// the cluster's networking agree - a promoted node relabels itself and the
// endpoint moves without any client changing configuration.
//
// It patches its own pod and nothing else. The RBAC that ships with the chart
// grants get/list/watch and patch on this one pod by name via resourceNames, so
// a compromised MQ cannot relabel the rest of the fleet.
package election

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// PodLabeler patches the component label on a single pod.
type PodLabeler struct {
	HTTPClient *http.Client
	Namespace  string
	Pod        string
	// LeaderValue is the value written for the leader label; the follower label
	// is set to FollowerValue. Writing both (rather than deleting one) keeps
	// the selector unambiguous and the pod's role legible with kubectl get pod.
	LeaderValue   string
	FollowerValue string
	Timeout       time.Duration
}

// NewPodLabeler returns a labeler for this pod. Pod may be passed explicitly;
// it defaults to $POD_NAME then the hostname.
func NewPodLabeler(client *http.Client, namespace, pod, _ string) *PodLabeler {
	if pod == "" {
		pod = os.Getenv("POD_NAME")
	}
	if pod == "" {
		if h, err := os.Hostname(); err == nil {
			pod = h
		}
	}
	return &PodLabeler{
		HTTPClient:    client,
		Namespace:     namespace,
		Pod:           pod,
		LeaderValue:   "leader",
		FollowerValue: "follower",
		Timeout:       10 * time.Second,
	}
}

// podURL is the API path for the pod.
func (p *PodLabeler) podURL() string {
	return fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", p.apiBase(), p.Namespace, p.Pod)
}

// apiBase reads the in-cluster API endpoint from the injected environment.
func (p *PodLabeler) apiBase() string {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return "https://kubernetes.default.svc"
	}
	return "https://" + host + ":" + port
}

// SetRole labels this pod leader (true) or follower (false).
//
// It uses a strategic-merge patch on metadata.labels rather than a read-modify
// write, so it cannot clobber a label another controller owns, and it is
// idempotent: calling it repeatedly with the same role is a no-op server-side.
func (p *PodLabeler) SetRole(ctx context.Context, leader bool) error {
	if p.Pod == "" {
		return fmt.Errorf("pod name unknown (set POD_NAME or -election-identity)")
	}
	value := p.FollowerValue
	if leader {
		value = p.LeaderValue
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{"app.kubernetes.io/component": value},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, p.podURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	token, err := os.ReadFile(defaultTokenPath)
	if err != nil {
		return fmt.Errorf("read service account token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	req.Header.Set("Accept", "application/json")

	timeout := p.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := p.HTTPClient.Do(req.WithContext(rctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("patch pod %s/%s: %d %s", p.Namespace, p.Pod, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	role := "follower"
	if leader {
		role = "leader"
	}
	fmt.Printf("pod %s/%s labelled %s\n", p.Namespace, p.Pod, role)
	return nil
}
