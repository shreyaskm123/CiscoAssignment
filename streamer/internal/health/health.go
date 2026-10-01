// Package health serves the liveness and readiness endpoints Kubernetes probes.
//
// Why this exists: the pod spec ran `command: ["/bin/true"]` as the probe, which
// cannot fail. A pod that had stopped making progress therefore still reported
// 1/1 Running, and a pipeline running at a fraction of its normal rate looked
// perfectly healthy. These endpoints make the pod's status reflect whether it is
// actually doing its job, so a wedged pod goes NotReady and, with a liveness
// probe, gets restarted.
//
// The two endpoints ask deliberately different questions:
//
//	GET /healthz   is this process still able to serve?      (liveness)
//	GET /readyz    can it do its job right now?               (readiness)
//
// Liveness must NOT depend on a dependency. If ClickHouse or the MQ is down and
// liveness reflected that, every pod would restart-loop at once, turning a
// dependency outage into a crash loop. So /healthz reports only that this server
// is up, and every dependency-shaped signal lives on /readyz.
//
// /readyz returns the whole Check as JSON, including diagnostics, so the numbers
// needed to explain a NotReady are available from the probe itself without
// shelling into the pod.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
)

// Check is a service's self-assessment: whether it is ready, why not, and the
// counters that explain the answer.
type Check struct {
	Ready  bool              `json:"ready"`
	Reason string            `json:"reason,omitempty"`
	Fields map[string]string `json:"fields,omitempty"`
}

// Server serves /healthz and /readyz. The zero value is not usable; call New.
type Server struct {
	addr  string
	check func() Check
}

// New builds a Server. An empty addr disables serving, which is useful for
// running the service without a probe listener (tests, one-off batch runs).
// A nil check means "always ready", which is only appropriate for a process with
// no dependencies to lose.
func New(addr string, check func() Check) *Server {
	return &Server{addr: addr, check: check}
}

// Handler returns the mux, exported so tests can exercise it without binding a
// port.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		// Answering at all is the signal: the process is running and its
		// scheduler is turning. Nothing dependency-related belongs here.
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		c := Check{Ready: true}
		if s.check != nil {
			c = s.check()
		}
		code := http.StatusOK
		if !c.Ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, c)
	})
	return mux
}

// Start serves until ctx is cancelled, then shuts down gracefully. It returns nil
// on a clean shutdown, so a caller can treat a cancelled context as success.
func (s *Server) Start(ctx context.Context) error {
	if s.addr == "" {
		log.Printf("health: no listen address configured, probes disabled")
		<-ctx.Done()
		return nil
	}
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("health: serving /healthz and /readyz on %s", s.addr)
		errCh <- srv.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		// A probe in flight during shutdown should not fail the process.
		if err := srv.Shutdown(shutdown); err != nil {
			log.Printf("health: shutdown: %v", err)
		}
		return nil
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
