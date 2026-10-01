// Command messagequeue is the MessageQueue service: it accepts the streamer's
// publish streams, acknowledges events, commits per-consumer offsets, and runs
// the partition registry that hands each streamer replica a unique partition
// index (see internal/registry). Run it alongside the streamer replicas:
//
//	messagequeue -addr :50051 &           # terminal 1 (leader)
//	streamer                              # terminal 2..N (registry assigns index)
//
// High availability is automatic and requires no manual step. Run the same
// binary in every MQ pod with a durable -data-dir; the pods elect a leader among
// themselves through a Kubernetes Lease (internal/election) and the winner
// promotes itself in place:
//
//	messagequeue -addr :50051 -data-dir /var/lib/mq/wal.log \
//	            -sync-replicas 1 \
//	            -election -election-namespace telemetry
//
// The elected node serves writes; the others replicate from it and serve reads
// from their own WAL. When the leader dies its lease goes stale, the survivor
// promotes itself within -election-lease-duration, and clients - which address
// the leader only through the Service selector - reconnect to the new leader on
// their own. The failover path is:
//
//   - every publish ack is withheld until -sync-replicas followers have applied
//     the event, so an acked event is already on the survivor's disk before the
//     leader can die;
//   - the survivor promotes in place from that durable log, so it does not have
//     to re-receive anything and clients keep their offsets;
//   - a deposed leader that is still alive but partitioned from the API server
//     fences itself read-only within -election-renew-deadline, so two nodes can
//     never both ack.
//
// Options -leader and -data-dir remain for running a dedicated replica against
// an externally managed leader; see RunFollower in internal/mq.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"messagequeue/internal/auth"
	"messagequeue/internal/election"
	"messagequeue/internal/mq"

	mqpb "streamer/proto"
)

var (
	addr         = flag.String("addr", ":50051", "gRPC listen address")
	healthAddr   = flag.String("health-addr", ":8081", "HTTP listen address for /readyz and /healthz (empty = disabled)")
	ttlSeconds   = flag.Int("ttl-seconds", 15, "default heartbeat TTL for the partition registry")
	dataDir      = flag.String("data-dir", "", "if set, persistence directory for the write-ahead log (empty = in-memory, no crash recovery)")
	leader       = flag.String("leader", "", "if set, run as a follower replicating from this leader gRPC address (requires -data-dir)")
	syncReplicas = flag.Int("sync-replicas", 0, "leader: withhold a publish ack until this many followers have applied the event (0 = async)")

	// Leader election. Enabled with -election in a Kubernetes deployment; the
	// two MQ pods then decide their own roles instead of being pinned to them
	// by which StatefulSet they were created in.
	electionOn      = flag.Bool("election", false, "contend for the leader lease and promote/demote this node automatically")
	electionNS      = flag.String("election-namespace", "", "namespace holding the leader Lease (default: $POD_NAMESPACE, else 'default')")
	electionName    = flag.String("election-lease", "mq-leader", "name of the coordination.k8s.io Lease used for leader election")
	electionIdent   = flag.String("election-identity", "", "unique identity per candidate (default: $HOSTNAME, else 'mq')")
	leaseDuration   = flag.Duration("election-lease-duration", election.DefaultLeaseDuration, "how long a lease stays valid without renewal; lower bound on failover time")
	renewDeadline   = flag.Duration("election-renew-deadline", election.DefaultRenewDeadline, "how long a leader keeps failing to renew before fencing itself read-only")
	retryPeriod     = flag.Duration("election-retry-period", election.DefaultRetryPeriod, "interval between acquire and renew attempts")
	leaderSvc       = flag.String("leader-service", "telemetry-messagequeue-leader", "Service that always resolves to the current leader; used by followers to find it and to publish the leader label")
	podNameEnv      = flag.String("pod-name-env", "POD_NAME", "environment variable holding this pod's name")
	podNamespaceEnv = flag.String("pod-namespace-env", "POD_NAMESPACE", "environment variable holding this pod's namespace")
)

// podIdentity returns this pod's name, falling back so a local run does not
// need the downward API.
func podIdentity(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if v := os.Getenv(*podNameEnv); v != "" {
		return v
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "mq"
}

func podNamespace(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if v := os.Getenv(*podNamespaceEnv); v != "" {
		return v
	}
	return "default"
}

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("messagequeue listening on %s (registry ttl=%ds)", lis.Addr(), *ttlSeconds)

	if *leader != "" && *dataDir == "" {
		log.Fatalf("-leader requires -data-dir: a follower must durably persist the replicated log")
	}
	if *syncReplicas < 0 {
		log.Fatalf("-sync-replicas must be >= 0, got %d", *syncReplicas)
	}
	if *electionOn && *leader != "" {
		log.Fatalf("-election and -leader are mutually exclusive: -election decides the role, -leader pins it")
	}
	if *electionOn && *dataDir == "" {
		log.Fatalf("-election requires -data-dir: a promoted node must hold an acked event on disk")
	}

	// Tokens come from the environment, never a flag: flags are visible in
	// `ps` and /proc to everything in the container. A malformed spec is fatal
	// so a typo cannot silently leave the queue open to any publisher.
	authn, err := auth.New(os.Getenv("MQ_AUTH_TOKENS_FILE"), os.Getenv("MQ_AUTH_TOKENS"))
	if err != nil {
		log.Fatalf("mq auth: %v", err)
	}
	if !authn.Enabled() {
		log.Printf("WARNING: MQ auth is DISABLED (set MQ_AUTH_TOKENS_FILE or MQ_AUTH_TOKENS); " +
			"any client that can reach this port may publish, consume and replicate")
	}

	var server *mq.Server
	if *dataDir != "" {
		store, err := mq.NewWalStore(*dataDir)
		if err != nil {
			log.Fatalf("open wal %s: %v", *dataDir, err)
		}
		defer store.Close()
		server = mq.NewWithStore(time.Duration(*ttlSeconds)*time.Second, store)
		log.Printf("messagequeue using durable write-ahead log: %s", *dataDir)
	} else {
		server = mq.New(time.Duration(*ttlSeconds) * time.Second)
	}

	// Published for /metrics before anything else starts, so the first scrape
	// already has the real numbers rather than a 503.
	setStatsProbe(server)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var followerCancel context.CancelFunc
	var ha *mq.HA
	if *electionOn {
		// Start fenced. Every node begins as a follower and only the lease
		// holder is promoted, so there is no window at boot where both nodes
		// consider themselves the leader.
		ha = mq.NewHA(server)
		ha.Demote()
		// Publish the role holder to the probe handlers before they start, so
		// /readyz reports the real role from the first request instead of
		// reporting "follower" for a node that is about to be promoted.
		setRoleProbe(ha)
		log.Printf("leader election enabled: identity=%s lease=%s/%s duration=%s renewDeadline=%s",
			podIdentity(*electionIdent), podNamespace(*electionNS), *electionName, *leaseDuration, *renewDeadline)
		go runElection(ctx, server, ha)
	} else if *leader != "" {
		if err := server.RequireDurableStore("-leader"); err != nil {
			log.Fatalf("%v", err)
		}
		followerCtx, cancel := context.WithCancel(ctx)
		followerCancel = cancel
		go func() {
			if err := mq.RunFollower(followerCtx, *leader, server, followerAuthOptions()...); err != nil && followerCtx.Err() == nil {
				log.Printf("follower replication stopped: %v", err)
			}
		}()
	} else {
		server.EnableSyncReplication(*syncReplicas)
		if *syncReplicas > 0 {
			log.Printf("messagequeue acting as leader; publish acks wait for %d follower(s) (-sync-replicas)", *syncReplicas)
		}
	}

	// The probes must answer in every mode, not just under election: the chart
	// renders liveness and readiness unconditionally, so a static -leader
	// deployment with no health listener would have its pods restarted forever
	// by a probe that can never connect. The role is read from roleProbe, which
	// is nil in those modes, so readiness is then always true.
	startHealth()

	// Interceptors guard every registered service, so MessageQueue
	// (publish/consume) AND Replication (follower sync) both require a token.
	// The stream interceptor is the load-bearing one: PublishEvents and Consume
	// are bidirectional streams, so unary-only enforcement would leave the whole
	// data path open.
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authn.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(authn.StreamServerInterceptor()),
	)
	mqpb.RegisterMessageQueueServer(srv, server)
	mqpb.RegisterReplicationServer(srv, server)

	go func() {
		<-ctx.Done()
		log.Printf("messagequeue shutting down")
		srv.GracefulStop()
		if followerCancel != nil {
			followerCancel() // leave the leader's stream cleanly
		}
	}()

	// -sync-replicas only gates acks on the node that accepts writes.
	if *syncReplicas > 0 && *leader != "" {
		log.Printf("note: -sync-replicas has no effect on a follower (it is read-only); set it on the leader")
	}

	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	if err := server.Close(); err != nil {
		log.Printf("close: %v", err)
	}
	log.Printf("messagequeue stopped")
}

// runElection wires the lease elector to the server's role and to the follower
// replication loop, and keeps this pod's labels in step with its role.
//
// Three loops run concurrently for the life of the process:
//
//	elector  - acquires/renews the Lease; on acquire it promotes the server,
//	           on loss it demotes (fences) the server.
//	follower - replicates from whoever currently holds the Lease. It runs only
//	           while this node is not the leader, and re-resolves the leader
//	           address on every retry, so a failover needs no reconfiguration.
//	labeler  - reflects the role onto this pod so the Service selector
//	           component=leader follows the election.
//
// The labeler matters for client routing: clients address the MQ through the
// leader Service, and the endpoints of that Service are selected by this label.
// Without it, promoting a node would not move any client onto it.
func runElection(ctx context.Context, server *mq.Server, ha *mq.HA) {
	identity := podIdentity(*electionIdent)
	ns := podNamespace(*electionNS)

	backend, err := election.NewKubeBackend("", "")
	if err != nil {
		// Without a coordination backend this node cannot prove it is the
		// leader, so it must stay a follower. Failing closed is the only safe
		// option: running as an elected leader without an election is exactly
		// the split brain this design exists to prevent.
		log.Fatalf("leader election requires the Kubernetes API: %v", err)
	}

	labeler := election.NewPodLabeler(backend.HTTPClient, ns, podIdentity(*electionIdent), *leaderSvc)

	// The follower loop re-resolves the leader from the Lease on every attempt,
	// so a promotion by the peer is picked up without any restart.
	follow := mq.NewFollowerLoop(server, func(ctx context.Context) (string, error) {
		l, err := backend.Get(ctx, ns, *electionName)
		if err != nil {
			return "", err
		}
		if l.HolderIdentity == "" || l.HolderIdentity == identity {
			// No leader recorded, or we are it: nothing to follow.
			return "", errNoLeaderYet
		}
		return leaderPodAddr(ctx, l.HolderIdentity), nil
	}, followerAuthOptions()...)

	followCtx, cancelFollow := context.WithCancel(ctx)
	defer cancelFollow()
	go func() {
		if err := follow.Run(followCtx); err != nil && followCtx.Err() == nil {
			log.Printf("follower replication stopped: %v", err)
		}
	}()

	el := election.New(election.Config{
		Identity:      identity,
		Namespace:     ns,
		LeaseName:     *electionName,
		LeaseDuration: *leaseDuration,
		RenewDeadline: *renewDeadline,
		RetryPeriod:   *retryPeriod,
		Logf:          log.Printf,
	}, backend)

	// Hand the running elector to the health server started in main. It is a
	// late bind because the elector is constructed here, after the listener is
	// already serving /healthz with the always-ready default.
	setLeaseProbe(el)

	// onStarted: we now hold the lease. Promote first, then label, so a client
	// can never be routed here before the node is able to serve writes. Order
	// matters - the reverse would send publishes to a read-only node.
	onStarted := func(leadCtx context.Context) error {
		if err := ha.Promote(*syncReplicas); err != nil {
			return fmt.Errorf("promote: %w", err)
		}
		if err := labeler.SetRole(ctx, true); err != nil {
			// The node is the leader but clients cannot reach it. Keep the
			// lease (failing to label is not a reason to abandon the term, or
			// the lease would flap and the pipeline would flap with it) and log
			// loudly; the retry in the labeler loop covers transient API errors.
			log.Printf("WARNING: elected leader but failed to set the leader label: %v (clients may not route to this pod)", err)
		}
		<-leadCtx.Done()
		return leadCtx.Err()
	}

	// onStopped: the lease is gone. Fence writes immediately - before anything
	// else and before a rival can be elected - and drop the leader label so no
	// client is routed here. This is the split-brain guard.
	onStopped := func(context.Context) error {
		ha.Demote()
		if err := labeler.SetRole(ctx, false); err != nil {
			log.Printf("WARNING: failed to clear the leader label: %v (this pod is read-only; publishes are fenced)", err)
		}
		// A node that lost the lease may win it back, or may end up following
		// the peer. Restarting the follower loop covers both.
		cancelFollow()
		return nil
	}

	if err := el.Run(ctx, onStarted, onStopped); err != nil && ctx.Err() == nil {
		log.Printf("election loop stopped: %v", err)
	}
}

// leaseProbe is set once election is wired up. It is late-bound because the
// health listener has to come up before the elector exists, and because
// /readyz must answer during that window rather than refusing connections.
//
// The element type is a holder around the one-method interface rather than
// *election.Elector so the split-brain guard can be exercised in a unit test.
// Reproducing that for real needs an API server a node can reach and then stop
// reaching; *election.Elector satisfies the interface, so production is
// unaffected. A struct is used rather than atomic.Value because the latter
// panics when two different concrete types are stored, which a test seam would
// eventually trigger.
type leaseFreshness interface{ LeaseFresh(now time.Time) bool }

type leaseHolder struct{ f leaseFreshness }

var leaseProbe atomic.Pointer[leaseHolder]

func setLeaseProbe(el *election.Elector) { leaseProbe.Store(&leaseHolder{f: el}) }

func loadLeaseProbe() leaseFreshness {
	if h := leaseProbe.Load(); h != nil {
		return h.f
	}
	return nil
}

// startHealth serves the probes that keep a leader that has lost the API server
// out of the leader Service's endpoints.
//
// The component=leader label is the routing signal, but a partitioned leader
// cannot patch its own label: it has no route to the API server, which is
// exactly why it lost contact. So the label is only cleared on the happy path,
// and during a partition the stale leader stays in the endpoint list and keeps
// receiving writes it must not accept.
//
// Readiness closes that hole without needing the API server. Kubelet evaluates
// it locally and removes NotReady pods from endpoints on its own, so a leader
// that can no longer renew its lease drops out of rotation on a local clock.
// /readyz is therefore role- and lease-aware; /healthz stays a plain liveness
// check because a follower is perfectly healthy and a leader that steps down
// must NOT be restarted - restarting it would destroy the quorum.
// roleProbe is the HA the probe handlers read, set as soon as the role exists
// and independent of when the listener comes up. Read through a pointer rather
// than captured directly so the handler can never be handed a stale copy of
// the role holder.
var roleProbe atomic.Pointer[mq.HA]

func setRoleProbe(ha *mq.HA) { roleProbe.Store(ha) }
func loadRoleProbe() *mq.HA  { return roleProbe.Load() }

// statsProbe is the live server, published for the /metrics handler. It stays
// nil until a server exists, and /metrics says so rather than guessing, exactly
// as /readyz tolerates a nil role probe.
//
// A mutex rather than atomic.Value because the value is allowed to be nil, and
// storing a nil interface in an atomic.Value panics.
var (
	statsProbeMu sync.Mutex
	statsProbe   mq.StatsProvider
)

func setStatsProbe(p mq.StatsProvider) {
	statsProbeMu.Lock()
	statsProbe = p
	statsProbeMu.Unlock()
}

func loadStatsProbe() mq.StatsProvider {
	statsProbeMu.Lock()
	defer statsProbeMu.Unlock()
	return statsProbe
}

// newHealthServer returns the probe server, or nil when the listener is
// disabled. Split out from startHealth so "empty = disabled" can be asserted
// directly.
//
// The guard belongs here rather than at the call site: http.Server treats an
// empty Addr as ":http", so an unguarded empty -health-addr would silently bind
// port 80 instead of turning probes off as the flag documents.
func newHealthServer() *http.Server {
	if *healthAddr == "" {
		return nil
	}
	return &http.Server{
		Addr:              *healthAddr,
		Handler:           buildHealthMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func startHealth() {
	srv := newHealthServer()
	if srv == nil {
		log.Printf("health endpoints disabled (-health-addr is empty)")
		return
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server: %v", err)
		}
	}()
	log.Printf("health endpoints listening on %s (/readyz, /healthz, /metrics)", srv.Addr)
}

// buildHealthMux returns the probe routes. Split out from startHealth so the
// handlers can be tested directly, without binding a port.
func buildHealthMux() *http.ServeMux {
	mux := http.NewServeMux()

	// ha is nil in the modes that have no role (no -election, no -leader), so
	// this must tolerate nil: the chart renders these probes for every mode and
	// a panic would close the connection with no response, which reads as a
	// dead process and gets the pod restarted forever.
	isLeader := func() bool {
		ha := loadRoleProbe()
		return ha != nil && ha.IsLeader()
	}

	// /healthz: liveness. The process is up. Deliberately independent of role
	// and lease state so a stepped-down leader is never killed by kubelet.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// /readyz: readiness. A follower is always ready so it can be promoted and
	// serve reads. A leader is ready only while its lease renewal is fresh.
	// With no elector (static -leader mode) there is no lease, so always ready.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		leader := isLeader()
		fresh := true
		if lp := loadLeaseProbe(); lp != nil {
			fresh = lp.LeaseFresh(time.Now())
		}
		if leader && !fresh {
			// Say why: this is the split-brain guard firing, and an operator
			// seeing 503 here is looking at a leader that lost the API server.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("leader but lease renewal is stale; not routable\n"))
			return
		}
		role := "follower"
		if leader {
			role = "leader"
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "ok role=%s\n", role)
	})

	// /metrics: Prometheus text exposition of the queue depth. Depth is the one
	// number an operator needs and cannot get any other way here: the process
	// log rotates within seconds, so the log head cannot be read from it
	// reliably, and the retained window is what tells you the queue is backing
	// up. A stalled or absent consumer stops the retention barrier, the log
	// grows, and eventually it fills its volume and the MQ refuses publishes.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		p := loadStatsProbe()
		if p == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no server registered; metrics unavailable\n"))
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, renderMetrics(p.Stats(), metricNode(), readProcStats(*dataDir)))
	})

	return mux
}

// metricNode labels every series with the pod that produced it, because these
// numbers are per-node: only the leader's head and base describe the log, and a
// follower's lag answers a different question.
func metricNode() string {
	if n := os.Getenv("POD_NAME"); n != "" {
		return n
	}
	h, _ := os.Hostname()
	return h
}

// procStats are the host-level numbers a scrape needs. They are deliberately not
// part of mq.Stats: none of them come from the queue, and reading them under the
// server lock would add filesystem work to a lock the data path contends for.
type procStats struct {
	// RSSBytes is the process resident set size, which is what the container
	// memory limit applies to. A Go process also reserves a large virtual
	// address space (~1.3GB here) that costs nothing until touched, so VMSize is
	// reported separately to stop it being mistaken for usage.
	RSSBytes    int64
	VMSizeBytes int64
	// HeapInuseBytes is live heap: allocated objects still reachable. This is the
	// number that grows with queue depth, unlike HeapAllocBytes (allocated,
	// including garbage not yet collected) and SysBytes (obtained from the OS,
	// including what Go is holding but not using).
	HeapInuseBytes uint64
	HeapAllocBytes uint64
	SysBytes       uint64
	Goroutines     int
	// WALBytes is the on-disk write-ahead log, the durable mirror of the queue.
	// It is the other half of "footprint": memory says how much is live now,
	// this says what a crash would have to replay.
	WALBytes int64
	// WALErr is set when the WAL could not be stat'd, so a scrape can report the
	// failure instead of silently reporting a zero-byte log.
	WALErr error
}

// readProcStats samples the process. Every read is best-effort: where /proc does
// not exist (macOS during development) the fields that come from there stay 0
// rather than failing the scrape.
func readProcStats(dataDir string) procStats {
	var p procStats
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	p.HeapInuseBytes, p.HeapAllocBytes, p.SysBytes = ms.HeapInuse, ms.HeapAlloc, ms.Sys
	p.Goroutines = runtime.NumGoroutine()

	if raw, err := os.ReadFile("/proc/self/statm"); err == nil {
		// Fields are page counts: size, resident, ...
		fields := strings.Fields(string(raw))
		if len(fields) > 0 {
			if pages, perr := strconv.ParseInt(fields[0], 10, 64); perr == nil {
				p.VMSizeBytes = pages * int64(os.Getpagesize())
			}
		}
		if len(fields) > 1 {
			if pages, perr := strconv.ParseInt(fields[1], 10, 64); perr == nil {
				p.RSSBytes = pages * int64(os.Getpagesize())
			}
		}
	}
	if dataDir != "" {
		if fi, err := os.Stat(dataDir); err == nil {
			p.WALBytes = fi.Size()
		} else {
			p.WALErr = err
		}
	}
	return p
}

// renderMetrics emits a snapshot in Prometheus text exposition format. Separate
// from the handler so the output can be asserted on without binding a port.
func renderMetrics(st mq.Stats, node string, proc procStats) string {
	var b strings.Builder
	role := 0
	if st.IsLeader {
		role = 1
	}
	// HELP and TYPE are written once per metric name, not once per sample. The
	// per-consumer metrics carry one sample per consumer, and repeating the
	// metadata for each one produces an exposition most parsers reject.
	written := make(map[string]bool)
	sample := func(name, help, typ string, value int64, labels string) {
		if !written[name] {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
			written[name] = true
		}
		fmt.Fprintf(&b, "%s{%s%s} %d\n", name, nodeLabels(node), labels, value)
	}
	sample("mq_role", "1 on the leader, 0 on a follower.", "gauge", int64(role), "")
	sample("mq_log_head", "Last log offset assigned to an event; -1 when empty.", "gauge", st.Head, "")
	sample("mq_log_base", "Offset of the first entry still retained; everything below it is committed and trimmed.", "gauge", st.Base, "")
	sample("mq_retained_entries", "Entries in the log that no consumer has committed yet (queue depth).", "gauge", st.Retained(), "")
	for _, c := range st.Consumers {
		// %q does the label escaping: it renders quotes, backslashes and
		// newlines the way a Prometheus label value needs them. Escaping by hand
		// as well would double the backslashes and change the value.
		labels := fmt.Sprintf(",group=%q,consumer=%q,kind=%q", c.Group, c.Consumer, c.Kind)
		sample("mq_consumer_committed_offset", "Position a consumer has committed; only kind=\"log-offset\" indexes this log.", "gauge", c.Committed, labels)
		// A lag only exists against a cursor in this log. The streamer group
		// commits a CSV row, and subtracting that from the head would report a
		// lag of millions, so it is withheld instead.
		if lag, ok := st.Lag(c); ok {
			sample("mq_consumer_lag", "How far a log-offset consumer is behind the log head.", "gauge", lag, labels)
		}
	}
	// Memory and disk. The memory limit is 1Gi and RSS is what it applies to, so
	// that pair is what an alert should be written against.
	sample("mq_process_resident_memory_bytes", "Process resident set size; this is what the container memory limit applies to.", "gauge", proc.RSSBytes, "")
	sample("mq_process_virtual_memory_bytes", "Virtual address space reserved by the Go runtime; largely untouched and not real usage.", "gauge", proc.VMSizeBytes, "")
	sample("mq_go_memstats_heap_inuse_bytes", "Live Go heap: allocated objects still reachable.", "gauge", int64(proc.HeapInuseBytes), "")
	sample("mq_go_memstats_heap_alloc_bytes", "Go heap allocated, including garbage not yet collected.", "gauge", int64(proc.HeapAllocBytes), "")
	sample("mq_go_memstats_sys_bytes", "Memory obtained from the OS for the Go runtime, including what it holds but is not using.", "gauge", int64(proc.SysBytes), "")
	sample("mq_go_goroutines", "Goroutines currently running.", "gauge", int64(proc.Goroutines), "")
	// A WAL that cannot be stat'd is reported as a failure, not a silent zero:
	// zero would read as "no log on disk" and hide the thing worth noticing.
	failed := int64(0)
	if proc.WALErr != nil {
		failed = 1
	}
	sample("mq_wal_stat_failed", "1 when the write-ahead log could not be stat'd.", "gauge", failed, "")
	if failed == 0 {
		sample("mq_wal_bytes", "Size of the write-ahead log on disk: the durable mirror of the queue.", "gauge", proc.WALBytes, "")
	}

	return b.String()
}

// nodeLabels renders the node label set every sample carries, so scrapes of both
// MQ pods stay distinguishable.
func nodeLabels(node string) string {
	return fmt.Sprintf("node=%q", node)
}

// leaderPodAddr is the address a follower uses to reach the elected leader.
// It resolves through the headless peer Service so a pod's changing IP is
// picked up by DNS rather than baked in.
func leaderPodAddr(_ context.Context, holder string) string {
	ns := podNamespace(*electionNS)
	return holder + ".telemetry-messagequeue-peers." + ns + ".svc.cluster.local:" + addrPort()
}

// addrPort extracts the port from -addr, defaulting to the MQ port.
func addrPort() string {
	if i := strings.LastIndex(*addr, ":"); i >= 0 {
		return (*addr)[i+1:]
	}
	return "50051"
}

var errNoLeaderYet = errors.New("no elected leader to follow yet")

// followerAuthOptions presents this node's own token to the leader, read from
// the environment (see the note on tokens above: never a flag).
func followerAuthOptions() []mq.FollowerOption {
	var opts []mq.FollowerOption
	if path := os.Getenv("MQ_AUTH_TOKEN_FILE"); path != "" {
		opts = append(opts, mq.WithFollowerTokenFile(path))
	}
	if token := os.Getenv("MQ_AUTH_TOKEN"); token != "" {
		opts = append(opts, mq.WithFollowerToken(token))
	}
	return opts
}
