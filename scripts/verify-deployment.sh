#!/usr/bin/env bash
# End-to-end verification of the telemetry stack running in kind.
#
# Deploys (or reuses) the release and then proves the properties that actually
# matter, in the order a failure would show up:
#
#   1. baseline   - release deployed, all pods Running/Ready
#   2. election   - exactly one leader, leader Service has exactly one endpoint
#   3. wal        - leader and follower WALs are both live and being written
#   4. ingestion  - rows are arriving in ClickHouse at a sane rate
#   5. api        - /healthz, token exchange, and an authenticated data route
#   6. failover   - remove the leader, confirm a survivor takes the lease and
#                   that offsets stay contiguous across it (no gaps, no dupes)
#   7. scale      - scale up and back down, confirming the topology re-forms
#
# Sections 1-5 only observe, so they run concurrently. Sections 6-7 scale the
# StatefulSet and delete the Lease, so they must run serially after the
# observation phase: overlapping them would let a scale-down race a WAL check
# and produce failures that say nothing about the system.
#
# Usage:
#   scripts/verify-deployment.sh                 # deploy if needed, then verify
#   scripts/verify-deployment.sh --skip-deploy   # verify the current release
#   scripts/verify-deployment.sh --cleanup       # helm uninstall + delete PVCs first
#   scripts/verify-deployment.sh --serial        # force sections 1-5 to run in order
#   scripts/verify-deployment.sh --help
#   IMG_TAG=mq-tag COLLECTOR_TAG=collector-tag scripts/verify-deployment.sh
#
# Exit code is 0 only if every check passed.

set -uo pipefail

NS="${NS:-telemetry}"
RELEASE="${RELEASE:-telemetry}"
CHART="${CHART:-helm-charts/telemetry}"
# Space-separated list of values files. It MUST include the generated
# machine-local overlay (dist/values-local.yaml) alongside values-prod.yaml.
#
# Deploying with values-prod.yaml alone is not a degraded mode, it is broken: the
# overlay is the only thing supplying the image tags, pullPolicy, and the API
# server address the leader-election NetworkPolicy needs. Omitting it reverts the
# images to `latest` (which was never built, so ImagePullBackOff) and deletes the
# egress CIDR (so no replica can win the lease). That is exactly what this script
# used to do in its own final `helm upgrade`, silently breaking the cluster it had
# just verified.
if [ -z "${VALUES:-}" ]; then
  VALUES="$CHART/values-prod.yaml"
  _local="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/dist/values-local.yaml"
  [ -f "$_local" ] && VALUES="$VALUES $_local"
  unset _local
fi

# Build the repeated -f arguments once so every helm call below uses them.
HELM_F=()
for f in $VALUES; do
  [ -f "$f" ] || { echo "values file not found: $f" >&2; exit 2; }
  HELM_F+=(-f "$f")
done

# set_image <subchart> <tag> -- emits the --set args pinning one component image.
# pullPolicy is pinned too: `latest` with IfNotPresent lets a kind node keep
# serving a stale cached image, so a rebuild would silently not take effect.
set_image() {
  printf -- '--set\n%s.image.tag=%s\n--set\n%s.image.pullPolicy=Never\n' "$1" "$2" "$1"
}

# IMG_TAG and COLLECTOR_TAG are separate on purpose. A single IMG_TAG reused for
# both silently asks the collector to pull the MQ tag, which does not exist for
# that repository; the resulting ImagePullBackOff then fails every later
# helm --wait for an unrelated-looking reason.
IMG_TAG="${IMG_TAG:-latest}"
COLLECTOR_TAG="${COLLECTOR_TAG:-$IMG_TAG}"

SKIP_DEPLOY=0
CLEANUP=0
PARALLEL=1
for arg in "$@"; do
  case "$arg" in
    --skip-deploy) SKIP_DEPLOY=1 ;;
    --cleanup) CLEANUP=1 ;;
    --serial) PARALLEL=0 ;;
    -h|--help) sed -n '2,28p' "$0"; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

PASS=0
FAIL=0
declare -a FAILURES=()
WORKDIR=""
RESULTS=""

# --- output helpers -------------------------------------------------------
if [ -t 1 ]; then G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; B=$'\033[1m'; N=$'\033[0m'
else G=""; R=""; Y=""; B=""; N=""; fi

section() { printf '\n%s== %s ==%s\n' "$B" "$1" "$N"; }
info()   { printf '   %s\n' "$1"; }
ok()     { printf '   %sPASS%s %s\n' "$G" "$N" "$1"
           [ -n "$RESULTS" ] && printf 'PASS\t%s\n' "$1" >> "$RESULTS"
           return 0; }
bad()    { printf '   %sFAIL%s %s\n' "$R" "$N" "$1"
           [ -n "$RESULTS" ] && printf 'FAIL\t%s\n' "$1" >> "$RESULTS"
           return 0; }

# eq <description> <actual> <expected>
eq() {
  if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1: got '$2', want '$3'"; fi
}

need() { command -v "$1" >/dev/null 2>&1 || { echo "required tool not found: $1" >&2; exit 2; }; }
need kubectl; need helm; need python3

# --- shared cluster accessors --------------------------------------------
ch_db() {
  kubectl -n "$NS" get deploy telemetry-collector \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_DB")].value}' 2>/dev/null
}
ch_table() {
  kubectl -n "$NS" get deploy telemetry-collector \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_TABLE")].value}' 2>/dev/null
}
chq() {
  kubectl -n "$NS" exec telemetry-clickhouse-0 -- clickhouse-client -q "$1" 2>/dev/null | tr -d '\r'
}
mq_pods() {
  kubectl -n "$NS" get pods -l app.kubernetes.io/name=messagequeue \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null
}
mq_replicas() {
  kubectl -n "$NS" get statefulset telemetry-messagequeue -o jsonpath='{.spec.replicas}' 2>/dev/null
}
mq_scale() {
  kubectl -n "$NS" scale statefulset telemetry-messagequeue --replicas="$1" >/dev/null 2>&1
}
mq_await_rollout() {
  kubectl -n "$NS" rollout status statefulset/telemetry-messagequeue --timeout=5m >/dev/null 2>&1
}

# wal_mtime is the newest modification time of a pod's log file. This is the one
# progress signal that works for BOTH roles: a follower logs almost nothing per
# event (only a checkpoint line when it adopts one), so scraping its output for
# an offset looks "stuck" on a perfectly healthy replica.
wal_mtime() {
  kubectl -n "$NS" exec "$1" -- sh -c 'stat -c %Y /var/lib/mq/wal.log' 2>/dev/null | tr -dc '0-9'
}

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/verify-XXXXXX")
cleanup_all() {
  # Port-forwards are children of the section that started them; kill any that
  # outlived their section rather than leaving them bound to the port.
  pkill -f "kubectl -n $NS port-forward svc/telemetry-apigateway" 2>/dev/null
  [ -n "$WORKDIR" ] && rm -rf "$WORKDIR"
  return 0
}
trap cleanup_all EXIT

# --- optional cleanup -----------------------------------------------------
if [ "$CLEANUP" = 1 ]; then
  section "cleanup"
  info "uninstalling $RELEASE and deleting MQ state"
  helm uninstall "$RELEASE" -n "$NS" --wait --timeout 5m >/dev/null 2>&1
  # PVCs survive helm uninstall, so a redeploy would silently reuse the old
  # WALs. That is exactly the stale-offset trap this script should not inherit.
  kubectl -n "$NS" delete pvc -l app.kubernetes.io/name=messagequeue --wait=false >/dev/null 2>&1
  info "waiting for PVCs to be released"
  for _ in $(seq 1 30); do
    left=$(kubectl -n "$NS" get pvc -l app.kubernetes.io/name=messagequeue -o name 2>/dev/null | wc -l | tr -d ' ')
    [ "$left" = "0" ] && break
    sleep 2
  done
  kubectl -n "$NS" delete pods -l app.kubernetes.io/name=messagequeue --wait=false >/dev/null 2>&1
  ok "clean slate"
fi

# --- deploy ---------------------------------------------------------------
if [ "$SKIP_DEPLOY" = 0 ]; then
  section "deploy ($IMG_TAG / collector $COLLECTOR_TAG)"
  if helm upgrade --install "$RELEASE" "$CHART" -n "$NS" "${HELM_F[@]}" \
        $(set_image streamer "$IMG_TAG") \
        --set collector.image.tag="$COLLECTOR_TAG" --set collector.image.pullPolicy=Never \
        $(set_image messagequeue "$IMG_TAG") \
        $(set_image apigateway "$IMG_TAG") \
        --wait --timeout 10m >"$WORKDIR/helm.log" 2>&1; then
    ok "helm upgrade --install succeeded"
  else
    tail -20 "$WORKDIR/helm.log" >&2
    bad "helm upgrade --install failed"
  fi
  mq_await_rollout
fi

REV=$(helm -n "$NS" list -o json 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin)[0]['revision'])" 2>/dev/null)
info "release revision: ${REV:-unknown}"

# ==========================================================================
# Section 1. baseline
# ==========================================================================
sec_baseline() {
  RESULTS="$1"

  # The release must be configured the way this script assumes, or every check
  # below is measuring the wrong thing. These exist because the script's own
  # final `helm upgrade` once ran with only values-prod.yaml, which reverted two
  # images to an unbuilt `latest` and deleted the API-server egress CIDR -- and
  # the checks still "passed" because the old pods were still running.
  relvals=$(helm -n "$NS" get values "$RELEASE" -o json 2>/dev/null)
  for pair in "streamer:$IMG_TAG" "messagequeue:$IMG_TAG" "apigateway:$IMG_TAG" "collector:$COLLECTOR_TAG"; do
    comp="${pair%%:*}"; want="${pair##*:}"
    got=$(echo "$relvals" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(''); raise SystemExit
print(d.get('$comp',{}).get('image',{}).get('tag',''))" 2>/dev/null)
    eq "$comp image tag" "$got" "$want"
  done

  # A missing API-server CIDR means no MQ replica can reach the Kubernetes API to
  # contend for the lease: every replica silently stays a follower while all of
  # them report Running.
  nblocks=$(echo "$relvals" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(''); raise SystemExit
print(len(d.get('messagequeue',{}).get('apiServer',{}).get('ipBlocks') or []))" 2>/dev/null)
  if [ "${nblocks:-0}" -ge 1 ]; then
    ok "MQ leader-election egress is configured ($nblocks apiServer rule(s))"
  else
    bad "messagequeue.apiServer.ipBlocks is empty: the MQ cannot reach the Kubernetes API," 
    bad "so every replica will stay a follower forever (is dist/values-local.yaml being passed?)"
  fi

  for d in messagequeue streamer collector apigateway clickhouse; do
    ready=$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=$d" \
              --field-selector=status.phase=Running -o json 2>/dev/null | python3 -c "import json,sys;i=json.load(sys.stdin)['items'];print(sum(1 for p in i if all(c['ready'] for c in p['status'].get('containerStatuses',[]))))" 2>/dev/null)
    total=$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=$d" -o name 2>/dev/null | wc -l | tr -d ' ')
    if [ -n "$ready" ] && [ "$ready" = "$total" ] && [ "$total" != "0" ]; then
      ok "$d: $ready/$total ready"
    else
      bad "$d: $ready/$total ready"
      kubectl -n "$NS" get pods -l "app.kubernetes.io/name=$d" --no-headers 2>&1 | sed 's/^/        /'
    fi
  done
}

# ==========================================================================
# Section 2. election and routing
# ==========================================================================
sec_election() {
  RESULTS="$1"
  # Exactly one leader. Two means split brain; zero means nobody can serve.
  leaders=$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=leader -o name 2>/dev/null | wc -l | tr -d ' ')
  eq "exactly one leader-labelled pod" "$leaders" "1"

  holder=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
  [ -n "$holder" ] && ok "lease holder: $holder" || bad "lease has no holder"

  # The lease is the source of truth, so the label must agree with it.
  leader_pod=$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=leader -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  eq "label agrees with lease holder" "$leader_pod" "$holder"

  eps=$(kubectl -n "$NS" get endpoints telemetry-messagequeue-leader -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | wc -w | tr -d ' ')
  eq "leader Service has exactly one endpoint" "$eps" "1"

  # Renewal must be advancing, or the holder is not really holding.
  r1=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.renewTime}' 2>/dev/null)
  sleep 7
  r2=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.renewTime}' 2>/dev/null)
  if [ -n "$r2" ] && [ "$r1" != "$r2" ]; then ok "lease is being renewed ($r1 -> $r2)"
  else bad "lease renewTime did not advance ($r1 -> $r2)"; fi
}

# ==========================================================================
# Section 3. WAL state
# ==========================================================================
sec_wal() {
  RESULTS="$1"
  # The startup line "wal open ...: base=N retained=M ..." is authoritative but
  # gets rotated out of the CRI buffer within minutes, because an ingest-rate
  # pod writes ~200 log lines/s and the kubelet caps the buffer. Scraping it is
  # therefore flaky by construction, so it is reported when present and skipped
  # when it has already rolled out of the buffer.
  for p in $(mq_pods); do
    role=$(kubectl -n "$NS" get pod "$p" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/component}' 2>/dev/null)
    head_line=$(kubectl -n "$NS" logs "$p" --tail=8000 2>/dev/null | grep -m1 -E 'wal open ')
    [ -z "$head_line" ] && head_line=$(kubectl -n "$NS" logs "$p" --previous --tail=8000 2>/dev/null | grep -m1 -E 'wal open ')
    if [ -n "$head_line" ]; then
      ok "$p ($role) opened: $(echo "$head_line" | sed 's/^[0-9/]* [0-9:]* //')"
    else
      info "$p ($role): startup line already rotated out of the log buffer"
    fi

    # An advancing mtime is the authoritative liveness signal: it proves bytes
    # are reaching the file, for both roles, regardless of how the pod is
    # logging. This is checked first because on a freshly deployed pod the WAL is
    # legitimately still empty and the checks below would all report a healthy
    # cluster as broken.
    m1=$(wal_mtime "$p")
    sleep 8
    m2=$(wal_mtime "$p")
    if [ -z "$m1" ] || [ -z "$m2" ]; then
      bad "$p: cannot stat the WAL file"
      continue
    elif [ "$m2" -gt "$m1" ]; then
      ok "$p WAL is being written (mtime advanced over 8s)"
      live=1
    else
      bad "$p WAL has not been written for 8s (mtime stuck): replication or ingestion is wedged"
      live=0
    fi

    # Sampled after the window so it reflects the writes just proven above.
    #
    # Deliberately NOT checked: that the file grows. A follower rewrites its
    # whole log whenever it adopts the leader's checkpoint, so a healthy
    # replica's byte count routinely falls from megabytes to kilobytes while it
    # is making progress. Progress is the mtime, not the size.
    bytes=$(kubectl -n "$NS" exec "$p" -- sh -c 'wc -c < /var/lib/mq/wal.log' 2>/dev/null | tr -dc '0-9')
    if [ -z "$bytes" ]; then
      bad "$p: cannot read the WAL size on disk"
    elif [ "$bytes" -eq 0 ]; then
      bad "$p WAL on disk is empty after 8s of writes: it could not promote into a leader that serves reads"
    else
      ok "$p WAL on disk: $bytes bytes (size is not monotonic: checkpoints rewrite it)"
    fi

    # Corroborating evidence only, now that liveness is settled. The two roles
    # report liveness differently:
    #   leader   -> "retention trim log_offset<=N (log A->B)"
    #   follower -> "wal rewritten: checkpoint(base=N) + M retained appends"
    # A follower never trims (it takes the leader's trimmed log verbatim) but
    # does rewrite its checkpoint on every batch, so requiring the leader's line
    # would fail a healthy replica. Neither line appears in the first seconds
    # after startup, so an absent line is only worth reporting when the mtime
    # says the WAL is actually idle.
    activity=$(kubectl -n "$NS" logs "$p" --tail=3000 2>/dev/null \
      | grep -m1 -E 'retention trim|wal rewritten' | sed 's/^[0-9/]* [0-9:]* //')
    if [ -n "$activity" ]; then
      ok "$p ($role) WAL activity logged: $activity"
    elif [ "$live" = "1" ]; then
      info "$p ($role): no trim/checkpoint line yet, but the WAL is being written"
    else
      bad "$p: WAL is idle and logged neither retention trimming nor checkpoint rewrites"
    fi
  done
}

# ==========================================================================
# Section 4. ingestion
# ==========================================================================
sec_ingestion() {
  RESULTS="$1"
  DB=$(ch_db); TBL=$(ch_table)
  if [ -z "$DB" ] || [ -z "$TBL" ]; then
    bad "cannot resolve the ClickHouse db/table from the collector"
    return 0
  fi
  info "table: $DB.$TBL"
  a=$(chq "SELECT count() FROM $DB.$TBL" | tail -1)
  if [ -z "$a" ]; then
    bad "ClickHouse query returned nothing (is telemetry-clickhouse-0 ready?)"
    return 0
  fi
  info "rows now: $a"
  sleep 20
  b=$(chq "SELECT count() FROM $DB.$TBL" | tail -1)
  rate=$(python3 -c "print(f'{($b-$a)/20:.1f}')" 2>/dev/null)
  info "rate: $rate rows/s"
  if python3 -c "import sys; sys.exit(0 if float('$rate') > 1 else 1)" 2>/dev/null; then
    ok "ingestion is flowing ($rate rows/s)"
  else
    bad "ingestion stalled ($rate rows/s)"
    kubectl -n "$NS" logs deploy/telemetry-collector --tail=10 2>&1 | cut -c1-150 | sed 's/^/        /'
  fi

  # uniqExact over millions of rows can exceed the query timeout and come back
  # empty. An empty answer is not "no duplicates", so sample instead of guessing.
  dupes=$(chq "SELECT count() - uniqExact(event_id) FROM (SELECT event_id FROM $DB.$TBL ORDER BY received_at DESC LIMIT 200000)" | tail -1)
  if [ -z "$dupes" ]; then
    bad "duplicate check returned nothing (uniqExact too slow; lower the LIMIT)"
  elif [ "$dupes" = "0" ]; then
    ok "no duplicate event_id in the most recent 200k rows"
  else
    bad "found $dupes duplicate event_id values in the most recent 200k rows"
  fi
}

# ==========================================================================
# Section 5. API
# ==========================================================================
sec_api() {
  RESULTS="$1"
  # A port-forward on a fixed port: only one section may run this at a time.
  kubectl -n "$NS" port-forward svc/telemetry-apigateway 18080:8080 >"$WORKDIR/pf.log" 2>&1 &
  PF=$!
  # Every check below talks through this forward, so it has to stay up until the
  # section ends. Tearing it down early makes every later request fail with a
  # connection error that looks like the API being broken.
  code=""
  for _ in $(seq 1 20); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 2 http://127.0.0.1:18080/healthz 2>/dev/null)
    [ "$code" = "200" ] && break
    sleep 1
  done

  if [ "$code" = "200" ]; then
    ok "GET /healthz returns 200 (200)"
  else
    bad "GET /healthz unreachable (port-forward failed? last status '$code')"
    # The forward log is the only way to tell a bad target from a dead pod.
    tail -3 "$WORKDIR/pf.log" 2>&1 | sed 's/^/        /'
    kill "$PF" 2>/dev/null; wait "$PF" 2>/dev/null
    return 0
  fi

  # Unauthenticated data access must be refused.
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:18080/api/v1/gpus 2>/dev/null)
  if [ "$code" = "401" ] || [ "$code" = "403" ]; then ok "unauthenticated /api/v1/gpus is rejected ($code)"
  else bad "unauthenticated /api/v1/gpus returned $code, want 401/403"; fi

  API_TOKEN=$(kubectl -n "$NS" get secret telemetry-api-tokens -o json 2>/dev/null \
    | python3 -c "
import json,sys,base64
d=json.load(sys.stdin)['data'].get('tokens','')
txt=base64.b64decode(d).decode()
# The secret is a map of identity=token, one per line. Any one of them is a
# valid static credential, so just take the first.
print(next((l.split('=',1)[1].strip() for l in txt.splitlines() if '=' in l), ''))" 2>/dev/null)

  if [ -z "$API_TOKEN" ]; then
    bad "could not read the API token from telemetry-api-tokens"
    kill "$PF" 2>/dev/null; wait "$PF" 2>/dev/null
    return 0
  fi

  # Exchange the long-lived token for a short-lived signed one.
  resp=$(curl -s -m 8 -X POST http://127.0.0.1:18080/api/v1/token \
           -H "Authorization: Bearer $API_TOKEN" 2>/dev/null)
  jwt=$(echo "$resp" | python3 -c "import json,sys;print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
  if [ -n "$jwt" ]; then ok "POST /api/v1/token returned a signed access token"
  else bad "POST /api/v1/token did not return an access_token: $(echo "$resp" | head -c 120)"; fi

  code=$(curl -s -o /dev/null -w '%{http_code}' -m 8 http://127.0.0.1:18080/api/v1/gpus \
           -H "Authorization: Bearer $jwt" 2>/dev/null)
  eq "GET /api/v1/gpus with a token returns 200" "$code" "200"

  code=$(curl -s -o /dev/null -w '%{http_code}' -m 8 "http://127.0.0.1:18080/api/v1/gpus/0/telemetry" \
           -H "Authorization: Bearer $jwt" 2>/dev/null)
  if [ "$code" = "200" ]; then ok "GET /api/v1/gpus/0/telemetry returns 200"
  else bad "GET /api/v1/gpus/0/telemetry returned $code (404 may just mean no GPU data yet)"; fi

  # Release the fixed port here rather than relying on the EXIT trap, so a later
  # run is free to bind it.
  kill "$PF" 2>/dev/null
  wait "$PF" 2>/dev/null
  return 0
}

# ==========================================================================
# Sections 1-5: run them
# ==========================================================================
ALL_RESULTS="$WORKDIR/results.tsv"
: > "$ALL_RESULTS"

if [ "$PARALLEL" = 1 ]; then
  section "1-5. observations (running concurrently)"
  info "these sections only read, so they overlap; 6-7 mutate the cluster and run after"
  t_phase=$(date +%s)

  # Each section writes its results to its own file because a background job is
  # a subshell: counters and the FAILURES array set inside it would be lost.
  sec_baseline  "$WORKDIR/r1.tsv" >"$WORKDIR/o1.txt" 2>&1 &
  p1=$!
  sec_election  "$WORKDIR/r2.tsv" >"$WORKDIR/o2.txt" 2>&1 &
  p2=$!
  sec_wal       "$WORKDIR/r3.tsv" >"$WORKDIR/o3.txt" 2>&1 &
  p3=$!
  sec_ingestion "$WORKDIR/r4.tsv" >"$WORKDIR/o4.txt" 2>&1 &
  p4=$!
  sec_api       "$WORKDIR/r5.tsv" >"$WORKDIR/o5.txt" 2>&1 &
  p5=$!

  wait "$p1" "$p2" "$p3" "$p4" "$p5"

  # Report in numeric order regardless of which finished first, so the output
  # reads the same as a serial run and can be diffed against one.
  titles=("1. baseline" "2. election and routing" "3. WAL state" "4. ingestion" "5. API")
  for i in 1 2 3 4 5; do
    section "${titles[$((i-1))]}"
    cat "$WORKDIR/o$i.txt"
    [ -f "$WORKDIR/r$i.tsv" ] && cat "$WORKDIR/r$i.tsv" >> "$ALL_RESULTS"
  done
  printf '   %sobservation phase: %ds%s\n' "$Y" "$(( $(date +%s) - t_phase ))" "$N"
else
  for fn in sec_baseline sec_election sec_wal sec_ingestion sec_api; do
    case "$fn" in
      sec_baseline)  section "1. baseline" ;;
      sec_election)  section "2. election and routing" ;;
      sec_wal)       section "3. WAL state" ;;
      sec_ingestion) section "4. ingestion" ;;
      sec_api)       section "5. API" ;;
    esac
    "$fn" "$ALL_RESULTS"
  done
fi

# ==========================================================================
# Section 6. failover
# ==========================================================================
section "6. failover (remove the leader)"
RESULTS="$WORKDIR/r6.tsv"
t6=$(date +%s)

DB=$(ch_db); TBL=$(ch_table)
replicas_before=$(mq_replicas)

# How to remove the leader for real, and why the obvious approaches fail:
#
#   * `kubectl delete pod <leader>` does NOT test failover. The StatefulSet
#     recreates the pod under the same identity and it re-acquires the lease
#     before a peer can win (observed: pod back in ~11s, lease re-acquired with
#     transitions unchanged).
#   * `kill -9 1` inside the container does nothing. PID 1 of a PID namespace
#     ignores signals it has no handler for, so even SIGKILL is dropped.
#   * `scale --replicas=N` only works when the leader sits on the HIGHEST
#     ordinal, because a StatefulSet always removes the highest ordinal first.
#     With the leader on -0, scaling to 1 deletes a follower and the leader
#     keeps its lease, which looks like broken failover but is correct behaviour.
#
# So: make the leader the highest ordinal, then scale below it. That removes the
# leader pod permanently while leaving a survivor to win the lease.
holder=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
ordinal="${holder##*-}"
trans_before=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.leaseTransitions}' 2>/dev/null)
pre_ts=$(date -u '+%Y-%m-%d %H:%M:%S')
info "leader $holder is ordinal $ordinal of $replicas_before replicas"

# Pod -0 almost always wins the lease because it starts first, so the
# survivor-promotion path is normally unreachable from a clean start. Deleting
# the Lease makes both pods re-race; whichever wins becomes the leader. Retry a
# few times to land on the top ordinal, otherwise fall back to cold recovery.
if [ "$ordinal" -lt 1 ] && [ "${replicas_before:-1}" -gt 1 ]; then
  for attempt in 1 2 3 4; do
    kubectl -n "$NS" delete lease mq-leader >/dev/null 2>&1
    h=""
    for i in $(seq 1 12); do
      h=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
      [ -n "$h" ] && break
      sleep 3
    done
    if [ -n "$h" ] && [ "${h##*-}" != "$ordinal" ]; then
      info "lease re-raced and is now held by $h (attempt $attempt)"
      holder="$h"; ordinal="${holder##*-}"
      break
    fi
    info "attempt $attempt: ${h:-none} won the race again, retrying"
  done
fi

if [ "${ordinal:-0}" -ge 1 ]; then
  # Make sure the leader really is the top ordinal, then scale below it.
  mq_scale $((ordinal + 1))
  mq_await_rollout
  sleep 10
  # Scaling may have handed the lease to the new pod; re-read before removing.
  holder=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
  ordinal="${holder##*-}"
  if [ "${ordinal:-0}" -ge 1 ]; then
    mq_scale "$ordinal"
    info "scaled to $ordinal to remove ordinal $ordinal (the leader) permanently"
    removal="scale-down"
  else
    info "the scale handed the lease back to ordinal 0; using cold recovery instead"
    mq_scale 0; sleep 8; mq_scale "$replicas_before"
    removal="cold-restart"
  fi
else
  # Ordinal 0 cannot be removed by scaling while keeping a survivor: no lower
  # replica exists. Test the whole set going cold and coming back, which verifies
  # recovery and contiguity but is not a promotion by a surviving peer. Report it
  # as such rather than as a failover.
  info "leader is ordinal 0: no scale-down can remove it while keeping a survivor,"
  info "so this run checks cold recovery of the whole set instead of promotion"
  mq_scale 0; sleep 8; mq_scale "$replicas_before"
  removal="cold-restart"
fi

promoted=""
for i in $(seq 1 25); do
  h=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
  ep=$(kubectl -n "$NS" get endpoints telemetry-messagequeue-leader -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)
  if [ -n "$h" ] && [ -n "$ep" ]; then promoted="$h"; break; fi
  sleep 3
done

if [ "$removal" = "scale-down" ]; then
  if [ -n "$promoted" ] && [ "$promoted" != "$holder" ]; then
    ok "survivor $promoted promoted and took the endpoint within 75s"
    kubectl -n "$NS" logs "$promoted" --tail=400 2>&1 | grep -m1 "PROMOTED" | sed 's/^/        /'
  else
    trans_after=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.leaseTransitions}' 2>/dev/null)
    bad "no promotion by a survivor after 75s (holder=${promoted:-$holder}, transitions $trans_before -> $trans_after)"
    mq_pods | sed 's/^/        /'
  fi
else
  if [ -n "$promoted" ]; then
    ok "the set recovered and re-elected $promoted from a cold start"
  else
    bad "no leader elected after the whole set was scaled to zero and back"
  fi
fi

# Restore the replica count BEFORE measuring ingestion.
#
# sync-replicas=1 means the leader withholds a publish ack until one follower has
# applied the event, so a single-replica topology stalls ingestion by design
# (fail-closed durability). Measuring right after a failover that left one
# replica up would report a stall that says nothing about the failover.
mq_scale "$replicas_before"
mq_await_rollout

followers=0
for i in $(seq 1 12); do
  followers=$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=replica -o name 2>/dev/null | wc -l | tr -d ' ')
  [ "${followers:-0}" -ge 1 ] && break
  sleep 5
done
if [ "${followers:-0}" -ge 1 ]; then
  ok "a follower rejoined after failover ($followers replica(s)), so sync-replicas can be satisfied"
else
  bad "no follower rejoined within 60s; ingestion cannot satisfy sync-replicas"
fi

p1=$(chq "SELECT count() FROM $DB.$TBL" | tail -1); sleep 20
p2=$(chq "SELECT count() FROM $DB.$TBL" | tail -1)
prate=$(python3 -c "print(f'{($p2-$p1)/20:.1f}')" 2>/dev/null)
if python3 -c "import sys; sys.exit(0 if float('$prate') > 1 else 1)" 2>/dev/null; then
  ok "ingestion resumed after the leader changed ($prate rows/s)"
else
  bad "ingestion did not resume after the leader changed ($prate rows/s)"
fi

# Contiguity across the window is the no-data-loss proof: the span between the
# min and max offset must equal the row count, which can only hold if there are
# no gaps. uniqExact must equal count to rule out duplicates.
# received_at is DateTime64(9), so the literal needs matching precision.
win=$(chq "
  SELECT
    min(mq_offset), max(mq_offset), count(), uniqExact(mq_offset),
    max(mq_offset) - min(mq_offset) + 1
  FROM $DB.$TBL
  WHERE received_at >= toDateTime64('$pre_ts', 9, 'UTC') AND mq_offset > 0" | tail -1)
read -r wmin wmax wcount wuniq wspan <<<"$win"
info "window: min=$wmin max=$wmax rows=$wcount span=$wspan"
eq "no duplicate offsets across the window" "$((wcount - wuniq))" "0"
eq "no offset gaps across the window" "$((wspan - wcount))" "0"

sleep 10
eq "back to the declared replica count ($replicas_before)" "$(mq_pods | wc -l | tr -d ' ')" "$replicas_before"
cat "$RESULTS" >> "$ALL_RESULTS"
printf '   %ssection 6: %ds%s\n' "$Y" "$(( $(date +%s) - t6 ))" "$N"

# ==========================================================================
# Section 7. scale up / down
# ==========================================================================
section "7. scale up and down"
RESULTS="$WORKDIR/r7.tsv"
t7=$(date +%s)

# Scale DOWN to 1. A StatefulSet removes the highest ordinal, so which pod
# disappears depends on where the leader sits: with the leader on the highest
# ordinal this is a real failover, and with it on the lowest this just removes a
# follower. Both are legitimate, so assert only that the topology and the leader
# endpoint end up correct, not that a specific pod vanished.
mq_scale 1
sleep 15
eq "scaled down to 1 replica" "$(mq_pods | wc -l | tr -d ' ')" "1"
ep=$(kubectl -n "$NS" get endpoints telemetry-messagequeue-leader -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | wc -w | tr -d ' ')
eq "leader endpoint survives scale down" "$ep" "1"

# Do NOT assert ingestion at 1 replica: sync-replicas=1 withholds acks with no
# follower, so a stall here is the intended fail-closed behaviour rather than a
# fault. Section 4 covers ingestion, and section 6 covers it across a failover
# once a follower is back. Assert only that the node is elected and Ready.
solo=$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=messagequeue \
  -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null)
eq "the surviving replica stays Ready at 1 replica" "$solo" "true"
solo_holder=$(kubectl -n "$NS" get lease mq-leader -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
if [ -n "$solo_holder" ]; then ok "a leader is still elected at 1 replica ($solo_holder)"
else bad "no leader elected at 1 replica"; fi

# Scale UP to 3. The extra pod must join as a follower without disturbing the
# leader or the endpoint, which is the case that would expose broken
# rebalancing or a second node claiming the lease.
mq_scale 3
mq_await_rollout
sleep 20
eq "scaled up to 3 replicas" "$(mq_pods | wc -l | tr -d ' ')" "3"
eq "still exactly one leader at 3 replicas" \
   "$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=leader -o name 2>/dev/null | wc -l | tr -d ' ')" "1"
eq "exactly one pod carries the leader label" \
   "$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=messagequeue \
       -o jsonpath='{range .items[*]}{.metadata.labels.app\.kubernetes\.io/component}{"\n"}{end}' 2>/dev/null \
      | grep -c '^leader$')" "1"

# Every pod must become Ready, or a scale-up that adds unready pods is a
# regression the replica count alone would hide.
for p in $(mq_pods); do
  r=$(kubectl -n "$NS" get pod "$p" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)
  eq "$p Ready at 3 replicas" "$r" "true"
done

mq_scale "$replicas_before"
mq_await_rollout
sleep 15

# helm --wait also blocks on every other workload, and the scale churn above can
# leave the collector mid-rollout. Wait for that to settle first, otherwise this
# upgrade fails on an unrelated pod and looks like a replica conflict.
kubectl -n "$NS" rollout status deploy/telemetry-collector --timeout=5m >/dev/null 2>&1
kubectl -n "$NS" rollout status deploy/telemetry-apigateway --timeout=5m >/dev/null 2>&1
mq_await_rollout

# helm will otherwise fail on the next upgrade: scaling sets .spec.replicas
# directly, which the chart's own apply then conflicts with. Re-syncing proves
# the release is left in a state a later upgrade can build on.
#
# This must use the SAME full set of values files as the deploy above. Using only
# values-prod.yaml here once reverted the images to `latest` (ImagePullBackOff)
# and deleted the API-server egress CIDR (no replica could win the lease) -- the
# script broke the very cluster it had just finished verifying.
if helm -n "$NS" upgrade "$RELEASE" "$CHART" -n "$NS" "${HELM_F[@]}" \
      $(set_image streamer "$IMG_TAG") \
      --set collector.image.tag="$COLLECTOR_TAG" --set collector.image.pullPolicy=Never \
      $(set_image messagequeue "$IMG_TAG") \
      $(set_image apigateway "$IMG_TAG") \
      --wait --timeout 5m >"$WORKDIR/helm-resync.log" 2>&1; then
  ok "helm re-syncs cleanly after manual scaling"
else
  bad "helm upgrade failed after scaling"
  tail -8 "$WORKDIR/helm-resync.log" | sed 's/^/        /'
fi
cat "$RESULTS" >> "$ALL_RESULTS"
printf '   %ssection 7: %ds%s\n' "$Y" "$(( $(date +%s) - t7 ))" "$N"

# --- summary --------------------------------------------------------------
section "summary"
PASS=$(grep -c '^PASS' "$ALL_RESULTS")
FAIL=$(grep -c '^FAIL' "$ALL_RESULTS")
printf '   %spassed: %d%s   %sfailed: %d%s\n' "$G" "$PASS" "$N" "$R" "$FAIL" "$N"
if [ "$FAIL" -gt 0 ]; then
  printf '\n   failed checks:\n'
  grep '^FAIL' "$ALL_RESULTS" | cut -f2- | sed 's/^/     - /'
  exit 1
fi
printf '\n   %sall checks passed%s\n' "$G" "$N"
exit 0