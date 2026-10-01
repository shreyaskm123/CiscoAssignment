#!/usr/bin/env bash
# Sanity-check ClickHouse high availability and replication.
#
#   ./scripts/verify-clickhouse-ha.sh               # state checks, no disruption
#   ./scripts/verify-clickhouse-ha.sh --failover    # also kill a ClickHouse replica and
#                                                   # the Keeper leader under live ingest
#
# What it checks:
#   1. pod counts      ClickHouse and Keeper pods all Ready, as many as configured
#   2. Keeper quorum   exactly one leader, the rest followers, all followers synced
#   3. replica health  every replica: Replicated engine, writable, session alive,
#                      sees all peers, empty replication queue, no lag
#   4. data converges  every replica holds the SAME events (count + content hash)
#   5. round trip      a row written on one replica appears on every other one
#   6. Service         the client Service routes to every Ready replica
#   7. ingestion       rows are still arriving
#   (--failover)       a replica / the Keeper leader is killed while ingestion runs;
#                      writes must continue, and afterwards the data must re-converge
#
# Operator tool: it runs queries INSIDE the pods with `kubectl exec`, as the
# in-container admin user. It needs kubectl access to the namespace and, for
# the round-trip test, .auth/ch-writer-pass.txt (written by
# bootstrap-auth-secrets.sh). It writes only a scratch table, never the events.
set -uo pipefail

NS="${NAMESPACE:-telemetry}"
REL="${RELEASE:-telemetry}"
CH_STS="$REL-clickhouse"
KP_STS="$REL-clickhouse-keeper"
CH_SVC="$REL-clickhouse"
DB="${CLICKHOUSE_DB:-telemetry}"
TABLE="${CLICKHOUSE_TABLE:-events}"
CLUSTER="${CLICKHOUSE_CLUSTER:-telemetry}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRITER_PW_FILE="$ROOT/.auth/ch-writer-pass.txt"
FAILOVER=0
[ "${1:-}" = "--failover" ] && FAILOVER=1

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$1"; }
info() { printf '        %s\n' "$1"; }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }

k() { kubectl -n "$NS" "$@"; }

# chq <pod> <sql>: run SQL inside a ClickHouse pod as the in-container admin.
chq() { k exec "$1" -c clickhouse -- clickhouse-client --query "$2" 2>&1; }

# chq_writer <pod> <sql>: same, as the collector's account (used for the DDL
# round trip, to prove that account can run ON CLUSTER DDL).
chq_writer() {
  k exec "$1" -c clickhouse -- clickhouse-client --user telemetry_writer \
    --password "$(cat "$WRITER_PW_FILE")" --query "$2" 2>&1
}

command -v kubectl >/dev/null || { echo "kubectl not found" >&2; exit 1; }
k get sts "$CH_STS" >/dev/null 2>&1 || { echo "statefulset $CH_STS not found in namespace $NS" >&2; exit 1; }

CH_N="$(k get sts "$CH_STS" -o jsonpath='{.spec.replicas}')"
KP_N="$(k get sts "$KP_STS" -o jsonpath='{.spec.replicas}')"
ch_pods() { local i=0; while [ "$i" -lt "$CH_N" ]; do echo "$CH_STS-$i"; i=$((i+1)); done; }
kp_pods() { local i=0; while [ "$i" -lt "$KP_N" ]; do echo "$KP_STS-$i"; i=$((i+1)); done; }

# keeper_mode <pod> -> leader|follower|standalone|""
keeper_mode() {
  k exec "$1" -- sh -c '(printf srvr; sleep 2) | nc 127.0.0.1 2181' 2>/dev/null \
    | awk -F': ' '/^Mode/ {print $2}'
}

# --------------------------------------------------------------- 1. pod counts
step "1. Pod counts"
ch_ready="$(k get sts "$CH_STS" -o jsonpath='{.status.readyReplicas}')"; ch_ready="${ch_ready:-0}"
kp_ready="$(k get sts "$KP_STS" -o jsonpath='{.status.readyReplicas}')"; kp_ready="${kp_ready:-0}"
[ "$ch_ready" = "$CH_N" ] && ok "ClickHouse: $ch_ready/$CH_N replicas Ready" || bad "ClickHouse: $ch_ready/$CH_N replicas Ready"
[ "$kp_ready" = "$KP_N" ] && ok "Keeper: $kp_ready/$KP_N members Ready" || bad "Keeper: $kp_ready/$KP_N members Ready"
[ "$CH_N" -ge 2 ] && ok "ClickHouse is configured with $CH_N replicas (needs >= 2 for HA)" || bad "ClickHouse has only $CH_N replica: no HA"
[ "$KP_N" -ge 3 ] && [ $((KP_N % 2)) -eq 1 ] && ok "Keeper ensemble is $KP_N (odd, tolerates $(( (KP_N-1)/2 )) failure(s))" || bad "Keeper ensemble is $KP_N: need an odd number >= 3 for a fault-tolerant quorum"
nodes="$(k get pods -l 'app.kubernetes.io/name in (clickhouse,clickhouse-keeper)' -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l | tr -d ' ')"
info "ClickHouse + Keeper pods are spread over $nodes node(s)"
[ "$nodes" -lt 2 ] && info "NOTE: a single node means pod-level HA only; a node loss still takes everything down"

# ------------------------------------------------------------- 2. Keeper quorum
step "2. Keeper quorum"
leaders=0; followers=0
for p in $(kp_pods); do
  m="$(keeper_mode "$p")"
  info "$p: ${m:-no answer}"
  [ "$m" = "leader" ] && leaders=$((leaders+1))
  [ "$m" = "follower" ] && followers=$((followers+1))
done
[ "$leaders" -eq 1 ] && ok "exactly one Keeper leader" || bad "Keeper leaders: $leaders (want 1)"
[ "$followers" -eq $((KP_N-1)) ] && ok "$followers follower(s)" || bad "Keeper followers: $followers (want $((KP_N-1)))"
leader_pod=""
for p in $(kp_pods); do [ "$(keeper_mode "$p")" = "leader" ] && leader_pod="$p"; done
if [ -n "$leader_pod" ]; then
  synced="$(k exec "$leader_pod" -- sh -c '(printf mntr; sleep 2) | nc 127.0.0.1 2181' 2>/dev/null | awk '/zk_synced_followers/ {print $2}')"
  [ "${synced:-0}" = "$((KP_N-1))" ] && ok "leader reports $synced synced follower(s)" || bad "leader reports ${synced:-?} synced follower(s) (want $((KP_N-1)))"
fi

# ------------------------------------------------------------ 3. replica health
step "3. Replica health (system.replicas)"
for p in $(ch_pods); do
  row="$(chq "$p" "SELECT engine, is_readonly, is_session_expired, total_replicas, active_replicas, queue_size, toUInt64(absolute_delay) FROM system.replicas WHERE database='$DB' AND table='$TABLE' FORMAT TSV")"
  if [ -z "$row" ] || echo "$row" | grep -q -i "exception"; then bad "$p: no replicated table $DB.$TABLE ($row)"; continue; fi
  engine=$(echo "$row" | cut -f1); ro=$(echo "$row" | cut -f2); exp=$(echo "$row" | cut -f3)
  tot=$(echo "$row" | cut -f4); act=$(echo "$row" | cut -f5); q=$(echo "$row" | cut -f6); dly=$(echo "$row" | cut -f7)
  info "$p: engine=$engine readonly=$ro session_expired=$exp replicas=$act/$tot queue=$q delay=${dly}s"
  case "$engine" in Replicated*) ok "$p: Replicated engine ($engine)";; *) bad "$p: engine $engine is not replicated";; esac
  [ "$ro" = "0" ] && [ "$exp" = "0" ] && ok "$p: writable, Keeper session alive" || bad "$p: read-only or session expired"
  [ "$tot" = "$CH_N" ] && [ "$act" = "$CH_N" ] && ok "$p: sees all $CH_N replicas active" || bad "$p: sees $act/$tot replicas active (want $CH_N)"
  [ "${q:-0}" -le 100 ] && [ "${dly:-0}" -le 10 ] && ok "$p: replication queue $q, lag ${dly}s" || bad "$p: replication behind (queue $q, lag ${dly}s)"
done

# -------------------------------------------------------- 4. data convergence
# Compare a CLOSED window (everything older than the cutoff). The newest rows are
# legitimately still in flight to the other replica, so fingerprinting "now"
# would false-alarm on every live system.
#
# The fingerprint is built from STREAMING aggregates only: count() plus the sum
# and xor of a row hash. ClickHouse computes those in constant memory, so this
# works on a memory-capped server. An earlier version used
# `SELECT count(), groupBitXor(...) FROM (SELECT DISTINCT event_id ...)` and was
# killed by the 1.8 GiB cap on this deployment (MEMORY_LIMIT_EXCEEDED), which
# says nothing about replication.
fingerprint() {
  chq "$1" "SELECT count(), sum(cityHash64(event_id)), groupBitXor(cityHash64(event_id))
FROM $DB.$TABLE WHERE source_ts < parseDateTime64BestEffort('$2', 9) FORMAT TSV"
}
converge() {
  local tries="${1:-6}" cut want="" all_same t p r
  cut="$(chq "$CH_STS-0" "SELECT toString(now64(3) - INTERVAL 30 SECOND)")"
  CONV_ERR=""; CONV_LAST=""
  t=0
  while [ "$t" -lt "$tries" ]; do
    all_same=1; want=""
    for p in $(ch_pods); do
      r="$(fingerprint "$p" "$cut")"
      case "$r" in
        *Exception*|*rror*)
          # The query itself failed (memory cap, replica busy). Not evidence of
          # divergence, and not something to report as a replication fault.
          CONV_ERR="$r"; all_same=-1 ;;
        *)
          [ -z "$want" ] && want="$r"
          [ "$r" != "$want" ] && all_same=0
          CONV_LAST="$CONV_LAST $p=[$(echo "$r" | tr '\t' ' ')]" ;;
      esac
    done
    if [ "$all_same" = 1 ] && [ -n "$want" ]; then
      CONV_ROWS="$(echo "$want" | cut -f1)"
      return 0
    fi
    if [ "$all_same" = -1 ] && [ -z "$CONV_LAST" ]; then
      CONV_ROWS=""
      return 2   # could not read every replica; caller reports it as such
    fi
    CONV_LAST=""; t=$((t+1)); sleep 5
  done
  CONV_ROWS="$(echo "$want" | cut -f1)"
  return 1
}
step "4. Data convergence (same events on every replica)"
CONV_LAST=""; CONV_ROWS=0; CONV_ERR=""
if converge 6; then
  info "fingerprint:$CONV_LAST"
  if [ "${CONV_ROWS:-0}" -gt 0 ]; then ok "all replicas hold the same $CONV_ROWS events (count and content hash match)"
  else bad "replicas match but hold no rows older than 30s: nothing to compare yet"; fi
elif [ "$?" = 2 ] || [ -n "$CONV_ERR" ]; then
  bad "could not fingerprint every replica (the query failed, which is a capacity problem, not divergence):"
  info "$(echo "$CONV_ERR" | head -2 | tr '\n' ' ')"
else
  bad "replicas DIFFER: $CONV_LAST"
fi

# ---------------------------------------------------------- 5. round trip
step "5. Replication round trip (scratch table, as the collector's account)"
if [ ! -f "$WRITER_PW_FILE" ]; then
  info "skipped: $WRITER_PW_FILE not found"
else
  scratch="$DB.ha_check_$$"
  out="$(chq_writer "$CH_STS-0" "CREATE TABLE $scratch ON CLUSTER $CLUSTER (id String, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$DB/ha_check_$$', '{replica}') ORDER BY id")"
  if echo "$out" | grep -q -i "exception"; then
    bad "ON CLUSTER DDL failed as telemetry_writer: $(echo "$out" | head -2 | tr '\n' ' ')"
  else
    ok "ON CLUSTER DDL ran on every replica as telemetry_writer"
    first="$CH_STS-0"; last="$CH_STS-$((CH_N-1))"
    chq_writer "$first" "INSERT INTO $scratch VALUES ('from-$first', 1)" >/dev/null
    seen=0; t=0
    while [ "$t" -lt 15 ]; do
      [ "$(chq_writer "$last" "SELECT count() FROM $scratch WHERE id='from-$first' SETTINGS select_sequential_consistency=0")" = "1" ] && { seen=1; break; }
      t=$((t+1)); sleep 1
    done
    [ "$seen" = 1 ] && ok "row written on $first appeared on $last (${t}s)" || bad "row written on $first never appeared on $last"
    chq_writer "$last" "INSERT INTO $scratch VALUES ('from-$last', 2)" >/dev/null
    seen=0; t=0
    while [ "$t" -lt 15 ]; do
      [ "$(chq_writer "$first" "SELECT count() FROM $scratch WHERE id='from-$last'")" = "1" ] && { seen=1; break; }
      t=$((t+1)); sleep 1
    done
    [ "$seen" = 1 ] && ok "row written on $last appeared on $first (${t}s)" || bad "row written on $last never appeared on $first"
    chq_writer "$first" "DROP TABLE IF EXISTS $scratch ON CLUSTER $CLUSTER SYNC" >/dev/null
  fi
fi

# -------------------------------------------------------------- 6. Service
step "6. Client Service routing"
svc_ips="$(k get endpoints "$CH_SVC" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | wc -w | tr -d ' ')"
[ "$svc_ips" = "$ch_ready" ] && ok "Service $CH_SVC has $svc_ips ready endpoint(s) = ready replicas" || bad "Service has $svc_ips endpoint(s), $ch_ready replicas Ready"

# ---------------------------------------------------------- 7. ingestion
rows() { chq "$1" "SELECT count() FROM $DB.$TABLE"; }
grows() { # grows <pod> <seconds> -> prints the increase
  local a b; a="$(rows "$1")"; sleep "$2"; b="$(rows "$1")"; echo $((b - a)) 2>/dev/null || echo "?"
}
step "7. Ingestion is flowing"
d="$(grows "$CH_STS-0" 8)"
{ [ "$d" != "?" ] && [ "$d" -gt 0 ]; } && ok "$CH_STS-0 gained $d rows in 8s" || bad "no new rows on $CH_STS-0 in 8s (delta=$d)"

# ============================================================ failover tests
if [ "$FAILOVER" = 1 ]; then
  step "F1. Kill a ClickHouse replica under live ingest"
  victim="$CH_STS-$((CH_N-1))"; survivor="$CH_STS-0"
  info "deleting $victim; ingestion must continue on $survivor"
  k delete pod "$victim" --wait=false >/dev/null
  sleep 12
  eps="$(k get endpoints "$CH_SVC" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | wc -w | tr -d ' ')"
  [ "$eps" = "$((CH_N-1))" ] && ok "Service dropped the dead replica ($eps endpoint(s))" || bad "Service has $eps endpoint(s), want $((CH_N-1))"
  d="$(grows "$survivor" 10)"
  { [ "$d" != "?" ] && [ "$d" -gt 0 ]; } && ok "ingestion continued on $survivor while $victim was down (+$d rows in 10s)" || bad "ingestion STOPPED while $victim was down (delta=$d)"
  [ "$(k get pod -l app.kubernetes.io/name=collector -o jsonpath='{.items[0].status.phase}')" = "Running" ] && ok "collector still Running" || bad "collector is not Running"
  info "waiting for $victim to come back and catch up (readiness waits for replication)..."
  if k wait --for=condition=Ready "pod/$victim" --timeout=240s >/dev/null 2>&1; then ok "$victim is Ready again (caught up)"; else bad "$victim did not become Ready"; fi
  sleep 5
  CONV_LAST=""
  if converge 12; then ok "replicas re-converged to the same $CONV_ROWS events after the failure"; else bad "replicas did NOT re-converge: $CONV_LAST"; fi

  step "F2. Kill the Keeper leader under live ingest"
  lp=""; for p in $(kp_pods); do [ "$(keeper_mode "$p")" = "leader" ] && lp="$p"; done
  if [ -z "$lp" ]; then bad "could not find the Keeper leader"; else
    info "deleting Keeper leader $lp"
    k delete pod "$lp" --wait=false >/dev/null
    sleep 15
    nl=""; for p in $(kp_pods); do [ "$p" != "$lp" ] && [ "$(keeper_mode "$p")" = "leader" ] && nl="$p"; done
    [ -n "$nl" ] && ok "a new Keeper leader was elected ($nl)" || bad "no new Keeper leader after 15s"
    # Poll rather than sample once. While Keeper elects a new leader a replica
    # can briefly lose its coordination session and go read-only until it
    # re-establishes one (the chart allows a 30s session timeout), so a single
    # sample is a coin flip under load - it reported a failure here that a
    # second run could not reproduce at all. What matters is that it comes back
    # on its own, so allow a bounded recovery window and report how long it took.
    for p in $(ch_pods); do
      t=0; ro=""
      while [ "$t" -lt 30 ]; do
        ro="$(chq "$p" "SELECT is_readonly FROM system.replicas WHERE database='$DB' AND table='$TABLE'")"
        [ "$ro" = "0" ] && break
        sleep 1; t=$((t+1))
      done
      if [ "$ro" = "0" ]; then
        if [ "$t" -gt 0 ]; then
          ok "$p was read-only for ${t}s during the Keeper election, then recovered on its own"
        else
          ok "$p stayed writable through the Keeper failover"
        fi
      else
        bad "$p is still read-only 30s after the Keeper leader died (is_readonly=$ro)"
      fi
    done
    d="$(grows "$CH_STS-0" 10)"
    { [ "$d" != "?" ] && [ "$d" -gt 0 ]; } && ok "ingestion continued through the Keeper failover (+$d rows in 10s)" || bad "ingestion stopped during the Keeper failover (delta=$d)"
    k wait --for=condition=Ready "pod/$lp" --timeout=180s >/dev/null 2>&1 && ok "$lp rejoined" || bad "$lp did not rejoin"
    sleep 10
    f=0; l=0; for p in $(kp_pods); do case "$(keeper_mode "$p")" in leader) l=$((l+1));; follower) f=$((f+1));; esac; done
    { [ "$l" = 1 ] && [ "$f" = $((KP_N-1)) ]; } && ok "Keeper back to 1 leader + $f follower(s)" || bad "Keeper ensemble not healthy ($l leader, $f follower)"
  fi
fi

printf '\n\033[1mResult: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
