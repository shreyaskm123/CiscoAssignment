#!/usr/bin/env bash
# Publish the MQ /metrics endpoints on the host so queue depth and memory can be
# watched in a browser or Postman, with no `kubectl exec` on every check.
#
#   ./scripts/mq-metrics-port.sh start    # then open the URLs it prints
#   ./scripts/mq-metrics-port.sh status
#   ./scripts/mq-metrics-port.sh stop
#
# Why this is `kubectl port-forward` and not a published container port:
#
# On this machine Docker runs inside a Colima VM, and Colima's port forwarder
# (the ssh mux owning the host listener) goes stale. Once it has held a port for
# a container that no longer exists it keeps accepting connections and replying
# "Empty reply from server" to every one of them, and it does not recover on its
# own -- observed holding 30920 for minutes after the container was gone, and
# failing a freshly published port too. Publishing through Docker is therefore
# not dependable here.
#
# A port-forward to the Service sidesteps that entirely: it rides the Kubernetes
# API server, which is already reachable, and needs nothing from the VM's network
# layer. It answered on the first attempt every time it was tried.
#
# One forward per replica, on consecutive host ports:
#
#   http://localhost:30920/metrics   replica 0
#   http://localhost:30921/metrics   replica 1
#
# Each replica gets its own URL so leader and follower are never interleaved into
# a single trend. Both are published by the chart (messagequeue.metrics.enabled);
# this script only forwards them.
set -euo pipefail

CLUSTER="${CLUSTER:-telemetry}"
NAMESPACE="${NAMESPACE:-telemetry}"
PORT="${PORT:-30920}"
REPLICAS="${REPLICAS:-2}"
LOGDIR="${LOGDIR:-$PWD/dist/mq-metrics}"
PIDS="$LOGDIR/pids"

say() { printf '%s\n' "$*"; }

metrics_svc() {
  printf '%s-messagequeue-metrics-%s' "$CLUSTER" "$1"
}

forward_pid() {
  local pidfile="$PIDS/$1.pid"
  [ -f "$pidfile" ] || return 1
  local pid
  pid=$(cat "$pidfile" 2>/dev/null) || return 1
  # A pid can be recycled, so confirm it is still our port-forward.
  kill -0 "$pid" 2>/dev/null || return 1
  ps -o command= -p "$pid" 2>/dev/null | grep -q 'port-forward' || return 1
  printf '%s' "$pid"
}

case "${1:-start}" in
stop)
  found=0
  for i in $(seq 0 $((REPLICAS - 1))); do
    if pid=$(forward_pid "$i"); then
      kill "$pid" 2>/dev/null || true
      found=1
    fi
    rm -f "$PIDS/$i.pid"
  done
  if [ "$found" -eq 1 ]; then
    say "MQ metrics endpoints are no longer served on localhost:${PORT}-$((PORT + REPLICAS - 1))."
  else
    say "no metrics port-forwards were running."
  fi
  ;;

status)
  for i in $(seq 0 $((REPLICAS - 1))); do
    url="http://localhost:$((PORT + i))/metrics"
    if body=$(curl -fsS --max-time 5 "$url" 2>/dev/null); then
      role=$(printf '%s' "$body" | sed -n 's/^mq_role{[^}]*} //p' | head -1)
      depth=$(printf '%s' "$body" | sed -n 's/^mq_retained_entries{[^}]*} //p' | head -1)
      rss=$(printf '%s' "$body" | sed -n 's/^mq_process_resident_memory_bytes{[^}]*} //p' | head -1)
      heap=$(printf '%s' "$body" | sed -n 's/^mq_go_memstats_heap_inuse_bytes{[^}]*} //p' | head -1)
      wal=$(printf '%s' "$body" | sed -n 's/^mq_wal_bytes{[^}]*} //p' | head -1)
      role_txt=$([ "$role" = "1" ] && echo leader || echo follower)
      say "$(printf 'replica-%s  %-8s depth=%-9s rss=%-7s heap=%-7s wal=%s' \
        "$i" "$role_txt" "$depth" \
        "$(printf '%s' "$rss" | awk '{printf "%.1fMiB", $1/1048576}')" \
        "$(printf '%s' "$heap" | awk '{printf "%.1fMiB", $1/1048576}')" \
        "$(printf '%s' "$wal" | awk '{printf "%.1fMiB", $1/1048576}')")"
    elif forward_pid "$i" >/dev/null; then
      say "replica-$i  forward up but not answering yet at $url"
    else
      say "replica-$i  down  ($url) — start with: $0 start"
    fi
  done
  ;;

start)
  command -v kubectl >/dev/null || { say "kubectl not found" >&2; exit 1; }

  missing=0
  for i in $(seq 0 $((REPLICAS - 1))); do
    kubectl -n "$NAMESPACE" get svc "$(metrics_svc "$i")" >/dev/null 2>&1 || {
      say "service $(metrics_svc "$i") does not exist. Enable the endpoint first:" >&2
      say "  helm upgrade $CLUSTER helm-charts/telemetry -n $NAMESPACE \\" >&2
      say "    -f dist/values-local.yaml --set ${CLUSTER}.messagequeue.metrics.enabled=true" >&2
      missing=1
    }
  done
  [ "$missing" -eq 0 ] || exit 1

  mkdir -p "$PIDS"
  for i in $(seq 0 $((REPLICAS - 1))); do
    host_port=$((PORT + i))
    if forward_pid "$i" >/dev/null; then
      say "replica-$i already forwarded on localhost:${host_port}"
      continue
    fi
    # nohup + setsid so the forward survives this script exiting and keeps
    # serving after the terminal that started it is closed.
    nohup kubectl -n "$NAMESPACE" port-forward "svc/$(metrics_svc "$i")" \
      "${host_port}:8081" >"$LOGDIR/$i.log" 2>&1 &
    echo $! >"$PIDS/$i.pid"
  done

  # Confirm before claiming success: a forward that silently failed to bind is
  # the failure mode this whole script exists to prevent.
  for i in $(seq 0 $((REPLICAS - 1))); do
    ok=0
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      sleep 1
      if curl -fsS --max-time 4 "http://localhost:$((PORT + i))/metrics" >/dev/null 2>&1; then
        ok=1
        break
      fi
    done
    if [ "$ok" -ne 1 ]; then
      say "replica-$i did not answer on localhost:$((PORT + i)). Forward log:" >&2
      tail -n 5 "$LOGDIR/$i.log" >&2 || true
      exit 1
    fi
  done

  say "MQ metrics are live at:"
  for i in $(seq 0 $((REPLICAS - 1))); do
    say "  http://localhost:$((PORT + i))/metrics    # replica $i"
  done
  say ""
  say "Open in a browser or Postman and refresh to poll. Nothing is cached."
  say "The forwards run in the background and survive closing this terminal."
  say ""
  say "Stop with: $0 stop"
  ;;

*)
  say "usage: $0 {start|status|stop}" >&2
  exit 2
  ;;
esac