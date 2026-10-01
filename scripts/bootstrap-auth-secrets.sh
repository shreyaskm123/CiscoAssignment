#!/usr/bin/env bash
# Create (or rotate) the Kubernetes Secrets required by values-prod.yaml.
#
#   ./scripts/bootstrap-auth-secrets.sh                 # create if missing, keep existing
#   ./scripts/bootstrap-auth-secrets.sh --force         # rotate everything
#   ./scripts/bootstrap-auth-secrets.sh --force --only api|mq|clickhouse|interserver|tokenkey
#
# Generated credentials are written to ./.auth/ (git-ignored) as well as
# /tmp for convenience, so Postman and the local authcheck tool can use them.
# Nothing here is ever printed to stdout.
set -euo pipefail

NAMESPACE="${NAMESPACE:-telemetry}"
FORCE=0
ONLY=""
OUT_DIR="${OUT_DIR:-./.auth}"

while [ $# -gt 0 ]; do
  case "$1" in
    --force) FORCE=1; shift ;;
    # shift inside a `for arg in "$@"` loop does not move the loop's word list,
    # so --only used to capture the literal string "--only" and every section
    # was then skipped: a silent no-op. Parse with a while loop instead.
    --only)
      [ $# -ge 2 ] || { echo "--only requires a value: api|mq|clickhouse|interserver|tokenkey" >&2; exit 2; }
      ONLY="$2"; shift 2 ;;
    --only=*) ONLY="${1#--only=}"; shift ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$ONLY" in
  ""|api|mq|clickhouse|interserver|tokenkey) ;;
  *) echo "--only must be one of: api|mq|clickhouse|interserver|tokenkey (got '$ONLY')" >&2; exit 2 ;;
esac

command -v kubectl >/dev/null || { echo "kubectl not found" >&2; exit 1; }
kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || { echo "namespace $NAMESPACE not found" >&2; exit 1; }
mkdir -p "$OUT_DIR"
chmod 700 "$OUT_DIR"

rand() { openssl rand -hex 32; }
sha256_of() { printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1; }

# write <file> <value>
write() { printf '%s' "$2" > "$OUT_DIR/$1"; chmod 600 "$OUT_DIR/$1"; }

# secret_value <name> <key> -> plaintext value of a Secret key
secret_value() {
  kubectl get secret "$1" -n "$NAMESPACE" \
    -o go-template='{{index .data "'"$2"'"}}' 2>/dev/null | base64 -d 2>/dev/null
}

# secret_exists <name> <key>
# Uses go-template `index` rather than jsonpath: keys such as "users.xml"
# contain a dot, which jsonpath reads as a nested field and silently returns
# nothing (which would make this check always report "missing").
secret_exists() {
  [ -n "$(kubectl get secret "$1" -n "$NAMESPACE" \
            -o go-template='{{index .data "'"$2"'"}}' 2>/dev/null)" ]
}

want() { [ -z "$ONLY" ] || [ "$ONLY" = "$1" ]; }

# ---------------------------------------------------------------- API tokens
if want api; then
  if [ "$FORCE" = 0 ] && secret_exists telemetry-api-tokens tokens; then
    # Not rotating, but still export the live value: the documented
    # `cat .auth/api-token.txt` must work even on a first run against a
    # pre-existing Secret.
    API_TOKEN="$(secret_value telemetry-api-tokens tokens | sed 's/^[^=]*=//')"
    write api-token.txt "$API_TOKEN"
    printf '%s' "$API_TOKEN" > /tmp/telemetry-api-token.txt; chmod 600 /tmp/telemetry-api-token.txt
    echo "api tokens: unchanged (use --force to rotate)"
  else
    API_TOKEN="$(rand)"
    kubectl create secret generic telemetry-api-tokens -n "$NAMESPACE" \
      --from-literal="tokens=shreyas=${API_TOKEN}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    write api-token.txt "$API_TOKEN"
    printf '%s' "$API_TOKEN" > /tmp/telemetry-api-token.txt; chmod 600 /tmp/telemetry-api-token.txt
    echo "api tokens: created/rotated (1 client identity: shreyas)"
  fi
fi

# --------------------------------------------------------------- MQ tokens
#
# Four identities, and the chart mounts a specific key for each:
#   streamer  -> mounted as /etc/telemetry/auth/token in the streamer pod
#   collector -> mounted as /etc/telemetry/auth/token in the collector pod
#   mq        -> mounted as /etc/telemetry/auth/mq in EVERY MQ replica; the
#                StatefulSet lists `mq` in items[], so a missing key makes the
#                volume unbindable and the pod never leaves ContainerCreating.
#   follower  -> the follower identity accepted from peer replication
# `tokens` (the full identity=token map) is mounted too, for the MQ to validate
# incoming client calls. Omitting any one of them yields an MQ that cannot start.
MQ_KEYS="streamer collector mq follower"

if want mq; then
  if [ "$FORCE" = 0 ] && secret_exists telemetry-mq-tokens mq; then
    for svc in $MQ_KEYS; do
      v="$(secret_value telemetry-mq-tokens "$svc")"
      write "mq-$svc.txt" "$v"; printf '%s' "$v" > "/tmp/tok-$svc.txt"; chmod 600 "/tmp/tok-$svc.txt"
    done
    echo "mq tokens: unchanged (use --force to rotate)"
  else
    MQ_STREAMER="$(rand)"; MQ_COLLECTOR="$(rand)"; MQ_SELF="$(rand)"; MQ_FOLLOWER="$(rand)"
    kubectl create secret generic telemetry-mq-tokens -n "$NAMESPACE" \
      --from-literal="tokens=streamer=${MQ_STREAMER},collector=${MQ_COLLECTOR},mq=${MQ_SELF},follower=${MQ_FOLLOWER}" \
      --from-literal="streamer=${MQ_STREAMER}" \
      --from-literal="collector=${MQ_COLLECTOR}" \
      --from-literal="mq=${MQ_SELF}" \
      --from-literal="follower=${MQ_FOLLOWER}" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    write mq-streamer.txt "$MQ_STREAMER"; printf '%s' "$MQ_STREAMER" > /tmp/tok-streamer.txt
    write mq-collector.txt "$MQ_COLLECTOR"; printf '%s' "$MQ_COLLECTOR" > /tmp/tok-collector.txt
    write mq-self.txt    "$MQ_SELF";    printf '%s' "$MQ_SELF"    > /tmp/tok-mq.txt
    write mq-follower.txt "$MQ_FOLLOWER"; printf '%s' "$MQ_FOLLOWER" > /tmp/tok-follower.txt
    chmod 600 /tmp/tok-streamer.txt /tmp/tok-collector.txt /tmp/tok-mq.txt /tmp/tok-follower.txt
    echo "mq tokens: created/rotated (streamer, collector, mq, follower)"
  fi
fi

# --------------------------------------------------------- ClickHouse users
if want clickhouse; then
  XML_TMPL="helm-charts/telemetry/charts/clickhouse/users-scoped.xml"
  [ -f "$XML_TMPL" ] || { echo "missing $XML_TMPL" >&2; exit 1; }
  if [ "$FORCE" = 0 ] && secret_exists telemetry-clickhouse-users users.xml; then
    echo "clickhouse users: already present (use --force to rotate)"
  else
    CH_WRITER_PW="$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)"
    CH_READER_PW="$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)"
    sed -e "s/__WRITER_HASH__/$(sha256_of "$CH_WRITER_PW")/" \
        -e "s/__READER_HASH__/$(sha256_of "$CH_READER_PW")/" \
        "$XML_TMPL" > "$OUT_DIR/users.xml"
    kubectl create secret generic telemetry-clickhouse-users -n "$NAMESPACE" \
      --from-file="users.xml=$OUT_DIR/users.xml" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl create secret generic telemetry-ch-collector -n "$NAMESPACE" \
      --from-literal="user=telemetry_writer" --from-literal="password=${CH_WRITER_PW}" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    kubectl create secret generic telemetry-ch-apigateway -n "$NAMESPACE" \
      --from-literal="user=telemetry_reader" --from-literal="password=${CH_READER_PW}" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    write ch-writer-pass.txt "$CH_WRITER_PW"; printf '%s' "$CH_WRITER_PW" > /tmp/ch-writer-pass.txt
    write ch-reader-pass.txt "$CH_READER_PW"; printf '%s' "$CH_READER_PW" > /tmp/ch-reader-pass.txt
    chmod 600 /tmp/ch-writer-pass.txt /tmp/ch-reader-pass.txt
    echo "clickhouse users: created/rotated (telemetry_writer, telemetry_reader)"
  fi
fi

# ------------------------------------------- ClickHouse replica credential
# Shared by the ClickHouse replicas only (never by a client): it authenticates
# interserver part fetches and lets `ON CLUSTER` DDL run as the issuing user.
# Injected into the pods as an environment variable, so a rotation needs a
# rolling restart of the ClickHouse StatefulSet.
if want interserver; then
  if [ "$FORCE" = 0 ] && secret_exists telemetry-ch-interserver secret; then
    write ch-interserver.txt "$(secret_value telemetry-ch-interserver secret)"
    echo "clickhouse interserver secret: unchanged (use --force to rotate)"
  else
    CH_INTERSERVER="$(rand)"
    kubectl create secret generic telemetry-ch-interserver -n "$NAMESPACE" \
      --from-literal="secret=${CH_INTERSERVER}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    write ch-interserver.txt "$CH_INTERSERVER"
    echo "clickhouse interserver secret: created/rotated"
    if [ "$FORCE" = 1 ]; then
      echo "  NOTE: restart the replicas to pick it up: kubectl rollout restart sts/telemetry-clickhouse -n $NAMESPACE"
    fi
  fi
fi

# ------------------------------------------------- API token signing key
# Signs the short-lived access tokens handed out by POST /api/v1/token.
# Rotating this key invalidates every access token already issued, which is the
# point: it is the only way to revoke them, since they are stateless.
if want tokenkey; then
  if [ "$FORCE" = 0 ] && secret_exists telemetry-api-token-key signing-key; then
    KEY="$(secret_value telemetry-api-token-key signing-key)"
    write token-signing-key.txt "$KEY"
    echo "token signing key: unchanged (use --force to rotate and revoke all access tokens)"
  else
    KEY="$(openssl rand -hex 32)"
    kubectl create secret generic telemetry-api-token-key -n "$NAMESPACE" \
      --from-literal="signing-key=${KEY}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    write token-signing-key.txt "$KEY"
    echo "token signing key: created/rotated"
    if [ "$FORCE" = 1 ]; then
      echo "  NOTE: the apiGateway reads this key at startup, so restart it for the"
      echo "        new key to take effect: kubectl rollout restart deploy/telemetry-apigateway -n $NAMESPACE"
    fi
  fi
fi

echo
echo "credentials written to $OUT_DIR/ (mode 600) and mirrored to /tmp"
echo "next: helm upgrade --install telemetry ./helm-charts/telemetry -n $NAMESPACE \\"
echo "        -f ./helm-charts/telemetry/values-prod.yaml \\"
echo "        -f ./dist/values-local.yaml --wait"
echo "      (both values files are required; scripts/kind-up.sh writes the second one)"
echo "or just:  ./scripts/quickstart.sh --tag <tag>"
