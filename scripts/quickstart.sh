#!/usr/bin/env bash
# One command from nothing to a running, verified telemetry stack.
#
#   scripts/quickstart.sh              # full run: cluster, build, load, deploy
#   scripts/quickstart.sh --skip-build # reuse images already built/loaded
#   scripts/quickstart.sh --recreate   # delete the cluster first (clean slate)
#   scripts/quickstart.sh --tag v1.0.0 # build and deploy this image tag
#   scripts/quickstart.sh --no-verify  # deploy only, skip the verification pass
#
# Steps, in the order they must happen:
#   1. kind cluster + namespace          (scripts/kind-up.sh)
#   2. build the four component images   (scripts/build-images.sh)
#   3. load them into the kind node      (scripts/load-images.sh)
#   4. create the auth Secrets           (scripts/bootstrap-auth-secrets.sh)
#   5. helm install with two values files
#   6. wait for every workload to be Ready
#   7. verify the running system         (scripts/verify-deployment.sh)
#
# The image tag defaults to "latest" so the documented commands work verbatim.
# For anything you intend to keep, pass --tag: a kind node caches `latest`, so
# with `pullPolicy: IfNotPresent` a rebuilt `latest` can leave the old binary
# running while helm reports a successful rollout.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER="${CLUSTER:-telemetry}"
NAMESPACE="${NAMESPACE:-telemetry}"
RELEASE="${RELEASE:-telemetry}"
TAG="${IMG_TAG:-latest}"
SKIP_BUILD=0
RECREATE=0
VERIFY=1

while [ $# -gt 0 ]; do
  case "$1" in
    --skip-build) SKIP_BUILD=1; shift ;;
    --recreate)   RECREATE=1; shift ;;
    --no-verify)  VERIFY=0; shift ;;
    --tag)        [ $# -ge 2 ] || { echo "--tag requires a value" >&2; exit 2; }; TAG="$2"; shift 2 ;;
    --tag=*)      TAG="${1#--tag=}"; shift ;;
    -h|--help)    sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

VALUES_PROD="$ROOT/helm-charts/telemetry/values-prod.yaml"
VALUES_LOCAL="$ROOT/dist/values-local.yaml"

say()  { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
step() { printf '\033[36m[%d/7]\033[0m %s\n' "$1" "$2"; }
die()  { printf '\033[31merror: %s\033[0m\n' "$1" >&2; exit 1; }

START=$SECONDS

# ------------------------------------------------------------------ preflight
say "preflight"
missing=""
for tool in docker kind kubectl helm python3 curl; do
  command -v "$tool" >/dev/null || missing="$missing $tool"
done
[ -z "$missing" ] || die "missing required tool(s):$missing
  kind:  brew install kind   (or: go install sigs.k8s.io/kind@latest)
  helm:  brew install helm
  kubectl / docker / python3 / curl: see docs/quickstart.md"
docker info >/dev/null 2>&1 || die "docker is not running (start Docker Desktop and retry)"

MIN_HELM_MAJOR=3
MIN_HELM_MINOR=8   # helm --wait across 5 subcharts + StatefulSets is unusable below this
HELM_VER=$(helm version --short 2>/dev/null | sed 's/^v//; s/+.*//')
HELM_MAJOR=${HELM_VER%%.*}
HELM_MINOR=$(printf '%s' "$HELM_VER" | cut -d. -f2)
HELM_MINOR=${HELM_MINOR:-0}
if [ "$HELM_MAJOR" -lt "$MIN_HELM_MAJOR" ] 2>/dev/null ||
   { [ "$HELM_MAJOR" -eq "$MIN_HELM_MAJOR" ] && [ "$HELM_MINOR" -lt "$MIN_HELM_MINOR" ]; } 2>/dev/null; then
  die "helm $HELM_VER is too old; need >= $MIN_HELM_MAJOR.$MIN_HELM_MINOR"
fi
echo "docker, kind, kubectl, helm $HELM_VER, python3, curl: all present"

# Disk is checked here, before anything is created, because running out of space
# does not fail cleanly: kind create or an image pull dies part-way through with
# an error that points nowhere near the cause. The volume measured is the one
# backing Docker's data root, which on a laptop is the VM's disk and not the
# host's. See scripts/lib/preflight.sh; override with MIN_FREE_GB / SKIP_DISK_CHECK.
. "$ROOT/scripts/lib/preflight.sh"
preflight_disk || die "not enough disk space to deploy (details above)"

# --------------------------------------------------------- 1. cluster + values
step 1 "kind cluster, namespace, generated values"
if [ "$RECREATE" = 1 ]; then
  ./scripts/kind-up.sh --recreate --tag "$TAG"
else
  ./scripts/kind-up.sh --tag "$TAG"
fi
[ -f "$VALUES_LOCAL" ] || die "expected $VALUES_LOCAL to be written by kind-up.sh"
echo "using: $VALUES_LOCAL"
cat "$VALUES_LOCAL" | sed -n 's/^/    /p'

# ------------------------------------------------------------------- 2. build
if [ "$SKIP_BUILD" = 1 ]; then
  step 2 "build images (skipped: --skip-build)"
else
  step 2 "build component images (tag $TAG)"
  IMG_TAG="$TAG" ./scripts/build-images.sh
fi

# -------------------------------------------------------------------- 3. load
step 3 "load images into the kind node"
CLUSTER="$CLUSTER" ./scripts/load-images.sh "$TAG"

# ----------------------------------------------------------------- 4. secrets
step 4 "create auth Secrets"
# Idempotent: existing Secrets are kept unless --force is passed, so rerunning
# the quickstart does not invalidate tokens a tester is already holding.
NAMESPACE="$NAMESPACE" ./scripts/bootstrap-auth-secrets.sh

# ------------------------------------------------------------------ 5. deploy
step 5 "helm install (release '$RELEASE' in namespace '$NAMESPACE')"
say_note="note: deploys use BOTH values files; values-prod alone leaves every MQ replica a follower"
echo "  $say_note"
if helm upgrade --install "$RELEASE" "$ROOT/helm-charts/telemetry" \
      -n "$NAMESPACE" \
      -f "$VALUES_PROD" \
      -f "$VALUES_LOCAL" \
      --wait --timeout 10m > /tmp/quickstart-helm.log 2>&1; then
  echo "  helm install succeeded"
else
  echo >&2
  tail -30 /tmp/quickstart-helm.log >&2
  die "helm install failed (full log: /tmp/quickstart-helm.log)"
fi

# ------------------------------------------------------------------- 6. ready
step 6 "wait for every workload to be Ready"
kubectl -n "$NAMESPACE" get pods -o wide
echo
NOTREADY=$(kubectl -n "$NAMESPACE" get pods \
  --field-selector=status.phase=Running \
  -o json | python3 -c "
import json,sys
items=json.load(sys.stdin)['items']
bad=[p['metadata']['name'] for p in items
     if not all(c['ready'] for c in p['status'].get('containerStatuses',[]))]
print(' '.join(bad))" 2>/dev/null || true)
if [ -n "$NOTREADY" ]; then
  printf '\n\033[33mwarning: not all containers report Ready:\033[0m %s\n' "$NOTREADY" >&2
  printf 'give the deployment another 30-60s, then: kubectl -n %s get pods\n' "$NAMESPACE" >&2
else
  echo "all running pods are Ready"
fi

echo
kubectl -n "$NAMESPACE" get lease mq-leader \
  -o jsonpath='leader lease: {.spec.holderIdentity}{"\n"}' 2>/dev/null || echo "leader lease: not created yet"

# ------------------------------------------------------------------ 7. verify
if [ "$VERIFY" = 1 ]; then
  step 7 "verify the running system"
  # The verifier's env vars are NS / RELEASE / CHART / VALUES (not NAMESPACE).
  # VALUES must list BOTH files: the verifier deploys again as part of its run,
  # and a single-file deploy silently reverts the images to an unbuilt `latest`
  # and deletes the API-server egress CIDR.
  IMG_TAG="$TAG" COLLECTOR_TAG="$TAG" NS="$NAMESPACE" RELEASE="$RELEASE" \
    CHART="$ROOT/helm-charts/telemetry" \
    VALUES="$VALUES_PROD $VALUES_LOCAL" \
    ./scripts/verify-deployment.sh --skip-deploy
else
  step 7 "verification skipped (--no-verify)"
fi

ELAPSED=$((SECONDS - START))
say "done in ${ELAPSED}s"
# Uses python3 rather than jq: jq is not in the preflight list above, so
# printing a hint that needs it would send people down a yak-shave on a machine
# that already has everything else they need.
cat <<EOF
Try the API:

  # keep this running
  kubectl -n $NAMESPACE port-forward svc/$RELEASE-apigateway 8080:8080 &

  # 1. long-lived token -> short-lived access token
  TOKEN=\$(cat .auth/api-token.txt)
  JWT=\$(curl -s -X POST localhost:8080/api/v1/token \\
        -H "Authorization: Bearer \$TOKEN" \\
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')

  # 2. use the access token
  curl -s localhost:8080/api/v1/gpus -H "Authorization: Bearer \$JWT" \\
    | python3 -m json.tool

  # 3. read one GPU's telemetry. GPU_ID, not GID: GID is zsh's group-id
  #    parameter and assigning to it fails on macOS's default shell.
  GPU_ID=\$(curl -s localhost:8080/api/v1/gpus -H "Authorization: Bearer \$JWT" \\
        | python3 -c "import json,sys; print(json.load(sys.stdin)['gpus'][0]['id'])")
  curl -s "localhost:8080/api/v1/gpus/\$GPU_ID/telemetry" \\
    -H "Authorization: Bearer \$JWT" | python3 -m json.tool | head -30

Unauthenticated access is rejected:
  curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/v1/gpus   # 401

Tear everything down:  scripts/kind-up.sh --down
Full walkthrough:      docs/quickstart.md
EOF