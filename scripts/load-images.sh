#!/usr/bin/env bash
# Load locally built images into the kind cluster's node.
#
#   scripts/load-images.sh              # tag "latest"
#   scripts/load-images.sh v1.0.0       # one tag for all four components
#
# A kind node has no registry to pull from, so images must be pushed into it
# with `kind load`. `--name` matters: the default is "kind", and loading into a
# different cluster is silently a no-op that only shows up later as
# ErrImageNeverPull.
set -euo pipefail

CLUSTER="${CLUSTER:-telemetry}"
IMG_TAG="${1:-${IMG_TAG:-latest}}"

command -v kind >/dev/null || { echo "kind not found" >&2; exit 1; }
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || {
  echo "no kind cluster named '$CLUSTER'; create it with scripts/kind-up.sh" >&2; exit 1; }

# The ClickHouse image is pulled from Docker Hub by the node itself, so it is
# deliberately absent here.
IMAGES="telemetry-streamer telemetry-collector telemetry-mq telemetry-api"

echo "==> loading telemetry-streamer:$IMG_TAG telemetry-collector:$IMG_TAG telemetry-mq:$IMG_TAG telemetry-api:$IMG_TAG into cluster '$CLUSTER'"
missing=0
for name in $IMAGES; do
  if ! docker image inspect "$name:$IMG_TAG" >/dev/null 2>&1; then
    echo "  MISSING $name:$IMG_TAG (build all four with: IMG_TAG=$IMG_TAG scripts/build-images.sh)" >&2
    missing=$((missing + 1))
    continue
  fi
  printf '  %s:%s ... ' "$name" "$IMG_TAG"
  if kind load docker-image "$name:$IMG_TAG" --name "$CLUSTER" >/dev/null 2>&1; then
    echo "ok"
  else
    echo "FAILED" >&2
    missing=$((missing + 1))
  fi
done

if [ "$missing" -gt 0 ]; then
  echo >&2
  echo "$missing image(s) not loaded. With pullPolicy: Never the pods will" >&2
  echo "fail with ErrImageNeverPull until this is fixed." >&2
  exit 1
fi

echo
echo "all images present on the '$CLUSTER' node"
echo "verify with: docker exec ${CLUSTER}-control-plane crictl images | grep telemetry-"