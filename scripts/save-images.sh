#!/usr/bin/env bash
# Save the built images to a single tarball so they can be used for testing
# on another machine / kind cluster without re-building.
#
# Usage:
#   scripts/save-images.sh                  # saves tag "latest"   -> dist/telemetry-images-latest.tar
#   scripts/save-images.sh 1.2.0            # saves tag "1.2.0"    -> dist/telemetry-images-1.2.0.tar
#
# Restore anywhere with:
#   docker load -i dist/telemetry-images-<tag>.tar
# or load straight into a kind cluster:
#   kind load image-archive dist/telemetry-images-<tag>.tar -n telemetry
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMG_TAG="${1:-latest}"
OUT_DIR="$ROOT/dist"
OUT="$OUT_DIR/telemetry-images-$IMG_TAG.tar"

command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }

mkdir -p "$OUT_DIR"
echo "==> saving telemetry images (tag $IMG_TAG) to $OUT"
docker save \
  -o "$OUT" \
  "telemetry-streamer:$IMG_TAG" \
  "telemetry-collector:$IMG_TAG" \
  "telemetry-mq:$IMG_TAG" \
  "telemetry-api:$IMG_TAG"

ls -lh "$OUT"
echo "restore with:  docker load -i '$OUT'"