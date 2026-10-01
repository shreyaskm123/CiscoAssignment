#!/usr/bin/env bash
# Build Docker images for every telemetry component.
#
# Usage:
#   scripts/build-images.sh              # all components, tag "latest"
#   scripts/build-images.sh messagequeue # only the message queue
#   IMG_TAG=1.2.0 scripts/build-images.sh
#   IMG_TAG=$(git rev-parse --short HEAD) scripts/build-images.sh
#
# Produces (image names must match the helm chart values):
#   telemetry-streamer, telemetry-collector, telemetry-mq, telemetry-api
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMG_TAG="${IMG_TAG:-latest}"

# module-dir -> image-name (chart defaults live in helm-charts/telemetry/values.yaml).
# Plain "dir:image" pairs: portable to macOS bash 3.2 (no associative arrays).
IMAGES="streamer:telemetry-streamer collector:telemetry-collector messageQueue:telemetry-mq apiGateway:telemetry-api"

command -v docker >/dev/null || { echo "docker not found (start Docker Desktop first)" >&2; exit 1; }

build_one() {
  local dir="$1" name="$2"
  echo "==> building $name:$IMG_TAG from ./$dir"
  # Context = repo ROOT: cross-module `replace` directives (../streamer/proto,
  # ../collector) resolve only when sibling modules are part of the build context.
  docker build --pull \
    -f "$ROOT/$dir/Dockerfile" \
    -t "$name:$IMG_TAG" \
    --label "org.opencontainers.image.title=$name" \
    --label "org.opencontainers.image.version=$IMG_TAG" \
    --label "org.opencontainers.image.revision=$(date +%Y%m%d-%H%M%S)" \
    "$ROOT"
}

dir_of() { echo "${1%%:*}"; }
name_of() { echo "${1##*:}"; }

if [ "$#" -eq 0 ]; then
  for entry in $IMAGES; do
    build_one "$(dir_of "$entry")" "$(name_of "$entry")"
  done
else
  for arg in "$@"; do
    found=""
    for entry in $IMAGES; do
      if [ "$(dir_of "$entry")" = "$arg" ]; then found="$entry"; break; fi
    done
    if [ -z "$found" ]; then
      echo "unknown component '$arg'; pick from: $(for e in $IMAGES; do dir_of "$e"; done | tr '\n' ' ')" >&2
      exit 1
    fi
    build_one "$(dir_of "$found")" "$(name_of "$found")"
  done
fi

echo
echo "done. images:"
for entry in $IMAGES; do
  n="$(name_of "$entry")"
  docker image inspect "$n:$IMG_TAG" >/dev/null 2>&1 && echo "  $n:$IMG_TAG"
done