#!/usr/bin/env bash
# Shared preflight checks for the deploy scripts. Sourced, not executed.
#
# The disk check exists because a full disk does not fail cleanly: `kind create`
# or an image pull dies part-way with a confusing error long after the cause, and
# on a laptop the *host* disk is usually fine while the VM that actually stores
# Docker images is the one that is full. So this measures the filesystem backing
# Docker's data root, not the machine's root.
#
# Override the threshold when a machine is genuinely small:
#   MIN_FREE_GB=4 ./scripts/quickstart.sh
# Skip it entirely:
#   SKIP_DISK_CHECK=1 ./scripts/quickstart.sh

# Space the stack needs before it is comfortable:
#   - the four component images and the kind node image: ~2G
#   - the kind node containers and everything kindest/node pulls: ~3G
#   - the ClickHouse PVCs the chart requests (5Gi x2, 1Gi x3) filled by ingest
# Anything under WARN is tight; under MIN the run will fail part-way.
: "${WARN_FREE_GB:=12}"
: "${MIN_FREE_GB:=8}"

# disk_free_gb PATH -> "<free_gib> <total_gib>" for the filesystem backing PATH,
# or non-zero exit if it cannot be determined.
#
# Measured from inside a throwaway container rather than with a host `df`: when
# Docker runs in a VM (Colima, Docker Desktop, Lima) the host's df reports the
# host disk, which is the wrong number. Binding the data root into a container
# reports the filesystem that will actually hold the images. Falls back to a host
# df for a plain Linux Docker install.
disk_free_gb() {
  local path="$1" line free total
  if [ -n "$DISK_PROBE_IMAGE" ]; then
    line=$(docker run --rm -v "$path:/probe" --entrypoint df "$DISK_PROBE_IMAGE" \
             -BG /probe 2>/dev/null | awk 'NR==2 {print $2, $4}')
    if [ -n "$line" ]; then
      total=${line%% *}; free=${line##* }
      case "$free:$total" in *[!0-9:]*|:*|:*) ;; *)
        # df -BG already reports GiB, so these are used as-is.
        printf '%s %s' "$free" "$total"; return 0 ;;
      esac
    fi
  fi
  line=$(df -BG "$path" 2>/dev/null | awk 'NR==2 {print $2, $4}')
  total=${line%% *}; free=${line##* }
  case "$free:$total" in *[!0-9:]*|:*|:*) return 1 ;; esac
  printf '%s %s' "$free" "$total"
}

# An image to run the probe with. Anything already present will do; the probe only
# needs df(1), which busybox provides. Preferring a local image avoids the check
# itself needing the network it is trying to protect.
disk_probe_image() {
  local img
  for img in alpine:latest alpine:3.20 busybox:latest busybox:1.36; do
    if docker image inspect "$img" >/dev/null 2>&1; then
      printf '%s' "$img"
      return 0
    fi
  done
  # Nothing local: fall back to the host df path rather than pulling an image.
  printf ''
}

# preflight_disk -> 0 to continue, 1 to abort. Prints what it measured either way.
preflight_disk() {
  if [ "${SKIP_DISK_CHECK:-0}" = "1" ]; then
    printf '  disk    : skipped (SKIP_DISK_CHECK=1)\n'
    return 0
  fi

  local root
  root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null)
  [ -n "$root" ] || root=/var/lib/docker
  DISK_PROBE_IMAGE=$(disk_probe_image)

  local sizes free total
  if ! sizes=$(disk_free_gb "$root"); then
    printf '  disk    : could not determine free space on %s (continuing)\n' "$root"
    return 0
  fi
  free=${sizes%% *}; total=${sizes##* }

  if [ "$free" -lt "$MIN_FREE_GB" ]; then
    printf '  disk    : FAIL only %s GiB free on the Docker volume (need >= %s GiB)\n' \
      "$free" "$MIN_FREE_GB" >&2
    printf '\n' >&2
    printf 'This is the filesystem that stores Docker images and the kind node,\n' >&2
    printf 'not the host disk. Free space before continuing:\n\n' >&2
    printf '  docker system df              # what is using it\n' >&2
    printf '  docker system prune -a        # remove unused images (rebuilds are slower after)\n' >&2
    printf '  rm -rf dist                   # generated values and coverage output\n\n' >&2
    printf 'Override with MIN_FREE_GB=<n> if this machine is genuinely small, or\n' >&2
    printf 'skip with SKIP_DISK_CHECK=1.\n' >&2
    return 1
  fi

  if [ "$free" -lt "$WARN_FREE_GB" ]; then
    printf '  disk    : WARN %s GiB free of %s GiB on the Docker volume (want >= %s GiB)\n' \
      "$free" "$total" "$WARN_FREE_GB" >&2
    printf '          docker system df   # %s reclaimable\n' \
      "$(docker system df --format '{{.Type}} {{.Reclaimable}}' 2>/dev/null | awk '$1=="Images"{print $2}')" >&2
  else
    printf '  disk    : ok %s GiB free of %s GiB on the Docker volume (%s)\n' \
      "$free" "$total" "$root"
  fi
  return 0
}