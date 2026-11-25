#!/usr/bin/env bash
# Build one control-plane OCI archive reproducibly.
#
# SOURCE_DATE_EPOCH is part of every image digest: buildx rewrites each file and
# layer timestamp to it. Changing it moves every digest with no code change, so
# it lives here, in the repo, and changes only on purpose.
#
# usage: build-oci.sh <role: controller|host-witness|worker> <runner-pool-dir> <binary-dir> <tag> <out-file>
set -euo pipefail

if [ "$#" -ne 5 ]; then
    echo "usage: build-oci.sh <role: controller|host-witness|worker> <runner-pool-dir> <binary-dir> <tag> <out-file>" >&2
    exit 2
fi

role="$1"
tree="$2"
bindir="$3"
tag="$4"
out="$5"

# 2025-09-13T00:00:00Z. See the header before changing it.
export SOURCE_DATE_EPOCH=1757721600

cd "$tree"

case "$role" in
  controller)
    docker buildx build --pull=false --no-cache --platform linux/amd64 \
      --build-context controller="$bindir" \
      -f image/controller/Dockerfile \
      -t "sjo-runner-pool-controller:$tag" \
      --output "type=oci,dest=$out,rewrite-timestamp=true" \
      image/controller
    ;;
  host-witness)
    docker buildx build --pull=false --no-cache --platform linux/amd64 \
      --build-context witness="$bindir" \
      --build-context host-tool="$bindir" \
      -f image/host-witness/Dockerfile \
      -t "sjo-runner-pool-host-witness:$tag" \
      --output "type=oci,dest=$out,rewrite-timestamp=true" \
      image/host-witness
    ;;
  worker)
    docker buildx build --pull=false --no-cache --platform linux/amd64 \
      --build-context worker="$bindir" \
      -f image/linux/Dockerfile \
      -t "sjo-runner-pool-linux-worker:$tag" \
      --output "type=oci,dest=$out,rewrite-timestamp=true" \
      image/linux
    ;;
  *)
    echo "unknown role: $role" >&2
    exit 2
    ;;
esac
