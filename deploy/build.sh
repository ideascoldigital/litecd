#!/usr/bin/env bash
# Build the litecd image (litecd:dev by default) on the Docker API in
# DOCKER_HOST: the static musl binary in rust:1-alpine, offline from crates
# vendored here first (target/vendor: a VM's network may not reach
# crates.io), its target dir in the Docker volume litecd-build, then
# deploy/Dockerfile around it. On
# litek8s's Docker API the image lands in the cluster's own image store, so
# pods use it without a registry.
#
#   deploy/build.sh [image]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
image=${1:-litecd:dev}
out=$root/target/linux
mkdir -p "$out"
(cd "$root" && cargo vendor --locked -q target/vendor >/dev/null)
docker run --rm -v "$root:/work" -w /work -v litecd-build:/cache \
    -e CARGO_TARGET_DIR=/cache/target \
    rust:1-alpine sh -c 'apk add --no-cache musl-dev gcc make >/dev/null &&
        cargo build --release --locked --offline \
            --config "source.crates-io.replace-with=\"vendored\"" --config "source.vendored.directory=\"target/vendor\"" &&
        cp /cache/target/release/litecd /work/target/linux/litecd.tmp' >&2
mv "$out/litecd.tmp" "$out/litecd"
ctx=$(mktemp -d)
trap 'rm -rf "$ctx"' EXIT
cp "$root/deploy/Dockerfile" "$out/litecd" "$ctx/"
DOCKER_BUILDKIT=0 docker build -q -t "$image" "$ctx" >&2
echo "$image ($(du -h "$out/litecd" | cut -f1) binary)"
