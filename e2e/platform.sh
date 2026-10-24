#!/usr/bin/env bash
# litek8s's GitOps platform example (examples/platform: Gitea, 12
# Applications in waves, Helm charts, Kustomize, an app of apps that manages
# itself, Gitea webhooks, CI, Atlantis) with litecd in Argo CD's place, on
# the cluster of the current kube context; then the example's smoke test.
#
#   e2e/platform.sh [smoke.sh args]      e.g. eval "$(compat/cluster.sh env)" first
#
# LITEK8S_DIR   a litek8s checkout (default: next to this repository). Its
#               examples/platform is copied and platform.patch applied to the
#               copy (CD_CONTROLLER=litecd: components/argocd installs
#               litecd); litek8s itself knows nothing of litecd.
# DOCKER_HOST   the cluster's Docker API, where deploy/build.sh puts the
#               image litecd:dev (skipped with LITECD_SKIP_BUILD=1)
# Other variables go to up.sh as they are (HTTP_PORT, HTTPS_PORT, ...).
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
litek8s=${LITEK8S_DIR:-$root/../litek8s}
[[ -f $litek8s/examples/platform/up.sh ]] || { echo "no examples/platform in LITEK8S_DIR=$litek8s" >&2; exit 2; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cp -R "$litek8s/examples/platform" "$work/platform"
if ! patch -s -d "$work/platform" -p1 <"$root/e2e/platform.patch"; then
    echo "e2e/platform.patch no longer applies to $litek8s/examples/platform: update it" >&2
    exit 1
fi

[[ -n ${LITECD_SKIP_BUILD:-} ]] || "$root/deploy/build.sh" >&2
CD_CONTROLLER=litecd LITECD_DIR=$root "$work/platform/up.sh"
"$work/platform/smoke.sh" "$@"
