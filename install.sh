#!/bin/sh
# Install litecd from a GitHub release on the cluster of the current kube
# context:
#
#   curl -fsSL https://raw.githubusercontent.com/ideascoldigital/litecd/main/install.sh | sh
#
# On a cluster without Argo CD: creates the namespace argocd if it is
# missing and applies the release's install.yaml (Argo CD's CRDs, RBAC,
# argocd-cm, the litecd Deployment on the release's image, a Service named
# argocd-server for webhooks).
#
# On a cluster where Argo CD is installed in namespace argocd (any of its
# application controller, repo server or API server): applies the release's
# takeover.yaml instead, only litecd's own ServiceAccount, RBAC and
# Deployment, and leaves Argo CD's CRDs, ConfigMaps and argocd-server
# Service to Argo CD's install. litecd then stands down while Argo CD's
# application controller runs and takes over once it is scaled to 0; the
# steps are in README.md, "Moving over from Argo CD".
#
# Either file is checked against the release's SHA256SUMS and applied with
# server-side apply; then it waits for litecd. Run it again to upgrade.
#
# Environment:
#   LITECD_VERSION       a tag such as v0.2.2 (default: the latest release)
#   LITECD_REPO          owner/name on GitHub (default: ideascoldigital/litecd)
#   LITECD_DOWNLOAD_URL  where the manifests and SHA256SUMS are (default: the
#                        release's download URL on GitHub; for mirrors and tests)
#   LITECD_MODE          install or takeover (default: detected as above)
#   KUBECTL              the kubectl to run, with any flags (default: kubectl)
set -eu

REPO="${LITECD_REPO:-ideascoldigital/litecd}"
VERSION="${LITECD_VERSION:-latest}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE=argocd

say() { printf '%s\n' "$*" >&2; }
die() { say "install.sh: $*"; exit 1; }
k() { $KUBECTL "$@"; }
command -v curl >/dev/null 2>&1 || die "needs curl"
command -v "${KUBECTL%% *}" >/dev/null 2>&1 || die "needs kubectl"

if [ -n "${LITECD_DOWNLOAD_URL:-}" ]; then
    base="$LITECD_DOWNLOAD_URL"
elif [ "$VERSION" = latest ]; then
    base="https://github.com/$REPO/releases/latest/download"
else
    base="https://github.com/$REPO/releases/download/$VERSION"
fi

ctx=$(k config current-context 2>/dev/null || echo "(none)")
k get --raw /version >/dev/null 2>&1 || die "cannot reach the cluster of context $ctx"

# Argo CD's workloads in the namespace: anything but litecd's own.
argocd=""
for w in statefulset/argocd-application-controller deployment/argocd-application-controller \
    deployment/argocd-repo-server deployment/argocd-server; do
    if k -n "$NAMESPACE" get "$w" >/dev/null 2>&1; then argocd="$argocd ${w#*/}"; fi
done
mode="${LITECD_MODE:-}"
[ -n "$mode" ] || { [ -n "$argocd" ] && mode=takeover || mode=install; }
case "$mode" in
install | takeover) ;;
*) die "LITECD_MODE is install or takeover, not $mode" ;;
esac
file="$mode.yaml"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
say "litecd: downloading $base/$file"
curl -fsSL "$base/$file" -o "$tmp/$file" || die "cannot download $base/$file"
curl -fsSL "$base/SHA256SUMS" -o "$tmp/SHA256SUMS" || die "cannot download $base/SHA256SUMS"
want=$(awk -v f="$file" '$2 == f || $2 == "*" f { print $1 }' "$tmp/SHA256SUMS")
[ -n "$want" ] || die "SHA256SUMS has no $file"
if command -v sha256sum >/dev/null 2>&1; then
    got=$(sha256sum "$tmp/$file" | awk '{ print $1 }')
else
    got=$(shasum -a 256 "$tmp/$file" | awk '{ print $1 }')
fi
[ "$got" = "$want" ] || die "$file does not match SHA256SUMS"
image=$(sed -n 's/^ *image: *//p' "$tmp/$file" | head -1)

if [ "$mode" = takeover ]; then
    say "litecd: Argo CD found in namespace $NAMESPACE (${argocd# }): installing next to it,"
    say "        leaving its CRDs, ConfigMaps and argocd-server Service alone"
fi
say "litecd: installing $image on context $ctx, namespace $NAMESPACE"
k get namespace "$NAMESPACE" >/dev/null 2>&1 \
    || k create namespace "$NAMESPACE" >/dev/null \
    || die "cannot create namespace $NAMESPACE"
k apply --server-side --field-manager=litecd-install -f "$tmp/$file" >/dev/null \
    || die "kubectl apply failed (a conflict? see above)"
k -n "$NAMESPACE" rollout status deployment/litecd --timeout=180s >&2 \
    || die "litecd did not become ready: kubectl -n $NAMESPACE logs deploy/litecd"

if [ "$mode" = takeover ]; then
    say "litecd: ready, standing down while Argo CD's application controller runs."
    say "  It acts on no Application until that controller is scaled to 0. Before that:"
    say "  turn off automated sync on any app that installs Argo CD itself (or it scales it back),"
    say "  then: kubectl -n $NAMESPACE scale statefulset/argocd-application-controller --replicas=0"
    say "  Steps and rollback: https://github.com/$REPO#moving-over-from-argo-cd"
else
    say "litecd: ready. Applications go in namespace $NAMESPACE:"
    say "  kubectl -n $NAMESPACE get applications"
fi
