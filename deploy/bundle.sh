#!/usr/bin/env bash
# Print deploy/install as one manifest, its Deployment on the given image:
# what each release publishes (.github/workflows/image.yml).
#
#   deploy/bundle.sh <image>             install.yaml: Argo CD's CRDs, then
#                                        litecd.yaml whole
#   deploy/bundle.sh --takeover <image>  takeover.yaml: only litecd's own
#                                        objects (ServiceAccount, RBAC,
#                                        Deployment), for a namespace where
#                                        Argo CD is installed
#
# install.yaml holds Argo CD's CRDs, argocd-cm, argocd-cmd-params-cm and a
# Service named argocd-server: applied next to Argo CD it would take over
# Argo CD's Service and fight its install over those objects. takeover.yaml
# leaves them all to Argo CD's install. Neither holds a Namespace, so
# deleting one never takes the namespace (and the Applications in it)
# along; install.sh creates the namespace.
set -euo pipefail
takeover=
if [[ ${1:-} == --takeover ]]; then takeover=1; shift; fi
image=${1:?usage: deploy/bundle.sh [--takeover] <image>}
dir=$(cd "$(dirname "$0")/install" && pwd)
grep -q '^          image: litecd:dev$' "$dir/litecd.yaml" \
    || { echo "bundle.sh: no 'image: litecd:dev' line in litecd.yaml" >&2; exit 1; }
set_image() { sed "s|^          image: litecd:dev$|          image: $image|" "$@"; }

if [[ $takeover ]]; then
    # litecd.yaml's documents, without the ones Argo CD's install owns.
    set_image "$dir/litecd.yaml" | awk '
        function flush() {
            if (doc != "" && doc !~ /\nkind: (ConfigMap|Service)\n/) { printf "%s%s", sep, doc; sep = "---\n" }
            doc = "\n"
        }
        BEGIN { doc = "\n" }
        /^---$/ { flush(); next }
        { doc = doc $0 "\n" }
        END { flush() }' | sed '1{/^$/d;}'
    exit 0
fi
first=1
for f in "$dir"/crds/*.yaml "$dir/litecd.yaml"; do
    [[ $first ]] || echo '---'
    first=
    set_image "$f"
done
