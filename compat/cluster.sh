#!/usr/bin/env bash
# A litek8s dev cluster for the corpus, apart from the user's own: the
# installed litek8s with its own LITEK8S_HOME (own VM or sandbox, data,
# kubeconfig), so nothing here touches their clusters or contexts.
#
#   compat/cluster.sh up        start it (4 GiB VM on macOS)
#   compat/cluster.sh env       eval "$(compat/cluster.sh env)": KUBECONFIG for it
#   compat/cluster.sh down      stop it
#   compat/cluster.sh delete    remove it and its data
#
# LITECD_LITEK8S_HOME (default ~/.litek8s-litecd) is its LITEK8S_HOME;
# LITEK8S_BIN another litek8s binary than the one on PATH.
set -euo pipefail
home=${LITECD_LITEK8S_HOME:-$HOME/.litek8s-litecd}
export LITEK8S_HOME=$home
kubeconfig=$home/kubeconfig
litek8s=${LITEK8S_BIN:-litek8s}
command -v "$litek8s" >/dev/null || { echo "litek8s is not installed (https://github.com/jairoFernandez/litek8s)" >&2; exit 2; }
case "${1:-}" in
    up)
        mkdir -p "$home"
        KUBECONFIG=$kubeconfig "$litek8s" dev up --name compat --memory "${LITECD_CLUSTER_MEMORY:-4096}"
        echo "kubeconfig: $kubeconfig"
        ;;
    env) echo "export KUBECONFIG=$kubeconfig" ;;
    down) "$litek8s" dev down --name compat ;;
    delete) "$litek8s" dev delete --name compat ;;
    *) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac
