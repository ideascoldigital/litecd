# Target litecd: the build under test, from LITECD_MANIFEST (the release
# YAML: Argo CD's CRDs, RBAC and the litecd Deployment). There is no
# deployable build before phase 1.

target_install() {
    if [[ -z "${LITECD_MANIFEST:-}" ]]; then
        echo "litecd has no deployable build yet (phase 1): set LITECD_MANIFEST to a release YAML" >&2
        return 1
    fi
    k create namespace "$ARGOCD_NS" >/dev/null 2>&1 || true
    k apply --server-side -n "$ARGOCD_NS" -f "$LITECD_MANIFEST" >/dev/null
    with_timeout 300 kubectl -n "$ARGOCD_NS" rollout status deploy/litecd --timeout 5m >/dev/null
}

target_uninstall() {
    k delete -n "$ARGOCD_NS" -f "$LITECD_MANIFEST" --wait >/dev/null
}

target_version() {
    echo "litecd $(k -n "$ARGOCD_NS" get deploy litecd -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://')"
}
