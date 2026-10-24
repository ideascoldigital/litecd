# Target litecd: the build under test, from LITECD_MANIFEST (the
# directory deploy/install: Argo CD's CRDs, RBAC and the litecd Deployment).

target_install() {
    if [[ -z "${LITECD_MANIFEST:-}" ]]; then
        echo "litecd has no deployable build yet (phase 1): set LITECD_MANIFEST to a release YAML" >&2
        return 1
    fi
    k create namespace "$ARGOCD_NS" >/dev/null 2>&1 || true
    k apply --server-side --force-conflicts -R -f "$LITECD_MANIFEST" >/dev/null
    k -n "$ARGOCD_NS" patch configmap argocd-cmd-params-cm --type merge \
        -p "{\"data\":{\"application.namespaces\":\"$APP_NAMESPACES\"}}" >/dev/null
    with_timeout 300 kubectl -n "$ARGOCD_NS" rollout status deploy/litecd --timeout 5m >/dev/null
}

target_uninstall() {
    k -n "$ARGOCD_NS" delete deploy/litecd --wait >/dev/null
}

target_version() {
    echo "litecd $(k -n "$ARGOCD_NS" get deploy litecd -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://')"
}
