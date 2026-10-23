# Target argocd: the baseline. Argo CD from the official chart, defaults
# except Dex and notifications (not litecd's scope). Pinned like litek8s's
# Argo CD bench: chart 10.10.5 = Argo CD v3.5.5.
ARGOCD_CHART_VERSION=${ARGOCD_CHART_VERSION:-10.10.5}

target_install() {
    with_timeout 600 helm upgrade --install argocd argo-cd --repo https://argoproj.github.io/argo-helm \
        --version "$ARGOCD_CHART_VERSION" --namespace "$ARGOCD_NS" --create-namespace --wait --timeout 9m \
        --set dex.enabled=false --set notifications.enabled=false >/dev/null
}

target_uninstall() {
    with_timeout 300 helm -n "$ARGOCD_NS" uninstall argocd --wait >/dev/null
}

target_version() {
    echo "Argo CD $(k -n "$ARGOCD_NS" get statefulset argocd-application-controller \
        -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://') (chart $ARGOCD_CHART_VERSION)"
}
