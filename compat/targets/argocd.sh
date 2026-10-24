# Target argocd: the baseline. Argo CD from the official chart, defaults
# except Dex and notifications (not litecd's scope) and the namespaces whose
# Applications it reconciles besides its own (APP_NAMESPACES, see lib.sh). Pinned like litek8s's
# Argo CD bench: chart 10.10.5 = Argo CD v3.5.5.
ARGOCD_CHART_VERSION=${ARGOCD_CHART_VERSION:-10.10.5}

# ARGOCD_CHART: a downloaded chart (.tgz) instead of the Helm repository,
# whose index is large and slow to fetch on a poor connection.
target_install() {
    local chart=(argo-cd --repo https://argoproj.github.io/argo-helm --version "$ARGOCD_CHART_VERSION")
    [[ -n ${ARGOCD_CHART:-} ]] && chart=("$ARGOCD_CHART")
    with_timeout 600 helm upgrade --install argocd "${chart[@]}" --namespace "$ARGOCD_NS" --create-namespace --wait --timeout 9m \
        --set dex.enabled=false --set notifications.enabled=false \
        --set-string "configs.params.application\.namespaces=$APP_NAMESPACES" >/dev/null
}

target_uninstall() {
    with_timeout 300 helm -n "$ARGOCD_NS" uninstall argocd --wait >/dev/null
}

target_version() {
    echo "Argo CD $(k -n "$ARGOCD_NS" get statefulset argocd-application-controller \
        -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://') (chart $ARGOCD_CHART_VERSION)"
}
