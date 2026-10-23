desc="Lua health for Applications from argocd-cm: the app of apps waits on a child that never gets healthy"
phase=1
t_case() {
    k -n "$ARGOCD_NS" patch configmap argocd-cm --type merge --patch-file "$CASE_DIR/argocd-cm-patch.yaml" >/dev/null
    trap 'kubectl -n "$ARGOCD_NS" patch configmap argocd-cm --type json \
        -p "[{\"op\":\"remove\",\"path\":\"/data/resource.customizations.health.argoproj.io_Application\"}]" >/dev/null' EXIT
    sleep 5 # Argo CD reloads argocd-cm from its watch
    repo_init
    app_apply app.yaml
    wait_app lua-health-a Synced Healthy
    wait_app lua-health-b Synced Progressing
    sleep 20
    # Wave 2 must not have started: its wave-1 sibling is not healthy.
    expect_eq "$(k -n "$ARGOCD_NS" get application lua-health-c -o name --ignore-not-found)" ""
    expect_eq "$(app_get lua-health .status.operationState.phase)" Running
    snapshot waiting lua-health lua-health-a lua-health-b
}
