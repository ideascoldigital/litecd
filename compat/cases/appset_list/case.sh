desc="ApplicationSet list generator: an Application per element; an element removed, its Application deleted"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    wait_app list-a Synced Healthy
    wait_app list-b Synced Healthy
    snapshot --appset list generated
    app_apply appset-one.yaml
    wait_for 120 sh -c "! kubectl -n $ARGOCD_NS get application list-b >/dev/null 2>&1"
    wait_app list-a Synced Healthy
    snapshot --appset list removed
}
