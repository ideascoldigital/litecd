desc="No automated sync: OutOfSync + Missing until an operation (as the argocd CLI/UI writes it) syncs"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_rev manual-sync
    wait_app manual-sync OutOfSync Missing
    snapshot before manual-sync
    app_sync manual-sync
    wait_op manual-sync Succeeded
    wait_app manual-sync Synced Healthy
    snapshot synced manual-sync
}
