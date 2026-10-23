desc="Automated sync without prune: a removed file leaves the app OutOfSync with the resource kept"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app no-prune Synced Healthy
    wait_op no-prune Succeeded
    rm "$(repo_dir)/m/removed.yaml"
    repo_commit "remove a ConfigMap"
    app_refresh no-prune
    wait_rev no-prune
    wait_app no-prune OutOfSync Healthy
    k -n "$NS" get configmap removed >/dev/null
    snapshot out-of-sync no-prune
}
