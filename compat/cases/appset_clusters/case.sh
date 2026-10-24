desc="ApplicationSet clusters generator: the local cluster (in-cluster) and its values"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    wait_app clusters-in-cluster-local Synced Healthy
    snapshot --appset clusters generated
}
