desc="ApplicationSet matrix generator: git directories times a list"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    for a in matrix-x-dev matrix-x-prod matrix-y-dev matrix-y-prod; do wait_app "$a" Synced Healthy; done
    snapshot --appset matrix generated
}
