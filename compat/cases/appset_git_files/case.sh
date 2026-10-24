desc="ApplicationSet git files generator: parameters from JSON files (nested keys, path, filename)"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    wait_app files-alpha Synced Healthy
    wait_app files-beta Synced Healthy
    snapshot --appset files generated
}
