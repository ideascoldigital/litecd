desc="PreSync and PostSync hook Jobs run around the sync and are deleted when they succeed (HookSucceeded)"
phase=2
t_case() {
    repo_init
    app_apply app.yaml
    wait_op hooks Succeeded 180
    wait_app hooks Synced Healthy
    wait_for 60 sh -c "[ -z \"\$(kubectl -n $NS get jobs -o name)\" ]"
    snapshot synced hooks
}
