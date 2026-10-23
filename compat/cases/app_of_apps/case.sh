desc="App of apps: a parent Application syncs child Applications (waves 0 and 1) from git"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app app-of-apps Synced Healthy
    wait_op app-of-apps Succeeded
    wait_app app-of-apps-a Synced Healthy
    wait_app app-of-apps-b Synced Healthy
    wait_op app-of-apps-b Succeeded
    snapshot synced app-of-apps app-of-apps-a app-of-apps-b
}
