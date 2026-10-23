desc="A Job that fails makes the app Degraded; the sync itself succeeds"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_op degraded-health Succeeded
    wait_app degraded-health Synced Degraded 120
    snapshot degraded degraded-health
}
