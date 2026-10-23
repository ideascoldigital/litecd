desc="A resource the API server rejects: the sync fails after 2 retries, the error is in the operation state"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_op sync-failure-retry Failed 120
    expect_eq "$(app_get sync-failure-retry .status.operationState.retryCount)" 2
    snapshot failed sync-failure-retry
}
