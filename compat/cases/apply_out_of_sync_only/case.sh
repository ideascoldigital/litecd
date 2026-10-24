desc="ApplyOutOfSyncOnly=true: a sync applies only what is out of sync; the rest is not written"
phase=2
t_case() {
    repo_init
    app_apply app.yaml
    wait_app out-of-sync-only Synced Healthy
    wait_op out-of-sync-only Succeeded
    local rv
    rv=$(k -n "$NS" get configmap b -o jsonpath='{.metadata.resourceVersion}')
    cp "$CASE_DIR/a-two.yaml" "$(repo_dir)/m/a.yaml"
    repo_commit "change a"
    app_refresh out-of-sync-only
    wait_rev out-of-sync-only
    wait_app out-of-sync-only Synced Healthy
    wait_op out-of-sync-only Succeeded
    expect_eq "$(k -n "$NS" get configmap a -o jsonpath='{.data.key}')" two
    expect_eq "$(k -n "$NS" get configmap b -o jsonpath='{.metadata.resourceVersion}')" "$rv"
    snapshot synced out-of-sync-only
}
