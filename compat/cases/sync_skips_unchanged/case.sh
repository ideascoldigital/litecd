desc="A sync leaves resources already in sync as they are (kubectl apply: unchanged, no write)"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app skips-unchanged Synced Healthy
    wait_op skips-unchanged Succeeded
    local rv
    rv=$(k -n "$NS" get configmap b -o jsonpath='{.metadata.resourceVersion}')
    cp "$CASE_DIR/a-two.yaml" "$(repo_dir)/m/a.yaml"
    repo_commit "change a"
    app_refresh skips-unchanged
    wait_rev skips-unchanged
    wait_app skips-unchanged Synced Healthy
    wait_op skips-unchanged Succeeded
    expect_eq "$(k -n "$NS" get configmap a -o jsonpath='{.data.key}')" two
    expect_eq "$(k -n "$NS" get configmap b -o jsonpath='{.metadata.resourceVersion}')" "$rv"
    snapshot synced skips-unchanged
}
