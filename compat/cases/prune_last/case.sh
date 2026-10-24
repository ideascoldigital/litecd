desc="PruneLast=true: a resource removed from git is pruned after every wave of the sync"
phase=2
t_case() {
    repo_init
    app_apply app.yaml
    wait_app prune-last Synced Healthy
    wait_op prune-last Succeeded
    rm "$(repo_dir)/m/gone.yaml"
    cp "$CASE_DIR/b-two.yaml" "$(repo_dir)/m/b.yaml"
    repo_commit "remove one, change the wave-1 one"
    app_refresh prune-last
    # Wave 1 (b) runs 2 s after wave 0: with PruneLast, gone is still
    # there when b has changed.
    local seen=""
    for _ in $(seq 240); do
        if [[ $(kubectl -n "$NS" get configmap b -o jsonpath='{.data.key}' 2>/dev/null) == two ]]; then
            seen=$(kubectl -n "$NS" get configmap gone -o name --ignore-not-found 2>/dev/null)
            break
        fi
        sleep 0.1
    done
    expect_eq "$seen" "configmap/gone"
    wait_rev prune-last
    wait_app prune-last Synced Healthy
    wait_op prune-last Succeeded
    expect_eq "$(k -n "$NS" get configmap gone -o name --ignore-not-found)" ""
    # Right after the prune Argo CD's cache still has the pruned object: it
    # self-heals at once with nothing to do, every time, so its last
    # operation is that empty one. Not compared; the prune is checked above.
    snapshot --skip-operation-state pruned prune-last
}
