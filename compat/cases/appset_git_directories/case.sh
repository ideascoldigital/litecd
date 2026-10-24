desc="ApplicationSet git directories generator: one Application per directory, an excluded one left out, a new one picked up"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    wait_app dirs-one Synced Healthy
    wait_app dirs-two Synced Healthy
    expect_eq "$(k -n "$ARGOCD_NS" get application dirs-skip -o name --ignore-not-found)" ""
    snapshot --appset dirs generated
    mkdir -p "$(repo_dir)/apps/three"
    cp "$CASE_DIR/three-cm.yaml" "$(repo_dir)/apps/three/cm.yaml"
    repo_commit "a third app"
    wait_for 120 kubectl -n "$ARGOCD_NS" get application dirs-three
    wait_app dirs-three Synced Healthy
    snapshot --appset dirs added
}
