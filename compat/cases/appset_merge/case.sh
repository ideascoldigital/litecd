desc="ApplicationSet merge generator: a second list overrides the first by mergeKeys"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    wait_app merge-a Synced Healthy
    wait_app merge-b Synced Healthy
    expect_eq "$(k -n "$ARGOCD_NS" get application merge-a -o jsonpath='{.metadata.annotations.compat-env}')" override
    snapshot --appset merge generated
}
