desc="ApplicationSet clusters generator with a selector: the labelled cluster Secret only, not the local cluster"
phase=3
t_case() {
    cluster_secret staging https://kubernetes.default.svc.cluster.local env=staging
    repo_init
    app_apply appset.yaml
    wait_app cs-staging Synced Healthy
    expect_eq "$(k -n "$ARGOCD_NS" get application cs-in-cluster -o name --ignore-not-found)" ""
    snapshot --appset clusters-secret generated cs-staging
}
