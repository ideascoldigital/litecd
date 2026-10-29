desc="Cluster-scoped objects and a CRD: a ClusterRole tracked in the destination namespace, the CRD untracked; still Synced after a hard refresh"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_rev cluster-scoped
    wait_app cluster-scoped Synced Healthy
    wait_op cluster-scoped Succeeded
    expect_eq "$(k get crd widgets.compat.litecd.dev -o jsonpath='{.status.conditions[?(@.type=="Established")].status}')" True
    snapshot synced cluster-scoped
    # Compared again from scratch: what the controller wrote must match git.
    app_refresh cluster-scoped hard
    wait_app cluster-scoped Synced Healthy
    snapshot refreshed cluster-scoped
}
