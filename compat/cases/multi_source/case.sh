desc='Two sources: a chart from the Helm repository with a values file from git ($values ref)'
phase=2
t_case() {
    repo_init
    publish_chart chart
    app_apply app.yaml
    wait_app multi-source Synced Healthy
    wait_op multi-source Succeeded
    expect_eq "$(k -n "$NS" get configmap multi-source-config -o jsonpath='{.data.message}')" from-git-values-file
    snapshot synced multi-source
}
