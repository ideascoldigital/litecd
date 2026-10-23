desc="Helm chart from a Helm repository (index.yaml over HTTP), pinned version, values as a string"
phase=1
t_case() {
    publish_chart chart
    app_apply app.yaml
    wait_app helm-repo-chart Synced Healthy
    wait_op helm-repo-chart Succeeded
    expect_eq "$(k -n "$NS" get configmap helm-repo-chart-config -o jsonpath='{.data.message}')" from-values-string
    snapshot synced helm-repo-chart
}
