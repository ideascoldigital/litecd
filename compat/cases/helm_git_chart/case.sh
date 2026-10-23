desc="Helm chart in a git path (detected by Chart.yaml): releaseName, valuesObject, parameters win over values"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app helm-git-chart Synced Healthy
    wait_op helm-git-chart Succeeded
    expect_eq "$(k -n "$NS" get configmap demo-config -o jsonpath='{.data.message}')" from-parameter
    expect_eq "$(k -n "$NS" get configmap demo-config -o jsonpath='{.data.extra}')" from-values-object
    expect_eq "$(k -n "$NS" get configmap demo-config -o jsonpath='{.data.namespace}')" "$NS"
    expect_eq "$(k -n "$NS" get deploy demo-web -o jsonpath='{.spec.replicas}')" 2
    expect_eq "$(app_get helm-git-chart .status.sourceType)" Helm
    snapshot synced helm-git-chart
}
