desc="ignoreDifferences on /spec/replicas: a scaled Deployment stays scaled and the app stays Synced"
phase=2
t_case() {
    repo_init
    app_apply app.yaml
    wait_app ignore-differences Synced Healthy
    wait_op ignore-differences Succeeded
    k -n "$NS" scale deploy web --replicas 2 >/dev/null
    app_refresh ignore-differences
    sleep 15
    expect_eq "$(k -n "$NS" get deploy web -o jsonpath='{.spec.replicas}')" 2
    wait_app ignore-differences Synced Healthy
    snapshot scaled ignore-differences
}
