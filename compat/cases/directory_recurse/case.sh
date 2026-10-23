desc="Directory with recurse, include (YAML and JSON) and exclude"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app directory-recurse Synced Healthy
    wait_op directory-recurse Succeeded
    k -n "$NS" get configmap top nested from-json >/dev/null
    expect_eq "$(k -n "$NS" get configmap skipped -o name --ignore-not-found)" ""
    snapshot synced directory-recurse
}
