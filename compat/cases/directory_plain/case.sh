desc="Plain YAML directory, automated sync: Synced + Healthy, tracking metadata on every resource"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_rev directory-plain
    wait_app directory-plain Synced Healthy
    wait_op directory-plain Succeeded
    expect_eq "$(k -n "$NS" get deploy web -o jsonpath='{.status.readyReplicas}')" 1
    snapshot synced directory-plain
}
