desc="Jsonnet files in a directory source: extVars and top-level arguments (tlas), next to plain YAML"
phase=2
t_case() {
    repo_init
    app_apply app.yaml
    wait_app jsonnet Synced Healthy
    wait_op jsonnet Succeeded
    expect_eq "$(k -n "$NS" get configmap generated-cm -o jsonpath='{.data.count} {.data.sum}')" "3 6"
    k -n "$NS" get configmap plain >/dev/null
    snapshot synced jsonnet
}
