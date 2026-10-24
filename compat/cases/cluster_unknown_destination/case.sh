desc="A destination no cluster matches (name or server): Argo CD's condition, nothing synced"
phase=3
_cond() { [[ -n "$(app_get "$1" '.status.conditions[*].type')" ]]; }
t_case() {
    k create namespace "$NS" >/dev/null
    repo_init
    app_apply app.yaml
    wait_for 120 _cond unknown-by-name
    wait_for 120 _cond unknown-by-server
    expect_eq "$(k -n "$NS" get configmap never -o name --ignore-not-found)" ""
    snapshot rejected unknown-by-name unknown-by-server
}
