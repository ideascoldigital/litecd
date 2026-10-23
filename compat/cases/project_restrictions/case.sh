desc="AppProject restrictions: a source repository the project does not permit is an InvalidSpecError, nothing synced"
phase=1
_has_condition() { [[ "$(app_get project-restrictions '.status.conditions[*].type')" == *InvalidSpecError* ]]; }
t_case() {
    k create namespace "$NS" >/dev/null
    repo_init
    app_apply project.yaml
    app_apply app.yaml
    wait_for 120 _has_condition || { app_describe project-restrictions >&2; return 1; }
    expect_eq "$(k -n "$NS" get configmap settings -o name --ignore-not-found)" ""
    snapshot rejected project-restrictions
}
