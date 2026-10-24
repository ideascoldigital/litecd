desc="Applications outside Argo CD's namespace: tracked as <ns>_<app>, healed and finalized; refused when the project lists no such source namespace; ignored in a namespace not enabled"
phase=3
_not_permitted() { [[ -n "$(APP_NS=$NS-apps app_get not-permitted '.status.conditions[*].type')" ]]; }
t_case() {
    k create namespace "$NS-apps" >/dev/null
    k create namespace "$NS-other" >/dev/null
    repo_init
    app_apply project.yaml
    APP_NS=$NS-apps app_apply apps.yaml
    APP_NS=$NS-other app_apply other.yaml
    APP_NS=$NS-apps wait_app any-ns Synced Healthy
    APP_NS=$NS-apps wait_op any-ns Succeeded
    wait_for 120 _not_permitted || { APP_NS=$NS-apps app_describe not-permitted >&2; return 1; }
    APP_NS=$NS-apps snapshot synced any-ns not-permitted
    APP_NS=$NS-other snapshot ignored not-enabled

    k -n "$NS" delete configmap cfg --wait=true >/dev/null
    wait_for 60 kubectl -n "$NS" get configmap cfg
    APP_NS=$NS-apps wait_app any-ns Synced Healthy
    APP_NS=$NS-apps snapshot healed any-ns

    k -n "$NS-apps" delete application any-ns --wait=false >/dev/null
    wait_for 120 sh -c "[ -z \"\$(kubectl -n $NS-apps get application any-ns -o name --ignore-not-found)\" ]"
    expect_eq "$(k -n "$NS" get configmap cfg -o name --ignore-not-found)" ""
}
