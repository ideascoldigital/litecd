desc="ServerSideApply=true: resources applied with server-side apply, field manager argocd-controller"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app server-side-apply Synced Healthy
    wait_op server-side-apply Succeeded
    k -n "$NS" get configmap settings -o jsonpath='{.metadata.managedFields[*].manager}' | grep -q argocd-controller
    snapshot synced server-side-apply
}
