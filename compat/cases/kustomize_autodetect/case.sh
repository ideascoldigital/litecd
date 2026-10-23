desc="Kustomize overlay detected by kustomization.yaml: namePrefix, labels, generated ConfigMap with hash suffix"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app kustomize-autodetect Synced Healthy
    wait_op kustomize-autodetect Succeeded
    k -n "$NS" get deploy prod-web >/dev/null
    expect_eq "$(app_get kustomize-autodetect .status.sourceType)" Kustomize
    snapshot synced kustomize-autodetect
}
