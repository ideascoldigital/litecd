desc="Kustomize options of the app: namePrefix/Suffix, images, replicas, commonLabels/Annotations, patches"
phase=2
t_case() {
    repo_init
    app_apply app.yaml
    wait_app kustomize-options Synced Healthy
    wait_op kustomize-options Succeeded
    expect_eq "$(k -n "$NS" get deploy app-web-v1 -o jsonpath='{.spec.replicas} {.spec.template.spec.containers[0].image} {.metadata.labels.team}')" "2 registry.k8s.io/pause:3.9 compat"
    expect_eq "$(k -n "$NS" get configmap app-settings-v1 -o jsonpath='{.data.patched} {.metadata.annotations.owner}')" "yes corpus"
    snapshot synced kustomize-options
}
