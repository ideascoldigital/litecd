desc="A resource another app already manages: a shared-resource condition, and with FailOnSharedResource=true the sync fails"
phase=2
t_case() {
    repo_init
    app_apply first.yaml
    wait_app shared-first Synced Healthy
    wait_op shared-first Succeeded
    app_apply second.yaml
    wait_op shared-second Failed 120
    # The first app still owns it.
    expect_eq "$(k -n "$NS" get configmap shared -o jsonpath='{.metadata.annotations.argocd\.argoproj\.io/tracking-id}')" "shared-first:/ConfigMap:$NS/shared"
    snapshot failed shared-first shared-second
}
