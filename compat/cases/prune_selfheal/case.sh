desc="A file removed from git is pruned; a deleted and a scaled Deployment are healed back"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app prune-selfheal Synced Healthy
    wait_op prune-selfheal Succeeded
    snapshot synced prune-selfheal

    rm "$(repo_dir)/m/removed.yaml"
    repo_commit "remove a ConfigMap"
    app_refresh prune-selfheal
    wait_rev prune-selfheal
    wait_for 60 sh -c "[ -z \"\$(kubectl -n $NS get configmap removed -o name --ignore-not-found)\" ]"
    wait_app prune-selfheal Synced Healthy
    wait_op prune-selfheal Succeeded
    snapshot pruned prune-selfheal

    k -n "$NS" delete deploy web --wait=true >/dev/null
    wait_for 60 kubectl -n "$NS" get deploy web
    k -n "$NS" scale deploy web --replicas 3 >/dev/null
    wait_for 60 sh -c "[ \"\$(kubectl -n $NS get deploy web -o jsonpath='{.spec.replicas}')\" = 1 ]"
    wait_app prune-selfheal Synced Healthy
    wait_op prune-selfheal Succeeded
}
