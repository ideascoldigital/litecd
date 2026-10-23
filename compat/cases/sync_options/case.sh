desc="Resource sync options: Prune=false keeps a removed resource, Replace=true replaces instead of applying"
phase=1
t_case() {
    repo_init
    app_apply app.yaml
    wait_app sync-options Synced Healthy
    wait_op sync-options Succeeded
    snapshot synced sync-options

    rm "$(repo_dir)/m/no-prune.yaml"
    sed -i.bak 's/key: one/key: two/' "$(repo_dir)/m/replaced.yaml" && rm "$(repo_dir)/m/replaced.yaml.bak"
    repo_commit "remove the Prune=false ConfigMap, change the replaced one"
    app_refresh sync-options
    wait_rev sync-options
    wait_for 60 sh -c "[ \"\$(kubectl -n $NS get configmap replaced -o jsonpath='{.data.key}')\" = two ]"
    wait_op sync-options Succeeded
    k -n "$NS" get configmap no-prune >/dev/null
    snapshot after-change sync-options
}
