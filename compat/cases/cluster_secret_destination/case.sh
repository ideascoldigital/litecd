desc="Destinations in another cluster from a cluster Secret: by name and by server, with its bearer token"
phase=3
t_case() {
    cluster_secret remote https://kubernetes.default.svc.cluster.local
    repo_init
    app_apply app.yaml
    wait_app remote-by-name Synced Healthy
    wait_app remote-by-server Synced Healthy
    wait_op remote-by-name Succeeded
    wait_op remote-by-server Succeeded
    k -n "$NS" get configmap by-name by-server >/dev/null
    snapshot synced remote-by-name remote-by-server
}
