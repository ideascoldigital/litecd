desc="Sync waves -1, 0, 1, 5: each wave created after the previous one is healthy (Argo CD waits 2 s between waves)"
phase=1
ts() { python3 -c 'import sys,datetime; print(int(datetime.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00")).timestamp()))' \
    "$(k -n "$NS" get "$1" -o jsonpath='{.metadata.creationTimestamp}')"; }
t_case() {
    repo_init
    app_apply app.yaml
    wait_app sync-waves Synced Healthy
    wait_op sync-waves Succeeded
    local a b c d
    a=$(ts configmap/first) b=$(ts deploy/web) c=$(ts service/web) d=$(ts configmap/last)
    echo "created at: first $a, web $b, service $c, last $d"
    ((a < b && b < c && c < d)) || { echo "waves out of order" >&2; return 1; }
    snapshot synced sync-waves
}
