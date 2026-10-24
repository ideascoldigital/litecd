desc="Sync windows: active deny and inactive allow windows block automated sync, matched by app, namespace or cluster (and AND); a manual sync waits blocked unless manualSync; a window removed lets both through"
phase=3
timeout=300
_blocked() { [[ "$(app_get "$1" .status.operationState.message)" == "Sync operation blocked by sync window" ]]; }
t_case() {
    repo_init
    app_apply project.yaml
    app_apply apps.yaml
    local a
    for a in open and-miss; do
        wait_app $a Synced Healthy
        wait_op $a Succeeded
    done
    for a in denied-auto manual-ok closed by-ns and-match denied-manual; do
        wait_rev $a
        wait_app $a OutOfSync Missing
    done
    snapshot windows open and-miss denied-auto manual-ok closed by-ns and-match denied-manual

    app_sync denied-manual
    wait_for 60 _blocked denied-manual || { app_describe denied-manual >&2; return 1; }
    app_sync manual-ok
    wait_op manual-ok Succeeded
    wait_app manual-ok Synced Healthy
    snapshot manual denied-manual manual-ok

    # The window over denied-* goes: refreshed (Argo CD does not refresh
    # apps when their project changes), the automated app syncs and the
    # blocked manual sync runs.
    k -n "$ARGOCD_NS" patch appproject compat-windows --type json -p '[{"op":"remove","path":"/spec/syncWindows/0"}]' >/dev/null
    app_refresh denied-auto
    app_refresh denied-manual
    wait_app denied-auto Synced Healthy
    wait_op denied-auto Succeeded
    wait_op denied-manual Succeeded
    wait_app denied-manual Synced Healthy
    snapshot opened denied-auto denied-manual
}
