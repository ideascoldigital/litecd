desc="Health from Argo CD's bundled Lua checks: a cert-manager Certificate Ready (Healthy) and not (Degraded)"
phase=2
t_case() {
    k apply -f "$CASE_DIR/certificate-crd.yaml" >/dev/null
    k wait --for=condition=Established crd/certificates.cert-manager.io --timeout=60s >/dev/null
    repo_init
    app_apply app.yaml
    wait_app lua-lib-ready Synced Healthy
    wait_app lua-lib-failed Synced Degraded
    wait_op lua-lib-ready Succeeded
    wait_op lua-lib-failed Succeeded
    snapshot synced lua-lib-ready lua-lib-failed
}
