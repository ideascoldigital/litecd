desc="Coexistence: while Argo CD's application controller runs (a stand-in StatefulSet), only apps annotated litecd.dev/manage are touched; litecd takes over when it goes, stands down when it comes back, and leaves a deleted app's finalizer to it"
phase=1
timeout=300
# litecd only (the stand-in would take Argo CD's own StatefulSet's place),
# and no golden file: the checks below are the whole case.
targets=litecd
golden=0

STS=argocd-application-controller

# The stand-in: a StatefulSet named as Argo CD's controller, running pause.
stand_in() { # replicas
    k -n "$ARGOCD_NS" apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: $STS
  labels: {app.kubernetes.io/name: $STS, compat.litecd.dev/case: $CASE_ID}
spec:
  replicas: $1
  serviceName: $STS
  selector: {matchLabels: {app.kubernetes.io/name: $STS}}
  template:
    metadata: {labels: {app.kubernetes.io/name: $STS}}
    spec:
      terminationGracePeriodSeconds: 0
      containers: [{name: pause, image: registry.k8s.io/pause:3.10}]
YAML
}
_sts_up() { [[ "$(k -n "$ARGOCD_NS" get sts $STS -o jsonpath='{.status.readyReplicas}')" == 1 ]]; }
# Scaled down and its pod gone (zero fields are left out of the status).
_sts_down() { [[ "$(k -n "$ARGOCD_NS" get sts $STS -o jsonpath='{.status.replicas}{.status.readyReplicas}')" =~ ^0*$ ]]; }
_no_status() { [[ -z "$(app_get "$1" .status)" ]]; }
# `! cmd` does not fail a `set -e` function: this does.
absent() {
    if "$@" >/dev/null 2>&1; then
        echo "expected to fail: $*" >&2
        return 1
    fi
}

t_case() {
    if k -n "$ARGOCD_NS" get sts $STS >/dev/null 2>&1 &&
        [[ -z "$(k -n "$ARGOCD_NS" get sts $STS -o jsonpath='{.metadata.labels.compat\.litecd\.dev/case}')" ]]; then
        echo "a $STS StatefulSet that is not this case's is in $ARGOCD_NS (Argo CD installed?)" >&2
        return 1
    fi
    trap 'k -n "$ARGOCD_NS" delete sts $STS --wait=false >/dev/null 2>&1' EXIT
    stand_in 1
    wait_for 120 _sts_up
    repo_init
    app_apply app.yaml

    # Argo CD's controller runs: the annotated app is litecd's, the other
    # is not touched at all (no status, no namespace, no resources).
    wait_rev coexist-managed
    wait_app coexist-managed Synced Healthy
    expect_eq "$(k -n "$NS" get cm settings -o jsonpath='{.data.key}')" one
    sleep 15
    _no_status coexist-left
    absent k -n "$ARGOCD_NS" get secret coexist-left.diff
    absent k get namespace "$NS-left"

    # Gone: litecd takes over.
    stand_in 0
    wait_for 60 _sts_down
    wait_app coexist-left Synced Healthy
    expect_eq "$(k -n "$NS-left" get cm settings -o jsonpath='{.data.key}')" one

    # Back: litecd stands down again; a push reaches only the annotated app.
    stand_in 1
    wait_for 120 _sts_up
    sleep 5
    local before
    before=$(app_get coexist-left .status.reconciledAt)
    perl -pi -e 's/key: one/key: two/' "$(repo_dir)/m/cm.yaml"
    repo_commit "two"
    app_refresh coexist-managed
    app_refresh coexist-left
    wait_rev coexist-managed
    wait_for 60 sh -c "[ \"\$(kubectl -n $NS get cm settings -o jsonpath='{.data.key}')\" = two ]"
    sleep 15
    expect_eq "$(k -n "$NS-left" get cm settings -o jsonpath='{.data.key}')" one
    expect_eq "$(app_get coexist-left .status.reconciledAt)" "$before"
    # The refresh annotation is Argo CD's to clear.
    expect_eq "$(app_get coexist-left '.metadata.annotations.argocd\.argoproj\.io/refresh')" normal

    # Deleted while it runs: the finalizer and the resources stay.
    k -n "$ARGOCD_NS" delete application coexist-left --wait=false >/dev/null
    sleep 10
    app_get coexist-left .metadata.deletionTimestamp | grep -q .
    k -n "$NS-left" get cm settings >/dev/null

    # Gone again: litecd finalizes it.
    stand_in 0
    wait_for 60 _sts_down
    wait_for 120 absent kubectl -n "$ARGOCD_NS" get application coexist-left
    absent k -n "$NS-left" get cm settings
}
