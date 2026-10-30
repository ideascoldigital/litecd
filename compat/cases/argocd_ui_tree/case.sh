desc="DEPRECATED argocd-ui-compat: the resource tree Argo CD's web UI reads from Redis (Deployment → ReplicaSet → Pods, a Job's pod) equals the one Argo CD's controller writes; litecd refuses a cache of another format and deletes a gone app's tree"
phase=1
timeout=420

# The tree's nodes: Deployment, ReplicaSet, its pods, Service, ConfigMap,
# Job and its pod.
nodes() { echo $((6 + $1)); }

redis_cli() { # args...: redis-cli in the argocd-redis pod, with the password
    local pw
    pw=$(k -n "$ARGOCD_NS" get secret argocd-redis -o jsonpath='{.data.auth}' | base64 -d)
    k -n "$ARGOCD_NS" exec deploy/argocd-redis -- env REDISCLI_AUTH="$pw" redis-cli "$@"
}

_logs_have() { kubectl -n "$ARGOCD_NS" logs deploy/litecd --tail=-1 | grep -q -- "$1"; }

# litecd only: a Redis named as Argo CD's (Deployment and Service
# argocd-redis, Secret argocd-redis key auth) and litecd started with the
# compatibility mode. There is no argocd-server, so the cache version is
# given.
ui_compat_on() {
    if k -n "$ARGOCD_NS" get deploy argocd-redis >/dev/null 2>&1; then
        echo "a Deployment argocd-redis is already in $ARGOCD_NS (Argo CD installed?)" >&2
        return 1
    fi
    k -n "$ARGOCD_NS" get secret argocd-redis >/dev/null 2>&1 ||
        k -n "$ARGOCD_NS" create secret generic argocd-redis --from-literal=auth="compat-$RANDOM$RANDOM" >/dev/null
    k -n "$ARGOCD_NS" apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: argocd-redis
  labels: {compat.litecd.dev/case: $CASE_ID}
spec:
  selector: {matchLabels: {app.kubernetes.io/name: argocd-redis}}
  template:
    metadata: {labels: {app.kubernetes.io/name: argocd-redis}}
    spec:
      containers:
        - name: redis
          image: public.ecr.aws/docker/library/redis:7.4-alpine
          args: ["sh", "-c", 'exec redis-server --save "" --appendonly no --requirepass "\$AUTH"']
          env: [{name: AUTH, valueFrom: {secretKeyRef: {name: argocd-redis, key: auth}}}]
          ports: [{containerPort: 6379}]
---
apiVersion: v1
kind: Service
metadata:
  name: argocd-redis
  labels: {compat.litecd.dev/case: $CASE_ID}
spec:
  selector: {app.kubernetes.io/name: argocd-redis}
  ports: [{port: 6379}]
YAML
    with_timeout 180 kubectl -n "$ARGOCD_NS" rollout status deploy/argocd-redis --timeout 3m >/dev/null
    # A tree of another cache version: litecd must refuse to write.
    redis_cli SET 'app|resources-tree|other|1.8.2.gz' x >/dev/null
    k -n "$ARGOCD_NS" set env deploy/litecd \
        LITECD_ARGOCD_UI_CACHE=redis://argocd-redis:6379 LITECD_ARGOCD_UI_CACHE_VERSION=1.8.3 >/dev/null
    with_timeout 180 kubectl -n "$ARGOCD_NS" rollout status deploy/litecd --timeout 3m >/dev/null
    if wait_for 30 _logs_have "no argocd-ui-compat feature"; then
        echo "litecd was built without the argocd-ui-compat feature (LITECD_FEATURES=argocd-ui-compat deploy/build.sh)" >&2
        return 1
    fi
    wait_for 30 _logs_have "DEPRECATED compatibility mode"
    wait_for 60 _logs_have "not writing resource trees: Redis holds app|resources-tree|other|1.8.2.gz"
    redis_cli DEL 'app|resources-tree|other|1.8.2.gz' >/dev/null
    k -n "$ARGOCD_NS" rollout restart deploy/litecd >/dev/null
    with_timeout 180 kubectl -n "$ARGOCD_NS" rollout status deploy/litecd --timeout 3m >/dev/null
    wait_for 60 _logs_have "writing trees as app|resources-tree|<app>|1.8.3.gz"
}

ui_compat_off() {
    k -n "$ARGOCD_NS" set env deploy/litecd LITECD_ARGOCD_UI_CACHE- LITECD_ARGOCD_UI_CACHE_VERSION- >/dev/null
    k -n "$ARGOCD_NS" delete deploy,service -l compat.litecd.dev/case="$CASE_ID" --wait=false >/dev/null
    with_timeout 180 kubectl -n "$ARGOCD_NS" rollout status deploy/litecd --timeout 3m >/dev/null
}

_no_tree() { [[ -z "$(redis_cli --raw EXISTS 'app|resources-tree|argocd-ui-tree|1.8.3.gz' | grep -v '^0$')" ]]; }

t_case() {
    if [[ $target == litecd ]]; then
        trap ui_compat_off EXIT
        ui_compat_on
    fi
    repo_init
    app_apply app.yaml
    wait_app argocd-ui-tree Synced Healthy 180
    python3 "$COMPAT_DIR/ui_tree.py" --app argocd-ui-tree --label two-replicas --ns "$NS" \
        --argocd-ns "$ARGOCD_NS" --nodes "$(nodes 2)" >>"$SNAPSHOTS"

    # One replica: the tree follows the pods.
    perl -pi -e 's/replicas: 2/replicas: 1/' "$(repo_dir)/m/web.yaml"
    repo_commit "one replica"
    app_refresh argocd-ui-tree
    wait_rev argocd-ui-tree
    python3 "$COMPAT_DIR/ui_tree.py" --app argocd-ui-tree --label one-replica --ns "$NS" \
        --argocd-ns "$ARGOCD_NS" --nodes "$(nodes 1)" >>"$SNAPSHOTS"

    if [[ $target == litecd ]]; then
        # A deleted app's tree goes with it.
        k -n "$ARGOCD_NS" delete application argocd-ui-tree --wait=false >/dev/null
        wait_for 120 _no_tree
    fi
}
