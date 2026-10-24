#!/usr/bin/env bash
# litecd's compatibility corpus: every case against the GitOps controller in
# the current kubeconfig's cluster, compared with what Argo CD does.
#
#   compat/run.sh --target argocd --update-golden   # baseline: write golden/
#   compat/run.sh --target argocd                   # baseline is reproducible
#   compat/run.sh --target litecd                   # litecd must match golden/
#   compat/run.sh --only directory_plain,no_prune   # some cases (comma-separated)
#   compat/run.sh --phases 1                        # cases of litecd's phase 1 only
#   compat/run.sh --no-install                      # target already installed
#   compat/run.sh --uninstall                       # remove the target after the run
#
# Needs kubectl, helm, git, perl, python3. A case passes when its own checks
# pass and its snapshots equal golden/<case>.json. Results:
# compat/results/<timestamp>-<target>/{results.jsonl,summary.md,logs/,snapshots/}
set -uo pipefail
set -m # background jobs get their own process group, so a timeout kills kubectl too

COMPAT_DIR=$(cd "$(dirname "$0")" && pwd)
export COMPAT_DIR
source "$COMPAT_DIR/lib.sh"

target=""
only=""
phases=""
update=""
install=1
uninstall=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --target) target=$2; shift 2 ;;
        --only) only=$2; shift 2 ;;
        --phases) phases=$2; shift 2 ;;
        --update-golden) update=--update; shift ;;
        --no-install) install=""; shift ;;
        --uninstall) uninstall=1; shift ;;
        -h | --help) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
[[ -f "$COMPAT_DIR/targets/$target.sh" ]] || { echo "--target argocd|litecd" >&2; exit 2; }
if [[ -n "$update" && "$target" != argocd ]]; then
    echo "golden files come from Argo CD: --update-golden needs --target argocd" >&2
    exit 2
fi
for tool in kubectl helm git perl python3; do
    command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }
done
source "$COMPAT_DIR/targets/$target.sh"

out="$COMPAT_DIR/results/$(date +%Y%m%d-%H%M%S)-$target"
mkdir -p "$out/logs" "$out/snapshots"
WORK=$(mktemp -d)
export WORK
pf_pid=""
cleanup() {
    [[ -n "$pf_pid" ]] && kill "$pf_pid" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "cluster: $(kubectl config current-context) ($(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'))"

# The git server.
k apply -f "$COMPAT_DIR/git/git-server.yaml" >/dev/null
k -n "$GIT_NS" create configmap git-server --from-file="$COMPAT_DIR/git/server.py" --dry-run=client -o yaml | k apply -f - >/dev/null
# Restarted only when server.py changes: its pod installs git at start.
sum=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest()[:16])' "$COMPAT_DIR/git/server.py")
k -n "$GIT_NS" patch deploy git --type merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"compat.litecd.dev/server\":\"$sum\"}}}}}" >/dev/null
with_timeout 300 kubectl -n "$GIT_NS" rollout status deploy/git --timeout 5m >/dev/null || { echo "git server not ready" >&2; exit 1; }
kubectl -n "$GIT_NS" port-forward svc/git :80 >"$WORK/pf.log" 2>&1 &
pf_pid=$!
for _ in $(seq 50); do
    port=$(sed -n 's/^Forwarding from 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/pf.log" | head -1)
    [[ -n "$port" ]] && break
    sleep 0.2
done
[[ -n "${port:-}" ]] || { echo "port-forward to the git server failed: $(cat "$WORK/pf.log")" >&2; exit 1; }
export GIT_LOCAL=http://127.0.0.1:$port/git
export CHART_REPO=$GIT_SVC/charts
echo "git server: ready"

if [[ -n "$install" ]]; then
    target_install || { echo "installing $target failed" >&2; exit 1; }
fi
version=$(target_version)
echo "target: $version"
echo "results: $out"
echo

results="$out/results.jsonl"
: >"$results"
record() { # id phase status seconds detail
    python3 -c 'import json,sys; print(json.dumps(dict(id=sys.argv[1],phase=int(sys.argv[2]),status=sys.argv[3],seconds=int(sys.argv[4]),detail=sys.argv[5])))' \
        "$@" >>"$results"
}

# What a case leaves: its Applications (with their resources: the cases use
# the resources finalizer), AppProjects, Secrets, then its namespaces.
case_cleanup() {
    local id=$1 ns=$2
    # ApplicationSets, then Applications (one whose AppProject is gone
    # cannot be finalized).
    k -n "$ARGOCD_NS" delete applicationsets -l compat.litecd.dev/case="$id" --wait=false >/dev/null 2>&1
    # Applications in any namespace (apps_any_namespace's are in its own).
    k delete applications -A -l compat.litecd.dev/case="$id" --wait=false >/dev/null 2>&1
    if ! wait_for 120 sh -c "[ -z \"\$(kubectl get applications -A -l compat.litecd.dev/case=$id -o name)\" ]" 2>/dev/null; then
        # A controller that never finalizes them must not block the next case.
        kubectl get applications -A -l compat.litecd.dev/case="$id" \
            -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' |
            while read -r n a; do
                k -n "$n" patch application "$a" --type merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1
            done
    fi
    k -n "$ARGOCD_NS" delete appprojects,secrets -l compat.litecd.dev/case="$id" --wait=false >/dev/null 2>&1
    for n in $(kubectl get namespaces -o name | sed 's#namespace/##' | grep -E "^$ns(-|$)"); do
        k delete namespace "$n" --wait=false >/dev/null 2>&1
    done
}

for dir in "$COMPAT_DIR"/cases/*/; do
    id=$(basename "$dir")
    [[ -n "$only" && ",$only," != *",$id,"* ]] && continue
    desc="" timeout=180 phase=1
    set +u; source "$dir/case.sh"; set -u
    [[ -n "$phases" && ",$phases," != *",$phase,"* ]] && continue
    log="$out/logs/$id.log"
    snaps="$out/snapshots/$id.jsonl"
    : >"$snaps"
    ns="compat-${id//_/-}"
    start=$SECONDS
    (
        set -eo pipefail
        export CASE_ID=$id CASE_DIR=${dir%/} NS=$ns SNAPSHOTS=$snaps
        export REPO_URL=$GIT_SVC/git/$id.git
        t_case
    ) >"$log" 2>&1 &
    pid=$!
    status=""
    while kill -0 "$pid" 2>/dev/null; do
        if ((SECONDS - start >= timeout)); then
            kill -TERM -- "-$pid" 2>/dev/null
            sleep 1
            kill -KILL -- "-$pid" 2>/dev/null
            status=timeout
            echo "timed out after ${timeout}s" >>"$log"
            break
        fi
        sleep 0.2
    done
    wait "$pid" 2>/dev/null
    rc=$?
    if [[ -z "$status" ]]; then
        if ((rc != 0)); then
            status=fail
        elif python3 "$COMPAT_DIR/compare.py" "$snaps" "$COMPAT_DIR/golden/$id.json" $update 2>>"$log"; then
            status=pass
        else
            status=differs
        fi
    fi
    elapsed=$((SECONDS - start))
    detail=$(grep -v '^[[:space:]]*$' "$log" | tail -1 | cut -c1-300)
    record "$id" "$phase" "$status" "$elapsed" "$detail"
    printf '%-24s p%s %-8s %4ss  %s\n' "$id" "$phase" "$status" "$elapsed" "$([[ $status == pass ]] || echo "$detail" | cut -c1-100)"
    case_cleanup "$id" "$ns"
done

[[ -n "$uninstall" ]] && target_uninstall

python3 - "$results" "$version" >"$out/summary.md" <<'EOF'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
passed = sum(r["status"] == "pass" for r in rows)
print(f"# {sys.argv[2]}: {passed}/{len(rows)} cases pass\n")
print("| case | phase | status | seconds | detail |\n|---|---|---|---|---|")
for r in rows:
    d = "" if r["status"] == "pass" else r["detail"].replace("|", "\\|")[:120]
    print(f"| {r['id']} | {r['phase']} | {r['status']} | {r['seconds']} | {d} |")
EOF
echo
head -1 "$out/summary.md"
