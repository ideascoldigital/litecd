# Helpers for the corpus, sourced by run.sh before every case. A case is
# cases/<id>/case.sh defining `desc`, `timeout` and `t_case`; t_case runs in
# its own subshell with `set -e`, so the first failing command fails it.
#
# Set by run.sh for each case:
#   CASE_ID     the case directory's name
#   CASE_DIR    compat/cases/<id>
#   NS          compat-<id>: the destination namespace (created by the case
#               or by CreateNamespace=true; deleted after the case)
#   REPO_URL    the case's git repository as the cluster sees it
#   CHART_REPO  the Helm repository as the cluster sees it
#   WORK        a scratch directory for this run
#   SNAPSHOTS   the file snapshot appends to

ARGOCD_NS=${ARGOCD_NS:-argocd}
GIT_NS=litecd-compat
GIT_SVC=http://git.$GIT_NS.svc.cluster.local

# Run a command with a hard timeout. macOS ships no `timeout`: perl's alarm,
# and exec keeps the PID so the alarm kills the command itself.
with_timeout() {
    local secs=$1
    shift
    perl -e 'alarm shift; exec @ARGV or die "exec failed: $!"' "$secs" "$@"
}

k() { with_timeout 60 kubectl "$@"; }

expect_eq() {
    if [[ "$1" != "$2" ]]; then
        echo "expected '$2', got '$1'" >&2
        return 1
    fi
}

# Poll `"$@"` once a second until it succeeds or <secs> elapse.
wait_for() {
    local secs=$1
    shift
    local deadline=$((SECONDS + secs))
    until "$@" >/dev/null 2>&1; do
        if ((SECONDS >= deadline)); then
            echo "timed out after ${secs}s waiting for: $*" >&2
            return 1
        fi
        sleep 1
    done
}

# --- the case's git repository ---------------------------------------------

# Commits are deterministic (fixed author, committer and dates), so the same
# case content gives the same revisions on every run and every target: the
# golden snapshots can hold real SHAs.
_commit_n=0
_git() {
    git -C "$WORK/repo-$CASE_ID" -c user.name=litecd-compat -c user.email=compat@litecd.invalid \
        -c commit.gpgsign=false -c core.autocrlf=false "$@"
}

# The case's repo/ directory becomes the repository's first commit on main.
repo_init() {
    local dir="$WORK/repo-$CASE_ID"
    rm -rf "$dir"
    mkdir -p "$dir"
    if [[ -d "$CASE_DIR/repo" ]]; then
        cp -R "$CASE_DIR/repo/." "$dir/"
        # Child Applications in the repository need the same placeholders.
        find "$dir" -type f -exec perl -pi -e \
            "s#<REPO_URL>#$REPO_URL#g; s#<CHART_REPO>#$CHART_REPO#g; s#<NS>#$NS#g; s#<CASE>#$CASE_ID#g" {} +
    fi
    k -n "$GIT_NS" exec deploy/git -- sh -c \
        "rm -rf /srv/git/$CASE_ID.git && git init -q --bare -b main /srv/git/$CASE_ID.git" >/dev/null
    _git init -q -b main
    _git remote add origin "$GIT_LOCAL/$CASE_ID.git"
    repo_commit "initial"
}

# Commit whatever changed in the working copy ($(repo_dir)) and push.
repo_commit() {
    _commit_n=$((_commit_n + 1))
    local date="$((1767225600 + _commit_n * 60)) +0000"
    _git add -A
    GIT_AUTHOR_DATE=$date GIT_COMMITTER_DATE=$date _git commit -q --allow-empty -m "$1"
    _git push -q origin main 2>&1 | grep -v '^$' >&2 || true
    _git ls-remote origin refs/heads/main | grep -q "$(repo_head)"
}

repo_dir() { echo "$WORK/repo-$CASE_ID"; }
repo_head() { _git rev-parse HEAD; }

# Package a chart directory of the case (relative to CASE_DIR) into the
# corpus's Helm repository and reindex it.
publish_chart() {
    local tmp="$WORK/charts"
    mkdir -p "$tmp"
    helm package -d "$tmp" "$CASE_DIR/$1" >/dev/null
    helm repo index "$tmp" --url "$CHART_REPO" >/dev/null
    # Fixed timestamps: the index's `created`/`generated` would change the
    # Helm repository's content on every run.
    perl -pi -e 's/^(\s*created:).*/$1 "2026-01-01T00:00:00Z"/; s/^(generated:).*/$1 "2026-01-01T00:00:00Z"/' "$tmp/index.yaml"
    local f
    for f in "$tmp"/*; do
        k -n "$GIT_NS" exec -i deploy/git -- sh -c "cat > /srv/charts/$(basename "$f")" <"$f"
    done
}

# --- Applications ---------------------------------------------------------

# Apply manifests from a file of the case (or stdin with -) into the Argo CD
# namespace, with <REPO_URL>, <CHART_REPO>, <NS> and <CASE> filled in. Every
# object gets the case label, so the runner can delete what the case left.
app_apply() {
    local src=$1
    [[ "$src" == - ]] || src="$CASE_DIR/$src"
    local objs
    objs=$(sed -e "s#<REPO_URL>#$REPO_URL#g" -e "s#<CHART_REPO>#$CHART_REPO#g" -e "s#<NS>#$NS#g" -e "s#<CASE>#$CASE_ID#g" \
        "$src" | k -n "$ARGOCD_NS" apply -f - -o name)
    local obj
    for obj in $objs; do
        k -n "$ARGOCD_NS" label "$obj" compat.litecd.dev/case="$CASE_ID" --overwrite >/dev/null
    done
}

# app_get <app> <jsonpath>
app_get() { k -n "$ARGOCD_NS" get application "$1" -o jsonpath="{$2}"; }

app_refresh() { k -n "$ARGOCD_NS" annotate application "$1" argocd.argoproj.io/refresh="${2:-normal}" --overwrite >/dev/null; }

# Start a sync the way the argocd CLI and UI do: an `operation` on the
# object. Like Argo CD's API server when the request names none, the
# operation carries the app's spec.syncPolicy.syncOptions.
app_sync() {
    local rev="" opts
    [[ -n "${2:-}" ]] && rev="\"revision\":\"$2\","
    opts=$(k -n "$ARGOCD_NS" get application "$1" -o json |
        python3 -c 'import json,sys; print(json.dumps(((json.load(sys.stdin)["spec"].get("syncPolicy") or {}).get("syncOptions")) or []))')
    k -n "$ARGOCD_NS" patch application "$1" --type merge -p \
        "{\"operation\":{\"initiatedBy\":{\"username\":\"compat\"},\"sync\":{$rev\"syncOptions\":$opts,\"syncStrategy\":{\"hook\":{}}}}}" >/dev/null
}

_app_state() { # app sync health → 0 when both match ("-" matches anything)
    local s h
    s=$(app_get "$1" .status.sync.status)
    h=$(app_get "$1" .status.health.status)
    [[ ("$2" == - || "$s" == "$2") && ("$3" == - || "$h" == "$3") ]]
}

# wait_app <app> <sync status> <health status> [secs]: "-" for either.
wait_app() {
    local secs=${4:-120}
    if ! wait_for "$secs" _app_state "$1" "$2" "$3"; then
        app_describe "$1" >&2
        return 1
    fi
}

_app_rev() { [[ "$(app_get "$1" .status.sync.revision)" == "$2" ]]; }

# Wait until the app has compared against <rev> (default: the repo's HEAD).
wait_rev() {
    local rev=${2:-$(repo_head)}
    if ! wait_for "${3:-120}" _app_rev "$1" "$rev"; then
        app_describe "$1" >&2
        return 1
    fi
}

_app_op() { [[ "$(app_get "$1" .status.operationState.phase)" == "$2" ]]; }

# wait_op <app> <Succeeded|Failed|Error|Running> [secs]
wait_op() {
    if ! wait_for "${3:-120}" _app_op "$1" "$2"; then
        app_describe "$1" >&2
        return 1
    fi
}

app_describe() {
    echo "--- application $1:"
    app_get "$1" '.status' | python3 -c 'import json,sys; d=json.load(sys.stdin); [d.pop(k,None) for k in ("history","summary")]; print(json.dumps(d,indent=1)[:4000])' 2>/dev/null || true
}

# snapshot [--skip-operation-state] <label> <app>...: the apps' normalized status and the tracking
# metadata of their resources, appended to the case's snapshot file under
# <label>. The runner compares the file with golden/<case>.json.
snapshot() {
    local flags=()
    [[ $1 == --skip-operation-state ]] && { flags=("$1"); shift; }
    local label=$1
    shift
    python3 "$COMPAT_DIR/snapshot.py" ${flags[@]+"${flags[@]}"} --namespace "$ARGOCD_NS" --label "$label" --repo "$REPO_URL" --ns "$NS" "$@" >>"$SNAPSHOTS"
}
