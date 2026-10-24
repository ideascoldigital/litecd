"""Normalized snapshot of Applications for the corpus: what Argo CD wrote in
their status and on the resources they manage, without what changes from run
to run (times, uids, resourceVersions). Prints one JSON line:
{"label": ..., "apps": {name: {...}}}; run.sh collects the lines of a case
into one document and compares it with golden/<case>.json.

Placeholders keep golden files independent of the case's namespace and
repository URL: the case's namespace becomes <NS>, its repository <REPO_URL>.
"""

import argparse
import json
import subprocess
import sys
import time

TIMES = {
    "reconciledAt",
    "observedAt",
    "startedAt",
    "finishedAt",
    "deployedAt",
    "deployStartedAt",
    "lastTransitionTime",
    "creationTimestamp",
}


def kubectl(*args):
    p = subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=60)
    if p.returncode != 0:
        return None
    return json.loads(p.stdout)


def strip_times(v):
    if isinstance(v, dict):
        return {k: strip_times(x) for k, x in v.items() if k not in TIMES}
    if isinstance(v, list):
        return [strip_times(x) for x in v]
    return v


def res_key(r):
    return (r.get("group", ""), r.get("kind", ""), r.get("namespace", ""), r.get("name", ""))


def tracking(meta):
    """The metadata a GitOps controller puts on what it manages."""
    labels = {k: v for k, v in (meta.get("labels") or {}).items() if "argoproj.io" in k or k == "app.kubernetes.io/instance"}
    ann = {}
    for k, v in (meta.get("annotations") or {}).items():
        if k == "kubectl.kubernetes.io/last-applied-configuration":
            ann[k] = "<present>"
        elif "argoproj.io" in k:
            ann[k] = v
    managers = sorted(
        {f"{m.get('manager')}/{m.get('operation')}" for m in meta.get("managedFields") or [] if str(m.get("manager", "")).startswith("argocd")}
    )
    out = {}
    if labels:
        out["labels"] = labels
    if ann:
        out["annotations"] = ann
    if managers:
        out["managers"] = managers
    return out


def op_state(o):
    """The last operation, without what depends on timing in Argo CD itself.
    Right after a sync its cache may not have seen the watch events of what
    it applied yet, so it sometimes self-heals at once with nothing to change
    (autoHealAttemptsCount 1, a subset of the resources, "unchanged"), and a
    first apply sometimes needs a retry. Compared: phase, message, who started
    it, revision, sync options; hooks; resources that did not sync, with their
    message; retries only when the operation did not succeed. What synced is
    in status.resources."""
    if not o:
        return
    sync = (o.get("operation") or {}).get("sync") or {}
    sync.pop("autoHealAttemptsCount", None)
    sync.pop("resources", None)
    if o.get("phase") == "Succeeded":
        o.pop("retryCount", None)
    sr = o.get("syncResult") or {}
    if "resources" in sr:
        kept = [r for r in sr["resources"] if r.get("hookType") or r.get("status") != "Synced"]
        sr["resources"] = sorted(kept, key=lambda r: (r.get("syncPhase", ""), res_key(r)))


def app_snapshot(ns, name):
    app = kubectl("-n", ns, "get", "application", name, "-o", "json")
    if app is None:
        return {"absent": True}
    meta = app["metadata"]
    status = strip_times(app.get("status") or {})
    for r in status.get("resources") or []:
        if isinstance(r.get("health"), dict):
            r["health"].pop("lastTransitionTime", None)
    if "resources" in status:
        status["resources"].sort(key=res_key)
    op_state(status.get("operationState"))
    for c in status.get("conditions") or []:
        c.pop("lastTransitionTime", None)

    objects = {}
    for r in (app.get("status") or {}).get("resources") or []:
        kind = r["kind"] + ("." + r["group"] if r.get("group") else "")
        args = ["get", kind, r["name"], "-o", "json"]
        if r.get("namespace"):
            args[:0] = ["-n", r["namespace"]]
        obj = kubectl(*args)
        key = "/".join(x for x in (r.get("group", ""), r["kind"], r.get("namespace", ""), r["name"]) if x)
        objects[key] = tracking(obj["metadata"]) if obj else {"absent": True}

    return {
        "annotations": {k: v for k, v in (meta.get("annotations") or {}).items() if "argoproj.io" in k and k != "argocd.argoproj.io/refresh"},
        "finalizers": meta.get("finalizers") or [],
        "operation": app.get("operation"),
        "status": status,
        "objects": dict(sorted(objects.items())),
    }


def settle(ns, names, quiet=5, limit=90):
    """Wait until the apps' normalized status has not changed for `quiet`
    seconds (at most `limit`): a snapshot must not catch a controller between
    two steps (a self-heal right after a sync, say)."""
    last, since, start = None, time.time(), time.time()
    while time.time() - start < limit:
        cur = [app_status(ns, n) for n in names]
        if cur != last:
            last, since = cur, time.time()
        elif time.time() - since >= quiet:
            return
        time.sleep(1)


def app_status(ns, name):
    app = kubectl("-n", ns, "get", "application", name, "-o", "json")
    return None if app is None else (strip_times(app.get("status") or {}), app.get("operation"))


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--namespace", required=True)
    a.add_argument("--label", required=True)
    a.add_argument("--repo", required=True)
    a.add_argument("--ns", required=True)
    a.add_argument("--skip-operation-state", action="store_true", help="leave status.operationState out (see README)")
    a.add_argument("apps", nargs="+")
    args = a.parse_args()
    settle(args.namespace, args.apps)
    snap = {"label": args.label, "apps": {n: app_snapshot(args.namespace, n) for n in args.apps}}
    if args.skip_operation_state:
        for a in snap["apps"].values():
            a.get("status", {}).pop("operationState", None)
    text = json.dumps(snap, sort_keys=True)
    text = text.replace(args.repo, "<REPO_URL>").replace(args.ns, "<NS>")
    sys.stdout.write(text + "\n")


if __name__ == "__main__":
    main()
