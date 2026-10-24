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


def appset_snapshot(ns, name):
    """An ApplicationSet's conditions and the Applications it owns: what
    the template made of each (spec, labels, annotations, finalizers, owner)
    and where each got (sync and health)."""
    aset = kubectl("-n", ns, "get", "applicationset", name, "-o", "json")
    if aset is None:
        return {"absent": True}
    st = aset.get("status") or {}
    conds = sorted(
        ({k: c.get(k) for k in ("type", "status", "reason", "message")} for c in st.get("conditions") or []),
        key=lambda c: c["type"] or "",
    )
    # Which Applications it lists; their sync and health there lag (Argo CD
    # refreshes them on its own schedule): those are under "generated".
    resources = sorted(
        ({k: r.get(k) for k in ("group", "kind", "name", "namespace", "version")} for r in st.get("resources") or []),
        key=lambda r: (r.get("name") or ""),
    )
    uid = aset["metadata"]["uid"]
    apps = kubectl("-n", ns, "get", "applications", "-o", "json") or {"items": []}
    generated = {}
    for a in apps["items"]:
        owners = [o for o in a["metadata"].get("ownerReferences") or [] if o.get("uid") == uid]
        if not owners:
            continue
        m = a["metadata"]
        generated[m["name"]] = {
            "spec": a.get("spec"),
            "labels": m.get("labels") or {},
            "annotations": {k: v for k, v in (m.get("annotations") or {}).items()
                            if k not in ("kubectl.kubernetes.io/last-applied-configuration", "argocd.argoproj.io/refresh")},
            "finalizers": m.get("finalizers") or [],
            "owner": [{k: o.get(k) for k in ("apiVersion", "kind", "name", "controller", "blockOwnerDeletion")} for o in owners],
            "sync": ((a.get("status") or {}).get("sync") or {}).get("status"),
            "health": ((a.get("status") or {}).get("health") or {}).get("status"),
        }
    return {"conditions": conds, "resources": resources, "generated": dict(sorted(generated.items()))}


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


def settle_appsets(ns, names, quiet=5, limit=120):
    """As settle, for ApplicationSets and what they generated."""
    last, since, start = None, time.time(), time.time()
    while time.time() - start < limit:
        cur = [appset_snapshot(ns, n) for n in names]
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
    a.add_argument("--appset", action="append", default=[], help="an ApplicationSet to record (repeatable)")
    a.add_argument("apps", nargs="*")
    args = a.parse_args()
    settle(args.namespace, args.apps)
    if args.appset:
        settle_appsets(args.namespace, args.appset)
    snap = {"label": args.label, "apps": {n: app_snapshot(args.namespace, n) for n in args.apps}}
    if args.appset:
        snap["appsets"] = {n: appset_snapshot(args.namespace, n) for n in args.appset}
    if args.skip_operation_state:
        for a in snap["apps"].values():
            a.get("status", {}).pop("operationState", None)
    text = json.dumps(snap, sort_keys=True)
    text = text.replace(args.repo, "<REPO_URL>").replace(args.ns, "<NS>")
    sys.stdout.write(text + "\n")


if __name__ == "__main__":
    main()
