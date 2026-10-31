"""ui_tree.py --app <instance> --label <label> --ns <NS> --nodes <n> [--managed <n>]

The resource tree of an Application as Argo CD's web UI reads it from
Redis (`app|resources-tree|<instance>|1.8.3.gz`, the Redis of the
Deployment argocd-redis in --argocd-ns), normalized like snapshot.py:
without uids, resource versions and times; pod names as `<owner>-*`; the
node name as <node>. Waits until the tree has --nodes nodes, every pod is
ready or done, and it is the same twice in a row. Prints one snapshot line:
{"label": ..., "tree": [...]}.
"""

import argparse
import base64
import gzip
import json
import os
import subprocess
import sys
import time


def kubectl(*args):
    p = subprocess.run(["kubectl", *args], capture_output=True, timeout=60)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode(errors="replace").strip())
    return p.stdout


def read_tree(argocd_ns, instance, what="resources-tree"):
    pw = json.loads(kubectl("-n", argocd_ns, "get", "secret", "argocd-redis", "-o", "json"))["data"]["auth"]
    pw = base64.b64decode(pw).decode()
    key = f"app|{what}|{instance}|1.8.3.gz"
    # --raw writes the value as it is, plus a newline.
    out = kubectl("-n", argocd_ns, "exec", "deploy/argocd-redis", "--", "sh", "-c",
                  'REDISCLI_AUTH="$1" redis-cli --raw GET "$2" | base64', "_", pw, key)
    raw = base64.b64decode(out)
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if not raw:
        return None
    if os.environ.get("UI_TREE_RAW_DIR"):  # the value as it is, for unit tests
        name = instance + ("" if what == "resources-tree" else "." + what) + ".json.gz"
        with open(os.path.join(os.environ["UI_TREE_RAW_DIR"], name), "wb") as f:
            f.write(raw)
    return json.loads(gzip.decompress(raw))


def normalize(tree, ns):
    def ref(r):
        r = {k: v for k, v in r.items() if k != "uid"}
        if r.get("namespace") == ns:
            r["namespace"] = "<NS>"
        return r

    out = []
    for n in tree.get("nodes") or []:
        n = dict(n)
        for k in ("uid", "resourceVersion", "createdAt"):
            n.pop(k, None)
        if n.get("namespace") == ns:
            n["namespace"] = "<NS>"
        n["parentRefs"] = [ref(r) for r in n.get("parentRefs") or []]
        if n.get("kind") == "Pod" and n["parentRefs"]:
            n["name"] = n["parentRefs"][0]["name"] + "-*"
        n["images"] = sorted(n.get("images") or [])
        info = []
        for i in n.get("info") or []:
            if i.get("name") == "Node" and i.get("value"):
                i = {"name": "Node", "value": "<node>"}
            info.append(i)
        n["info"] = info
        labels = (n.get("networkingInfo") or {}).get("labels")
        if labels:
            n["networkingInfo"]["labels"] = {k: ("<uid>" if "controller-uid" in k else v) for k, v in labels.items()}
        out.append({k: v for k, v in n.items() if v not in ([], None)})
    return sorted(out, key=lambda n: json.dumps(n, sort_keys=True))


def normalize_managed(items, ns):
    """The managed resources (the UI's resource details and Diff tab):
    which states are there, the objects they name, a Secret's masked data."""
    out = []
    for r in items or []:
        e = {k: r.get(k) for k in ("group", "kind", "name", "hook", "modified") if r.get(k)}
        e["namespace"] = "<NS>" if r.get("namespace") == ns else r.get("namespace", "")
        for field in ("targetState", "liveState", "normalizedLiveState", "predictedLiveState"):
            obj = json.loads(r[field]) if r.get(field) else None
            if obj is None:
                e[field] = None
                continue
            seen = {"kind": obj.get("kind"), "name": (obj.get("metadata") or {}).get("name")}
            if obj.get("kind") == "Secret":
                seen["data"] = obj.get("data")
                seen["stringData"] = obj.get("stringData")
            e[field] = seen
        e["resourceVersion"] = bool(r.get("resourceVersion"))
        out.append(e)
    return out


def settled(nodes, want):
    if len(nodes) != want:
        return False
    for n in nodes:
        if n.get("kind") != "Pod":
            continue
        info = {i.get("name"): i.get("value") for i in n.get("info") or []}
        if info.get("Status Reason") not in ("Running", "Completed"):
            return False
        if info.get("Status Reason") == "Running":
            ready, total = info.get("Containers", "0/1").split("/")
            if ready != total:
                return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--ns", required=True)
    ap.add_argument("--nodes", type=int, required=True)
    ap.add_argument("--managed", type=int, default=0, help="also app|managed-resources, with this many items")
    ap.add_argument("--argocd-ns", default="argocd")
    ap.add_argument("--timeout", type=int, default=180)
    a = ap.parse_args()
    deadline = time.time() + a.timeout
    last, err = None, ""
    while time.time() < deadline:
        try:
            tree = read_tree(a.argocd_ns, a.app)
            nodes = normalize(tree, a.ns) if tree else []
            managed = normalize_managed(read_tree(a.argocd_ns, a.app, "managed-resources"), a.ns) if a.managed else None
            snap = (nodes, managed)
            if settled(nodes, a.nodes) and (managed is None or len(managed) == a.managed) and snap == last:
                out = {"label": a.label, "tree": nodes}
                if managed is not None:
                    out["managed"] = managed
                print(json.dumps(out, sort_keys=True))
                return 0
            last, err = snap, f"{len(nodes)} nodes, {len(managed or [])} managed"
        except Exception as e:  # noqa: BLE001 - retried until the deadline
            err = str(e)
        time.sleep(3)
    print(f"no settled tree for {a.app} in {a.timeout}s: {err}; last: {json.dumps(last)[:2000]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
