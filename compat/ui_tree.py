"""ui_tree.py --app <instance> --label <label> --ns <NS> --nodes <n>

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


def read_tree(argocd_ns, instance):
    pw = json.loads(kubectl("-n", argocd_ns, "get", "secret", "argocd-redis", "-o", "json"))["data"]["auth"]
    pw = base64.b64decode(pw).decode()
    key = f"app|resources-tree|{instance}|1.8.3.gz"
    # --raw writes the value as it is, plus a newline.
    out = kubectl("-n", argocd_ns, "exec", "deploy/argocd-redis", "--", "sh", "-c",
                  'REDISCLI_AUTH="$1" redis-cli --raw GET "$2" | base64', "_", pw, key)
    raw = base64.b64decode(out)
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if not raw:
        return None
    if os.environ.get("UI_TREE_RAW_DIR"):  # the value as it is, for unit tests
        with open(os.path.join(os.environ["UI_TREE_RAW_DIR"], instance + ".json.gz"), "wb") as f:
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
    ap.add_argument("--argocd-ns", default="argocd")
    ap.add_argument("--timeout", type=int, default=180)
    a = ap.parse_args()
    deadline = time.time() + a.timeout
    last, err = None, ""
    while time.time() < deadline:
        try:
            tree = read_tree(a.argocd_ns, a.app)
            nodes = normalize(tree, a.ns) if tree else []
            if settled(nodes, a.nodes) and nodes == last:
                print(json.dumps({"label": a.label, "tree": nodes}, sort_keys=True))
                return 0
            last, err = nodes, f"{len(nodes)} nodes"
        except Exception as e:  # noqa: BLE001 - retried until the deadline
            err = str(e)
        time.sleep(3)
    print(f"no settled tree for {a.app} in {a.timeout}s: {err}; last: {json.dumps(last)[:2000]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
