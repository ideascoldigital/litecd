"""compare.py <snapshots.jsonl> <golden.json> [--update]

Compares a case's snapshots (one JSON line per `snapshot` call) with its
golden file, written from a run against Argo CD. Exit 0: equal (or written
with --update), 1: different (a unified diff on stderr), 2: no golden file.
"""

import difflib
import json
import os
import sys


def pretty(v):
    return json.dumps(v, indent=2, sort_keys=True) + "\n"


def main():
    snaps_path, golden_path = sys.argv[1], sys.argv[2]
    update = "--update" in sys.argv[3:]
    snaps = []
    if os.path.exists(snaps_path):
        with open(snaps_path) as f:
            snaps = [json.loads(line) for line in f if line.strip()]
    got = pretty(snaps)
    if update:
        os.makedirs(os.path.dirname(golden_path), exist_ok=True)
        with open(golden_path, "w") as f:
            f.write(got)
        return 0
    if not os.path.exists(golden_path):
        print(f"no golden file {golden_path}: run against argocd with --update-golden", file=sys.stderr)
        return 2
    with open(golden_path) as f:
        want = f.read()
    if want == got:
        return 0
    diff = list(difflib.unified_diff(want.splitlines(True), got.splitlines(True), "golden (Argo CD)", "this run"))
    sys.stderr.writelines(diff[:120])
    if len(diff) > 120:
        print(f"... {len(diff) - 120} more diff lines", file=sys.stderr)
    first = next((d[1:].strip() for d in diff[2:] if d[:1] in "+-"), "")
    print(f"differs from golden: {first}"[:200], file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
