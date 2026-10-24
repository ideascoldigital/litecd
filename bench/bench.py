"""litecd against Argo CD on the cluster of the current kube context: memory
and CPU of the GitOps controller's pods, and how long syncs take.

  python3 bench/bench.py --target argocd --chart <argo-cd chart .tgz>
  python3 bench/bench.py --target litecd
  python3 bench/report.py bench/results/<run>/*.json > bench/results/<run>/report.md

Uses the corpus's git server (compat/git). Per target: install it, idle
with no apps, then the timed steps (3 repeats each, medians in the report):
  first     an Application created → Synced + Healthy (its namespace new)
  change    a commit, then a refresh as a webhook would ask → Synced on it
  drift     the managed Deployment deleted → back, Available, app Healthy
  burst     20 Applications at once → all Synced + Healthy
then idle with the 20 apps, then uninstall. Memory is each container's
working set (cgroup memory.current minus inactive_file, as kubelet counts
it), CPU the cgroup's usage over the idle window.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NS = "argocd"
GIT_NS = "litecd-compat"
GIT_SVC = f"http://git.{GIT_NS}.svc.cluster.local/git"
REPO = "bench"
APPS = 20
REPEATS = 3


def sh(*args, check=True, timeout=600, input=None):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout, input=input)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}: {p.stderr.strip()}")
    return p.stdout


def k(*args, **kw):
    return sh("kubectl", *args, **kw)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# --- git ----------------------------------------------------------------------

class Repo:
    """The bench's repository: apps/app-NN, pushed through a port-forward."""

    def __init__(self, work):
        self.dir = os.path.join(work, "repo")
        self.n = 0
        k("apply", "-f", os.path.join(ROOT, "compat/git/git-server.yaml"))
        cm = k("-n", GIT_NS, "create", "configmap", "git-server", f"--from-file={ROOT}/compat/git/server.py",
               "--dry-run=client", "-o", "yaml")
        k("apply", "-f", "-", input=cm)
        k("-n", GIT_NS, "rollout", "status", "deploy/git", "--timeout", "300s", timeout=320)
        self.pf = subprocess.Popen(["kubectl", "-n", GIT_NS, "port-forward", "svc/git", ":80"],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        line = self.pf.stdout.readline()
        port = re.search(r"127\.0\.0\.1:(\d+)", line).group(1)
        self.remote = f"http://127.0.0.1:{port}/git/{REPO}.git"
        k("-n", GIT_NS, "exec", "deploy/git", "--", "sh", "-c",
          f"rm -rf /srv/git/{REPO}.git && git init -q --bare -b main /srv/git/{REPO}.git")
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)
        for i in range(APPS):
            self.write_app(i, "one")
        self.git("init", "-q", "-b", "main")
        self.git("remote", "add", "origin", self.remote)
        self.commit("bench apps")

    def git(self, *args):
        return sh("git", "-C", self.dir, "-c", "user.name=bench", "-c", "user.email=bench@litecd.invalid",
                  "-c", "commit.gpgsign=false", *args)

    def write_app(self, i, value):
        d = os.path.join(self.dir, "apps", f"app-{i:02d}")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "app.yaml"), "w") as f:
            f.write(f"""apiVersion: v1
kind: ConfigMap
metadata: {{name: settings}}
data: {{key: {value}}}
---
apiVersion: v1
kind: Service
metadata: {{name: web}}
spec:
  selector: {{app: web}}
  ports: [{{port: 80}}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {{name: web}}
spec:
  replicas: 1
  selector: {{matchLabels: {{app: web}}}}
  template:
    metadata: {{labels: {{app: web}}}}
    spec:
      containers:
        - name: main
          image: registry.k8s.io/pause:3.10
          resources: {{requests: {{cpu: 1m, memory: 4Mi}}}}
""")

    def commit(self, msg):
        self.n += 1
        env = dict(os.environ, GIT_AUTHOR_DATE=f"{1767225600 + self.n * 60} +0000",
                   GIT_COMMITTER_DATE=f"{1767225600 + self.n * 60} +0000")
        subprocess.run(["git", "-C", self.dir, "-c", "user.name=bench", "-c", "user.email=bench@litecd.invalid",
                        "-c", "commit.gpgsign=false", "add", "-A"], check=True, env=env)
        subprocess.run(["git", "-C", self.dir, "-c", "user.name=bench", "-c", "user.email=bench@litecd.invalid",
                        "-c", "commit.gpgsign=false", "commit", "-q", "--allow-empty", "-m", msg], check=True, env=env)
        sh("git", "-C", self.dir, "push", "-q", "origin", "main")
        return self.git("rev-parse", "HEAD").strip()

    def close(self):
        self.pf.terminate()


# --- targets ------------------------------------------------------------------

def install(target, chart):
    if target == "argocd":
        sh("helm", "upgrade", "--install", "argocd", chart, "--namespace", NS, "--create-namespace", "--wait",
           "--timeout", "9m", "--set", "dex.enabled=false", "--set", "notifications.enabled=false", timeout=620)
    else:
        k("create", "namespace", NS, check=False)
        k("apply", "--server-side", "--force-conflicts", "-R", "-f", os.path.join(ROOT, "deploy/install"), timeout=300)
        k("-n", NS, "rollout", "status", "deploy/litecd", "--timeout", "300s", timeout=320)


def uninstall(target):
    if target == "argocd":
        sh("helm", "-n", NS, "uninstall", "argocd", "--wait", check=False, timeout=320)
    else:
        k("delete", "-f", os.path.join(ROOT, "deploy/install/litecd.yaml"), "--wait=true", check=False, timeout=320)


# --- measuring ------------------------------------------------------------------

def containers():
    pods = json.loads(k("-n", NS, "get", "pods", "-o", "json"))
    out = []
    for p in pods["items"]:
        if p["status"].get("phase") != "Running":
            continue
        for c in p["spec"]["containers"]:
            out.append((p["metadata"]["name"], c["name"]))
    return out


def cgroup(pod, container):
    """Working set (bytes), anonymous memory (bytes), CPU used (µs)."""
    txt = k("-n", NS, "exec", pod, "-c", container, "--", "sh", "-c",
            "cat /sys/fs/cgroup/memory.current; grep -E '^(inactive_file|anon) ' /sys/fs/cgroup/memory.stat; "
            "grep usage_usec /sys/fs/cgroup/cpu.stat", check=False, timeout=30)
    nums = {}
    lines = txt.split("\n")
    try:
        nums["current"] = int(lines[0])
    except (ValueError, IndexError):
        return None
    for l in lines[1:]:
        parts = l.split()
        if len(parts) == 2:
            nums[parts[0]] = int(parts[1])
    return {"working_set": nums["current"] - nums.get("inactive_file", 0), "anon": nums.get("anon", 0),
            "cpu_usec": nums.get("usage_usec", 0)}


def sample():
    return {f"{p}/{c}": cgroup(p, c) for p, c in containers()}


def idle(label, seconds=120):
    log(f"idle {label}: settling 60 s, then {seconds} s")
    time.sleep(60)
    a = sample()
    t0 = time.time()
    time.sleep(seconds)
    b = sample()
    dt = time.time() - t0
    out = {}
    for key, v in b.items():
        if v is None or a.get(key) is None:
            continue
        out[key] = {"working_set_mib": round(v["working_set"] / 2**20, 1), "anon_mib": round(v["anon"] / 2**20, 1),
                    "cpu_percent": round((v["cpu_usec"] - a[key]["cpu_usec"]) / 1e6 / dt * 100, 3)}
    total = {"working_set_mib": round(sum(x["working_set_mib"] for x in out.values()), 1),
             "anon_mib": round(sum(x["anon_mib"] for x in out.values()), 1),
             "cpu_percent": round(sum(x["cpu_percent"] for x in out.values()), 3)}
    log(f"idle {label}: {total}")
    return {"containers": out, "total": total}


# --- the timed steps ---------------------------------------------------------------

def app_yaml(i):
    return f"""apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: bench-{i:02d}
  namespace: {NS}
  finalizers: [resources-finalizer.argocd.argoproj.io]
spec:
  project: default
  source: {{repoURL: {GIT_SVC}/{REPO}.git, targetRevision: main, path: apps/app-{i:02d}}}
  destination: {{server: https://kubernetes.default.svc, namespace: bench-{i:02d}}}
  syncPolicy:
    automated: {{prune: true, selfHeal: true}}
    syncOptions: [CreateNamespace=true]
"""


def app_state(i):
    p = subprocess.run(["kubectl", "-n", NS, "get", "application", f"bench-{i:02d}", "-o",
                        "jsonpath={.status.sync.status} {.status.health.status} {.status.sync.revision}"],
                       capture_output=True, text=True, timeout=30)
    return p.stdout.split() if p.returncode == 0 else []


def wait(pred, timeout=600, poll=0.25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return time.time() - t0
        time.sleep(poll)
    raise TimeoutError("timed out")


def healthy(i, rev=None):
    s = app_state(i)
    return len(s) >= 2 and s[0] == "Synced" and s[1] == "Healthy" and (rev is None or (len(s) > 2 and s[2] == rev))


def delete_apps(indexes):
    for i in indexes:
        k("-n", NS, "delete", "application", f"bench-{i:02d}", "--wait=false", check=False)
    wait(lambda: not any(app_state(i) or k("-n", NS, "get", "application", f"bench-{i:02d}", "-o", "name",
                                           check=False).strip() for i in indexes), timeout=600, poll=1)
    for i in indexes:
        k("delete", "namespace", f"bench-{i:02d}", "--wait=false", check=False)
    wait(lambda: not any(k("get", "namespace", f"bench-{i:02d}", "-o", "name", check=False).strip() for i in indexes),
         timeout=600, poll=1)


def step_first():
    times = []
    for r in range(REPEATS):
        t0 = time.time()
        k("apply", "-f", "-", input=app_yaml(0))
        wait(lambda: healthy(0))
        times.append(round(time.time() - t0, 2))
        log(f"first #{r + 1}: {times[-1]} s")
        if r + 1 < REPEATS:
            delete_apps([0])
    return times


def step_change(repo):
    times = []
    for r in range(REPEATS):
        repo.write_app(0, f"v{r + 2}")
        rev = repo.commit(f"change {r}")
        t0 = time.time()
        k("-n", NS, "annotate", "application", "bench-00", "argocd.argoproj.io/refresh=normal", "--overwrite")
        wait(lambda: healthy(0, rev))
        times.append(round(time.time() - t0, 2))
        log(f"change #{r + 1}: {times[-1]} s")
        time.sleep(5)
    return times


def available():
    p = subprocess.run(["kubectl", "-n", "bench-00", "get", "deploy", "web", "-o", "jsonpath={.status.availableReplicas}"],
                       capture_output=True, text=True)
    return p.returncode == 0 and p.stdout.strip() == "1"


def step_drift():
    times = []
    for r in range(REPEATS):
        # Self-heal backs off between attempts: wait it out as a user would.
        time.sleep(30)
        t0 = time.time()
        k("-n", "bench-00", "delete", "deploy", "web", "--wait=true")
        wait(lambda: available() and healthy(0))
        times.append(round(time.time() - t0, 2))
        log(f"drift #{r + 1}: {times[-1]} s")
    return times


def step_burst():
    peak = {}
    indexes = list(range(1, APPS))
    t0 = time.time()
    k("apply", "-f", "-", input="---\n".join(app_yaml(i) for i in indexes))
    done = False
    while not done:
        s = sample()
        for key, v in s.items():
            if v:
                peak[key] = max(peak.get(key, 0), v["working_set"])
        done = all(healthy(i) for i in indexes)
        if time.time() - t0 > 900:
            raise TimeoutError("burst timed out")
        time.sleep(1)
    took = round(time.time() - t0, 2)
    log(f"burst of {len(indexes)}: {took} s")
    return {"seconds": took, "apps": len(indexes),
            "peak_working_set_mib": round(sum(peak.values()) / 2**20, 1),
            "peak_by_container_mib": {k_: round(v / 2**20, 1) for k_, v in peak.items()}}


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--target", choices=["argocd", "litecd"], required=True)
    a.add_argument("--chart", help="argo-cd chart .tgz (target argocd)")
    a.add_argument("--out", default=os.path.join(ROOT, "bench/results", time.strftime("%Y%m%d")))
    args = a.parse_args()
    os.makedirs(args.out, exist_ok=True)
    work = tempfile.mkdtemp()
    result = {"target": args.target, "started": time.strftime("%Y-%m-%dT%H:%M:%S"),
              "cluster": k("version", "-o", "json", check=False)}
    try:
        server = json.loads(result["cluster"] or "{}").get("serverVersion", {}).get("gitVersion")
        result["cluster"] = server
        repo = Repo(work)
        log(f"installing {args.target}")
        t0 = time.time()
        install(args.target, args.chart)
        result["install_seconds"] = round(time.time() - t0, 1)
        result["idle_no_apps"] = idle("no apps")
        result["first"] = step_first()
        result["change"] = step_change(repo)
        result["drift"] = step_drift()
        result["burst"] = step_burst()
        result["idle_20_apps"] = idle(f"{APPS} apps")
        log("cleaning up")
        delete_apps(list(range(APPS)))
        repo.close()
    finally:
        uninstall(args.target)
        shutil.rmtree(work, ignore_errors=True)
    path = os.path.join(args.out, f"{args.target}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    log(f"wrote {path}")


if __name__ == "__main__":
    sys.exit(main())
