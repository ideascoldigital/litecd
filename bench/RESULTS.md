# litecd against Argo CD: results

`bench/bench.py` run once per target on the same cluster, one target at a
time, on 2026-10-24. Raw results: [results/20261024/](results/20261024/)
(`argocd.json`, `litecd.json`, `report.md` from `bench/report.py`).

## Setup

- Host: omarchy, x86_64, AMD Ryzen 7 5800X (8 cores, 16 threads), 31 GiB.
- Cluster: litek8s 0.2.8 dev cluster, rootless, one node, Kubernetes
  v1.33.0+litek8s, created fresh for the run (`compat/cluster.sh up`).
- Argo CD: chart argo-cd 10.10.5 (Argo CD v3.5.5) with
  `dex.enabled=false` and `notifications.enabled=false`, as in the
  compatibility corpus: five pods (application controller, repo server,
  server, ApplicationSet controller, redis).
- litecd: `litecd:dev` built from main (`deploy/build.sh`, 17 MB static
  binary in an Alpine image with git, helm, kustomize and jsonnet),
  installed from `deploy/install`: one pod.
- Workload: 20 Applications from the corpus's git server, each a
  directory with a ConfigMap, a Service and a one-replica Deployment
  (`pause`), automated sync with prune and self-heal, `CreateNamespace`.
  Timed steps run 3 times; the table gives the median and the range.

Memory is per container, read from its cgroup and summed over the target's
pods. **Working set** is `memory.current` minus `inactive_file`, what
kubelet and `kubectl top` report; it includes the page cache charged to the
container (git checkouts, rendered manifests, files read), so it moves with
I/O as well as with the process. **Anonymous** is `anon` in `memory.stat`:
heap and stacks, what the process itself allocates. Pages of a binary
already in the page cache are charged to whoever read them first (here the
image import), not to the pod: litecd's process shows 10.8 MiB of RSS, of
which 9 MiB is its mapped binary, against 2.2 MiB of working set. The same
applies to Argo CD's binaries, so the comparison is like for like, but
neither column is RSS.

## Results

| | Argo CD | litecd |
|---|---|---|
| Memory, no apps (working set) | 194.2 MiB | 2.2 MiB |
| … anonymous (heap) of that | 169.4 MiB | 1.2 MiB |
| Memory, 20 apps synced, idle (working set) | 310.2 MiB | 6.9 MiB |
| … anonymous (heap) of that | 225.0 MiB | 4.1 MiB |
| Memory, peak during the burst (working set) | 325.5 MiB | 9.4 MiB |
| CPU idle, no apps (% of one CPU) | 0.24 % | 0.004 % |
| CPU idle, 20 apps | 0.382 % | 0.147 % |
| Install → ready | 28.7 s | 0.6 s |
| App created → Synced + Healthy (median, range) | 0.46 s (0.46–1.1) | 0.46 s (0.45–2.86) |
| Commit + refresh → Synced on it | 0.39 s (0.39–0.39) | 0.68 s (0.39–0.68) |
| Deployment deleted → healed, Healthy | 0.43 s (0.43–0.44) | 0.77 s (0.77–0.77) |
| 19 apps at once → all Synced + Healthy | 5.72 s | 3.28 s |

Argo CD with 20 apps idle, per container (working set / anonymous):
application controller 161.4 / 114.0 MiB, repo server 46.2 / 31.7 MiB,
ApplicationSet controller 45.7 / 35.8 MiB, server 44.6 / 32.4 MiB, redis
12.3 / 11.1 MiB.

## Where the differences come from

**Memory.** Argo CD runs five Go processes, each with its own client-go
informers, caches and runtime; litecd is one Rust process. The largest
share is Argo CD's application controller (161 MiB with 20 apps), which
keeps a cache of every resource of every watched kind in the cluster, not
only the ones the apps manage. litecd watches only the kinds its apps
manage, keeps no copy of what it sees (an event just wakes the app that
owns the object) and reads the live resources when it compares, so its
memory grows with the apps (2.2 → 6.9 MiB) rather than with the cluster. The
server, repo server and ApplicationSet controller cost Argo CD about
45 MiB each even with nothing to do; litecd has no API server or UI, and
renders and runs ApplicationSets inside the same process. Redis (12 MiB)
caches manifests for Argo CD; litecd keeps its rendering cache in memory.
The gap between working set and anonymous memory (85 MiB for Argo CD,
2.8 MiB for litecd with 20 apps) is page cache: binaries' data, the repo
server's checkouts and files read since the pods started.

**CPU idle.** Argo CD's idle CPU is mostly redis (0.11 %) and the
application controller's resyncs; litecd sleeps until a watch event or an
app's timer (periodic refresh) wakes it. With 20 apps the increase
for both comes mostly from the periodic refresh of each app (every 180 s
by default).

**Install.** Argo CD's 28.7 s include pulling its images (600 MB for
`quay.io/argoproj/argocd`, plus redis) into a fresh cluster and starting
five pods; litecd's image was already in the cluster's store (built
there by `deploy/build.sh`), so its 0.6 s are a pod start. A pull of the
148 MB litecd image would add a few seconds.

**Single-app timings.** These sit at the harness's resolution: each
`kubectl` call takes about 0.05 s and the bench polls every 0.25 s, so
0.39 s and 0.46 s mean "done by the second poll" for either target, and
the two cannot be told apart below ~0.3 s. Where litecd was one poll
slower (a commit, 0.68 s; a heal, 0.77 s against 0.43 s) the cause was its
coalescing: every time an app's task woke it waited 200 ms to merge a
burst of events (`src/controller.rs`), and a heal takes several wakes
(the Deployment deleted, re-applied, then Available); at 20 ms they match
(below). The first sync after
start was slower for litecd (2.86 s against 1.1 s): both clone the repo
and discover the API then, and the later repeats are equal.

**Burst.** 19 apps created at once were all Synced and Healthy in 3.3 s
with litecd against 5.7 s with Argo CD. litecd runs one task per
Application, each cloning, rendering and applying on its own; Argo CD
queues apps through the application controller's processors and the repo
server's manifest generation, and moves them through comparison and
operation in separate passes.

## After shortening the coalescing wait

The same bench, litecd only, after its wait to merge events went from
200 ms to 20 ms (same cluster, same day; the build also has apps in any
namespace and sync windows). Raw results:
[results/20261024-coalesce/litecd.json](results/20261024-coalesce/litecd.json).

| | Argo CD | litecd, 200 ms | litecd, 20 ms |
|---|---|---|---|
| App created → Synced + Healthy (median, range) | 0.46 s (0.46–1.1) | 0.46 s (0.45–2.86) | 0.46 s (0.45–0.46) |
| Commit + refresh → Synced on it | 0.39 s (0.39–0.39) | 0.68 s (0.39–0.68) | 0.39 s (0.39–0.39) |
| Deployment deleted → healed, Healthy | 0.43 s (0.43–0.44) | 0.77 s (0.77–0.77) | 0.43 s (0.43–0.43) |
| 19 apps at once → all Synced + Healthy | 5.72 s | 3.28 s | 3.27 s |
| Memory, 20 apps idle (working set / anonymous) | 310.2 / 225.0 MiB | 6.9 / 4.1 MiB | 7.6 / 5.4 MiB |
| CPU idle, 20 apps | 0.382 % | 0.147 % | 0.164 % |

Single syncs and heals now land on the same poll as Argo CD's; the burst,
memory and idle CPU stay where they were (the memory and CPU differences
are within run-to-run noise and the larger binary).

## Reproduce

```bash
export LITEK8S_BIN=/path/to/litek8s   # 0.2.8 here
compat/cluster.sh up && eval "$(compat/cluster.sh env)"
DOCKER_HOST=unix://$HOME/.litek8s-litecd/compat/docker.sock deploy/build.sh
python3 bench/bench.py --target litecd
kubectl delete -f deploy/install/crds    # Argo CD's chart brings its own CRDs
python3 bench/bench.py --target argocd --chart argo-cd-10.10.5.tgz
python3 bench/report.py bench/results/<date>/argocd.json bench/results/<date>/litecd.json
```
