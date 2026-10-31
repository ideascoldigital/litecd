<p align="center"><img src="docs/brand/litecd.svg" width="112" alt="litecd logo"></p>

# litecd

**GitOps for small clusters: your Argo CD Applications, synced by one Rust process instead of seven.**

On a 2 GiB machine with one synced app, the whole Kubernetes control plane
takes 90 MiB and Argo CD's seven pods take 359. GitOps costs four times what
Kubernetes does. litecd is the fix for home labs, edge boxes and small
clusters ([litek8s](https://github.com/jairoFernandez/litek8s), k3s, kind):
it reads Argo CD's `Application`, `AppProject` and `ApplicationSet`
manifests unchanged and writes the same `.status`. Your GitOps repo, your
`kubectl get applications` and your webhooks keep working, and Argo CD's
web UI can stay.

```bash
curl -fsSL https://raw.githubusercontent.com/ideascoldigital/litecd/main/install.sh | sh
```

How it went on a production cluster, including what did not go to plan:
[Replacing Argo CD's controller with litecd: 666 MiB to 100, and what it cost](https://litek8s.dev/blog/replacing-argo-cd-with-litecd/)
([en español](https://litek8s.dev/es/blog/reemplazar-argo-cd-con-litecd/)).
Docs: [GitOps with litecd](https://litek8s.dev/docs/litecd/).

## The numbers

| | Argo CD | litecd |
|---|---:|---:|
| Memory, idle (all Argo CD pods vs litecd) | 359 MiB | **8–9 MiB** |
| 20 Applications, working set ([bench](bench/RESULTS.md)) | 310 MiB | **6.9 MiB** |
| Install → ready | 28.7 s | **0.6 s** |
| 19 apps synced at once | 5.7 s | **3.3 s** |
| litek8s GitOps platform (12 apps, Helm, Kustomize, app of apps, CI) | 723 MiB | **28 MiB** |
| Production k3s, 59 apps, ~690 resources: application controller | 666 MiB | **~100 MiB** |
| Same cluster, Argo CD's namespace (UI kept) | 1,322 MiB | **594 MiB** |

Why the gap: Argo CD's controller watches every resource type the cluster
serves and keeps it all in memory, across seven processes with their own
caches. litecd watches only the types your apps use, drops objects it does
not track, and runs as one process. It shells out to git, helm and
kustomize as Argo CD's repo server does, so renders come out the same.

Single syncs and self-heals are as fast as Argo CD's. The platform run is
reproducible with `e2e/platform.sh` (a patch on a copy of litek8s's
`examples/platform`, whose whole smoke test passes with litecd in Argo CD's
place).

## Why litecd

- **Drop-in.** Same CRDs, same tracking ids, same field manager, same
  `.status`. Nothing in your GitOps repo changes, and going back to Argo CD
  is the same steps in reverse.
- **Safe to try.** `litecd check` tells you up front whether your manifests
  are supported. `--observe` runs beside Argo CD and logs where it would
  disagree, writing nothing. Installed next to Argo CD, litecd stands down
  until you scale Argo CD's controller to 0.
- **Proven against the real thing.** All 38 cases of the
  [compatibility corpus](compat/) pass, with expected results taken from
  Argo CD v3.5.5 itself. On a production k3s cluster, observe mode agreed
  with Argo CD on 58 of 59 apps before the switch, and deploys kept rolling
  after it.
- **One process.** No repo server, no Redis, no Dex, no API server to run.

## What you give up

- **No web UI of its own yet.** Argo CD's UI keeps working for lists, sync
  and health, but its resource tree, resource details and Diff tab stay
  empty without Argo CD's controller
  ([a deprecated mode](#deprecated-compatibility-argo-cds-web-ui) fills
  them). `litek8s ui` shows apps, resources and diffs from any
  cluster.
- **No SSO, notifications or `argocd` CLI** in litecd itself; keep Argo
  CD's for those, or go without.
- **Memory grows with what it renders.** 7 MiB in the lab, ~100 MiB with 59
  apps and Helm umbrellas in production, then flat. Six times less than the
  controller it replaced, not fifty.
- **Charts with random values** (`randAlphaNum` in a Secret) sync once per
  restart: Argo CD hides them with its manifest cache.

## What it does

- **Sources:** git directories, Helm charts (git or Helm repository),
  Kustomize (with its options), jsonnet; single or multiple sources with
  `$ref` value files; app of apps.
- **Sync:** automated sync, prune, self-heal, retries, sync waves, hooks
  (PreSync, Sync, PostSync, SyncFail, Helm install/upgrade hooks, delete
  policies), PruneLast, ApplyOutOfSyncOnly, Replace, server-side apply,
  shared-resource detection (FailOnSharedResource).
- **Diff:** `ignoreDifferences` (JSON pointers, jq paths, field managers;
  per app and from argocd-cm).
- **Health:** built-in checks, Argo CD's own Lua library, custom Lua from
  argocd-cm.
- **ApplicationSets:** list, clusters, git (directories and files), matrix
  and merge generators; fasttemplate and goTemplate.
- **Multi-cluster and multi-tenant:** destinations from Argo CD's cluster
  Secrets, Applications outside Argo CD's namespace
  (`application.namespaces`, `sourceNamespaces`), AppProject sync windows.
- **Webhooks:** `/api/webhook` for GitHub, Gitea, Gogs and GitLab behind a
  Service named `argocd-server`, so hooks made for Argo CD keep working.
  Signatures are checked as Argo CD checks them, against `argocd-secret`'s
  `webhook.github.secret`, `webhook.gogs.secret` (Gitea and Gogs) and
  `webhook.gitlab.secret`; a provider without a secret takes any push.

**Status:** phases 1 to 3 done; phase 4 (UX, litecd's own UI) next. Plan
and scope: [docs/spec.md](docs/spec.md).

## Install

On the cluster of your current kube context, from the latest release:

```bash
curl -fsSL https://raw.githubusercontent.com/ideascoldigital/litecd/main/install.sh | sh
```

[install.sh](install.sh) picks one of the release's two manifests, checks
it against the release's `SHA256SUMS`, applies it with server-side apply
and waits for litecd to be ready; run it again to upgrade.
`LITECD_VERSION=v0.2.2` pins a release.

- **`install.yaml`**, on a cluster without Argo CD: Argo CD's own CRDs
  (unchanged), RBAC, `argocd-cm`, the litecd Deployment and a Service named
  `argocd-server` for webhooks, in the namespace `argocd` (created if
  missing).
- **`takeover.yaml`**, when Argo CD is installed in `argocd` (its
  application controller, repo server or API server is there): only
  litecd's ServiceAccount, RBAC and Deployment. Argo CD's CRDs, ConfigMaps
  and `argocd-server` Service stay with Argo CD's install; litecd stands
  down until you scale Argo CD's controller to 0
  ([Moving over from Argo CD](#moving-over-from-argo-cd)).

Neither holds a Namespace, so `kubectl delete -f` on one leaves your
Applications alone. Without the script:

```bash
kubectl create namespace argocd
kubectl apply --server-side -f https://github.com/ideascoldigital/litecd/releases/latest/download/install.yaml
```

From a checkout, with the image built on the Docker API in `DOCKER_HOST`
(on litek8s it lands in the cluster's own image store):

```bash
deploy/build.sh                     # litecd:dev
kubectl create namespace argocd
kubectl apply --server-side -R -f deploy/install
```

## Moving over from Argo CD

litecd keeps Argo CD's resource tracking and field manager, so what Argo CD
synced stays owned after the switch, and going back is the same steps the
other way.

litecd will not fight Argo CD for an app. While Argo CD's application
controller runs in litecd's namespace (`argocd-application-controller`, by
name or `app.kubernetes.io/name`, as a StatefulSet or a Deployment, from the
moment it has replicas until its last pod is gone), litecd writes nothing
for an Application: no status, sync, prune, finalizer removal, diff Secret
or webhook refresh, and it creates no default AppProject. It logs when it
stands down and when it takes over, and takes over as soon as the controller
is scaled to 0 and its pods are gone. An Application annotated
`litecd.dev/manage: "true"` is litecd's either way; Argo CD keeps
reconciling it too, so annotate only apps you mean both to act on (a test
app, or one Argo CD is told to leave alone). ApplicationSets
also wait for Argo CD's applicationset controller
(`argocd-applicationset-controller`) to be gone, since both write the
Applications they generate.

1. **Check the manifests:** `litecd check -f <gitops repo>` lists the
   features they use and whether litecd supports each.
2. **Observe:** `--observe` (`LITECD_OBSERVE=true`) compares every
   Application as litecd would and logs where its status differs from the
   one Argo CD stored, and writes nothing (no status, sync, prune,
   finalizer, ApplicationSet or webhook refresh). It runs beside Argo CD,
   from a laptop with a kubeconfig or in the cluster; in the cluster its
   renders match Argo CD's (same DNS, same Helm major), so differences there
   are real:

   ```bash
   litecd controller --observe --cache-dir /tmp/litecd-observe
   ```

   A chart that renders random values (`randAlphaNum` in a Secret) differs
   on every render: Argo CD hides it by caching manifests, litecd syncs it
   once per restart.
3. **Install litecd** with install.sh (it applies `takeover.yaml`): it
   stands down.
4. **If Argo CD manages itself** (an app of apps whose children install
   Argo CD), turn off automated sync on those apps first, while Argo CD
   still runs, or self-heal scales its controller back up:

   ```bash
   kubectl -n argocd patch application <app> --type merge -p '{"spec":{"syncPolicy":{"automated":null}}}'
   ```

5. **Scale Argo CD's application controller to 0** and wait for its pod to
   go; litecd logs that it takes over:

   ```bash
   kubectl -n argocd scale statefulset/argocd-application-controller --replicas=0
   kubectl -n argocd logs deploy/litecd | grep coexistence
   ```

6. **Keep or drop the rest.** argocd-server, its repo server and Redis can
   keep serving Argo CD's web UI and its webhook (which sets the refresh
   annotation litecd honours); the notifications, ApplicationSet (without
   ApplicationSets) and Dex workloads can go to 0 if unused. The UI's
   resource tree stays empty without Argo CD's controller (see
   [below](#deprecated-compatibility-argo-cds-web-ui)).

Rollback: scale litecd to 0 and wait for its pod to go, scale Argo CD's
workloads back (repo server before the controller), then turn automated
sync back on (children before the root app). Take a snapshot of the
Applications and of Argo CD's replica counts before step 4.

On a production k3s cluster (6 nodes, 59 Applications, ~690 managed
resources, Helm, Kustomize, hooks, waves, `ignoreDifferences`, an Argo CD
that manages itself): observe agreed with Argo CD on 58 of 59 apps (the
other renders a random Secret); after the switch Argo CD's namespace went
from 1,322 MiB to 594 MiB with argocd-server and its UI kept, the
application controller's 666 MiB replaced by litecd's ~100 MiB; deploys
went through litecd with rollouts available throughout and no memory
pressure on the nodes.

## Deprecated compatibility: Argo CD's web UI

**Born deprecated.** litecd has no web UI of its own yet; until it does,
this mode lets a cluster keep Argo CD's, and then it goes away. Nothing
else in litecd depends on it.

Some clusters keep Argo CD's web UI (argocd-server, its repo server and
Redis) running with litecd in place of Argo CD's application controller.
Lists, sync and health work there as they are, but the UI's resource tree
(Deployments, ReplicaSets, Pods), a resource's details and the Diff tab stay
empty: argocd-server reads them from a Redis cache only Argo CD's controller
fills. A compatibility mode fills it, Secrets masked as Argo CD masks them:

```bash
# image ghcr.io/<owner>/litecd:vX.Y.Z-argocd-ui, or:
LITECD_FEATURES=argocd-ui-compat deploy/build.sh litecd:dev-argocd-ui
litecd controller --argocd-ui-cache redis://argocd-redis-ha-haproxy:6379
kubectl apply -f deploy/argocd-ui-compat/networkpolicy.yaml  # Argo CD's chart lets only its pods reach Redis
```

The password comes from the Secret `argocd-redis` (key `auth`). It costs
about 4 MiB for 20 apps with 40 pods. The mode is
**deprecated from birth**: it is not in the default build or image, it is
off unless `--argocd-ui-cache` (`LITECD_ARGOCD_UI_CACHE`) is set, it logs a
warning when it starts, it is coupled to Argo CD's internal cache format
(it writes nothing when it cannot tell that format is the one it knows),
and it will be removed once litecd has its own UI, with an app's resources
computed on demand from the Kubernetes API. The Diff tab may show fields
Argo CD would hide as scheme defaults. Purpose, coupling, cost and
removal: [docs/spec.md, Deprecated compatibility](docs/spec.md#deprecated-compatibility).

## Check your manifests

`litecd check` reads your manifests and tells you which features they use
and in which phase litecd supports each:

```bash
cargo run -- check -f path/to/gitops
```

litecd is not affiliated with the Argo project; "Argo" and "Argo CD" are
trademarks of The Linux Foundation.

## License

Apache-2.0.
