# litecd

GitOps continuous delivery in one small Rust binary that reads Argo CD's
`Application`, `AppProject` and `ApplicationSet` manifests unchanged. Built
for small clusters (home labs, edge, [litek8s](https://github.com/jairoFernandez/litek8s)),
where Argo CD's seven pods use more memory than the orchestrator itself.

**Status: phases 1 to 3 done; phase 4 (UX) next.** The
controller syncs Applications and
AppProjects from git directories, Helm charts (in git or a Helm repository)
and Kustomize, single or multiple sources (`$ref` value files), with
automated sync, prune, self-heal, sync waves, hooks (PreSync, Sync,
PostSync, SyncFail, Helm's install/upgrade hooks, delete policies),
`ignoreDifferences` (JSON pointers, jq paths, field managers; from the app
and argocd-cm), Kustomize options, jsonnet, shared-resource detection
(FailOnSharedResource), retries, the usual sync options
(PruneLast and ApplyOutOfSyncOnly among them), built-in health checks, Argo
CD's own Lua library of them and Lua ones from argocd-cm, and the
app-of-apps pattern, and writes the same `.status` Argo CD does. Its
ApplicationSet controller has the list, clusters, git (directories and
files), matrix and merge generators, fasttemplate and goTemplate; and
destinations in other clusters come from Argo CD's cluster Secrets.
Applications can live outside Argo CD's namespace
(`application.namespaces`, the project's `sourceNamespaces`), and projects'
sync windows hold automated and manual syncs as Argo CD's do. It
passes all 38 cases of the compatibility corpus
([compat/](compat/)), whose expected results come from Argo CD v3.5.5. The plan is in [docs/spec.md](docs/spec.md).

On litek8s the controller process takes 8–9 MiB of RSS through the whole
corpus and next to no CPU idle; Argo CD's pods take 359 MiB idle on the same
kind of cluster. litek8s's GitOps platform example (12 Applications in
waves, Helm charts, Kustomize, an app of apps that manages itself, Gitea
webhooks, CI, Atlantis) comes up with litecd in Argo CD's place and passes
its whole smoke test, at 28 MiB against Argo CD's 723 MiB:
`e2e/platform.sh` (a patch on a copy of litek8s's `examples/platform`;
litek8s itself does not know litecd).

Side by side on one litek8s cluster with 20 Applications
([bench/RESULTS.md](bench/RESULTS.md)): 6.9 MiB of working set against
Argo CD's 310 MiB (4.1 against 225 MiB anonymous), 19 apps at once
synced in 3.3 s against 5.7 s, single syncs and heals as fast.

Pushes: litecd answers `/api/webhook` (GitHub, Gitea, Gogs, GitLab) behind
a Service named `argocd-server`, so webhooks made for Argo CD keep working.
Signatures are checked as Argo CD checks them, against `argocd-secret`'s
`webhook.github.secret`, `webhook.gogs.secret` (Gitea and Gogs) and
`webhook.gitlab.secret`; a provider without a secret takes any push.

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
(Deployments, ReplicaSets, Pods) stays empty: argocd-server reads it from a
Redis cache only Argo CD's controller fills. A compatibility mode fills it:

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
computed on demand from the Kubernetes API. Argo CD's Diff tab stays empty
even with it (it reads another cache of Argo CD's controller). Purpose, coupling, cost and
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
