# litecd: plan

GitOps continuous delivery in one small Rust binary that reads Argo CD's
manifests unchanged: `Application`, `AppProject` and `ApplicationSet`
(`argoproj.io/v1alpha1`), repository and cluster Secrets, `argocd-cm`. A team
moves to litecd without rewriting a manifest and back to Argo CD the same way.

## Why

On litek8s's Argo CD bench (litek8s repository, `bench/RESULTS.md`, section
"Argo CD on k3s and on litek8s", 2 vCPU / 2 GiB, Argo CD v3.5.5 chart
defaults, one synced app, idle) the whole litek8s orchestrator takes 90 MiB
and Argo CD's seven pods 359 MiB. Where Argo CD's cost comes from:

- the application controller mirrors every resource type the cluster serves
  (79 watches on litek8s, 103 on k3s) and keeps the whole cluster in memory;
- seven processes, each with its own Go runtime and caches: application
  controller, repo server, API server, ApplicationSet controller,
  notifications, Dex, Redis (Redis only shares generated manifests between
  them; Dex alone was 93 MiB);
- without a webhook an app is looked at every 180 s (`timeout.reconciliation`)
  plus jitter, and self-heal backs off.

## Scope

**Compatible:** the CRDs as Argo CD publishes them (installed unchanged), every
`spec` field litecd supports behaving as in Argo CD, the same annotations and
labels on managed resources, the same `.status` shape (`sync`, `health`,
`operationState`, `resources[]`, `history`, `conditions`, `reconciledAt`,
`summary`), `.operation` to start a sync, the repository/cluster Secrets and the
`argocd-cm` keys it reads. Tools that read or write Applications through the
Kubernetes API (kubectl, scripts, Kargo, CI) keep working.

**Not in v1:** Argo CD's web UI and the gRPC API the `argocd` CLI uses (phase
4 decides), SSO/Dex, notifications controller, config management plugins,
ApplicationSet progressive sync, SCM provider and pull request generators.

**Not litek8s-specific:** litecd uses only the public Kubernetes API (list,
watch, server-side apply, dry-run) and runs on any conformant cluster. litek8s
is the first target and the bench's reference; k3s (k3d) is the baseline in
CI. It shares no code with litek8s.

`litecd check -f <dir>` reports, for a set of manifests, which features they
use and in which phase litecd supports each (`src/check.rs`: the table there
is the source of truth for the phases below).

## Design

One binary, one Deployment (one replica; a Lease when there are more).

| piece | how | notes |
|---|---|---|
| Kubernetes client | `kube-rs`, typed CRDs for the argoproj.io kinds | |
| Cache | lazy informers: only the resource types that appear in some app's desired manifests; objects not carrying a litecd/Argo tracking id dropped on arrival | the main memory difference from Argo CD, which mirrors the cluster |
| Git | the `git` CLI, as Argo CD's repo server runs it (auth, protocols and odd servers behave the same; `gix` would save the binary in the image), a bare cache per repository, trees by commit, manifests cached by (revision, path, params); `repository`/`repo-creds` Secrets with username/password or `sshPrivateKey` (host keys from `argocd-ssh-known-hosts-cm`, unchecked when `insecure`) | `/api/webhook` with GitHub, Gitea and GitLab payloads (same path as argocd-server), polling as in `timeout.reconciliation` |
| Render: directory | native: YAML/JSON, `recurse`, `include`, `exclude` | jsonnet in phase 2 (`jrsonnet`) |
| Render: Helm, Kustomize | run `helm template` / `kustomize build`, as Argo CD's repo server does | identical output; the image ships both binaries. A native Go-template + sprig engine is out of scope |
| Helm repositories | `index.yaml`, OCI, chart cache | `values`, `valuesObject`, `valueFiles`, `parameters`, `releaseName`, `skipCrds`, multi-source `$ref` values |
| Diff | server-side apply dry-run against the live object, Argo CD's normalizations | `ignoreDifferences`: `jsonPointers`, `jqPathExpressions` (`jaq`), `managedFieldsManagers` |
| Sync | phases PreSync/Sync/PostSync/SyncFail, waves, hooks and `hook-delete-policy`, prune, `.operation`, retry with backoff | sync options in `src/check.rs`; field manager `argocd-controller` so ownership carries over |
| Tracking | annotation `argocd.argoproj.io/tracking-id` (Argo CD v3 default) or label `app.kubernetes.io/instance`, from `argocd-cm` `application.resourceTrackingMethod` | resources synced by Argo CD stay owned after the switch |
| Health | built in: Deployment, StatefulSet, DaemonSet, ReplicaSet, Pod, Job, PVC, Service, Ingress, APIService, HPA, Application | Lua: `resource.customizations.health.*` in `argocd-cm` and Argo CD's `resource_customizations` library vendored (Apache-2.0, NOTICE), on Lua 5.1 (`mlua`), as gopher-lua is 5.1 |
| Projects | `sourceRepos`, `destinations`, cluster/namespace resource allow and deny lists, `sourceNamespaces`, sync windows (cron schedule in a time zone, duration, apps/namespaces/clusters with OR or AND, `manualSync`, `syncOverrun`) | a held sync stays Running with Argo CD's message until a refresh finds the window open; like Argo CD, a project change does not refresh its apps |
| Apps in any namespace | `application.namespaces` in argocd-cmd-params-cm (globs, `/regex/`; `--application-namespaces` when it has none), read live; resources tracked as `<namespace>_<app>` | ApplicationSets stay in the controller's namespace |
| Clusters | in-cluster; Secrets `argocd.argoproj.io/secret-type=cluster` (bearer token, username/password, TLS: insecure, caData, certData/keyData, serverName), a client per server cached by the Secret's resourceVersion | the destination is resolved before the project check, which sees its server |
| Coexistence | litecd acts on nothing (status, operation, sync, prune, finalizer, diff Secret, webhook refresh, default AppProject) for an Application while Argo CD's application controller runs in the controller's namespace, unless the Application carries `litecd.dev/manage: "true"`; ApplicationSets also wait for Argo CD's applicationset controller. A controller runs from the moment it has replicas until its last pod is gone (StatefulSet or Deployment `argocd-application-controller` / `argocd-applicationset-controller`, by name or `app.kubernetes.io/name`; spec.replicas, status.replicas or readyReplicas above 0), watched, so litecd takes over and stands down at once and logs both; before both watches have listed, it stands down. A sync under way stops before its next write (no SyncFail hooks, no hook cleanup) | moving over: uninstall Argo CD's workloads, keep its CRDs; `src/coexist.rs`; compat case `coexistence` (litecd only, a stand-in StatefulSet) |

## Phases

0. **Compatibility corpus** (done 2026-10-23, 19 cases) (`compat/`): each case is a set of manifests and
   expected outcomes, run against Argo CD (the baseline, as litek8s's
   `compat/baseline-k3d.sh`: every case must pass there) and against litecd,
   comparing the resulting objects and the Applications' status. Sources:
   `argoproj/argocd-example-apps`, Argo CD's e2e fixtures (Apache-2.0),
   litek8s's `examples/platform` (13 Applications: sync waves, Helm
   `valuesObject`, `Replace=true`, the refresh annotation, a repository Secret,
   a Gitea webhook).
1. **MVP** (done 2026-10-23: 16/16 corpus cases): Application and AppProject; directory, Helm and Kustomize
   sources (`examples/platform` has Kustomize directories Argo CD detects by
   their `kustomization.yaml`); sync,
   prune, self-heal, waves, retry, `.operation`, refresh; built-in health and
   Lua health checks from `argocd-cm` (`examples/platform` makes its app of
   apps wait on child Applications with one); status as Argo CD writes it. Done when `examples/platform` comes up with
   litecd in place of Argo CD, all apps Synced + Healthy, and litecd idles
   under 40 MiB.
2. **Coverage** (done 2026-10-24: the corpus passes 26/26): hooks,
   multiple sources, `ignoreDifferences`, Kustomize options, jsonnet
   (`jsonnet` binary, extVars, tlas, libs), Argo CD's bundled Lua health
   library, PruneLast, ApplyOutOfSyncOnly, shared resources
   (SharedResourceWarning, FailOnSharedResource). Left for later: jq
   expressions beyond paths and `select`, more of Argo CD's error
   conditions, word for word.
3. **ApplicationSet** (list, git directories/files, clusters, matrix, merge;
   fasttemplate and goTemplate with sprig's common functions: done
   2026-10-24, 7 corpus cases), other clusters (cluster Secrets: bearer
   token, TLS, by name or server, the clusters generator's selector: done
   2026-10-24, 3 cases), apps in any namespace and sync windows (done
   2026-10-24, 2 cases); the bench against Argo CD on litek8s
   (bench/RESULTS.md).
4. **UX:** an Applications view (tree, diff, sync) in `litek8s ui` and MCP
   tools, both through the Kubernetes API only; the `argocd` CLI's gRPC API if
   people ask for it. The diff: the controller publishes each app that is not
   in sync as a Secret `<app>.diff` in the app's namespace (label
   `gitops.litek8s.dev/diff=true`, owned by the Application, key `diff.json`:
   `{revisions, resources: [{group, version, kind, namespace, name, status,
   desired, live, omitted?}]}`, `live` cut to the fields git and last-applied
   set, Secret values masked, the biggest resources `omitted` past 900 KiB),
   written when it changes, deleted once the app is in sync (done 2026-10-26).

## Deprecated compatibility

Modes that exist only so litecd can sit in clusters that keep parts of Argo
CD. Each is deprecated from the day it lands, behind a Cargo feature that is
off by default (the default build and image do not contain it), off at run
time unless asked for, in a module of its own that nothing else depends on,
and logged as deprecated when it starts.

### Argo CD's web UI: resource trees in Redis (`argocd-ui-compat`)

**Purpose.** With litecd in place of Argo CD's application controller,
argocd-server (Argo CD's web UI and API) can be kept running: lists, sync
and health work, because they come from the Application objects litecd
writes. The resource tree (Deployment → ReplicaSets → Pods with health,
images and pod info, and through it pod logs and live manifests) is empty:
argocd-server does not compute it, it reads the one Argo CD's controller
keeps in Redis. This mode writes that tree.

**How.** Build with `--features argocd-ui-compat` (image tag
`vX.Y.Z-argocd-ui`) and start litecd with
`--argocd-ui-cache redis://<argocd-server's Redis>:6379`
(`LITECD_ARGOCD_UI_CACHE`; with Argo CD's HA chart,
`redis://argocd-redis-ha-haproxy:6379`). The password is the key `auth` of
the Secret `argocd-redis` in litecd's namespace (`--argocd-ui-cache-secret`;
a password in the URL wins). After each comparison the controller hands the
app's managed resources (live objects and health) to `src/compat/argocd_ui.rs`;
the module watches only the child kinds those resources need (ReplicaSets
under Deployments and Rollouts; Pods under ReplicaSets, StatefulSets,
DaemonSets and Jobs; Jobs under CronJobs), only in the namespaces of the
managed resources of such kinds, and stops a watch when no app needs it. It
builds the tree from ownerReferences, with Argo CD's node info (pod status
reason, node, containers, restarts, requests, images, labels; ReplicaSet and
Deployment revision; Service selector; `link.argocd.argoproj.io/*` links),
and writes it as Argo CD's controller does: key
`app|resources-tree|<instance>|<cache version>` plus `.gz` with gzip
compression, value the `ApplicationTree` JSON (gzipped), TTL 1 h
(`--argocd-ui-cache-ttl`), then a publish on
`app|managed-resources|<instance>|<cache version>` so an open UI reloads it.
A tree is written only when it changed and again at half its TTL, only for
apps litecd compared in the last three reconcile intervals (never for one
Argo CD's controller owns while it runs), and deleted when the app goes.
Pod logs and live manifests in the UI then go to the Kubernetes API through
argocd-server, as with Argo CD. Argo CD's chart admits only its own pods to
Redis with a NetworkPolicy: `deploy/argocd-ui-compat/networkpolicy.yaml`
admits litecd too (a connection that times out says so in the log).

**Coupling.** This is Argo CD's internal cache, not an API: key names, the
cache version (`common.CacheVersion`, "1.8.3" from v2.0 to v3.5), the
compression and the JSON shape may change in any release. litecd writes only
when the format is known: argocd-server's image is a version it was checked
against (v2.6 to v3.5) or `--argocd-ui-cache-version` names the cache
version; the compression it is started with (`--redis-compress`,
`REDIS_COMPRESSION` or the argocd-cmd-params-cm key it names) agrees with
`--argocd-ui-cache-compression` when given; every tree key already in Redis
has that format and the first one decodes. Otherwise it logs why and writes
nothing; it checks again every 5 minutes. Not written: orphaned resources,
hosts (the UI's node view), Ingress and child Application node info, other
child kinds (ControllerRevisions, EndpointSlices, PVCs of a StatefulSet),
`app|managed-resources|` (the UI's Diff tab: use the `<app>.diff` Secret),
Redis over TLS or Sentinel. Compat case `argocd_ui_tree` compares the tree
litecd writes with the one Argo CD v3.5.5 writes for the same app.

**Cost.** Measured 2026-10-30 on the compat cluster (litek8s dev cluster, one
node) with Argo CD v3.5.5's argocd-server and Redis, its application
controller scaled to 0, and litecd built with the feature: 20 Applications,
each a Deployment (one replica, a sidecar), a Service, a ConfigMap and a Job
(40 pods, 40 child watches), idle after syncing, litecd's container read
from its cgroup every 20 s over 3 to 4 minutes on a freshly started pod.
Mode off: 7 MiB anonymous, 14 to 15 MiB RSS, 8 MiB working set. Mode on:
11 MiB anonymous, 18 to 19 MiB RSS, 12 MiB working set; about 4 MiB for
the watches and the trees (each tree 1 KiB gzipped in Redis). A build with
the feature and the mode off runs no watch and opens no connection.

**Removal.** When litecd has its own view of an app's resources, computed on
demand from the Kubernetes API (phase 4: `litek8s ui`), and the clusters
that keep argocd-server for its tree have moved to it, the feature, its
image tag and this section are removed.

## Risks

- Argo CD's diff and health semantics in the details (defaulted fields,
  status, the order of waves with hooks): the corpus of phase 0 is there to
  find them before they reach users.
- `helm` and `kustomize` stay Go binaries in the image on purpose: they
  guarantee the same output. Replacing them is not planned.
- Names: litecd does not use "Argo" in its name, logo or image names (CNCF
  trademark); it says "reads Argo CD manifests".
