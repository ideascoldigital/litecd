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
| Git | the `git` CLI, as Argo CD's repo server runs it (auth, protocols and odd servers behave the same; `gix` would save the binary in the image), a bare cache per repository, trees by commit, manifests cached by (revision, path, params) | `/api/webhook` with GitHub, Gitea and GitLab payloads (same path as argocd-server), polling as in `timeout.reconciliation` |
| Render: directory | native: YAML/JSON, `recurse`, `include`, `exclude` | jsonnet in phase 2 (`jrsonnet`) |
| Render: Helm, Kustomize | run `helm template` / `kustomize build`, as Argo CD's repo server does | identical output; the image ships both binaries. A native Go-template + sprig engine is out of scope |
| Helm repositories | `index.yaml`, OCI, chart cache | `values`, `valuesObject`, `valueFiles`, `parameters`, `releaseName`, `skipCrds`, multi-source `$ref` values |
| Diff | server-side apply dry-run against the live object, Argo CD's normalizations | `ignoreDifferences`: `jsonPointers`, `jqPathExpressions` (`jaq`), `managedFieldsManagers` |
| Sync | phases PreSync/Sync/PostSync/SyncFail, waves, hooks and `hook-delete-policy`, prune, `.operation`, retry with backoff | sync options in `src/check.rs`; field manager `argocd-controller` so ownership carries over |
| Tracking | annotation `argocd.argoproj.io/tracking-id` (Argo CD v3 default) or label `app.kubernetes.io/instance`, from `argocd-cm` `application.resourceTrackingMethod` | resources synced by Argo CD stay owned after the switch |
| Health | built in: Deployment, StatefulSet, DaemonSet, ReplicaSet, Pod, Job, PVC, Service, Ingress, APIService, HPA, Application | Lua: `resource.customizations.health.*` in `argocd-cm` and Argo CD's `resource_customizations` library vendored (Apache-2.0, NOTICE), on Lua 5.1 (`mlua`), as gopher-lua is 5.1 |
| Projects | `sourceRepos`, `destinations`, cluster/namespace resource allow and deny lists | sync windows in phase 3 |
| Clusters | in-cluster; Secrets `argocd.argoproj.io/secret-type=cluster` (bearer token, TLS) in phase 3 | |
| Coexistence | litecd acts on nothing while an Argo CD application controller runs in the cluster (its StatefulSet, Ready), unless an Application carries `litecd.dev/manage: "true"` | moving over: uninstall Argo CD's workloads, keep its CRDs |

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
2. **Coverage:** hooks, multiple sources, `ignoreDifferences`, Kustomize
   options, Argo CD's bundled Lua health library, PruneLast and
   ApplyOutOfSyncOnly (done 2026-10-24: the corpus passes 24/24); still to
   do: jsonnet, FailOnSharedResource, jq expressions beyond paths and
   `select`, error conditions as Argo CD reports them.
3. **ApplicationSet** (list, git directories/files, clusters, matrix, merge;
   fasttemplate and goTemplate), other clusters, apps in any namespace, sync
   windows; a bench phase against Argo CD on litek8s and k3s.
4. **UX:** an Applications view (tree, diff, sync) in `litek8s ui` and MCP
   tools, both through the Kubernetes API only; the `argocd` CLI's gRPC API if
   people ask for it.

## Risks

- Argo CD's diff and health semantics in the details (defaulted fields,
  status, the order of waves with hooks): the corpus of phase 0 is there to
  find them before they reach users.
- `helm` and `kustomize` stay Go binaries in the image on purpose: they
  guarantee the same output. Replacing them is not planned.
- Names: litecd does not use "Argo" in its name, logo or image names (CNCF
  trademark); it says "reads Argo CD manifests".
