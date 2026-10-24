# Compatibility corpus

What litecd must do, measured against Argo CD. Every case is run against
Argo CD (the baseline) and its outcome recorded in `golden/<case>.json`;
litecd passes a case when its own checks pass and its snapshots equal the
golden file.

```bash
compat/cluster.sh up                         # a litek8s cluster of its own (optional)
eval "$(compat/cluster.sh env)"
compat/run.sh --target argocd --update-golden  # write golden/ from Argo CD
compat/run.sh --target argocd --no-install     # the baseline is reproducible
compat/run.sh --target litecd --phases 1       # litecd against golden/ (from phase 1)
```

Any cluster in the current kubeconfig works; `cluster.sh` starts the
installed litek8s with its own `LITEK8S_HOME` (`~/.litek8s-litecd`), so the
runs never touch the user's own clusters. Needs kubectl, helm, git, perl,
python3. Results: `results/<timestamp>-<target>/` (`summary.md`, a log and
the snapshots of every case), not committed.

## How a case works

`cases/<id>/` holds:

- `repo/`: the content of the case's git repository, pushed as its first
  commit to the corpus's git server (`git/`: `git http-backend` behind a small
  Python server, plus a Helm repository under `/charts/`). Commits are made
  with fixed authors and dates, so a case's revisions are the same on every
  run and in the golden files.
- `app.yaml` (and others): Applications, AppProjects and Secrets the case
  applies, with `<REPO_URL>`, `<CHART_REPO>`, `<NS>` and `<CASE>` filled in
  (in `repo/` as well, for child Applications).
- `case.sh`: `desc`, `phase` (the litecd phase that must pass it, from
  docs/spec.md), optional `timeout`, and `t_case`, which uses `lib.sh`:
  `repo_init`, `repo_commit`, `publish_chart`, `app_apply`, `app_sync` (an
  `operation`, as the CLI and UI start a sync), `app_refresh`, `wait_app`,
  `wait_rev`, `wait_op`, and `snapshot <label> <app>...`.

A snapshot (`snapshot.py`) waits until the apps' status has been still for
5 s, then records each Application's status, operation, annotations and
finalizers, and the tracking metadata on every resource it manages
(`argoproj.io` labels and annotations, whether `last-applied-configuration`
is there, `argocd-*` field managers). Times are removed; the case's
namespace and repository URL become `<NS>` and `<REPO_URL>`.

Not compared, because Argo CD itself varies from run to run: right after a
sync its cache may not have seen the watch events of what it just applied,
so it sometimes self-heals at once with nothing to change (a subset of the
resources, "unchanged"), and a first apply sometimes needs a retry. Of the
last operation the snapshot keeps its phase, message, initiator, revision
and options, its hooks, the resources that failed (with their messages) and,
when it did not succeed, its retry count; what synced is in
`status.resources`.

A case can leave the operation state out of one snapshot
(`snapshot --skip-operation-state`) when Argo CD's own last operation there
is an artifact of that race every time: prune_last, where the PruneLast
prune is the last thing a sync does and an empty self-heal follows it.

Golden files are Argo CD on litek8s: messages that come from the API server
(a rejected object, a Deployment's progress deadline) are litek8s's wording,
so litecd is compared on the same kind of cluster.

## Cases

| case | phase | what |
|---|---|---|
| directory_plain | 1 | plain YAML directory, automated sync, tracking annotations |
| directory_recurse | 1 | `recurse`, `include` (YAML and JSON), `exclude` |
| helm_git_chart | 1 | chart in git: `releaseName`, `valuesObject`, `parameters` precedence |
| helm_repo_chart | 1 | chart from a Helm repository, pinned version, `values` string |
| kustomize_autodetect | 1 | overlay detected by `kustomization.yaml`, generator hash suffix |
| sync_waves | 1 | waves -1, 0, 1, 5 in order |
| prune_selfheal | 1 | prune a removed file; heal a deleted and a scaled Deployment |
| no_prune | 1 | automated without prune: OutOfSync, resource kept |
| manual_sync | 1 | no automation: OutOfSync + Missing, then an `operation` |
| sync_options | 1 | `Prune=false`, `Replace=true`, `Validate=false` |
| server_side_apply | 1 | `ServerSideApply=true`, field manager `argocd-controller` |
| degraded_health | 1 | a failed Job → Degraded, sync still Succeeded |
| app_of_apps | 1 | parent syncs child Applications in waves |
| app_of_apps_lua_health | 1 | Lua health for Applications from `argocd-cm` holds the next wave |
| sync_failure_retry | 1 | API server rejects an object: Failed after 2 retries |
| project_restrictions | 1 | source repository not permitted: `InvalidSpecError` |
| hooks | 2 | PreSync/PostSync Jobs with `HookSucceeded` |
| ignore_differences | 2 | `/spec/replicas` ignored with `RespectIgnoreDifferences` |
| multi_source | 2 | chart from a Helm repository, values file from git (`$values`) |

## litecd

2026-10-24, litecd with phase 1 and most of phase 2 on a fresh cluster
(Argo CD's CRDs only): 24/24 cases equal their golden files, twice in a
row. The five newest cases' golden files were written from Argo CD the
same day and reproduced twice before litecd ran them.

```bash
deploy/build.sh && LITECD_MANIFEST=deploy/install compat/run.sh --target litecd
```

## Baseline

2026-10-23, Argo CD v3.5.5 (chart 10.10.5) on litek8s 0.2.8 (`cluster.sh`,
macOS VM): golden files written, then three runs against them, 19/19 each
(one after a reboot of the machine). A run takes about 5 minutes.

What the baseline found in litek8s 0.2.8, worked around in the cases and
fixed in litek8s separately:

- a Deployment past `progressDeadlineSeconds` never gets
  `ProgressDeadlineExceeded`, so Argo CD keeps it Progressing
  (degraded_health uses a failed Job);
- built-in objects are barely validated: a Deployment without a selector, a
  Service port 70000, a Pod without containers, a ConfigMap named `Bad_Name`
  are all accepted (sync_failure_retry uses an invalid ConfigMap key, which
  litek8s does reject).

And in the harness, what litecd must copy from Argo CD: a sync started with
an `operation` that names no sync options gets the app's
`spec.syncPolicy.syncOptions` (Argo CD's API server copies them; without
them `CreateNamespace=true` is lost).
