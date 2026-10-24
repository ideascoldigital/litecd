# litecd

GitOps continuous delivery in one small Rust binary that reads Argo CD's
`Application`, `AppProject` and `ApplicationSet` manifests unchanged. Built
for small clusters (home labs, edge, [litek8s](https://github.com/jairoFernandez/litek8s)),
where Argo CD's seven pods use more memory than the orchestrator itself.

**Status: phase 1.** The controller syncs Applications and AppProjects
from git directories, Helm charts (in git or a Helm repository) and Kustomize,
with automated sync, prune, self-heal, sync waves, retries, the usual sync
options, built-in and Lua health checks and the app-of-apps pattern, and
writes the same `.status` Argo CD does: it passes all 16 phase 1 cases of the
compatibility corpus ([compat/](compat/)), whose expected results come from
Argo CD v3.5.5. The plan is in [docs/spec.md](docs/spec.md).

On litek8s the controller process takes 8–9 MiB of RSS through the whole
corpus and next to no CPU idle; Argo CD's pods take 359 MiB idle on the same
kind of cluster. litek8s's GitOps platform example (`examples/platform`,
`CD_CONTROLLER=litecd`: 12 Applications in waves, Helm charts, Kustomize,
an app of apps that manages itself, Gitea webhooks) comes up with litecd in
Argo CD's place and passes its whole smoke test, at 28 MiB.

Pushes: litecd answers `/api/webhook` (GitHub, Gitea, Gogs, GitLab) behind
a Service named `argocd-server`, so webhooks made for Argo CD keep working.

## Install

```bash
deploy/build.sh                     # litecd:dev on the Docker API in DOCKER_HOST
kubectl apply --server-side -R -f deploy/install
```

`deploy/install` holds Argo CD's own CRDs (unchanged), RBAC, `argocd-cm`
and the litecd Deployment in the `argocd` namespace. Do not run it next to
Argo CD's application controller: uninstall Argo CD's workloads and keep its
CRDs, Applications and Secrets.

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
