# litecd

GitOps continuous delivery in one small Rust binary that reads Argo CD's
`Application`, `AppProject` and `ApplicationSet` manifests unchanged. Built
for small clusters (home labs, edge, [litek8s](https://github.com/jairoFernandez/litek8s)),
where Argo CD's seven pods use more memory than the orchestrator itself.

**Status: planning.** The plan is in [docs/spec.md](docs/spec.md). Today only
`litecd check` works: it reads your manifests and tells you which features
they use and in which phase litecd supports each.

```bash
cargo run -- check -f path/to/gitops
```

```
Application argocd/argo-workflows (gitops/cluster/argo-workflows.yaml): phase 1
  phase 1      annotation argocd.argoproj.io/sync-wave
  phase 1      Helm chart from a Helm repository
  phase 1      automated selfHeal
  ...
```

`-o json` for scripts.

litecd is not affiliated with the Argo project; "Argo" and "Argo CD" are
trademarks of The Linux Foundation.

## License

Apache-2.0.
