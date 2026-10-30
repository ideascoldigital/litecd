# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is litecd

GitOps continuous delivery in one Rust binary that reads Argo CD's manifests (`argoproj.io/v1alpha1` Application, AppProject, ApplicationSet, repository/cluster Secrets, `argocd-cm`) unchanged and writes the same `.status`. Plan, scope and phases: docs/spec.md. `litecd check` (`src/check.rs`) is a static report of the features a set of manifests uses and the phase that supports each; its table is the source of truth for the phases.

`appset.rs` (ApplicationSets: generators, fasttemplate, owned Applications, status in the status subresource) and `gotemplate.rs` (goTemplate on gtmpl with sprig's common functions). `litecd controller` (phases 1 and 2: hooks, multi-source, ignoreDifferences in `ignore.rs`, Kustomize options, jsonnet via the `jsonnet` binary, PruneLast, ApplyOutOfSyncOnly, shared resources; Argo CD's Lua health library vendored in `vendor/argo-cd`, embedded by `build.rs`): `controller.rs` (one task per Application, by instance name `<ns>_<app>` outside the controller's namespace, Applications watched in every namespace and reconciled where argocd-cmd-params-cm's `application.namespaces` allows; woken by watches and timers; status written as a merge patch with Argo CD's field-presence rules; auto-sync, self-heal backoff, retries, history, finalizer; the diff of an app that is not in sync published as a Secret `<app>.diff`, docs/spec.md phase 4), `app.rs` (settings from argocd-cm, rendering cache, comparison, project checks, tracking ids), `sync.rs` (client-side dry run, waves with health waits and 2 s delay, Helm's kind order, apply/replace/server-side apply, prune), `windows.rs` (AppProject sync windows: own five-field cron, matching, can-sync with manualSync and syncOverrun; a held operation stays Running), `diff.rs` (three-way: desired subset of live + last-applied for dropped fields, a server-side apply dry run to confirm), `health.rs` (gitops-engine checks, Lua via mlua 5.1), `render.rs` (directory, `helm template`, `kustomize build`), `repo.rs` (`git` CLI, `helm pull`), `kubeapi.rs` (untyped objects over kube-rs, discovery), `coexist.rs` (stands down while Argo CD's application/applicationset controllers run, unless an app carries `litecd.dev/manage: "true"`; every write path checks `ctx.guard`). `src/compat/` holds deprecated compatibility modes, each behind an off-by-default Cargo feature that nothing else depends on: `argocd_ui.rs` (feature `argocd-ui-compat`, flag `--argocd-ui-cache`) writes resource trees to the Redis of Argo CD's web UI (docs/spec.md, Deprecated compatibility); test it with `cargo test --features argocd-ui-compat`.

## Build & Test

```bash
cargo build
cargo test
cargo run -- check -f ../litek8s/examples/platform/gitops
deploy/build.sh                       # image on the Docker API in DOCKER_HOST (crates vendored first)
compat/cluster.sh up && eval "$(compat/cluster.sh env)"
LITECD_MANIFEST=deploy/install compat/run.sh --target litecd --phases 1
```

`e2e/platform.sh`: litek8s's examples/platform with litecd in Argo CD's place (`e2e/platform.patch` applied to a copy; never add litecd to the litek8s repository).

A change to the status litecd writes must keep every phase 1 corpus case equal to its golden file; golden files only ever come from Argo CD (`--target argocd --update-golden`).

## Conventions

- Rust edition 2024, `anyhow::Result` with `.context()`, tokio when async arrives.
- Public Kubernetes API only (list, watch, server-side apply, dry-run): litecd must run on any conformant cluster and shares no code with litek8s. Bugs litecd finds in litek8s are fixed in the litek8s repository with their own test.
- Behaviour follows Argo CD v3.5; when in doubt, the corpus case run against real Argo CD decides.
- Never "Argo" in names, images or logos; say "reads Argo CD manifests". Code vendored from Argo CD (Lua health scripts, e2e fixtures) keeps its Apache-2.0 notice in NOTICE.
