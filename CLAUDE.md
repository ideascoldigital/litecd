# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is litecd

GitOps continuous delivery in one Rust binary that reads Argo CD's manifests (`argoproj.io/v1alpha1` Application, AppProject, ApplicationSet, repository/cluster Secrets, `argocd-cm`) unchanged and writes the same `.status`. Plan, scope and phases: docs/spec.md. `litecd check` (`src/check.rs`) is a static report of the features a set of manifests uses and the phase that supports each; its table is the source of truth for the phases.

`litecd controller` (phase 1, and phase 2's hooks, multi-source and ignoreDifferences in `ignore.rs`): `controller.rs` (one task per Application, woken by watches and timers; status written as a merge patch with Argo CD's field-presence rules; auto-sync, self-heal backoff, retries, history, finalizer), `app.rs` (settings from argocd-cm, rendering cache, comparison, project checks, tracking ids), `sync.rs` (client-side dry run, waves with health waits and 2 s delay, Helm's kind order, apply/replace/server-side apply, prune), `diff.rs` (three-way: desired subset of live + last-applied for dropped fields, a server-side apply dry run to confirm), `health.rs` (gitops-engine checks, Lua via mlua 5.1), `render.rs` (directory, `helm template`, `kustomize build`), `repo.rs` (`git` CLI, `helm pull`), `kubeapi.rs` (untyped objects over kube-rs, discovery).

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
