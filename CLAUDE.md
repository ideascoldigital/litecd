# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is litecd

GitOps continuous delivery in one Rust binary that reads Argo CD's manifests (`argoproj.io/v1alpha1` Application, AppProject, ApplicationSet, repository/cluster Secrets, `argocd-cm`) unchanged and writes the same `.status`. Plan, scope and phases: docs/spec.md. Today only `litecd check` (`src/check.rs`) exists: a static report of the features a set of manifests uses and the phase that supports each; its table is the source of truth for the phases.

## Build & Test

```bash
cargo build
cargo test
cargo run -- check -f ../litek8s/examples/platform/gitops
```

## Conventions

- Rust edition 2024, `anyhow::Result` with `.context()`, tokio when async arrives.
- Public Kubernetes API only (list, watch, server-side apply, dry-run): litecd must run on any conformant cluster and shares no code with litek8s. Bugs litecd finds in litek8s are fixed in the litek8s repository with their own test.
- Behaviour follows Argo CD v3.5; when in doubt, the corpus case run against real Argo CD decides.
- Never "Argo" in names, images or logos; say "reads Argo CD manifests". Code vendored from Argo CD (Lua health scripts, e2e fixtures) keeps its Apache-2.0 notice in NOTICE.
