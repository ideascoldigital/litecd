//! `litecd check`: reads Argo CD manifests (Application, AppProject,
//! ApplicationSet) and reports which features each one uses and in which
//! phase of docs/spec.md litecd supports them. Nothing talks to a cluster or
//! a repository: features chosen by the repository's content (Helm or
//! Kustomize auto-detection, hooks and waves on rendered resources) are not
//! visible here.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

/// When litecd supports a feature.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Support {
    Phase1,
    Phase2,
    Phase3,
    NotPlanned,
    /// A value litecd does not know (a typo, or newer than Argo CD v3.5).
    Unknown,
}

impl Support {
    fn label(self) -> &'static str {
        match self {
            Support::Phase1 => "phase 1",
            Support::Phase2 => "phase 2",
            Support::Phase3 => "phase 3",
            Support::NotPlanned => "not planned",
            Support::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Feature {
    pub name: String,
    pub support: Support,
}

#[derive(Debug, Serialize)]
pub struct Object {
    pub file: PathBuf,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub features: Vec<Feature>,
}

impl Object {
    /// The latest phase this object needs: the one where all of it works.
    pub fn needs(&self) -> Support {
        self.features
            .iter()
            .map(|f| f.support)
            .max()
            .unwrap_or(Support::Phase1)
    }
}

/// Every Argo CD object in `paths` (files, or directories walked for
/// `.yaml`, `.yml` and `.json`), in path order.
pub fn scan(paths: &[PathBuf]) -> Result<Vec<Object>> {
    let mut files = Vec::new();
    for p in paths {
        collect(p, &mut files)?;
    }
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let text =
            std::fs::read_to_string(&f).with_context(|| format!("reading {}", f.display()))?;
        out.extend(check_text(&f, &text)?);
    }
    Ok(out)
}

fn collect(p: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if p.is_dir() {
        for e in std::fs::read_dir(p).with_context(|| format!("reading {}", p.display()))? {
            let path = e?.path();
            if path.is_dir() {
                collect(&path, files)?;
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yaml" | "yml" | "json")
            ) {
                files.push(path);
            }
        }
    } else {
        files.push(p.to_path_buf());
    }
    Ok(())
}

/// The Argo CD objects in one file. Documents that are not YAML (templates
/// with placeholders, say) are skipped, not errors: gitops repositories are
/// full of them.
pub fn check_text(file: &Path, text: &str) -> Result<Vec<Object>> {
    let mut out = Vec::new();
    // Split by hand: after a bad document serde_yaml's multi-document
    // iterator keeps returning the same error.
    for doc in documents(text) {
        let Ok(v) = serde_yaml::from_str::<Value>(doc) else {
            continue;
        };
        let api = v["apiVersion"].as_str().unwrap_or_default();
        let kind = v["kind"].as_str().unwrap_or_default();
        let features = match (api, kind) {
            (a, "Application") if a.starts_with("argoproj.io/") => application(&v),
            (a, "AppProject") if a.starts_with("argoproj.io/") => project(&v),
            (a, "ApplicationSet") if a.starts_with("argoproj.io/") => application_set(&v),
            // A kustomize patch of argocd-cm may have no apiVersion.
            (_, "ConfigMap") if v["metadata"]["name"] == "argocd-cm" => argocd_cm(&v),
            ("v1", "Secret")
                if matches!(
                    v["metadata"]["labels"]["argocd.argoproj.io/secret-type"].as_str(),
                    Some("repository" | "repo-creds")
                ) =>
            {
                repository(&v)
            }
            _ => continue,
        };
        out.push(Object {
            file: file.to_path_buf(),
            kind: kind.to_string(),
            namespace: v["metadata"]["namespace"].as_str().map(str::to_string),
            name: v["metadata"]["name"].as_str().unwrap_or("?").to_string(),
            features,
        });
    }
    Ok(out)
}

/// The documents of a YAML stream: the text between `---` lines.
pub(crate) fn documents(text: &str) -> Vec<&str> {
    let mut docs = Vec::new();
    let mut start = 0;
    let mut pos = 0;
    for line in text.split_inclusive('\n') {
        let t = line.trim_end();
        if t == "---" || t.starts_with("--- ") || t == "..." {
            docs.push(&text[start..pos]);
            start = pos + line.len();
        }
        pos += line.len();
    }
    docs.push(&text[start..]);
    docs.retain(|d| !d.trim().is_empty());
    docs
}

fn push(out: &mut Vec<Feature>, name: impl Into<String>, support: Support) {
    let name = name.into();
    if !out.iter().any(|f| f.name == name) {
        out.push(Feature { name, support });
    }
}

fn application(v: &Value) -> Vec<Feature> {
    let mut out = Vec::new();
    metadata(&v["metadata"], &mut out);
    spec(&v["spec"], &mut out);
    out
}

fn metadata(m: &Value, out: &mut Vec<Feature>) {
    if let Some(ns) = m["namespace"].as_str()
        && ns != "argocd"
    {
        push(
            out,
            format!("Application in namespace {ns} (apps in any namespace)"),
            Support::Phase3,
        );
    }
    if let Some(a) = m["annotations"].as_object() {
        for k in a.keys() {
            let support = match k.as_str() {
                "argocd.argoproj.io/sync-wave"
                | "argocd.argoproj.io/refresh"
                | "argocd.argoproj.io/sync-options" => Support::Phase1,
                "argocd.argoproj.io/manifest-generate-paths" => Support::Phase2,
                k if k.starts_with("notifications.argoproj.io/") => Support::NotPlanned,
                k if k.starts_with("argocd.argoproj.io/") => Support::Unknown,
                _ => continue,
            };
            push(out, format!("annotation {k}"), support);
        }
    }
    if let Some(f) = m["finalizers"].as_array() {
        for f in f.iter().filter_map(Value::as_str) {
            let support = match f {
                "resources-finalizer.argocd.argoproj.io"
                | "resources-finalizer.argocd.argoproj.io/foreground" => Support::Phase1,
                "resources-finalizer.argocd.argoproj.io/background" => Support::Phase2,
                _ => continue,
            };
            push(out, format!("finalizer {f}"), support);
        }
    }
}

fn spec(s: &Value, out: &mut Vec<Feature>) {
    if s["source"].is_object() {
        source(&s["source"], out);
    }
    if let Some(sources) = s["sources"].as_array() {
        push(out, "multiple sources", Support::Phase2);
        for src in sources {
            source(src, out);
        }
    }
    destination(&s["destination"], out);
    let p = &s["syncPolicy"];
    if p["automated"].is_object() {
        push(out, "automated sync", Support::Phase1);
        for k in ["prune", "selfHeal", "allowEmpty"] {
            if p["automated"][k].as_bool() == Some(true) {
                push(out, format!("automated {k}"), Support::Phase1);
            }
        }
    }
    if let Some(opts) = p["syncOptions"].as_array() {
        for o in opts.iter().filter_map(Value::as_str) {
            push(out, format!("syncOption {o}"), sync_option(o));
        }
    }
    if p["retry"].is_object() {
        push(out, "sync retry", Support::Phase1);
    }
    if p["managedNamespaceMetadata"].is_object() {
        push(out, "managedNamespaceMetadata", Support::Phase2);
    }
    if s["ignoreDifferences"].is_array() {
        push(out, "ignoreDifferences", Support::Phase2);
    }
}

fn source(src: &Value, out: &mut Vec<Feature>) {
    if src["chart"].is_string() {
        let oci = src["repoURL"]
            .as_str()
            .is_some_and(|u| u.starts_with("oci://") || !u.contains("://"));
        push(
            out,
            if oci {
                "Helm chart from an OCI registry"
            } else {
                "Helm chart from a Helm repository"
            },
            Support::Phase1,
        );
    }
    if src["helm"].is_object() {
        push(out, "helm options", Support::Phase1);
        if src["helm"]["fileParameters"].is_array() {
            push(out, "helm fileParameters", Support::Phase2);
        }
    }
    if src["kustomize"].is_object() {
        push(out, "kustomize", Support::Phase1);
    }
    if src["plugin"].is_object() {
        push(out, "config management plugin", Support::NotPlanned);
    }
    if src["ref"].is_string() {
        push(
            out,
            "source ref (values from another source)",
            Support::Phase2,
        );
    }
    let d = &src["directory"];
    if d.is_object() {
        push(out, "directory options", Support::Phase1);
        if d["jsonnet"].as_object().is_some_and(|j| !j.is_empty()) {
            push(out, "jsonnet", Support::Phase2);
        }
    }
    if !src["chart"].is_string()
        && !src["helm"].is_object()
        && !src["kustomize"].is_object()
        && !src["plugin"].is_object()
    {
        // Argo CD picks Helm or Kustomize from the repository's files.
        push(
            out,
            "git path: directory, or Helm/Kustomize detected from the repository",
            Support::Phase1,
        );
    }
}

fn destination(d: &Value, out: &mut Vec<Feature>) {
    let in_cluster = match (d["server"].as_str(), d["name"].as_str()) {
        (Some(s), _) => s == "https://kubernetes.default.svc",
        (None, Some(n)) => n == "in-cluster",
        (None, None) => true,
    };
    if !in_cluster {
        push(out, "destination: another cluster", Support::Phase3);
    }
}

fn sync_option(o: &str) -> Support {
    let (k, _) = o.split_once('=').unwrap_or((o, ""));
    match k {
        "CreateNamespace"
        | "Replace"
        | "ServerSideApply"
        | "Prune"
        | "Validate"
        | "SkipDryRunOnMissingResource"
        | "Delete"
        | "Force" => Support::Phase1,
        "PruneLast"
        | "ApplyOutOfSyncOnly"
        | "RespectIgnoreDifferences"
        | "FailOnSharedResource"
        | "PrunePropagationPolicy"
        | "ClientSideApplyMigration" => Support::Phase2,
        _ => Support::Unknown,
    }
}

fn project(v: &Value) -> Vec<Feature> {
    let mut out = Vec::new();
    let s = &v["spec"];
    push(
        &mut out,
        "project: sourceRepos, destinations, resource allow/deny lists",
        Support::Phase1,
    );
    for (k, support) in [
        ("syncWindows", Support::Phase3),
        ("roles", Support::NotPlanned),
        ("signatureKeys", Support::NotPlanned),
        ("orphanedResources", Support::Phase3),
        ("sourceNamespaces", Support::Phase3),
        ("destinationServiceAccounts", Support::Phase3),
    ] {
        if s[k].as_array().is_some_and(|a| !a.is_empty()) || s[k].is_object() {
            push(&mut out, format!("project {k}"), support);
        }
    }
    out
}

fn application_set(v: &Value) -> Vec<Feature> {
    let mut out = Vec::new();
    let s = &v["spec"];
    push(&mut out, "ApplicationSet", Support::Phase3);
    if let Some(gens) = s["generators"].as_array() {
        generators(gens, &mut out);
    }
    if s["goTemplate"].as_bool() == Some(true) {
        push(&mut out, "goTemplate", Support::Phase3);
    }
    if s["strategy"].is_object() {
        push(&mut out, "progressive sync strategy", Support::NotPlanned);
    }
    spec(&s["template"]["spec"], &mut out);
    out
}

fn generators(gens: &[Value], out: &mut Vec<Feature>) {
    for g in gens {
        let Some(g) = g.as_object() else { continue };
        for (k, inner) in g {
            let support = match k.as_str() {
                "list" | "git" | "clusters" | "matrix" | "merge" => Support::Phase3,
                "scmProvider" | "pullRequest" | "clusterDecisionResource" | "plugin" => {
                    Support::NotPlanned
                }
                "selector" | "values" => continue,
                _ => Support::Unknown,
            };
            push(out, format!("generator {k}"), support);
            if let Some(nested) = inner["generators"].as_array() {
                generators(nested, out);
            }
        }
    }
}

/// The settings in argocd-cm that change how litecd must behave.
fn argocd_cm(v: &Value) -> Vec<Feature> {
    let mut out = Vec::new();
    let Some(data) = v["data"].as_object() else {
        return out;
    };
    for k in data.keys() {
        let (name, support) = if let Some(kind) = k.strip_prefix("resource.customizations.health.")
        {
            (format!("Lua health check for {kind}"), Support::Phase1)
        } else if k.starts_with("resource.customizations.ignoreDifferences.") {
            (
                format!("{k} (ignoreDifferences for every app)"),
                Support::Phase2,
            )
        } else if k.starts_with("resource.customizations.actions.") {
            (format!("{k} (resource actions)"), Support::NotPlanned)
        } else if k == "resource.customizations" || k.starts_with("resource.customizations.") {
            (k.clone(), Support::Phase2)
        } else {
            match k.as_str() {
                "timeout.reconciliation"
                | "application.resourceTrackingMethod"
                | "application.instanceLabelKey"
                | "resource.exclusions"
                | "resource.inclusions" => (k.clone(), Support::Phase1),
                "kustomize.buildOptions" | "helm.valuesFileSchemes" | "resource.compareoptions" => {
                    (k.clone(), Support::Phase2)
                }
                "url"
                | "admin.enabled"
                | "users.anonymous.enabled"
                | "statusbadge.enabled"
                | "exec.enabled" => continue,
                k if k.starts_with("dex.")
                    || k.starts_with("oidc.")
                    || k.starts_with("accounts.") =>
                {
                    continue;
                }
                _ => (k.to_string(), Support::Unknown),
            }
        };
        push(&mut out, name, support);
    }
    out
}

/// A repository (or repo-creds) Secret: how litecd reaches the repository.
fn repository(v: &Value) -> Vec<Feature> {
    let mut out = Vec::new();
    let has = |k: &str| v["data"][k].is_string() || v["stringData"][k].is_string();
    if has("username") || has("password") {
        push(
            &mut out,
            "repository credentials: username/password",
            Support::Phase1,
        );
    }
    if has("sshPrivateKey") {
        push(
            &mut out,
            "repository credentials: SSH private key",
            Support::Phase1,
        );
    }
    for (k, name) in [
        ("githubAppPrivateKey", "repository credentials: GitHub App"),
        (
            "tlsClientCertData",
            "repository credentials: TLS client certificate",
        ),
        (
            "gcpServiceAccountKey",
            "repository credentials: Google Cloud service account",
        ),
        ("bearerToken", "repository credentials: bearer token"),
        ("proxy", "repository proxy"),
    ] {
        if has(k) {
            push(&mut out, name, Support::NotPlanned);
        }
    }
    out
}

/// The text report: one block per object, then a summary.
pub fn report(objects: &[Object]) -> String {
    let mut s = String::new();
    for o in objects {
        let ns = o
            .namespace
            .as_deref()
            .map(|n| format!("{n}/"))
            .unwrap_or_default();
        let _ = writeln!(
            s,
            "{} {ns}{} ({}): {}",
            o.kind,
            o.name,
            o.file.display(),
            o.needs().label()
        );
        for f in &o.features {
            let _ = writeln!(s, "  {:<12} {}", f.support.label(), f.name);
        }
    }
    let count = |p: Support| objects.iter().filter(|o| o.needs() == p).count();
    let _ = writeln!(
        s,
        "\n{} objects: {} work in phase 1, {} need phase 2, {} phase 3, {} use features not planned, {} unknown",
        objects.len(),
        count(Support::Phase1),
        count(Support::Phase2),
        count(Support::Phase3),
        count(Support::NotPlanned),
        count(Support::Unknown),
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(yaml: &str) -> Object {
        let mut objs = check_text(Path::new("t.yaml"), yaml).unwrap();
        assert_eq!(objs.len(), 1);
        objs.remove(0)
    }

    fn support(o: &Object, name: &str) -> Support {
        o.features
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("no feature {name}: {o:?}"))
            .support
    }

    #[test]
    fn helm_application_with_waves_needs_phase1() {
        let o = one(r#"
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: argo-workflows
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "40"
  finalizers: [resources-finalizer.argocd.argoproj.io]
spec:
  project: default
  source:
    repoURL: https://argoproj.github.io/argo-helm
    chart: argo-workflows
    targetRevision: 0.45.0
    helm:
      valuesObject: {server: {enabled: true}}
  destination: {server: https://kubernetes.default.svc, namespace: argo}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true, Replace=true]
    retry: {limit: -1}
"#);
        assert_eq!(o.needs(), Support::Phase1);
        assert_eq!(
            support(&o, "Helm chart from a Helm repository"),
            Support::Phase1
        );
        assert_eq!(support(&o, "syncOption Replace=true"), Support::Phase1);
        assert_eq!(support(&o, "automated selfHeal"), Support::Phase1);
        assert!(
            o.features.iter().all(|f| !f.name.starts_with("git path")),
            "{o:?}"
        );
    }

    #[test]
    fn multi_source_kustomize_and_remote_cluster() {
        let o = one(r#"
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: web}
spec:
  sources:
    - {repoURL: https://charts.example.com, chart: web, helm: {valueFiles: [$values/web.yaml]}}
    - {repoURL: https://git.example.com/values.git, ref: values}
    - {repoURL: https://git.example.com/app.git, path: overlays/prod, kustomize: {namePrefix: prod-}}
  destination: {name: edge-1, namespace: web}
"#);
        assert_eq!(support(&o, "multiple sources"), Support::Phase2);
        assert_eq!(support(&o, "kustomize"), Support::Phase1);
        assert_eq!(support(&o, "destination: another cluster"), Support::Phase3);
        assert_eq!(o.needs(), Support::Phase3);
    }

    #[test]
    fn application_set_generators_nest() {
        let o = one(r#"
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata: {name: apps, namespace: argocd}
spec:
  goTemplate: true
  generators:
    - matrix:
        generators:
          - git: {repoURL: https://git.example.com/apps.git, directories: [{path: apps/*}]}
          - pullRequest: {gitea: {owner: me, repo: apps}}
  template:
    metadata: {name: '{{.path.basename}}'}
    spec:
      project: default
      source: {repoURL: https://git.example.com/apps.git, path: '{{.path.path}}'}
      destination: {server: https://kubernetes.default.svc}
"#);
        assert_eq!(support(&o, "generator matrix"), Support::Phase3);
        assert_eq!(support(&o, "generator git"), Support::Phase3);
        assert_eq!(support(&o, "generator pullRequest"), Support::NotPlanned);
        assert_eq!(o.needs(), Support::NotPlanned);
    }

    #[test]
    fn argocd_cm_lua_health_is_phase1() {
        let o = one(r#"
apiVersion: v1
kind: ConfigMap
metadata: {name: argocd-cm}
data:
  url: https://argocd.example.com
  resource.customizations.health.argoproj.io_Application: |
    hs = {}
    return hs
  resource.customizations.actions.apps_Deployment: "..."
"#);
        assert_eq!(o.kind, "ConfigMap");
        assert_eq!(
            support(&o, "Lua health check for argoproj.io_Application"),
            Support::Phase1
        );
        assert_eq!(o.needs(), Support::NotPlanned);
        assert!(o.features.iter().all(|f| f.name != "url"), "{o:?}");
    }

    #[test]
    fn repository_secrets() {
        let o = one(
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: r\n  labels:\n    argocd.argoproj.io/secret-type: repository\nstringData:\n  url: git@github.com:o/r.git\n  sshPrivateKey: k\n  githubAppPrivateKey: g\n",
        );
        assert_eq!(
            support(&o, "repository credentials: SSH private key"),
            Support::Phase1
        );
        assert_eq!(
            support(&o, "repository credentials: GitHub App"),
            Support::NotPlanned
        );
        // Other Secrets are not Argo CD's.
        assert!(
            check_text(
                Path::new("x"),
                "apiVersion: v1\nkind: Secret\nmetadata:\n  name: s\n"
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn unknown_sync_option_and_other_documents() {
        let objs = check_text(
            Path::new("t.yaml"),
            r#"
apiVersion: v1
kind: ConfigMap
metadata: {name: x}
---
not: [valid
---
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: a, namespace: argocd}
spec:
  source: {repoURL: https://git.example.com/a.git, path: k8s}
  destination: {server: https://kubernetes.default.svc}
  syncPolicy: {syncOptions: [CreateNamespce=true]}
"#,
        )
        .unwrap();
        assert_eq!(objs.len(), 1);
        assert_eq!(
            support(&objs[0], "syncOption CreateNamespce=true"),
            Support::Unknown
        );
    }
}
