//! Manifest generation for a source, as Argo CD's repo server does it:
//! plain directories natively, Helm and Kustomize through their CLIs.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use globset::GlobBuilder;
use serde_json::Value;

use crate::repo::{helm, run_command};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceType {
    Directory,
    Helm,
    Kustomize,
}

impl SourceType {
    pub fn as_str(self) -> &'static str {
        match self {
            SourceType::Directory => "Directory",
            SourceType::Helm => "Helm",
            SourceType::Kustomize => "Kustomize",
        }
    }
}

const KUSTOMIZATIONS: [&str; 3] = ["kustomization.yaml", "kustomization.yml", "Kustomization"];

/// The type of a source: the fields it sets, else what is in its directory.
pub fn source_type(source: &Value, dir: Option<&Path>) -> SourceType {
    if source["chart"].is_string() || source["helm"].is_object() {
        return SourceType::Helm;
    }
    if source["kustomize"].is_object() {
        return SourceType::Kustomize;
    }
    if source["directory"].is_object() {
        return SourceType::Directory;
    }
    if let Some(d) = dir {
        if KUSTOMIZATIONS.iter().any(|k| d.join(k).exists()) {
            return SourceType::Kustomize;
        }
        if d.join("Chart.yaml").exists() {
            return SourceType::Helm;
        }
    }
    SourceType::Directory
}

/// What a render needs besides the source.
pub struct RenderCtx<'a> {
    pub app_name: &'a str,
    pub namespace: &'a str,
    pub kube_version: String,
    pub api_versions: Vec<String>,
    pub cache: &'a Path,
    pub kustomize_build_options: String,
}

/// Parse a YAML (or JSON) stream into objects: Lists flattened, empty
/// documents dropped.
pub fn parse_manifests(text: &str) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for doc in crate::check::documents(text) {
        let v: Value = serde_yaml::from_str(doc).context("parsing YAML")?;
        push_object(v, &mut out)?;
    }
    Ok(out)
}

fn push_object(v: Value, out: &mut Vec<Value>) -> Result<()> {
    match v {
        Value::Null => {}
        Value::Array(items) => {
            for i in items {
                push_object(i, out)?;
            }
        }
        Value::Object(ref m) if m.is_empty() => {}
        Value::Object(ref m) => {
            let kind = m.get("kind").and_then(Value::as_str).unwrap_or_default();
            if kind.ends_with("List") && m.get("items").is_some_and(Value::is_array) {
                if let Some(Value::Array(items)) = m.get("items").cloned() {
                    for i in items {
                        push_object(i, out)?;
                    }
                }
            } else {
                if m.get("apiVersion").is_none() || kind.is_empty() {
                    bail!(
                        "object without apiVersion or kind: {}",
                        serde_json::to_string(&v)?
                            .chars()
                            .take(120)
                            .collect::<String>()
                    );
                }
                out.push(v);
            }
        }
        other => bail!("not a Kubernetes object: {other}"),
    }
    Ok(())
}

fn glob(pattern: &str) -> Result<globset::GlobMatcher> {
    Ok(GlobBuilder::new(pattern)
        .literal_separator(false)
        .build()
        .context("include/exclude pattern")?
        .compile_matcher())
}

/// A plain directory: YAML and JSON files, recursively when asked, with
/// include and exclude globs matched against the path below the source path.
pub fn directory(dir: &Path, directory: &Value) -> Result<Vec<Value>> {
    let recurse = directory["recurse"].as_bool().unwrap_or(false);
    let include = directory["include"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(glob)
        .transpose()?;
    let exclude = directory["exclude"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(glob)
        .transpose()?;
    let mut files = Vec::new();
    walk(dir, recurse, &mut files)?;
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let rel = f
            .strip_prefix(dir)
            .unwrap_or(&f)
            .to_string_lossy()
            .to_string();
        let name = f
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if !(name.ends_with(".yaml") || name.ends_with(".yml") || name.ends_with(".json")) {
            continue;
        }
        if exclude.as_ref().is_some_and(|g| g.is_match(&rel)) {
            continue;
        }
        if include.as_ref().is_some_and(|g| !g.is_match(&rel)) {
            continue;
        }
        let text = std::fs::read_to_string(&f).with_context(|| format!("reading {rel}"))?;
        out.extend(parse_manifests(&text).with_context(|| format!("in {rel}"))?);
    }
    Ok(out)
}

fn walk(dir: &Path, recurse: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    for e in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let e = e?;
        let p = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        if p.is_dir() {
            if recurse {
                walk(&p, recurse, out)?;
            }
        } else {
            out.push(p);
        }
    }
    Ok(())
}

/// `helm template` of a chart directory with the source's helm options.
pub async fn helm_template(
    chart: &Path,
    source: &Value,
    cx: &RenderCtx<'_>,
    refs: &std::collections::HashMap<String, PathBuf>,
) -> Result<Vec<Value>> {
    let h = &source["helm"];
    let release = h["releaseName"]
        .as_str()
        .filter(|s| !s.is_empty())
        .unwrap_or(cx.app_name);
    // Dependencies not vendored in charts/: build them as Argo CD does.
    let chart_yaml = std::fs::read_to_string(chart.join("Chart.yaml")).unwrap_or_default();
    if chart_yaml.contains("dependencies:") && !chart.join("charts").exists() {
        run_command(
            helm(cx.cache).arg("dependency").arg("build").arg(chart),
            "helm dependency build",
        )
        .await?;
    }
    let mut cmd = helm(cx.cache);
    cmd.arg("template")
        .arg(release)
        .arg(chart)
        .args(["--namespace", cx.namespace]);
    if !cx.kube_version.is_empty() {
        cmd.args([
            "--kube-version",
            cx.kube_version
                .trim_start_matches('v')
                .split('+')
                .next()
                .unwrap_or_default(),
        ]);
    }
    for v in &cx.api_versions {
        cmd.args(["--api-versions", v]);
    }
    if h["skipCrds"].as_bool() != Some(true) {
        cmd.arg("--include-crds");
    }
    if h["skipTests"].as_bool() == Some(true) {
        cmd.arg("--skip-tests");
    }
    if h["skipSchemaValidation"].as_bool() == Some(true) {
        cmd.arg("--skip-schema-validation");
    }
    let ignore_missing = h["ignoreMissingValueFiles"].as_bool() == Some(true);
    for f in h["valueFiles"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        if f.starts_with("http://") || f.starts_with("https://") {
            cmd.args(["--values", f]);
            continue;
        }
        // `$<ref>/path`: a file of another source (multi-source).
        let path = match f.strip_prefix('$').and_then(|r| r.split_once('/')) {
            Some((r, rest)) => match refs.get(r) {
                Some(root) => root.join(rest),
                None => bail!("values file {f}: no source with ref {r}"),
            },
            None => chart.join(f),
        };
        if !path.exists() {
            if ignore_missing {
                continue;
            }
            bail!("values file {f} not found");
        }
        cmd.arg("--values").arg(path);
    }
    // valuesObject wins over values when both are set.
    let inline = if h["valuesObject"].is_object() {
        Some(serde_yaml::to_string(&h["valuesObject"])?)
    } else {
        h["values"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
    };
    let tmp;
    if let Some(text) = inline {
        tmp = cx
            .cache
            .join("tmp")
            .join(format!("values-{}-{}.yaml", std::process::id(), unique()));
        std::fs::create_dir_all(tmp.parent().unwrap())?;
        std::fs::write(&tmp, text)?;
        cmd.arg("--values").arg(&tmp);
    } else {
        tmp = PathBuf::new();
    }
    for p in h["parameters"].as_array().into_iter().flatten() {
        let name = p["name"].as_str().unwrap_or_default();
        let value = p["value"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| p["value"].to_string());
        let flag = if p["forceString"].as_bool() == Some(true) {
            "--set-string"
        } else {
            "--set"
        };
        cmd.args([flag, &format!("{name}={value}")]);
    }
    let out = run_command(&mut cmd, "helm template").await;
    if !tmp.as_os_str().is_empty() {
        let _ = std::fs::remove_file(&tmp);
    }
    parse_manifests(&String::from_utf8_lossy(&out?))
}

/// `kustomize build` of a directory.
pub async fn kustomize_build(dir: &Path, cx: &RenderCtx<'_>) -> Result<Vec<Value>> {
    let mut cmd = tokio::process::Command::new("kustomize");
    cmd.arg("build").arg(dir);
    for o in cx.kustomize_build_options.split_whitespace() {
        cmd.arg(o);
    }
    let out = run_command(&mut cmd, "kustomize build").await?;
    parse_manifests(&String::from_utf8_lossy(&out))
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn names(objs: &[Value]) -> Vec<&str> {
        objs.iter()
            .map(|o| o["metadata"]["name"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn directory_include_exclude_recurse() {
        let tmp = std::env::temp_dir().join(format!("litecd-render-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        for (p, name) in [
            ("top.yaml", "top"),
            ("a/b/nested.yaml", "nested"),
            ("skip/skipped.yaml", "skipped"),
        ] {
            let f = tmp.join(p);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(
                f,
                format!("apiVersion: v1\nkind: ConfigMap\nmetadata: {{name: {name}}}\n"),
            )
            .unwrap();
        }
        std::fs::write(
            tmp.join("a/list.json"),
            r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"from-json"}}"#,
        )
        .unwrap();
        std::fs::write(tmp.join("README.md"), "not yaml").unwrap();
        assert_eq!(names(&directory(&tmp, &Value::Null).unwrap()), ["top"]);
        let all = directory(
            &tmp,
            &json!({"recurse": true, "include": "{*.yaml,*.json}", "exclude": "skip/*"}),
        )
        .unwrap();
        assert_eq!(names(&all), ["nested", "from-json", "top"]);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn lists_flatten_and_empty_documents_drop() {
        let objs = parse_manifests(
            "---\n# comment only\n---\napiVersion: v1\nkind: List\nitems:\n- {apiVersion: v1, kind: ConfigMap, metadata: {name: a}}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: b}\n",
        )
        .unwrap();
        assert_eq!(names(&objs), ["a", "b"]);
        assert!(parse_manifests("kind: ConfigMap\n").is_err());
    }

    #[test]
    fn types() {
        assert_eq!(source_type(&json!({"chart": "x"}), None), SourceType::Helm);
        assert_eq!(
            source_type(&json!({"path": "x"}), None),
            SourceType::Directory
        );
        assert_eq!(
            source_type(&json!({"kustomize": {}}), None),
            SourceType::Kustomize
        );
    }
}
