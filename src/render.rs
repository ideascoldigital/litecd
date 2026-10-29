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
pub fn directory(dir: &Path, directory: &Value, repo_root: &Path) -> Result<Vec<Value>> {
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
        let jsonnet = name.ends_with(".jsonnet");
        if !(name.ends_with(".yaml")
            || name.ends_with(".yml")
            || name.ends_with(".json")
            || jsonnet)
        {
            continue;
        }
        if exclude.as_ref().is_some_and(|g| g.is_match(&rel)) {
            continue;
        }
        if include.as_ref().is_some_and(|g| !g.is_match(&rel)) {
            continue;
        }
        let text = if jsonnet {
            evaluate_jsonnet(&f, &directory["jsonnet"], repo_root)
                .with_context(|| format!("in {rel}"))?
        } else {
            std::fs::read_to_string(&f).with_context(|| format!("reading {rel}"))?
        };
        out.extend(parse_manifests(&text).with_context(|| format!("in {rel}"))?);
    }
    Ok(out)
}

/// The command line `jsonnet` gets for a directory source's jsonnet
/// options: extVars and tlas (as strings, or as code), libs relative to the
/// repository.
pub fn jsonnet_args(opts: &Value, repo_root: &Path) -> Vec<String> {
    let mut args = Vec::new();
    for (list, flag) in [("extVars", "ext"), ("tlas", "tla")] {
        for v in opts[list].as_array().into_iter().flatten() {
            let name = v["name"].as_str().unwrap_or_default();
            let value = v["value"].as_str().unwrap_or_default();
            let kind = if v["code"].as_bool() == Some(true) {
                "code"
            } else {
                "str"
            };
            args.push(format!("--{flag}-{kind}"));
            args.push(format!("{name}={value}"));
        }
    }
    for lib in opts["libs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        args.push("-J".into());
        args.push(
            repo_root
                .join(lib.trim_start_matches('/'))
                .display()
                .to_string(),
        );
    }
    args
}

fn evaluate_jsonnet(file: &Path, opts: &Value, repo_root: &Path) -> Result<String> {
    let out = std::process::Command::new("jsonnet")
        .args(jsonnet_args(opts, repo_root))
        .arg(file)
        .output()
        .context("running jsonnet")?;
    if !out.status.success() {
        bail!("jsonnet: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
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
    let mut out = run_command(&mut cmd, "helm template").await;
    // Dependencies missing from charts/: build them and template again, as
    // Argo CD's repo server does.
    if out
        .as_ref()
        .is_err_and(|e| is_missing_dependency(&format!("{e:#}")))
    {
        out = match dependency_build(chart, cx).await {
            Ok(()) => run_command(&mut cmd, "helm template").await,
            Err(e) => Err(e),
        };
    }
    if !tmp.as_os_str().is_empty() {
        let _ = std::fs::remove_file(&tmp);
    }
    parse_manifests(&String::from_utf8_lossy(&out?))
}

/// Helm's words for a dependency Chart.yaml lists that charts/ lacks.
fn is_missing_dependency(err: &str) -> bool {
    err.contains("found in Chart.yaml, but missing in charts/ directory")
        || err.contains("found in requirements.yaml, but missing in charts/ directory")
}

/// The Helm repositories (http or https) a chart's dependencies name by URL.
fn dependency_repos(chart_yaml: &str) -> Vec<String> {
    let v: Value = serde_yaml::from_str(chart_yaml).unwrap_or_default();
    let mut out: Vec<String> = v["dependencies"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|d| d["repository"].as_str())
        .filter(|r| r.starts_with("http://") || r.starts_with("https://"))
        .map(|r| r.trim_end_matches('/').to_string())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// `helm dependency build`, after `helm repo add` of each repository the
/// dependencies name by URL (`dependency build` only reads repositories it
/// knows), as Argo CD's repo server does.
async fn dependency_build(chart: &Path, cx: &RenderCtx<'_>) -> Result<()> {
    let chart_yaml = std::fs::read_to_string(chart.join("Chart.yaml")).unwrap_or_default();
    for url in dependency_repos(&chart_yaml) {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&url, &mut h);
        let name = format!("litecd-{:016x}", std::hash::Hasher::finish(&h));
        run_command(
            helm(cx.cache).args(["repo", "add", "--force-update", &name, &url]),
            &format!("helm repo add {url}"),
        )
        .await?;
    }
    run_command(
        helm(cx.cache).arg("dependency").arg("build").arg(chart),
        "helm dependency build",
    )
    .await?;
    Ok(())
}

/// `kustomize build` of a directory; with the source's kustomize options
/// applied (as Argo CD does with `kustomize edit`) to a copy of the tree.
pub async fn kustomize_build(
    tree: &Path,
    dir: &Path,
    source: &Value,
    cx: &RenderCtx<'_>,
) -> Result<Vec<Value>> {
    let opts = &source["kustomize"];
    let has_opts = opts
        .as_object()
        .is_some_and(|o| o.keys().any(|k| k != "version"));
    let (work, build_dir) = if has_opts {
        let work =
            cx.cache
                .join("tmp")
                .join(format!("kustomize-{}-{}", std::process::id(), unique()));
        copy_dir(tree, &work)?;
        let rel = dir.strip_prefix(tree).unwrap_or(Path::new("."));
        let d = work.join(rel);
        edit_kustomization(&d, opts)?;
        (Some(work), d)
    } else {
        (None, dir.to_path_buf())
    };
    let mut cmd = tokio::process::Command::new("kustomize");
    cmd.arg("build").arg(&build_dir);
    for o in cx.kustomize_build_options.split_whitespace() {
        cmd.arg(o);
    }
    let out = run_command(&mut cmd, "kustomize build").await;
    if let Some(w) = work {
        let _ = std::fs::remove_dir_all(w);
    }
    parse_manifests(&String::from_utf8_lossy(&out?))
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let (src, dst) = (e.path(), to.join(e.file_name()));
        if e.file_type()?.is_dir() {
            copy_dir(&src, &dst)?;
        } else {
            std::fs::copy(&src, &dst)?;
        }
    }
    Ok(())
}

/// `[old=]new[:tag|@digest]`, as `kustomize edit set image` takes it.
pub fn image_entry(spec: &str) -> Value {
    let (name, rest) = match spec.split_once('=') {
        Some((n, r)) => (n.to_string(), r.to_string()),
        None => (String::new(), spec.to_string()),
    };
    let mut e = serde_json::Map::new();
    let (image, digest) = match rest.split_once('@') {
        Some((i, d)) => (i.to_string(), Some(d.to_string())),
        None => (rest.clone(), None),
    };
    // A tag is after the last ':' that comes after the last '/' (a registry port is not a tag).
    let slash = image.rfind('/').map(|i| i + 1).unwrap_or(0);
    let (new_name, tag) = match image[slash..].rfind(':') {
        Some(c) => (
            image[..slash + c].to_string(),
            Some(image[slash + c + 1..].to_string()),
        ),
        None => (image.clone(), None),
    };
    if name.is_empty() {
        e.insert("name".into(), Value::String(new_name));
    } else {
        e.insert("name".into(), Value::String(name.clone()));
        if new_name != name {
            e.insert("newName".into(), Value::String(new_name));
        }
    }
    if let Some(t) = tag {
        e.insert("newTag".into(), Value::String(t));
    }
    if let Some(d) = digest {
        e.insert("digest".into(), Value::String(d));
    }
    Value::Object(e)
}

/// Apply Argo CD's kustomize options to the kustomization in `dir`.
pub fn edit_kustomization(dir: &Path, opts: &Value) -> Result<()> {
    let file = KUSTOMIZATIONS
        .iter()
        .map(|k| dir.join(k))
        .find(|p| p.exists())
        .context("no kustomization file")?;
    let mut k: Value = serde_yaml::from_str(&std::fs::read_to_string(&file)?)
        .context("parsing the kustomization")?;
    if !k.is_object() {
        k = Value::Object(Default::default());
    }
    for f in ["namePrefix", "nameSuffix", "namespace"] {
        if let Some(v) = opts[f].as_str().filter(|v| !v.is_empty()) {
            k[f] = Value::String(v.to_string());
        }
    }
    let merge_list = |k: &mut Value, field: &str, entries: Vec<Value>, key: &str| {
        let mut list = k[field].as_array().cloned().unwrap_or_default();
        for e in entries {
            match list.iter_mut().find(|x| x[key] == e[key]) {
                Some(x) => *x = e,
                None => list.push(e),
            }
        }
        k[field] = Value::Array(list);
    };
    let images: Vec<Value> = opts["images"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(image_entry)
        .collect();
    if !images.is_empty() {
        merge_list(&mut k, "images", images, "name");
    }
    let replicas: Vec<Value> = opts["replicas"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|r| {
            let count = r["count"]
                .as_i64()
                .or_else(|| r["count"].as_str().and_then(|c| c.parse().ok()))
                .unwrap_or(0);
            serde_json::json!({"name": r["name"], "count": count})
        })
        .collect();
    if !replicas.is_empty() {
        merge_list(&mut k, "replicas", replicas, "name");
    }
    if let Some(labels) = opts["commonLabels"].as_object().filter(|l| !l.is_empty()) {
        if opts["labelWithoutSelector"].as_bool() == Some(true) {
            let mut list = k["labels"].as_array().cloned().unwrap_or_default();
            let mut entry = serde_json::json!({"pairs": labels, "includeSelectors": false});
            if opts["labelIncludeTemplates"].as_bool() == Some(true) {
                entry["includeTemplates"] = Value::Bool(true);
            }
            list.push(entry);
            k["labels"] = Value::Array(list);
        } else {
            let mut m = k["commonLabels"].as_object().cloned().unwrap_or_default();
            m.extend(labels.clone());
            k["commonLabels"] = Value::Object(m);
        }
    }
    if let Some(ann) = opts["commonAnnotations"]
        .as_object()
        .filter(|a| !a.is_empty())
    {
        let mut m = k["commonAnnotations"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        m.extend(ann.clone());
        k["commonAnnotations"] = Value::Object(m);
    }
    for f in ["patches", "components"] {
        if let Some(extra) = opts[f].as_array().filter(|a| !a.is_empty()) {
            let mut list = k[f].as_array().cloned().unwrap_or_default();
            list.extend(extra.iter().cloned());
            k[f] = Value::Array(list);
        }
    }
    std::fs::write(&file, serde_yaml::to_string(&k)?)?;
    Ok(())
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    #[test]
    fn helm_dependency_repos() {
        let y = "dependencies:\n  - {name: a, repository: http://cm.svc:8080/}\n  - {name: b, repository: 'http://cm.svc:8080'}\n  - {name: c, repository: file://charts/c}\n  - {name: d, repository: oci://ghcr.io/x}\n  - {name: e, repository: '@stable'}\n";
        assert_eq!(super::dependency_repos(y), vec!["http://cm.svc:8080"]);
        assert!(super::is_missing_dependency(
            "helm template: Error: An error occurred while checking for chart dependencies. You may need to run `helm dependency build` to fetch missing dependencies: found in Chart.yaml, but missing in charts/ directory: a"
        ));
    }

    use super::*;
    use serde_json::json;

    /// An umbrella as the gitops repositories write them: one subchart vendored
    /// under charts/ and one more to fetch (charts/ exists, so the old check skipped the build).
    #[tokio::test]
    async fn builds_dependencies_next_to_vendored_ones() {
        if std::process::Command::new("helm")
            .arg("version")
            .output()
            .is_err()
        {
            eprintln!("helm not installed: skipped");
            return;
        }
        let tmp = std::env::temp_dir().join(format!("litecd-helmdeps-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let files = [
            (
                "umbrella/Chart.yaml",
                "apiVersion: v2\nname: umbrella\nversion: 0.1.0\ndependencies:\n- {name: db, version: 1.0.0, repository: \"file://charts/db\"}\n- {name: app, version: 2.0.0, repository: \"file://../app\"}\n",
            ),
            (
                "umbrella/charts/db/Chart.yaml",
                "apiVersion: v2\nname: db\nversion: 1.0.0\n",
            ),
            (
                "umbrella/charts/db/templates/cm.yaml",
                "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: db}\n",
            ),
            (
                "app/Chart.yaml",
                "apiVersion: v2\nname: app\nversion: 2.0.0\n",
            ),
            (
                "app/templates/cm.yaml",
                "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\n",
            ),
        ];
        for (p, body) in files {
            let f = tmp.join(p);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(f, body).unwrap();
        }
        let cache = tmp.join("cache");
        let cx = RenderCtx {
            app_name: "umbrella",
            namespace: "default",
            kube_version: String::new(),
            api_versions: Vec::new(),
            cache: &cache,
            kustomize_build_options: String::new(),
        };
        let objs = helm_template(&tmp.join("umbrella"), &json!({}), &cx, &Default::default())
            .await
            .unwrap();
        let mut got = names(&objs);
        got.sort();
        assert_eq!(got, ["app", "db"]);
        let _ = std::fs::remove_dir_all(&tmp);
    }

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
        assert_eq!(
            names(&directory(&tmp, &Value::Null, &tmp).unwrap()),
            ["top"]
        );
        let all = directory(
            &tmp,
            &json!({"recurse": true, "include": "{*.yaml,*.json}", "exclude": "skip/*"}),
            &tmp,
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
    fn jsonnet_command_line() {
        let opts = json!({"extVars": [{"name": "prefix", "value": "generated"}], "tlas": [{"name": "count", "value": "3", "code": true}], "libs": ["vendor"]});
        assert_eq!(
            jsonnet_args(&opts, Path::new("/repo")),
            [
                "--ext-str",
                "prefix=generated",
                "--tla-code",
                "count=3",
                "-J",
                "/repo/vendor"
            ]
        );
    }

    #[test]
    fn kustomize_images() {
        assert_eq!(
            image_entry("registry.k8s.io/pause=registry.k8s.io/pause:3.9"),
            json!({"name": "registry.k8s.io/pause", "newTag": "3.9"})
        );
        assert_eq!(
            image_entry("nginx=my.reg:5000/nginx:1.2"),
            json!({"name": "nginx", "newName": "my.reg:5000/nginx", "newTag": "1.2"})
        );
        assert_eq!(
            image_entry("nginx:1.2"),
            json!({"name": "nginx", "newTag": "1.2"})
        );
        assert_eq!(
            image_entry("nginx@sha256:abc"),
            json!({"name": "nginx", "digest": "sha256:abc"})
        );
    }

    #[test]
    fn kustomize_edits() {
        let tmp = std::env::temp_dir().join(format!("litecd-kedit-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(
            tmp.join("kustomization.yaml"),
            "resources: [a.yaml]\ncommonLabels: {x: y}\n",
        )
        .unwrap();
        edit_kustomization(&tmp, &json!({"namePrefix": "p-", "replicas": [{"name": "web", "count": "2"}], "commonLabels": {"team": "t"},
            "patches": [{"patch": "x"}]})).unwrap();
        let k: Value =
            serde_yaml::from_str(&std::fs::read_to_string(tmp.join("kustomization.yaml")).unwrap())
                .unwrap();
        assert_eq!(k["namePrefix"], "p-");
        assert_eq!(k["replicas"], json!([{"name": "web", "count": 2}]));
        assert_eq!(k["commonLabels"], json!({"x": "y", "team": "t"}));
        assert_eq!(k["patches"], json!([{"patch": "x"}]));
        let _ = std::fs::remove_dir_all(&tmp);
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
