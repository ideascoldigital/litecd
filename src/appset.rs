//! ApplicationSets: generators (list, clusters, git directories and files,
//! matrix, merge) give parameter sets; the template, rendered with each
//! (fasttemplate `{{key}}`, or Go templates with goTemplate), gives an
//! Application owned by the ApplicationSet. Applications no longer
//! generated are deleted.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use futures::StreamExt;
use kube::api::{Api, DynamicObject};
use kube::runtime::watcher::{self, Event};
use serde_json::{Map, Value, json};
use tracing::{info, warn};

use crate::app::{self, Ctx, IN_CLUSTER};
use crate::diff::merge_patch;

/// One parameter set: a JSON object (nested; fasttemplate flattens it).
pub type Params = Map<String, Value>;

const DEFAULT_REQUEUE: Duration = Duration::from_secs(180);

/// `foo.bar` keys for fasttemplate, as Argo CD flattens nested parameters.
pub fn flatten(p: &Params) -> BTreeMap<String, String> {
    fn walk(prefix: &str, v: &Value, out: &mut BTreeMap<String, String>) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&key, x, out);
                }
            }
            Value::String(s) => {
                out.insert(prefix.to_string(), s.clone());
            }
            Value::Null => {
                out.insert(prefix.to_string(), String::new());
            }
            other => {
                out.insert(prefix.to_string(), other.to_string());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk("", &Value::Object(p.clone()), &mut out);
    out
}

/// fasttemplate: `{{ key }}` replaced by its parameter; an unknown key is
/// left as it is, as Argo CD leaves it.
pub fn fasttemplate(text: &str, params: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let key = after[..end].trim();
                match params.get(key) {
                    Some(v) => out.push_str(v),
                    None => {
                        out.push_str("{{");
                        out.push_str(&after[..end]);
                        out.push_str("}}");
                    }
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// Render every string of a template (keys included) with `f`.
fn render_strings(v: &Value, f: &dyn Fn(&str) -> Result<String>) -> Result<Value> {
    Ok(match v {
        Value::String(s) => Value::String(f(s)?),
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|x| render_strings(x, f))
                .collect::<Result<_>>()?,
        ),
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, x) in m {
                out.insert(f(k)?, render_strings(x, f)?);
            }
            Value::Object(out)
        }
        other => other.clone(),
    })
}

/// The template rendered with one parameter set.
pub fn render_template(
    template: &Value,
    params: &Params,
    go: bool,
    go_options: &[String],
) -> Result<Value> {
    if go {
        let missing_error = go_options.iter().any(|o| o == "missingkey=error");
        render_strings(template, &|s| {
            crate::gotemplate::render(s, &Value::Object(params.clone()), missing_error)
        })
    } else {
        let flat = flatten(params);
        render_strings(template, &|s| Ok(fasttemplate(s, &flat)))
    }
}

fn normalized(name: &str) -> String {
    // As Argo CD's nameNormalized: lowercase, anything not [a-z0-9-.] → '-'.
    name.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Path parameters of the git generators: `path` as fasttemplate and goTemplate see them.
fn path_params(dir: &str, file: Option<&str>, go: bool) -> Params {
    let basename = Path::new(dir)
        .file_name()
        .map(|b| b.to_string_lossy().to_string())
        .unwrap_or_default();
    let segments: Vec<Value> = dir
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| json!(s))
        .collect();
    let mut m = Params::new();
    if go {
        let mut p = Map::new();
        p.insert("path".into(), json!(dir));
        p.insert("basename".into(), json!(basename));
        p.insert("basenameNormalized".into(), json!(normalized(&basename)));
        p.insert("segments".into(), Value::Array(segments));
        if let Some(f) = file {
            p.insert("filename".into(), json!(f));
            p.insert("filenameNormalized".into(), json!(normalized(f)));
        }
        m.insert("path".into(), Value::Object(p));
    } else {
        // fasttemplate: "path" itself plus "path.basename", "path[0]", ...
        m.insert("path".into(), json!(dir));
        m.insert("path.basename".into(), json!(basename));
        m.insert(
            "path.basenameNormalized".into(),
            json!(normalized(&basename)),
        );
        for (i, s) in segments.iter().enumerate() {
            m.insert(format!("path[{i}]"), s.clone());
        }
        if let Some(f) = file {
            m.insert("path.filename".into(), json!(f));
            m.insert("path.filenameNormalized".into(), json!(normalized(f)));
        }
    }
    m
}

/// `path.Match`-style globs: `*` does not cross '/'.
fn path_glob(pattern: &str) -> Result<globset::GlobMatcher> {
    Ok(globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .build()?
        .compile_matcher())
}

fn list_tree(
    root: &Path,
    dir: &Path,
    dirs: &mut Vec<String>,
    files: &mut Vec<String>,
) -> Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        let rel = p
            .strip_prefix(root)
            .unwrap_or(&p)
            .to_string_lossy()
            .to_string();
        if e.file_type()?.is_dir() {
            dirs.push(rel);
            list_tree(root, &p, dirs, files)?;
        } else {
            files.push(rel);
        }
    }
    Ok(())
}

/// What a set of generators gives, and how soon to look again.
pub struct Generated {
    pub params: Vec<Params>,
    pub requeue: Option<Duration>,
}

fn requeue_of(g: &Value) -> Option<Duration> {
    g["requeueAfterSeconds"].as_u64().map(Duration::from_secs)
}

/// Run one generator.
pub async fn generate(ctx: &Ctx, g: &Value, go: bool) -> Result<Generated> {
    let (kind, spec) = g
        .as_object()
        .and_then(|m| m.iter().find(|(k, _)| *k != "selector"))
        .map(|(k, v)| (k.as_str(), v))
        .context("empty generator")?;
    let mut out = match kind {
        "list" => {
            let mut params = Vec::new();
            for e in spec["elements"].as_array().into_iter().flatten() {
                match e {
                    Value::Object(m) => params.push(m.clone()),
                    other => bail!("list generator: element is not an object: {other}"),
                }
            }
            if let Some(y) = spec["elementsYaml"].as_str()
                && let Ok(Value::Array(items)) = serde_yaml::from_str::<Value>(y)
            {
                params.extend(items.into_iter().filter_map(|v| v.as_object().cloned()));
            }
            Generated {
                params,
                requeue: None,
            }
        }
        "clusters" => Generated {
            params: clusters(ctx, spec, go).await?,
            requeue: requeue_of(spec).or(Some(DEFAULT_REQUEUE)),
        },
        "git" => Generated {
            params: git(ctx, spec, go).await?,
            requeue: requeue_of(spec).or(Some(DEFAULT_REQUEUE)),
        },
        "matrix" => {
            let gens = spec["generators"]
                .as_array()
                .context("matrix: generators")?;
            if gens.len() != 2 {
                bail!("matrix generator needs exactly 2 generators");
            }
            let a = Box::pin(generate(ctx, &gens[0], go)).await?;
            let b = Box::pin(generate(ctx, &gens[1], go)).await?;
            let mut params = Vec::new();
            for x in &a.params {
                for y in &b.params {
                    let mut m = x.clone();
                    for (k, v) in y {
                        m.insert(k.clone(), v.clone());
                    }
                    params.push(m);
                }
            }
            Generated {
                params,
                requeue: min_requeue(a.requeue, b.requeue),
            }
        }
        "merge" => {
            let gens = spec["generators"].as_array().context("merge: generators")?;
            let keys: Vec<String> = spec["mergeKeys"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect();
            if gens.is_empty() {
                bail!("merge generator needs generators");
            }
            let base = Box::pin(generate(ctx, &gens[0], go)).await?;
            let mut params = base.params;
            let mut requeue = base.requeue;
            let key_of = |p: &Params| -> Vec<String> {
                let flat = flatten(p);
                keys.iter()
                    .map(|k| flat.get(k).cloned().unwrap_or_default())
                    .collect()
            };
            for g in &gens[1..] {
                let more = Box::pin(generate(ctx, g, go)).await?;
                requeue = min_requeue(requeue, more.requeue);
                for m in more.params {
                    let k = key_of(&m);
                    for p in params.iter_mut().filter(|p| key_of(p) == k) {
                        for (kk, v) in &m {
                            p.insert(kk.clone(), v.clone());
                        }
                    }
                }
            }
            Generated { params, requeue }
        }
        other => bail!("{other} generator is not supported by litecd yet"),
    };
    // A generator's own `values` (rendered with its parameters) join them as values.*.
    if spec["values"].is_object() && !matches!(kind, "clusters") {
        for p in out.params.iter_mut() {
            let rendered = render_template(&spec["values"], p, go, &[])?;
            p.insert("values".into(), rendered);
        }
    }
    Ok(out)
}

fn min_requeue(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

/// The clusters generator: the local cluster, then the cluster Secrets.
async fn clusters(ctx: &Ctx, spec: &Value, go: bool) -> Result<Vec<Params>> {
    let selector = &spec["selector"];
    let mut out = Vec::new();
    let mut add = |name: &str,
                   server: &str,
                   labels: Map<String, Value>,
                   annotations: Map<String, Value>|
     -> Result<()> {
        let mut p = Params::new();
        p.insert("name".into(), json!(name));
        p.insert("nameNormalized".into(), json!(normalized(name)));
        p.insert("server".into(), json!(server));
        p.insert("project".into(), json!(""));
        if go {
            p.insert(
                "metadata".into(),
                json!({"labels": labels, "annotations": annotations}),
            );
        } else {
            for (k, v) in &labels {
                p.insert(format!("metadata.labels.{k}"), v.clone());
            }
            for (k, v) in &annotations {
                p.insert(format!("metadata.annotations.{k}"), v.clone());
            }
        }
        if spec["values"].is_object() {
            let rendered = render_template(&spec["values"], &p, go, &[])?;
            if go {
                p.insert("values".into(), rendered);
            } else {
                for (k, v) in rendered.as_object().into_iter().flatten() {
                    p.insert(format!("values.{k}"), v.clone());
                }
            }
        }
        out.push(p);
        Ok(())
    };
    let secrets = match ctx.kube.known("v1", "Secret").await {
        Some(k) => ctx
            .kube
            .list(
                &k,
                Some(&ctx.namespace),
                Some("argocd.argoproj.io/secret-type=cluster"),
            )
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    };
    let decode = |s: &Value, k: &str| -> String {
        use base64::Engine;
        s["data"][k]
            .as_str()
            .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
            .map(|b| String::from_utf8_lossy(&b).to_string())
            .unwrap_or_default()
    };
    let matches = |labels: &Map<String, Value>| -> bool {
        selector["matchLabels"]
            .as_object()
            .is_none_or(|m| m.iter().all(|(k, v)| labels.get(k) == Some(v)))
    };
    let local_secret = secrets.iter().any(|s| decode(s, "server") == IN_CLUSTER);
    // The local cluster is there unless a selector asks for labels it lacks.
    if !local_secret
        && selector["matchLabels"]
            .as_object()
            .is_none_or(|m| m.is_empty())
        && selector["matchExpressions"].is_null()
    {
        add("in-cluster", IN_CLUSTER, Map::new(), Map::new())?;
    }
    for s in &secrets {
        let labels = s["metadata"]["labels"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        if !matches(&labels) {
            continue;
        }
        let annotations = s["metadata"]["annotations"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        add(
            &decode(s, "name"),
            &decode(s, "server"),
            labels,
            annotations,
        )?;
    }
    Ok(out)
}

/// The git generators: directories or files of a repository at a revision.
async fn git(ctx: &Ctx, spec: &Value, go: bool) -> Result<Vec<Params>> {
    let url = spec["repoURL"].as_str().context("git generator: repoURL")?;
    let rev = spec["revision"].as_str().unwrap_or("HEAD");
    let source = json!({"repoURL": url, "targetRevision": rev});
    // Fresh every pass: the generator's requeueAfterSeconds is how often it looks.
    let sha = app::resolve(ctx, &source, true).await?;
    let creds = app::creds_for(ctx, url).await;
    let tree = ctx.repos.checkout(url, &sha, creds.as_ref()).await?;
    let (mut dirs, mut files) = (Vec::new(), Vec::new());
    list_tree(&tree, &tree, &mut dirs, &mut files)?;
    dirs.sort();
    files.sort();
    let mut out = Vec::new();
    if let Some(ds) = spec["directories"].as_array() {
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        for d in ds {
            let g = path_glob(d["path"].as_str().unwrap_or_default())?;
            if d["exclude"].as_bool() == Some(true) {
                exclude.push(g)
            } else {
                include.push(g)
            }
        }
        for d in &dirs {
            if include.iter().any(|g| g.is_match(d)) && !exclude.iter().any(|g| g.is_match(d)) {
                out.push(path_params(d, None, go));
            }
        }
    }
    if let Some(fs) = spec["files"].as_array() {
        for f in fs {
            let g = path_glob(f["path"].as_str().unwrap_or_default())?;
            let exclude = f["exclude"].as_bool() == Some(true);
            if exclude {
                continue;
            }
            for file in files.iter().filter(|x| g.is_match(x)) {
                let text = std::fs::read_to_string(tree.join(file))?;
                let docs: Vec<Value> = if file.ends_with(".json") {
                    match serde_json::from_str::<Value>(&text)? {
                        Value::Array(a) => a,
                        v => vec![v],
                    }
                } else {
                    match serde_yaml::from_str::<Value>(&text)? {
                        Value::Array(a) => a,
                        v => vec![v],
                    }
                };
                let p = Path::new(file);
                let dir = p
                    .parent()
                    .map(|d| d.to_string_lossy().to_string())
                    .unwrap_or_default();
                let name = p
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                for doc in docs {
                    let mut params = doc.as_object().cloned().unwrap_or_default();
                    for (k, v) in path_params(&dir, Some(&name), go) {
                        params.insert(k, v);
                    }
                    out.push(params);
                }
            }
        }
    }
    Ok(out)
}

/// How often a set that stands down looks again, besides each change of the
/// guard.
const STAND_DOWN_RECHECK: Duration = Duration::from_secs(60);

/// The ApplicationSet controller: one reconcile per change, and again when a
/// generator asks (requeueAfterSeconds; git and clusters every 3 minutes).
pub async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let Ok(kind) = ctx
        .kube
        .kind("argoproj.io/v1alpha1", "ApplicationSet")
        .await
    else {
        info!("no ApplicationSet CRD: ApplicationSets off");
        return Ok(());
    };
    let api: Api<DynamicObject> = Api::namespaced_with(
        ctx.kube.client.clone(),
        &ctx.namespace,
        &kind.api_resource(),
    );
    let timers: Arc<tokio::sync::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>> =
        Arc::default();
    let mut s = watcher::watcher(api, watcher::Config::default()).boxed();
    info!("watching ApplicationSets in {}", ctx.namespace);
    while let Some(ev) = s.next().await {
        let o = match ev {
            Ok(Event::Apply(o) | Event::InitApply(o)) => o,
            Ok(_) => continue,
            Err(e) => {
                warn!("watch ApplicationSets: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let Some(name) = o.metadata.name.clone() else {
            continue;
        };
        let ctx = ctx.clone();
        let timers2 = timers.clone();
        let handle = tokio::spawn(async move {
            let mut guard = ctx.guard.subscribe();
            loop {
                // Seen before the pass: a change during it wakes the next.
                guard.borrow_and_update();
                let next = match reconcile(&ctx, &name).await {
                    Ok(n) => n,
                    Err(e) => {
                        warn!("applicationset {name}: {e:#}");
                        Some(Duration::from_secs(30))
                    }
                };
                match next {
                    // A change of the coexistence guard (Argo CD's
                    // controllers gone or back) is a reason to look again.
                    Some(d) => {
                        tokio::select! {
                            _ = tokio::time::sleep(d) => {}
                            _ = guard.changed() => {}
                        }
                    }
                    None => break,
                }
            }
            timers2.lock().await.remove(&name);
        });
        let name = o.metadata.name.clone().unwrap_or_default();
        if let Some(old) = timers.lock().await.insert(name, handle) {
            old.abort();
        }
    }
    Ok(())
}

fn condition(ty: &str, status: bool, reason: &str, message: &str) -> Value {
    json!({"type": ty, "status": if status { "True" } else { "False" }, "reason": reason, "message": message,
           "lastTransitionTime": app::now()})
}

/// Keep a condition's lastTransitionTime when nothing about it changed.
fn keep_times(new: &mut [Value], old: &Value) {
    for c in new.iter_mut() {
        if let Some(o) = old
            .as_array()
            .into_iter()
            .flatten()
            .find(|o| o["type"] == c["type"])
            && o["status"] == c["status"]
            && o["message"] == c["message"]
        {
            c["lastTransitionTime"] = o["lastTransitionTime"].clone();
        }
    }
}

/// One pass: generate, render, create/update/delete the Applications,
/// write the status. Ok(Some(d)): look again in d; Ok(None): gone.
async fn reconcile(ctx: &Ctx, name: &str) -> Result<Option<Duration>> {
    let set_kind = ctx
        .kube
        .kind("argoproj.io/v1alpha1", "ApplicationSet")
        .await?;
    let app_kind = ctx.kube.kind("argoproj.io/v1alpha1", "Application").await?;
    let ns = ctx.namespace.clone();
    let Some(set) = ctx.kube.get(&set_kind, Some(&ns), name).await? else {
        return Ok(None);
    };
    if set["metadata"]["deletionTimestamp"].is_string() {
        return Ok(None);
    }
    // Argo CD's controllers own it while they run (docs/spec.md,
    // Coexistence); a change of the guard wakes the set again.
    if !ctx.guard.may_act_on_appset(&set) {
        return Ok(Some(STAND_DOWN_RECHECK));
    }
    let spec = &set["spec"];
    let go = spec["goTemplate"].as_bool() == Some(true);
    let go_options: Vec<String> = spec["goTemplateOptions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    let uid = set["metadata"]["uid"]
        .as_str()
        .unwrap_or_default()
        .to_string();

    let mut params = Vec::new();
    let mut requeue: Option<Duration> = None;
    let mut error: Option<String> = None;
    for g in spec["generators"].as_array().into_iter().flatten() {
        match generate(ctx, g, go).await {
            Ok(out) => {
                requeue = min_requeue(requeue, out.requeue);
                // A generator's own template overrides the ApplicationSet's.
                for p in out.params {
                    params.push((
                        p,
                        g.as_object()
                            .and_then(|m| m.values().next())
                            .map(|v| v["template"].clone())
                            .unwrap_or(Value::Null),
                    ));
                }
            }
            Err(e) => error = Some(format!("{e:#}")),
        }
    }

    let mut wanted: BTreeMap<String, Value> = BTreeMap::new();
    if error.is_none() {
        for (p, gen_template) in &params {
            let mut template = spec["template"].clone();
            if gen_template.is_object() {
                merge_into(&mut template, gen_template);
            }
            match render_template(&template, p, go, &go_options) {
                Ok(t) => {
                    let app_name = t["metadata"]["name"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    if app_name.is_empty() {
                        error = Some("application name is empty".into());
                        break;
                    }
                    wanted.insert(app_name, t);
                }
                Err(e) => {
                    error = Some(format!("{e:#}"));
                    break;
                }
            }
        }
    }

    let policy = spec["syncPolicy"]["applicationsSync"]
        .as_str()
        .unwrap_or("sync");
    // Generating can take a while (git): look again before writing.
    if !ctx.guard.may_act_on_appset(&set) {
        return Ok(Some(STAND_DOWN_RECHECK));
    }
    if error.is_none() {
        for (app_name, t) in &wanted {
            let mut meta = json!({
                "name": app_name,
                "namespace": ns,
                "ownerReferences": [{"apiVersion": "argoproj.io/v1alpha1", "kind": "ApplicationSet", "name": name, "uid": uid,
                                     "controller": true, "blockOwnerDeletion": true}],
            });
            for f in ["labels", "annotations", "finalizers"] {
                if !t["metadata"][f].is_null() {
                    meta[f] = t["metadata"][f].clone();
                }
            }
            let desired = json!({"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "metadata": meta, "spec": t["spec"]});
            match ctx.kube.get(&app_kind, Some(&ns), app_name).await? {
                None => {
                    ctx.kube
                        .create(&app_kind, Some(&ns), &desired, false)
                        .await
                        .with_context(|| format!("creating {app_name}"))?;
                    info!("applicationset {name}: created {app_name}");
                }
                Some(live) => {
                    if policy == "create-only" {
                        continue;
                    }
                    // Spec, labels, annotations, finalizers and owner as the
                    // template says; other annotations (refresh) stay.
                    let mut want = live.clone();
                    want["spec"] = desired["spec"].clone();
                    for f in ["labels", "finalizers", "ownerReferences"] {
                        if desired["metadata"][f].is_null() {
                            if let Some(m) = want["metadata"].as_object_mut() {
                                m.remove(f);
                            }
                        } else {
                            want["metadata"][f] = desired["metadata"][f].clone();
                        }
                    }
                    let mut ann = desired["metadata"]["annotations"]
                        .as_object()
                        .cloned()
                        .unwrap_or_default();
                    if let Some(r) = live["metadata"]["annotations"][app::REFRESH].as_str() {
                        ann.insert(app::REFRESH.into(), json!(r));
                    }
                    if ann.is_empty() {
                        if let Some(m) = want["metadata"].as_object_mut() {
                            m.remove("annotations");
                        }
                    } else {
                        want["metadata"]["annotations"] = Value::Object(ann);
                    }
                    let strip = |v: &Value| json!({"metadata": v["metadata"], "spec": v["spec"]});
                    if let Some(patch) = merge_patch(&strip(&live), &strip(&want)) {
                        ctx.kube
                            .merge_patch(&app_kind, Some(&ns), app_name, &patch)
                            .await
                            .with_context(|| format!("updating {app_name}"))?;
                        info!("applicationset {name}: updated {app_name}");
                    }
                }
            }
        }
        // Owned Applications no longer generated.
        if policy == "sync" || policy == "create-delete" {
            for a in ctx.kube.list(&app_kind, Some(&ns), None).await? {
                let owned = a["metadata"]["ownerReferences"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|o| o["uid"] == uid.as_str());
                let n = a["metadata"]["name"].as_str().unwrap_or_default();
                if owned && !wanted.contains_key(n) && a["metadata"]["deletionTimestamp"].is_null()
                {
                    ctx.kube
                        .delete(&app_kind, Some(&ns), n, "Background")
                        .await?;
                    info!("applicationset {name}: deleted {n}");
                }
            }
        }
    }

    // Status as Argo CD writes it.
    let mut conds = match &error {
        None => vec![
            condition(
                "ErrorOccurred",
                false,
                "ApplicationSetUpToDate",
                "All applications have been generated successfully",
            ),
            condition(
                "ParametersGenerated",
                true,
                "ParametersGenerated",
                "Successfully generated parameters for all Applications",
            ),
            condition(
                "ResourcesUpToDate",
                true,
                "ApplicationSetUpToDate",
                "All applications have been generated successfully",
            ),
        ],
        Some(e) => vec![
            condition(
                "ErrorOccurred",
                true,
                "ApplicationGenerationFromParamsError",
                e,
            ),
            condition("ParametersGenerated", false, "ErrorOccurred", e),
            condition(
                "ResourcesUpToDate",
                false,
                "ApplicationGenerationFromParamsError",
                e,
            ),
        ],
    };
    keep_times(&mut conds, &set["status"]["conditions"]);
    let mut resources = Vec::new();
    for app_name in wanted.keys() {
        let live = ctx
            .kube
            .get(&app_kind, Some(&ns), app_name)
            .await
            .ok()
            .flatten();
        let st = live
            .as_ref()
            .map(|l| l["status"].clone())
            .unwrap_or(Value::Null);
        let mut r = json!({"group": "argoproj.io", "version": "v1alpha1", "kind": "Application", "name": app_name, "namespace": ns,
                           "health": st["health"].clone()});
        if let Some(s) = st["sync"]["status"].as_str() {
            r["status"] = json!(s);
        }
        resources.push(r);
    }
    let status = json!({"conditions": conds, "resources": resources});
    if let Some(p) = merge_patch(&set["status"], &status) {
        // ApplicationSets have a status subresource (Applications do not).
        ctx.kube
            .merge_patch_status(&set_kind, Some(&ns), name, &json!({"status": p}))
            .await?;
    }
    Ok(Some(requeue.unwrap_or(DEFAULT_REQUEUE)))
}

fn merge_into(base: &mut Value, over: &Value) {
    match (base, over) {
        (Value::Object(b), Value::Object(o)) => {
            for (k, v) in o {
                merge_into(b.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (b, o) => *b = o.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(v: Value) -> Params {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn fasttemplate_flattens_and_keeps_unknown() {
        let params = p(json!({"name": "a", "app": {"env": "dev"}, "n": 2, "path[0]": "apps"}));
        let flat = flatten(&params);
        assert_eq!(
            fasttemplate("x-{{name}}-{{ app.env }}-{{n}}-{{path[0]}}-{{nope}}", &flat),
            "x-a-dev-2-apps-{{nope}}"
        );
    }

    #[test]
    fn path_parameters() {
        let m = path_params("apps/one", None, false);
        assert_eq!(m["path"], "apps/one");
        assert_eq!(m["path.basename"], "one");
        assert_eq!(m["path[1]"], "one");
        let g = path_params("config/alpha", Some("config.json"), true);
        assert_eq!(g["path"]["basename"], "alpha");
        assert_eq!(g["path"]["filename"], "config.json");
        assert_eq!(g["path"]["segments"], json!(["config", "alpha"]));
    }

    #[test]
    fn globs_do_not_cross_slashes() {
        let g = path_glob("apps/*").unwrap();
        assert!(g.is_match("apps/one"));
        assert!(!g.is_match("apps/one/deep"));
        assert!(
            path_glob("config/*/config.json")
                .unwrap()
                .is_match("config/a/config.json")
        );
    }

    #[test]
    fn templates_render_keys_and_values() {
        let t = json!({"metadata": {"name": "list-{{name}}", "annotations": {"env": "{{env}}"}}, "spec": {"n": 1}});
        let out = render_template(&t, &p(json!({"name": "a", "env": "dev"})), false, &[]).unwrap();
        assert_eq!(out["metadata"]["name"], "list-a");
        assert_eq!(out["metadata"]["annotations"]["env"], "dev");
        assert_eq!(out["spec"]["n"], 1);
    }
}
