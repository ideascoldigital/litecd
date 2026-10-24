//! An Application's comparison: its source rendered at a revision, against
//! the live objects, giving what Argo CD writes in `.status`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};

use crate::diff;
use crate::health::{self, Customizations, Health, Status};
use crate::kubeapi::{Kind, Kube, split_api_version};
use crate::render::{self, SourceType};
use crate::repo::{Creds, Repos};

pub const TRACKING_ID: &str = "argocd.argoproj.io/tracking-id";
pub const INSTANCE_LABEL: &str = "app.kubernetes.io/instance";
pub const SYNC_WAVE: &str = "argocd.argoproj.io/sync-wave";
pub const SYNC_OPTIONS: &str = "argocd.argoproj.io/sync-options";
pub const HOOK: &str = "argocd.argoproj.io/hook";
pub const REFRESH: &str = "argocd.argoproj.io/refresh";
pub const IN_CLUSTER: &str = "https://kubernetes.default.svc";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tracking {
    Annotation,
    Label,
    AnnotationAndLabel,
}

/// What argocd-cm says, as far as litecd reads it.
#[derive(Clone, Debug)]
pub struct Settings {
    pub reconcile: Duration,
    pub tracking: Tracking,
    pub instance_label: String,
    pub health: Customizations,
    pub kustomize_build_options: String,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            reconcile: Duration::from_secs(180),
            tracking: Tracking::Annotation,
            instance_label: INSTANCE_LABEL.into(),
            health: Customizations::default(),
            kustomize_build_options: String::new(),
        }
    }
}

impl Settings {
    pub fn from_config(cm: Option<&Value>) -> Self {
        let mut s = Settings::default();
        let Some(data) = cm.and_then(|c| c["data"].as_object()) else {
            return s;
        };
        if let Some(t) = data.get("timeout.reconciliation").and_then(Value::as_str)
            && let Some(d) = parse_duration(t)
        {
            s.reconcile = d;
        }
        s.tracking = match data
            .get("application.resourceTrackingMethod")
            .and_then(Value::as_str)
        {
            Some("label") => Tracking::Label,
            Some("annotation+label") => Tracking::AnnotationAndLabel,
            _ => Tracking::Annotation,
        };
        if let Some(l) = data
            .get("application.instanceLabelKey")
            .and_then(Value::as_str)
            .filter(|l| !l.is_empty())
        {
            s.instance_label = l.to_string();
        }
        s.health = Customizations::from_config(data);
        s.kustomize_build_options = data
            .get("kustomize.buildOptions")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        s
    }
}

/// "180s", "3m", "1h", "0" (Go durations, the ones argocd-cm uses).
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s == "0" {
        return Some(Duration::ZERO);
    }
    let mut total = 0f64;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            continue;
        }
        let mut unit = c.to_string();
        if c == 'm' && chars.peek() == Some(&'s') {
            unit.push(chars.next()?);
        }
        let n: f64 = num.parse().ok()?;
        num.clear();
        total += n * match unit.as_str() {
            "ms" => 0.001,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
    }
    if !num.is_empty() {
        return None;
    }
    Some(Duration::from_secs_f64(total))
}

/// Everything the controller shares between apps.
pub struct Ctx {
    pub kube: Kube,
    pub repos: Repos,
    pub namespace: String,
    pub settings: RwLock<Settings>,
    manifests: Mutex<HashMap<String, Arc<Rendered>>>,
    /// Server-side dry runs that found a live object in sync: (live
    /// resourceVersion, desired) → in sync.
    dry_runs: Mutex<HashMap<(String, String), bool>>,
}

pub struct Rendered {
    pub source_type: SourceType,
    pub objects: Vec<Value>,
}

impl Ctx {
    pub fn new(kube: Kube, repos: Repos, namespace: String, settings: Settings) -> Self {
        Ctx {
            kube,
            repos,
            namespace,
            settings: RwLock::new(settings),
            manifests: Mutex::default(),
            dry_runs: Mutex::default(),
        }
    }
}

/// A resource as the app sees it: group, kind, namespace, name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    pub group: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

impl Key {
    pub fn of(obj: &Value) -> Key {
        let (group, _) = split_api_version(obj["apiVersion"].as_str().unwrap_or_default());
        Key {
            group: group.to_string(),
            kind: obj["kind"].as_str().unwrap_or_default().to_string(),
            namespace: obj["metadata"]["namespace"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            name: obj["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        }
    }

    /// "group/Kind/name", as Argo CD names a resource in messages.
    pub fn display(&self) -> String {
        format!("{}/{}/{}", self.group, self.kind, self.name)
    }
}

/// `helm.sh/hook: test` (or the older test-success / test-failure).
pub fn is_helm_test(obj: &Value) -> bool {
    obj["metadata"]["annotations"]["helm.sh/hook"]
        .as_str()
        .is_some_and(|h| h.split(',').any(|h| h.trim().starts_with("test")))
}

/// Whether a resource is the app's own Application.
pub fn is_self(app: &Value, k: &Key) -> bool {
    k.group == "argoproj.io"
        && k.kind == "Application"
        && app["metadata"]["name"] == k.name.as_str()
        && app["metadata"]["namespace"] == k.namespace.as_str()
}

/// The tracking id Argo CD writes: `<app>:<group>/<Kind>:<namespace>/<name>`.
pub fn tracking_id(app: &str, k: &Key) -> String {
    format!("{app}:{}/{}:{}/{}", k.group, k.kind, k.namespace, k.name)
}

/// Which app a live object belongs to, by its tracking metadata.
pub fn owner(obj: &Value, settings: &Settings) -> Option<String> {
    let meta = &obj["metadata"];
    match settings.tracking {
        Tracking::Label => meta["labels"][&settings.instance_label]
            .as_str()
            .map(str::to_string),
        _ => meta["annotations"][TRACKING_ID]
            .as_str()
            .and_then(|t| t.split(':').next())
            .map(str::to_string),
    }
}

/// A desired object, normalized: namespace, tracking metadata.
#[derive(Clone, Debug)]
pub struct Desired {
    pub key: Key,
    pub kind: Option<Kind>,
    pub version: String,
    pub obj: Value,
    pub wave: i64,
    pub options: Vec<String>,
    pub hook: Option<String>,
}

impl Desired {
    pub fn has_option(&self, opt: &str) -> bool {
        self.options.iter().any(|o| o == opt)
    }
}

pub fn options_of(obj: &Value) -> Vec<String> {
    obj["metadata"]["annotations"][SYNC_OPTIONS]
        .as_str()
        .map(|s| {
            s.split(',')
                .map(|o| o.trim().to_string())
                .filter(|o| !o.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

pub fn wave_of(obj: &Value) -> i64 {
    obj["metadata"]["annotations"][SYNC_WAVE]
        .as_str()
        .and_then(|w| w.trim().parse().ok())
        .unwrap_or(0)
}

/// One entry of status.resources, with what the controller needs besides.
#[derive(Clone, Debug)]
pub struct ResourceState {
    pub key: Key,
    pub version: String,
    pub synced: bool,
    pub requires_pruning: bool,
    pub wave: i64,
    pub live: Option<Value>,
    pub health: Option<Health>,
}

impl ResourceState {
    pub fn to_json(&self) -> Value {
        let mut r = serde_json::Map::new();
        if !self.key.group.is_empty() {
            r.insert("group".into(), json!(self.key.group));
        }
        r.insert("kind".into(), json!(self.key.kind));
        r.insert("name".into(), json!(self.key.name));
        if !self.key.namespace.is_empty() {
            r.insert("namespace".into(), json!(self.key.namespace));
        }
        r.insert(
            "status".into(),
            json!(if self.synced { "Synced" } else { "OutOfSync" }),
        );
        r.insert("version".into(), json!(self.version));
        if self.wave != 0 {
            r.insert("syncWave".into(), json!(self.wave));
        }
        if self.requires_pruning {
            r.insert("requiresPruning".into(), json!(true));
        }
        Value::Object(r)
    }
}

/// The result of comparing an app with the cluster.
pub struct Comparison {
    pub revision: String,
    pub source_type: SourceType,
    pub desired: Vec<Desired>,
    pub resources: Vec<ResourceState>,
    pub synced: bool,
    pub health: Status,
    pub images: Vec<String>,
    pub app_of_apps: bool,
}

/// Why an app could not be compared: an invalid spec (project rules) or
/// a source that could not be rendered.
#[derive(Debug)]
pub enum CompareError {
    InvalidSpec(String),
    Comparison(String),
}

pub fn spec_source(app: &Value) -> Value {
    app["spec"]["source"].clone()
}

pub fn destination_namespace(app: &Value) -> String {
    app["spec"]["destination"]["namespace"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// Repository credentials for a URL from the Argo CD Secrets.
pub async fn creds_for(ctx: &Ctx, url: &str) -> Option<Creds> {
    let secret = ctx.kube.known("v1", "Secret").await?;
    let norm = |u: &str| {
        u.trim_end_matches('/')
            .trim_end_matches(".git")
            .to_lowercase()
    };
    let want = norm(url);
    let decode = |s: &Value, k: &str| -> String {
        use base64::Engine;
        if let Some(v) = s["stringData"][k].as_str() {
            return v.to_string();
        }
        s["data"][k]
            .as_str()
            .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
            .map(|b| String::from_utf8_lossy(&b).to_string())
            .unwrap_or_default()
    };
    let secrets = ctx
        .kube
        .list(
            &secret,
            Some(&ctx.namespace),
            Some("argocd.argoproj.io/secret-type"),
        )
        .await
        .ok()?;
    let mut best: Option<(usize, Creds)> = None;
    for s in &secrets {
        let ty = s["metadata"]["labels"]["argocd.argoproj.io/secret-type"]
            .as_str()
            .unwrap_or_default();
        let u = norm(&decode(s, "url"));
        let c = Creds {
            username: decode(s, "username"),
            password: decode(s, "password"),
        };
        match ty {
            "repository" if u == want => return Some(c),
            "repo-creds" if !u.is_empty() && want.starts_with(&u) => {
                if best.as_ref().is_none_or(|(l, _)| u.len() > *l) {
                    best = Some((u.len(), c));
                }
            }
            _ => {}
        }
    }
    best.map(|(_, c)| c)
}

/// The revision a source points at now.
pub async fn resolve(ctx: &Ctx, source: &Value, fresh: bool) -> Result<String> {
    let url = source["repoURL"].as_str().unwrap_or_default();
    let rev = source["targetRevision"].as_str().unwrap_or_default();
    if source["chart"].is_string() {
        return Ok(rev.to_string());
    }
    let creds = creds_for(ctx, url).await;
    let max_age = ctx.settings.read().await.reconcile;
    ctx.repos
        .resolve(url, rev, creds.as_ref(), max_age, fresh)
        .await
}

/// Render a source at a revision (cached: a revision does not change).
pub async fn render(
    ctx: &Ctx,
    app: &Value,
    source: &Value,
    revision: &str,
    hard: bool,
) -> Result<Arc<Rendered>> {
    let name = app["metadata"]["name"].as_str().unwrap_or_default();
    let ns = destination_namespace(app);
    let key = format!("{name}|{ns}|{revision}|{source}");
    if !hard && let Some(r) = ctx.manifests.lock().await.get(&key) {
        return Ok(r.clone());
    }
    let url = source["repoURL"].as_str().unwrap_or_default();
    let creds = creds_for(ctx, url).await;
    let settings = ctx.settings.read().await.clone();
    let cx = render::RenderCtx {
        app_name: name,
        namespace: &ns,
        kube_version: ctx.kube.server_version().await,
        api_versions: ctx.kube.api_versions().await,
        cache: ctx.repos.cache_dir(),
        kustomize_build_options: settings.kustomize_build_options.clone(),
    };
    let rendered = if let Some(chart) = source["chart"].as_str() {
        let dir = ctx
            .repos
            .chart(url, chart, revision, creds.as_ref())
            .await?;
        Rendered {
            source_type: SourceType::Helm,
            objects: render::helm_template(&dir, source, &cx, None).await?,
        }
    } else {
        let tree = ctx.repos.checkout(url, revision, creds.as_ref()).await?;
        let path = source["path"]
            .as_str()
            .unwrap_or(".")
            .trim_start_matches('/');
        let dir = tree.join(if path.is_empty() { "." } else { path });
        if !dir.starts_with(&tree) || path.split('/').any(|p| p == "..") {
            bail!("{path}: app path is outside the repository");
        }
        if !dir.is_dir() {
            bail!("{path}: app path does not exist");
        }
        let ty = render::source_type(source, Some(&dir));
        let objects = match ty {
            SourceType::Directory => render::directory(&dir, &source["directory"])?,
            SourceType::Helm => render::helm_template(&dir, source, &cx, Some(&tree)).await?,
            SourceType::Kustomize => render::kustomize_build(&dir, &cx).await?,
        };
        Rendered {
            source_type: ty,
            objects,
        }
    };
    let r = Arc::new(rendered);
    let mut cache = ctx.manifests.lock().await;
    if cache.len() > 256 {
        cache.clear();
    }
    cache.insert(key, r.clone());
    Ok(r)
}

/// Normalize rendered objects: namespace for namespaced kinds, tracking.
pub async fn desired_objects(ctx: &Ctx, app: &Value, rendered: &Rendered) -> Vec<Desired> {
    let name = app["metadata"]["name"].as_str().unwrap_or_default();
    let dest_ns = destination_namespace(app);
    let settings = ctx.settings.read().await;
    let mut out = Vec::new();
    for o in &rendered.objects {
        // Helm test hooks are for `helm test`: Argo CD never applies them.
        if is_helm_test(o) {
            continue;
        }
        let mut obj = o.clone();
        let api = obj["apiVersion"].as_str().unwrap_or_default().to_string();
        let kind_name = obj["kind"].as_str().unwrap_or_default().to_string();
        let kind = ctx.kube.known(&api, &kind_name).await;
        if let Some(m) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
            m.remove("creationTimestamp");
            let namespaced = kind.as_ref().map(|k| k.namespaced).unwrap_or(true);
            if namespaced {
                if m.get("namespace")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                    && !dest_ns.is_empty()
                {
                    m.insert("namespace".into(), json!(dest_ns));
                }
            } else {
                m.remove("namespace");
            }
        }
        let key = Key::of(&obj);
        let hook = obj["metadata"]["annotations"][HOOK]
            .as_str()
            .map(str::to_string);
        if hook.is_none() {
            let id = tracking_id(name, &key);
            let meta = obj["metadata"]
                .as_object_mut()
                .expect("rendered objects have metadata");
            if settings.tracking != Tracking::Label {
                let ann = meta.entry("annotations").or_insert_with(|| json!({}));
                if !ann.is_object() {
                    *ann = json!({});
                }
                ann[TRACKING_ID] = json!(id);
            }
            if settings.tracking != Tracking::Annotation {
                let labels = meta.entry("labels").or_insert_with(|| json!({}));
                labels[&settings.instance_label] = json!(name);
            }
        }
        let (_, version) = split_api_version(&api);
        out.push(Desired {
            wave: wave_of(&obj),
            options: options_of(&obj),
            version: version.to_string(),
            key,
            kind,
            obj,
            hook,
        });
    }
    out
}

/// The last-applied-configuration kubectl would write for `obj`.
pub fn with_last_applied(obj: &Value) -> Value {
    let mut o = obj.clone();
    if let Some(a) = o["metadata"]["annotations"].as_object_mut() {
        a.remove(diff::LAST_APPLIED);
    }
    let text = serde_json::to_string(&o).unwrap_or_default();
    let meta = o["metadata"]
        .as_object_mut()
        .expect("objects have metadata");
    let ann = meta.entry("annotations").or_insert_with(|| json!({}));
    ann[diff::LAST_APPLIED] = json!(format!("{text}\n"));
    o
}

/// Whether a live object matches the desired one.
async fn in_sync(ctx: &Ctx, d: &Desired, live: &Value, ssa: bool) -> bool {
    let candidate = if ssa {
        d.obj.clone()
    } else {
        with_last_applied(&d.obj)
    };
    let mut ok = diff::subset(&d.obj, live);
    if ok && !ssa {
        let last = live["metadata"]["annotations"][diff::LAST_APPLIED]
            .as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok());
        if let Some(last) = last
            && diff::stale(&last, &d.obj, live)
        {
            ok = false;
        }
        return ok;
    }
    if ok {
        return true;
    }
    // Values may differ only in form: ask the API server what an apply
    // would leave.
    let Some(kind) = &d.kind else { return false };
    let rv = live["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let cache_key = (
        format!("{}|{rv}", tracking_id("", &d.key)),
        candidate.to_string(),
    );
    if let Some(v) = ctx.dry_runs.lock().await.get(&cache_key) {
        return *v;
    }
    let ns = (!d.key.namespace.is_empty()).then_some(d.key.namespace.as_str());
    let result = match ctx
        .kube
        .apply(kind, ns, &d.key.name, &candidate, true)
        .await
    {
        Ok(after) => diff::strip_volatile(&after) == diff::strip_volatile(live),
        Err(_) => false,
    };
    let mut cache = ctx.dry_runs.lock().await;
    if cache.len() > 4096 {
        cache.clear();
    }
    cache.insert(cache_key, result);
    result
}

/// Images of a live workload, for status.summary.
fn images(obj: &Value, out: &mut BTreeSet<String>) {
    let spec = match obj["kind"].as_str() {
        Some("Pod") => &obj["spec"],
        Some("CronJob") => &obj["spec"]["jobTemplate"]["spec"]["template"]["spec"],
        Some(
            "Deployment"
            | "StatefulSet"
            | "DaemonSet"
            | "ReplicaSet"
            | "Job"
            | "ReplicationController",
        ) => &obj["spec"]["template"]["spec"],
        _ => return,
    };
    for c in spec["initContainers"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(spec["containers"].as_array().into_iter().flatten())
    {
        if let Some(i) = c["image"].as_str() {
            out.insert(i.to_string());
        }
    }
}

pub fn app_options(op_or_spec_options: &Value) -> Vec<String> {
    op_or_spec_options
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

/// Project rules: the source repository and the destination must be
/// permitted.
pub async fn check_project(ctx: &Ctx, app: &Value) -> Result<(), CompareError> {
    let project = app["spec"]["project"]
        .as_str()
        .filter(|p| !p.is_empty())
        .unwrap_or("default");
    let Some(kind) = ctx.kube.known("argoproj.io/v1alpha1", "AppProject").await else {
        return Ok(());
    };
    let proj = match ctx.kube.get(&kind, Some(&ctx.namespace), project).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            return Err(CompareError::InvalidSpec(format!(
                "Application referencing project {project} which does not exist"
            )));
        }
        Err(e) => return Err(CompareError::Comparison(e.to_string())),
    };
    let sources: Vec<Value> = match app["spec"]["sources"].as_array() {
        Some(s) => s.clone(),
        None => vec![spec_source(app)],
    };
    let repos: Vec<&str> = proj["spec"]["sourceRepos"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    for s in &sources {
        let url = s["repoURL"].as_str().unwrap_or_default();
        if !repos.iter().any(|p| glob_match(p, url)) {
            return Err(CompareError::InvalidSpec(format!(
                "application repo {url} is not permitted in project '{project}'"
            )));
        }
    }
    let dest = &app["spec"]["destination"];
    let server = dest["server"].as_str().unwrap_or_default();
    let dname = dest["name"].as_str().unwrap_or_default();
    let dns = dest["namespace"].as_str().unwrap_or_default();
    let ok = proj["spec"]["destinations"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|d| {
            let ns_ok = glob_match(d["namespace"].as_str().unwrap_or_default(), dns);
            let server_ok = match (d["server"].as_str(), d["name"].as_str()) {
                (Some(s), _) if !server.is_empty() => glob_match(s, server),
                (_, Some(n)) if !dname.is_empty() => glob_match(n, dname),
                (Some(s), _) => dname == "in-cluster" && glob_match(s, IN_CLUSTER),
                _ => false,
            };
            ns_ok && server_ok
        });
    if !ok {
        return Err(CompareError::InvalidSpec(if server.is_empty() {
            format!(
                "application destination {{{dname} {dns}}} is not permitted in project '{project}'"
            )
        } else {
            format!(
                "application destination server '{server}' and namespace '{dns}' do not match any of the allowed destinations in project '{project}'"
            )
        }));
    }
    Ok(())
}

/// Argo CD's project globs: `*` matches anything (also `/`), braces alternate.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    globset::GlobBuilder::new(pattern)
        .literal_separator(false)
        .build()
        .map(|g| g.compile_matcher().is_match(text))
        .unwrap_or(pattern == text)
}

/// Compare an app with the cluster at `revision`.
pub async fn compare(
    ctx: &Ctx,
    app: &Value,
    source: &Value,
    revision: &str,
    hard: bool,
) -> Result<Comparison, CompareError> {
    let rendered = render(ctx, app, source, revision, hard)
        .await
        .map_err(|e| CompareError::Comparison(format!("{e:#}")))?;
    let mut desired = desired_objects(ctx, app, &rendered).await;
    // Kinds discovery did not know yet (a CRD of this very app): look again.
    if desired.iter().any(|d| d.kind.is_none()) {
        let _ = ctx.kube.rediscover().await;
        for d in desired.iter_mut().filter(|d| d.kind.is_none()) {
            d.kind = ctx
                .kube
                .known(
                    d.obj["apiVersion"].as_str().unwrap_or_default(),
                    &d.key.kind,
                )
                .await;
        }
    }
    let name = app["metadata"]["name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let settings = ctx.settings.read().await.clone();
    let options = app_options(&app["spec"]["syncPolicy"]["syncOptions"]);
    let app_ssa = options.iter().any(|o| o == "ServerSideApply=true");
    let mut resources = Vec::new();
    let mut seen = BTreeSet::new();
    for d in desired.iter().filter(|d| d.hook.is_none()) {
        seen.insert(d.key.clone());
        let live = match &d.kind {
            Some(k) => {
                let ns = (!d.key.namespace.is_empty()).then_some(d.key.namespace.as_str());
                ctx.kube
                    .get(k, ns, &d.key.name)
                    .await
                    .map_err(|e| CompareError::Comparison(e.to_string()))?
            }
            None => None,
        };
        let ssa = app_ssa || d.has_option("ServerSideApply=true");
        let replace = d.has_option("Replace=true") || options.iter().any(|o| o == "Replace=true");
        let synced = match &live {
            Some(l) => {
                if replace {
                    diff::subset(&d.obj, l)
                } else {
                    in_sync(ctx, d, l, ssa).await
                }
            }
            None => false,
        };
        let health = match &live {
            // An app that manages its own Application: Argo CD does not
            // assess that one's health (it would wait on itself).
            Some(_) if is_self(app, &d.key) => None,
            Some(l) => health::assess(l, &settings.health).unwrap_or_else(|e| {
                Some(Health {
                    status: Status::Unknown,
                    message: e.to_string(),
                })
            }),
            None => None,
        };
        resources.push(ResourceState {
            key: d.key.clone(),
            version: d.version.clone(),
            synced,
            requires_pruning: false,
            wave: d.wave,
            live,
            health,
        });
    }
    // What the app managed before and git no longer has: prune candidates.
    for r in app["status"]["resources"].as_array().into_iter().flatten() {
        let key = Key {
            group: r["group"].as_str().unwrap_or_default().into(),
            kind: r["kind"].as_str().unwrap_or_default().into(),
            namespace: r["namespace"].as_str().unwrap_or_default().into(),
            name: r["name"].as_str().unwrap_or_default().into(),
        };
        if seen.contains(&key) || r["hook"].as_bool() == Some(true) {
            continue;
        }
        let version = r["version"].as_str().unwrap_or_default().to_string();
        let api = if key.group.is_empty() {
            version.clone()
        } else {
            format!("{}/{version}", key.group)
        };
        let Some(kind) = ctx.kube.known(&api, &key.kind).await else {
            continue;
        };
        let ns = (!key.namespace.is_empty()).then_some(key.namespace.as_str());
        let Ok(Some(live)) = ctx.kube.get(&kind, ns, &key.name).await else {
            continue;
        };
        if owner(&live, &settings).as_deref() != Some(name.as_str()) {
            continue;
        }
        seen.insert(key.clone());
        let health = health::assess(&live, &settings.health).ok().flatten();
        resources.push(ResourceState {
            wave: wave_of(&live),
            key,
            version,
            synced: false,
            requires_pruning: true,
            live: Some(live),
            health,
        });
    }
    resources.sort_by(|a, b| a.key.cmp(&b.key));

    let synced = resources.iter().all(|r| r.synced && !r.requires_pruning);
    let any_live = resources
        .iter()
        .any(|r| r.live.is_some() && !r.requires_pruning);
    let mut health = Status::Healthy;
    if !resources.is_empty() && !any_live {
        health = Status::Missing;
    } else {
        for r in resources.iter().filter(|r| !r.requires_pruning) {
            if let Some(h) = &r.health
                && h.status > health
            {
                // A child Application that is Missing or Unknown does not
                // count for its parent.
                if r.key.kind == "Application"
                    && matches!(h.status, Status::Missing | Status::Unknown)
                {
                    continue;
                }
                health = h.status;
            }
        }
    }
    let mut imgs = BTreeSet::new();
    for r in &resources {
        if let Some(l) = &r.live {
            images(l, &mut imgs);
        }
    }
    let app_of_apps = resources
        .iter()
        .any(|r| r.key.group == "argoproj.io" && r.key.kind == "Application");
    Ok(Comparison {
        revision: revision.to_string(),
        source_type: rendered.source_type,
        desired,
        resources,
        synced,
        health,
        images: imgs.into_iter().collect(),
        app_of_apps,
    })
}

/// The live objects of a set of keys, for waiting on health.
pub async fn live_of(ctx: &Ctx, d: &Desired) -> Result<Option<Value>> {
    let Some(kind) = &d.kind else {
        return Err(anyhow!("unknown kind {}", d.key.kind));
    };
    let ns = (!d.key.namespace.is_empty()).then_some(d.key.namespace.as_str());
    ctx.kube.get(kind, ns, &d.key.name).await
}

pub fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("180s"), Some(Duration::from_secs(180)));
        assert_eq!(parse_duration("3m"), Some(Duration::from_secs(180)));
        assert_eq!(parse_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("x"), None);
    }

    #[test]
    fn tracking_ids() {
        let k = Key {
            group: "".into(),
            kind: "ConfigMap".into(),
            namespace: "ns".into(),
            name: "a".into(),
        };
        assert_eq!(tracking_id("app", &k), "app:/ConfigMap:ns/a");
        let obj = json!({"metadata": {"annotations": {TRACKING_ID: "app:/ConfigMap:ns/a"}}});
        assert_eq!(owner(&obj, &Settings::default()).as_deref(), Some("app"));
    }

    #[test]
    fn last_applied_is_the_object_without_itself() {
        let o = json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "a", "annotations": {"x": "1"}}});
        let la = with_last_applied(&with_last_applied(&o));
        let text = la["metadata"]["annotations"][diff::LAST_APPLIED]
            .as_str()
            .unwrap();
        let back: Value = serde_json::from_str(text).unwrap();
        assert_eq!(back, o);
    }

    #[test]
    fn helm_tests_are_not_applied() {
        assert!(is_helm_test(
            &json!({"metadata": {"annotations": {"helm.sh/hook": "test"}}})
        ));
        assert!(is_helm_test(
            &json!({"metadata": {"annotations": {"helm.sh/hook": "pre-install,test-success"}}})
        ));
        assert!(!is_helm_test(
            &json!({"metadata": {"annotations": {"helm.sh/hook": "pre-install"}}})
        ));
        assert!(!is_helm_test(&json!({"metadata": {}})));
    }

    #[test]
    fn project_globs() {
        assert!(glob_match("*", "https://x/y.git"));
        assert!(glob_match(
            "https://git.example.com/*",
            "https://git.example.com/a/b.git"
        ));
        assert!(!glob_match(
            "https://git.example.com/allowed.git",
            "http://other/x.git"
        ));
        assert!(glob_match("compat-*", "compat-a"));
    }
}
