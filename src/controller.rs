//! The controller: one task per Application, woken by watches on
//! Applications, AppProjects, argocd-cm and the kinds the apps manage, and by
//! its own timers (periodic refresh, self-heal backoff).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use futures::StreamExt;
use kube::api::{Api, DynamicObject};
use kube::runtime::watcher::{self, Event};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};

use crate::app::{self, CompareError, Comparison, Ctx, REFRESH, Settings, Sources};
use crate::diff::merge_patch;
use crate::kubeapi::Kind;
use crate::sync::{self, Request, TaskResult};
use crate::windows;

pub const FINALIZER: &str = "resources-finalizer.argocd.argoproj.io";
const FINALIZER_FG: &str = "resources-finalizer.argocd.argoproj.io/foreground";
const FINALIZER_BG: &str = "resources-finalizer.argocd.argoproj.io/background";

pub struct Controller {
    ctx: Arc<Ctx>,
    /// A task per app, by instance name (`<namespace>_<name>` outside the
    /// controller's namespace).
    apps: Mutex<HashMap<String, mpsc::Sender<()>>>,
    /// Every Application seen, reconciled or not: instance name →
    /// (namespace, name), to start those application.namespaces enables.
    seen: Mutex<HashMap<String, (String, String)>>,
    watched: Mutex<HashSet<String>>,
    /// application.namespaces when argocd-cmd-params-cm has none (the
    /// controller's flag).
    default_app_namespaces: String,
}

impl Controller {
    pub fn new(ctx: Arc<Ctx>, default_app_namespaces: String) -> Arc<Self> {
        Arc::new(Controller {
            ctx,
            default_app_namespaces,
            apps: Mutex::default(),
            seen: Mutex::default(),
            watched: Mutex::default(),
        })
    }

    pub async fn kick(&self, name: &str) {
        if let Some(tx) = self.apps.lock().await.get(name) {
            let _ = tx.try_send(());
        }
    }

    async fn kick_all(&self) {
        for tx in self.apps.lock().await.values() {
            let _ = tx.try_send(());
        }
    }

    async fn ensure_task(self: &Arc<Self>, ns: &str, name: &str) {
        if !self.ctx.app_namespace_allowed(ns).await {
            return;
        }
        let instance = self.instance(ns, name);
        let mut apps = self.apps.lock().await;
        if let Some(tx) = apps.get(&instance)
            && !tx.is_closed()
        {
            let _ = tx.try_send(());
            return;
        }
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.try_send(());
        apps.insert(instance.clone(), tx);
        let me = self.clone();
        let (ns, name) = (ns.to_string(), name.to_string());
        tokio::spawn(async move {
            let mut task = AppTask {
                c: me.clone(),
                ns,
                name,
                instance: instance.clone(),
                last_fresh: None,
            };
            task.run(rx).await;
            me.apps.lock().await.remove(&instance);
        });
    }

    fn instance(&self, ns: &str, name: &str) -> String {
        app::instance_name(
            &json!({"metadata": {"namespace": ns, "name": name}}),
            &self.ctx.namespace,
        )
    }

    /// Watch a kind the apps manage: its changes wake the owning app.
    /// Watch a kind the apps manage, in the cluster `kube` talks to.
    pub async fn watch_kind(
        self: &Arc<Self>,
        kube: &crate::kubeapi::Kube,
        server: &str,
        kind: &Kind,
    ) {
        let id = format!("{server}|{}/{}", kind.api_version(), kind.kind);
        if !self.watched.lock().await.insert(id.clone()) {
            return;
        }
        let api: Api<DynamicObject> = Api::all_with(kube.client.clone(), &kind.api_resource());
        let me = self.clone();
        tokio::spawn(async move {
            let mut stream = watcher::watcher(api, watcher::Config::default()).boxed();
            while let Some(ev) = stream.next().await {
                let objs = match ev {
                    Ok(Event::Apply(o)) | Ok(Event::Delete(o)) => vec![o],
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("watch {id}: {e}");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
                let settings = me.ctx.settings.read().await.clone();
                for o in objs {
                    let Ok(v) = serde_json::to_value(&o) else {
                        continue;
                    };
                    if let Some(owner) = app::owner(&v, &settings) {
                        me.kick(&owner).await;
                    }
                }
            }
        });
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        ensure_default_project(&self.ctx).await;
        let app_kind = self
            .ctx
            .kube
            .kind("argoproj.io/v1alpha1", "Application")
            .await
            .context("the Application CRD")?;
        let ns = self.ctx.namespace.clone();
        let client = self.ctx.kube.client.clone();

        // argocd-cm: settings, then every app.
        {
            let me = self.clone();
            let api: Api<k8s_openapi::api::core::v1::ConfigMap> =
                Api::namespaced(client.clone(), &ns);
            tokio::spawn(async move {
                let cfg = watcher::Config::default().fields("metadata.name=argocd-cm");
                let mut s = watcher::watcher(api, cfg).boxed();
                while let Some(ev) = s.next().await {
                    if let Ok(Event::Apply(o) | Event::InitApply(o)) = ev {
                        let v = serde_json::to_value(&o).unwrap_or_default();
                        *me.ctx.settings.write().await = Settings::from_config(Some(&v));
                        me.kick_all().await;
                    }
                }
            });
        }
        // argocd-cmd-params-cm: application.namespaces, then the apps it
        // enables (an app it no longer enables ends its own task).
        {
            let me = self.clone();
            let api: Api<k8s_openapi::api::core::v1::ConfigMap> =
                Api::namespaced(client.clone(), &ns);
            tokio::spawn(async move {
                let cfg = watcher::Config::default().fields("metadata.name=argocd-cmd-params-cm");
                let mut s = watcher::watcher(api, cfg).boxed();
                while let Some(ev) = s.next().await {
                    let data = match ev {
                        Ok(Event::Apply(o) | Event::InitApply(o)) => o.data.unwrap_or_default(),
                        Ok(Event::Delete(_)) => Default::default(),
                        _ => continue,
                    };
                    let list = data
                        .get("application.namespaces")
                        .cloned()
                        .unwrap_or_else(|| me.default_app_namespaces.clone());
                    let list = app::split_list(&list);
                    if *me.ctx.app_namespaces.read().await == list {
                        continue;
                    }
                    info!("application namespaces: {list:?}");
                    *me.ctx.app_namespaces.write().await = list;
                    let seen: Vec<(String, String)> =
                        me.seen.lock().await.values().cloned().collect();
                    for (ns, name) in seen {
                        me.ensure_task(&ns, &name).await;
                    }
                    me.kick_all().await;
                }
            });
        }
        // AppProjects: every app.
        if let Ok(pk) = self
            .ctx
            .kube
            .kind("argoproj.io/v1alpha1", "AppProject")
            .await
        {
            let me = self.clone();
            let api: Api<DynamicObject> =
                Api::namespaced_with(client.clone(), &ns, &pk.api_resource());
            tokio::spawn(async move {
                let mut s = watcher::watcher(api, watcher::Config::default()).boxed();
                while let Some(ev) = s.next().await {
                    if matches!(ev, Ok(Event::Apply(_)) | Ok(Event::Delete(_))) {
                        me.kick_all().await;
                    }
                }
            });
        }
        // Applications, in every namespace (application.namespaces says
        // which are reconciled): their own task, and the parent of a child
        // app.
        let api: Api<DynamicObject> = Api::all_with(client, &app_kind.api_resource());
        let mut s = watcher::watcher(api, watcher::Config::default()).boxed();
        info!("watching Applications ({ns} and application.namespaces)");
        while let Some(ev) = s.next().await {
            let o = match ev {
                Ok(Event::Apply(o) | Event::InitApply(o) | Event::Delete(o)) => o,
                Ok(_) => continue,
                Err(e) => {
                    warn!("watch Applications: {e}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let (Some(name), Some(app_ns)) =
                (o.metadata.name.clone(), o.metadata.namespace.clone())
            else {
                continue;
            };
            let instance = self.instance(&app_ns, &name);
            self.seen
                .lock()
                .await
                .insert(instance.clone(), (app_ns.clone(), name.clone()));
            self.ensure_task(&app_ns, &name).await;
            let v = serde_json::to_value(&o).unwrap_or_default();
            let settings = self.ctx.settings.read().await.clone();
            if let Some(parent) = app::owner(&v, &settings)
                && parent != instance
            {
                self.kick(&parent).await;
            }
        }
        Ok(())
    }
}

async fn ensure_default_project(ctx: &Ctx) {
    let Ok(k) = ctx.kube.kind("argoproj.io/v1alpha1", "AppProject").await else {
        return;
    };
    if let Ok(Some(_)) = ctx.kube.get(&k, Some(&ctx.namespace), "default").await {
        return;
    }
    let p = json!({
        "apiVersion": "argoproj.io/v1alpha1", "kind": "AppProject",
        "metadata": {"name": "default", "namespace": ctx.namespace},
        "spec": {"sourceRepos": ["*"], "destinations": [{"namespace": "*", "server": "*"}],
                 "clusterResourceWhitelist": [{"group": "*", "kind": "*"}]}
    });
    if let Err(e) = ctx.kube.create(&k, Some(&ctx.namespace), &p, false).await {
        warn!("creating the default AppProject: {e}");
    }
}

/// What a reconcile asks for next.
enum Next {
    Wait(Duration),
    Again,
    Gone,
}

struct AppTask {
    c: Arc<Controller>,
    ns: String,
    name: String,
    /// What its resources are tracked by (app::instance_name).
    instance: String,
    last_fresh: Option<Instant>,
}

/// A Kubernetes duration for retry backoffs ("5s", "3m").
fn backoff_delay(retry: &Value, attempt: u32) -> Duration {
    let b = &retry["backoff"];
    let base = b["duration"]
        .as_str()
        .and_then(app::parse_duration)
        .unwrap_or(Duration::from_secs(5));
    let factor = b["factor"].as_f64().unwrap_or(2.0);
    let max = b["maxDuration"]
        .as_str()
        .and_then(app::parse_duration)
        .unwrap_or(Duration::from_secs(180));
    let d = base.as_secs_f64() * factor.powi(attempt.saturating_sub(1) as i32);
    Duration::from_secs_f64(d.min(max.as_secs_f64()))
}

fn upsert_condition(conds: &mut Vec<Value>, ty: &str, message: &str) {
    if let Some(c) = conds.iter_mut().find(|c| c["type"] == ty) {
        if c["message"] != message {
            c["message"] = json!(message);
            c["lastTransitionTime"] = json!(app::now());
        }
    } else {
        conds.push(json!({"type": ty, "message": message, "lastTransitionTime": app::now()}));
    }
}

fn remove_conditions(status: &mut serde_json::Map<String, Value>, types: &[&str]) {
    if let Some(Value::Array(c)) = status.get_mut("conditions") {
        c.retain(|c| !types.iter().any(|t| c["type"] == *t));
        if c.is_empty() {
            status.remove("conditions");
        }
    }
}

fn set_condition(status: &mut serde_json::Map<String, Value>, ty: &str, message: &str) {
    let mut conds = status
        .get("conditions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    upsert_condition(&mut conds, ty, message);
    status.insert("conditions".into(), Value::Array(conds));
}

/// status.sync.comparedTo: destination, source(s), ignoreDifferences.
fn compared_to(app: &Value, src: &Sources) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("destination".into(), app["spec"]["destination"].clone());
    m.extend(src.status_fields());
    if app["spec"]["ignoreDifferences"].is_array() {
        m.insert(
            "ignoreDifferences".into(),
            app["spec"]["ignoreDifferences"].clone(),
        );
    }
    Value::Object(m)
}

/// The status a comparison gives, on top of the stored one.
fn status_from(
    ctx: &Ctx,
    app: &Value,
    cmp: &Result<Comparison, CompareError>,
    reconciled: bool,
) -> Value {
    let old = app["status"].as_object().cloned().unwrap_or_default();
    let mut s = old.clone();
    match cmp {
        Ok(c) => {
            let src = Sources::of_app(app);
            let mut sync = serde_json::Map::new();
            sync.insert("comparedTo".into(), compared_to(app, &src));
            sync.extend(src.revision_fields(&c.revisions));
            sync.insert(
                "status".into(),
                json!(if c.synced { "Synced" } else { "OutOfSync" }),
            );
            s.insert("sync".into(), Value::Object(sync));
            s.insert("health".into(), json!({"status": c.health.as_str()}));
            s.insert(
                "resources".into(),
                Value::Array(c.resources.iter().map(|r| r.to_json()).collect()),
            );
            if src.multi {
                s.remove("sourceType");
                s.insert("sourceTypes".into(), json!(c.source_types));
            } else {
                s.remove("sourceTypes");
                s.insert(
                    "sourceType".into(),
                    json!(c.source_types.first().cloned().unwrap_or_default()),
                );
            }
            s.insert("controllerNamespace".into(), json!(ctx.namespace));
            s.insert("resourceHealthSource".into(), json!("appTree"));
            let mut summary = serde_json::Map::new();
            if !c.images.is_empty() {
                summary.insert("images".into(), json!(c.images));
            }
            if c.app_of_apps {
                summary.insert("isAppOfApps".into(), json!(true));
            }
            // Empty, only once written (Argo CD's own syncs write it).
            if !summary.is_empty() || s.contains_key("summary") {
                s.insert("summary".into(), Value::Object(summary));
            }
            remove_conditions(
                &mut s,
                &["ComparisonError", "InvalidSpecError", "UnknownError"],
            );
            // One SharedResourceWarning per shared resource, kept in place.
            let shared: Vec<String> = c
                .resources
                .iter()
                .filter_map(|r| r.shared_message(app))
                .collect();
            let mut conds = s
                .get("conditions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            conds.retain(|x| {
                x["type"] != "SharedResourceWarning"
                    || shared.iter().any(|m| x["message"] == m.as_str())
            });
            for m in &shared {
                if !conds
                    .iter()
                    .any(|x| x["type"] == "SharedResourceWarning" && x["message"] == m.as_str())
                {
                    conds.push(json!({"type": "SharedResourceWarning", "message": m, "lastTransitionTime": app::now()}));
                }
            }
            if conds.is_empty() {
                s.remove("conditions");
            } else {
                s.insert("conditions".into(), Value::Array(conds));
            }
        }
        Err(e @ (CompareError::InvalidSpec(msg) | CompareError::Unknown(msg))) => {
            s.retain(|k, _| {
                matches!(
                    k.as_str(),
                    "conditions" | "history" | "operationState" | "sourceHydrator" | "summary"
                )
            });
            s.insert("health".into(), json!({"status": "Unknown"}));
            s.insert("sync".into(), json!({"status": "Unknown"}));
            let ty = match e {
                CompareError::Unknown(_) => "UnknownError",
                _ => "InvalidSpecError",
            };
            remove_conditions(
                &mut s,
                &["ComparisonError", "InvalidSpecError", "UnknownError"],
            );
            set_condition(&mut s, ty, msg);
        }
        Err(CompareError::Comparison(msg)) => {
            s.insert(
                "sync".into(),
                json!({"comparedTo": compared_to(app, &Sources::of_app(app)), "status": "Unknown"}),
            );
            remove_conditions(&mut s, &["InvalidSpecError", "UnknownError"]);
            set_condition(&mut s, "ComparisonError", msg);
        }
    }
    if reconciled || !s.contains_key("reconciledAt") {
        s.insert("reconciledAt".into(), json!(app::now()));
    }
    Value::Object(s)
}

impl AppTask {
    fn ctx(&self) -> &Ctx {
        &self.c.ctx
    }

    async fn run(&mut self, mut rx: mpsc::Receiver<()>) {
        let mut wait = Duration::ZERO;
        loop {
            if !wait.is_zero() {
                tokio::select! {
                    k = rx.recv() => if k.is_none() { return },
                    _ = tokio::time::sleep(wait) => {},
                }
                // Coalesce a burst of events (the objects of one apply
                // arrive within a few ms; one more wake lands in the
                // channel's single slot while the reconcile runs).
                tokio::time::sleep(Duration::from_millis(20)).await;
                while rx.try_recv().is_ok() {}
            }
            wait = match self.reconcile().await {
                Ok(Next::Gone) => return,
                Ok(Next::Again) => Duration::ZERO,
                Ok(Next::Wait(d)) => d,
                Err(e) => {
                    warn!("app {}: {e:#}", self.instance);
                    Duration::from_secs(10)
                }
            };
        }
    }

    async fn app_kind(&self) -> Result<Kind> {
        self.ctx()
            .kube
            .kind("argoproj.io/v1alpha1", "Application")
            .await
    }

    async fn get(&self) -> Result<Option<Value>> {
        let k = self.app_kind().await?;
        self.ctx().kube.get(&k, Some(&self.ns), &self.name).await
    }

    async fn patch(&self, patch: &Value) -> Result<Value> {
        let k = self.app_kind().await?;
        self.ctx()
            .kube
            .merge_patch(&k, Some(&self.ns), &self.name, patch)
            .await
    }

    /// Write `status` over what is stored, as a merge patch of the
    /// difference (plus whatever else `extra` holds, top-level).
    async fn write_status(
        &self,
        app: &Value,
        status: &Value,
        extra: Option<Value>,
    ) -> Result<Value> {
        let mut patch = json!({});
        if let Some(p) = merge_patch(&app["status"], status) {
            patch["status"] = p;
        }
        if let Some(Value::Object(e)) = extra {
            for (k, v) in e {
                patch[k] = v;
            }
        }
        if patch.as_object().is_some_and(|m| m.is_empty()) {
            return Ok(app.clone());
        }
        self.patch(&patch).await
    }

    async fn reconcile(&mut self) -> Result<Next> {
        if !self.ctx().app_namespace_allowed(&self.ns).await {
            return Ok(Next::Gone);
        }
        let Some(app) = self.get().await? else {
            return Ok(Next::Gone);
        };
        if app["metadata"]["deletionTimestamp"].is_string() {
            return self.finalize(&app).await;
        }
        let reconcile_every = self.ctx().settings.read().await.reconcile;
        // An operation a sync window holds back waits for the next
        // refresh; the app is still compared meanwhile.
        if app["operation"].is_object() && self.operate(app.clone()).await? {
            return Ok(Next::Again);
        }
        let Some(app) = self.get().await? else {
            return Ok(Next::Gone);
        };
        let refresh = app["metadata"]["annotations"][REFRESH]
            .as_str()
            .map(str::to_string);
        let fresh = refresh.is_some()
            || self
                .last_fresh
                .is_none_or(|t| t.elapsed() >= reconcile_every);
        let hard = refresh.as_deref() == Some("hard");

        let (cmp, revisions) = self.compare(&app, fresh, hard).await;
        if fresh {
            self.last_fresh = Some(Instant::now());
        }
        let status = status_from(self.ctx(), &app, &cmp, fresh);
        let extra = refresh.map(|_| json!({"metadata": {"annotations": {REFRESH: null}}}));
        let app = self.write_status(&app, &status, extra).await?;
        if let Ok(c) = &cmp {
            let server = self
                .ctx()
                .target(&app["spec"]["destination"])
                .await
                .map(|t| t.1)
                .unwrap_or_default();
            for r in &c.desired {
                if let Some(k) = &r.kind {
                    self.c.watch_kind(&c.kube, &server, k).await;
                }
            }
        }
        let periodic = Next::Wait(reconcile_every.max(Duration::from_secs(5)));
        let Ok(cmp) = cmp else { return Ok(periodic) };
        if app["operation"].is_object()
            || !self.windows_allow(&app, false, None).await.unwrap_or(false)
        {
            return Ok(periodic);
        }
        match self.auto_sync(&app, &cmp, &revisions) {
            AutoSync::No => Ok(periodic),
            AutoSync::Later(d) => Ok(Next::Wait(d.min(reconcile_every))),
            AutoSync::Now(op) => {
                info!(
                    "app {}: automated sync to {}",
                    self.instance,
                    revisions.join(", ")
                );
                let app = self.patch(&json!({"operation": op})).await?;
                self.operate(app).await?;
                Ok(Next::Again)
            }
        }
    }

    /// Whether the project's sync windows let a sync of `app` run now.
    async fn windows_allow(
        &self,
        app: &Value,
        manual: bool,
        started: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<bool, String> {
        let Some(project) = app::project_of(self.ctx(), app).await else {
            return Ok(true);
        };
        let w = windows::matching(&project, app);
        windows::can_sync(&w, manual, started, chrono::Utc::now())
    }

    /// An operation a sync window holds back: Running, with Argo CD's
    /// message, its result naming only the requested revision and source.
    async fn hold(
        &self,
        app: &Value,
        op: &Value,
        started: &str,
        err: Option<String>,
    ) -> Result<()> {
        let sync = &op["sync"];
        let mut result = json!({"revision": sync["revision"].as_str().unwrap_or_default()});
        if sync["source"].is_object() || sync["sources"].is_array() {
            if sync["source"].is_object() {
                result["source"] = sync["source"].clone();
            }
            if sync["sources"].is_array() {
                result["sources"] = sync["sources"].clone();
            }
        } else {
            if app["spec"]["sources"].is_array() {
                result["sources"] = app["spec"]["sources"].clone();
            }
            let source = if app["spec"]["sources"]
                .as_array()
                .is_some_and(|s| !s.is_empty())
            {
                app["spec"]["sources"][0].clone()
            } else {
                app["spec"]["source"].clone()
            };
            if source.is_object() {
                result["source"] = source;
            }
        }
        if sync["revisions"].as_array().is_some_and(|r| !r.is_empty()) {
            result["revisions"] = sync["revisions"].clone();
        }
        let message = match err {
            Some(e) => format!("{}: {e}", windows::BLOCKED),
            None => windows::BLOCKED.to_string(),
        };
        let mut status = app["status"].as_object().cloned().unwrap_or_default();
        status.insert(
            "operationState".into(),
            json!({"operation": op, "phase": "Running", "startedAt": started, "message": message, "syncResult": result}),
        );
        self.write_status(app, &Value::Object(status), None).await?;
        Ok(())
    }

    /// A comparison at given revisions against the app's destination.
    async fn target_compare(
        &self,
        app: &Value,
        src: &Sources,
        revs: &[String],
    ) -> Result<Comparison, CompareError> {
        let (kube, _) = self
            .ctx()
            .target(&app["spec"]["destination"])
            .await
            .map_err(CompareError::InvalidSpec)?;
        app::compare(self.ctx(), &kube, app, src, revs, false).await
    }

    async fn compare(
        &self,
        app: &Value,
        fresh: bool,
        hard: bool,
    ) -> (Result<Comparison, CompareError>, Vec<String>) {
        // The destination cluster first: a project checks its server.
        let (kube, server) = match self.ctx().target(&app["spec"]["destination"]).await {
            Ok(t) => t,
            Err(m) => return (Err(CompareError::InvalidSpec(m)), Vec::new()),
        };
        if let Err(e) = app::check_project(self.ctx(), app, &server).await {
            return (Err(e), Vec::new());
        }
        let src = Sources::of_app(app);
        let revisions = match app::resolve_all(self.ctx(), &src, fresh).await {
            Ok(r) => r,
            Err(e) => return (Err(CompareError::Comparison(format!("{e:#}"))), Vec::new()),
        };
        (
            app::compare(self.ctx(), &kube, app, &src, &revisions, hard).await,
            revisions,
        )
    }

    fn auto_sync(&self, app: &Value, cmp: &Comparison, revisions: &[String]) -> AutoSync {
        let policy = &app["spec"]["syncPolicy"];
        let auto = &policy["automated"];
        if !auto.is_object() || auto["enabled"].as_bool() == Some(false) || cmp.synced {
            return AutoSync::No;
        }
        let prune = auto["prune"].as_bool().unwrap_or(false);
        let self_heal = auto["selfHeal"].as_bool().unwrap_or(false);
        let prunable = |r: &app::ResourceState| {
            prune
                && !r
                    .live
                    .as_ref()
                    .is_some_and(|l| app::options_of(l).iter().any(|o| o == "Prune=false"))
        };
        let needed: Vec<&app::ResourceState> = cmp
            .resources
            .iter()
            .filter(|r| {
                if r.requires_pruning {
                    prunable(r)
                } else {
                    !r.synced
                }
            })
            .collect();
        if needed.is_empty() {
            return AutoSync::No;
        }
        if cmp.desired.is_empty() && auto["allowEmpty"].as_bool() != Some(true) {
            return AutoSync::No;
        }
        let src = Sources::of_app(app);
        let last = &app["status"]["operationState"];
        let same = src.same(revisions, &last["syncResult"]);
        let mut sync = serde_json::Map::new();
        sync.extend(src.revision_fields(revisions));
        sync.extend(src.operation_fields());
        let mut sync = Value::Object(sync);
        if prune {
            sync["prune"] = json!(true);
        }
        if let Some(o) = policy["syncOptions"].as_array().filter(|o| !o.is_empty()) {
            sync["syncOptions"] = json!(o);
        }
        if same {
            if last["phase"] != "Succeeded" || !self_heal {
                return AutoSync::No;
            }
            let attempts = last["operation"]["sync"]["autoHealAttemptsCount"]
                .as_i64()
                .unwrap_or(0);
            let delay = Duration::from_secs_f64((2.0 * 3f64.powi(attempts as i32)).min(300.0));
            if let Some(fin) = last["finishedAt"]
                .as_str()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            {
                let since = chrono::Utc::now()
                    .signed_duration_since(fin)
                    .to_std()
                    .unwrap_or_default();
                if since < delay {
                    return AutoSync::Later(delay - since);
                }
            }
            sync["autoHealAttemptsCount"] = json!(attempts + 1);
            sync["resources"] = Value::Array(
                needed
                    .iter()
                    .map(|r| {
                        let mut v = json!({"kind": r.key.kind, "name": r.key.name});
                        if !r.key.group.is_empty() {
                            v["group"] = json!(r.key.group);
                        }
                        if !r.key.namespace.is_empty() {
                            v["namespace"] = json!(r.key.namespace);
                        }
                        v
                    })
                    .collect(),
            );
        }
        let retry = if policy["retry"].is_object() {
            policy["retry"].clone()
        } else {
            json!({"limit": 5})
        };
        AutoSync::Now(json!({"initiatedBy": {"automated": true}, "retry": retry, "sync": sync}))
    }

    /// Run the operation the app holds, to its end; false: a sync window
    /// holds it back.
    async fn operate(&mut self, mut app: Value) -> Result<bool> {
        let mut op = app["operation"].clone();
        if !op["initiatedBy"].is_object() {
            op["initiatedBy"] = json!({});
        }
        if !op["retry"].is_object() {
            op["retry"] = json!({});
        }
        // An operation already Running (held back by a window) keeps its
        // start.
        let last = &app["status"]["operationState"];
        let started = match last["startedAt"].as_str() {
            Some(t) if last["phase"] == "Running" && last["operation"] == op => t.to_string(),
            _ => app::now(),
        };
        let manual = op["initiatedBy"]["automated"].as_bool() != Some(true);
        let since = chrono::DateTime::parse_from_rfc3339(&started)
            .ok()
            .map(|t| t.with_timezone(&chrono::Utc));
        match self.windows_allow(&app, manual, since).await {
            Ok(true) => {}
            Ok(false) => {
                self.hold(&app, &op, &started, None).await?;
                return Ok(false);
            }
            Err(e) => {
                self.hold(&app, &op, &started, Some(e)).await?;
                return Ok(false);
            }
        }
        let mut status = app["status"].as_object().cloned().unwrap_or_default();
        status.insert(
            "operationState".into(),
            json!({"operation": op, "phase": "Running", "startedAt": started}),
        );
        // Argo CD writes them (empty) with the operations it starts itself:
        // it updates the whole Application.
        if op["initiatedBy"]["automated"].as_bool() == Some(true) {
            status.entry("sourceHydrator").or_insert(json!({}));
            status.entry("summary").or_insert(json!({}));
        }
        app = self
            .write_status(&app, &Value::Object(status), None)
            .await?;

        let src = Sources::of_operation(&op["sync"], &app);
        let mut req = Request::from_operation(&op);
        req.ignore = app::ignore_rules(&app, &*self.ctx().settings.read().await);
        let retry = op["retry"].clone();
        let limit = retry["limit"].as_i64().unwrap_or(0);
        let mut retries: i64 = 0;
        let mut no_more_tasks = false;
        let (phase, message, results, revision) = loop {
            let given: Option<Vec<String>> = if src.multi {
                op["sync"]["revisions"].as_array().map(|r| {
                    r.iter()
                        .map(|x| x.as_str().unwrap_or_default().to_string())
                        .collect()
                })
            } else {
                op["sync"]["revision"]
                    .as_str()
                    .filter(|r| !r.is_empty())
                    .map(|r| vec![r.to_string()])
            };
            let revision = match given {
                Some(r) => Ok(r),
                None => app::resolve_all(self.ctx(), &src, false).await,
            };
            let attempt = match revision {
                Err(e) => Err((format!("{e:#}"), vec![], Vec::new())),
                Ok(rev) => match self.target_compare(&app, &src, &rev).await {
                    Err(
                        CompareError::Comparison(m)
                        | CompareError::InvalidSpec(m)
                        | CompareError::Unknown(m),
                    ) => Err((m, vec![], rev)),
                    Ok(cmp) => {
                        let mut progress = OpProgress {
                            task: self,
                            op: op.clone(),
                            started: started.clone(),
                            retries,
                            result_fields: result_fields(&src, &rev),
                        };
                        match sync::run(self.ctx(), &app, &cmp, &req, &mut progress).await {
                            Ok(done) => {
                                no_more_tasks = done.no_more_tasks;
                                Ok((done.results, rev))
                            }
                            Err(f) => Err((f.message, f.results, rev)),
                        }
                    }
                },
            };
            match attempt {
                Ok((results, rev)) => {
                    let msg = if no_more_tasks {
                        "successfully synced (no more tasks)"
                    } else {
                        "successfully synced (all tasks run)"
                    };
                    break ("Succeeded", msg.to_string(), results, rev);
                }
                Err((msg, results, rev)) => {
                    if msg == sync::TERMINATED {
                        info!("app {}: operation terminated", self.instance);
                        let Some(app) = self.get().await? else {
                            return Ok(true);
                        };
                        if app["metadata"]["deletionTimestamp"].is_string() {
                            return Ok(true);
                        }
                        break ("Failed", msg, results, rev);
                    }
                    if limit < 0 || retries < limit {
                        retries += 1;
                        let delay = backoff_delay(&retry, retries as u32);
                        let fresh = self.get().await?.unwrap_or(app.clone());
                        let mut st = fresh["status"].as_object().cloned().unwrap_or_default();
                        st.insert(
                            "operationState".into(),
                            json!({"operation": op, "phase": "Running", "startedAt": started, "retryCount": retries,
                                   "message": format!("Retrying attempt #{retries} at {}: {msg}", chrono::Utc::now().format("%-I:%M%P"))}),
                        );
                        app = self.write_status(&fresh, &Value::Object(st), None).await?;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    let msg = if retries > 0 {
                        format!("{msg} (retried {retries} times).")
                    } else {
                        msg
                    };
                    break ("Failed", msg, results, rev);
                }
            }
        };
        info!("app {}: sync {phase}: {message}", self.instance);
        let app = self.get().await?.unwrap_or(app);
        let mut st = app["status"].as_object().cloned().unwrap_or_default();
        let mut state = json!({
            "operation": op, "phase": phase, "message": message, "startedAt": started, "finishedAt": app::now(),
        });
        let mut result = result_fields(&src, &revision);
        // Argo CD leaves the list out when no task ran.
        if !results.is_empty() {
            result["resources"] =
                json!(results.iter().map(TaskResult::to_json).collect::<Vec<_>>());
        }
        state["syncResult"] = result;
        if retries > 0 {
            state["retryCount"] = json!(retries);
        }
        st.insert("operationState".into(), state);
        if phase == "Succeeded" {
            remove_conditions(&mut st, &["SyncError"]);
            let mut history = st
                .get("history")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let last = history.last();
            if last.is_none_or(|h| !src.same(&revision, h)) {
                let id = last
                    .and_then(|h| h["id"].as_i64())
                    .map(|i| i + 1)
                    .unwrap_or(0);
                let mut entry = json!({"id": id, "initiatedBy": op["initiatedBy"], "deployStartedAt": started, "deployedAt": app::now()});
                for (k, v) in src
                    .revision_fields(&revision)
                    .into_iter()
                    .chain(src.status_fields())
                {
                    entry[k] = v;
                }
                history.push(entry);
                let keep = app["spec"]["revisionHistoryLimit"].as_u64().unwrap_or(10) as usize;
                if history.len() > keep {
                    history.drain(..history.len() - keep);
                }
                st.insert("history".into(), Value::Array(history));
            }
        } else {
            set_condition(
                &mut st,
                "SyncError",
                &format!(
                    "Failed last sync attempt to [{}]: {message}",
                    revision.join(" ")
                ),
            );
        }
        self.write_status(&app, &Value::Object(st), Some(json!({"operation": null})))
            .await?;
        Ok(true)
    }

    /// Delete what the app manages, then let the app go.
    async fn finalize(&self, app: &Value) -> Result<Next> {
        let finalizers: Vec<String> = app["metadata"]["finalizers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        let ours: Vec<&String> = finalizers
            .iter()
            .filter(|f| [FINALIZER, FINALIZER_FG, FINALIZER_BG].contains(&f.as_str()))
            .collect();
        if ours.is_empty() {
            return Ok(Next::Wait(Duration::from_secs(5)));
        }
        let propagation = if ours.iter().any(|f| f.as_str() == FINALIZER_BG) {
            "Background"
        } else {
            "Foreground"
        };
        let settings = self.ctx().settings.read().await.clone();
        // What it manages lives in its destination cluster; a destination
        // no cluster matches has nothing to delete.
        let kube = match self.ctx().target(&app["spec"]["destination"]).await {
            Ok((k, _)) => k,
            Err(_) => self.ctx().kube.clone(),
        };
        let mut remaining = 0;
        for r in app["status"]["resources"].as_array().into_iter().flatten() {
            let group = r["group"].as_str().unwrap_or_default();
            let version = r["version"].as_str().unwrap_or_default();
            let api = if group.is_empty() {
                version.to_string()
            } else {
                format!("{group}/{version}")
            };
            let Ok(kind) = kube
                .kind(&api, r["kind"].as_str().unwrap_or_default())
                .await
            else {
                continue;
            };
            let ns = r["namespace"].as_str().filter(|n| !n.is_empty());
            let name = r["name"].as_str().unwrap_or_default();
            let Ok(Some(live)) = kube.get(&kind, ns, name).await else {
                continue;
            };
            if app::owner(&live, &settings).as_deref() != Some(self.instance.as_str()) {
                continue;
            }
            remaining += 1;
            if live["metadata"]["deletionTimestamp"].is_null() {
                let _ = kube.delete(&kind, ns, name, propagation).await;
            }
        }
        if remaining > 0 {
            return Ok(Next::Wait(Duration::from_secs(1)));
        }
        let keep: Vec<&String> = finalizers.iter().filter(|f| !ours.contains(f)).collect();
        self.patch(&json!({"metadata": {"finalizers": keep}}))
            .await?;
        Ok(Next::Wait(Duration::from_secs(1)))
    }
}

/// syncResult without its resources: revision(s) and source(s); a
/// multi-source one also has an empty revision and source, as Argo CD's.
fn result_fields(src: &Sources, revs: &[String]) -> Value {
    let mut m = serde_json::Map::new();
    if src.multi {
        m.insert("revision".into(), json!(""));
    }
    m.extend(src.revision_fields(revs));
    m.extend(src.status_fields());
    Value::Object(m)
}

enum AutoSync {
    No,
    Later(Duration),
    Now(Value),
}

/// While a sync waits on a wave: the message in operationState, and the
/// app's status kept current.
struct OpProgress<'a> {
    task: &'a AppTask,
    op: Value,
    started: String,
    retries: i64,
    result_fields: Value,
}

impl sync::Progress for OpProgress<'_> {
    async fn waiting(&mut self, message: String, results: &[TaskResult]) -> bool {
        let t = self.task;
        let Ok(Some(app)) = t.get().await else {
            return false;
        };
        if app["metadata"]["deletionTimestamp"].is_string() || !app["operation"].is_object() {
            return false;
        }
        let (cmp, _) = t.compare(&app, false, false).await;
        let mut status = status_from(t.ctx(), &app, &cmp, false);
        let mut result = self.result_fields.clone();
        // Argo CD leaves the list out when no task ran.
        if !results.is_empty() {
            result["resources"] =
                json!(results.iter().map(TaskResult::to_json).collect::<Vec<_>>());
        }
        let mut state = json!({"operation": self.op, "phase": "Running", "startedAt": self.started, "message": message, "syncResult": result});
        if self.retries > 0 {
            state["retryCount"] = json!(self.retries);
        }
        status["operationState"] = state;
        if let Err(e) = t.write_status(&app, &status, None).await {
            warn!("app {}: {e:#}", t.instance);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoffs() {
        let r = json!({"backoff": {"duration": "1s", "factor": 2, "maxDuration": "10s"}});
        assert_eq!(backoff_delay(&r, 1), Duration::from_secs(1));
        assert_eq!(backoff_delay(&r, 2), Duration::from_secs(2));
        assert_eq!(backoff_delay(&r, 9), Duration::from_secs(10));
        assert_eq!(backoff_delay(&json!({}), 1), Duration::from_secs(5));
    }

    #[test]
    fn conditions_keep_their_transition_time() {
        let mut c = vec![json!({"type": "SyncError", "message": "a", "lastTransitionTime": "t0"})];
        upsert_condition(&mut c, "SyncError", "a");
        assert_eq!(c[0]["lastTransitionTime"], "t0");
        upsert_condition(&mut c, "SyncError", "b");
        assert_ne!(c[0]["lastTransitionTime"], "t0");
    }
}
