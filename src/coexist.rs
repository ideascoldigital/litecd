//! Coexistence with Argo CD: litecd acts on nothing while Argo CD's own
//! controllers run in the cluster, so the two never fight over an app.
//!
//! Argo CD's application controller (a StatefulSet, or a Deployment with
//! dynamic cluster distribution, `argocd-application-controller` in the
//! controller's namespace) is "running" from the moment it is asked for
//! replicas until its last pod is gone: spec.replicas, status.replicas or
//! status.readyReplicas above 0. A pod that is starting or terminating can
//! already, or still, act, so Ready alone is not enough. While it runs, an
//! Application is left alone unless it carries `litecd.dev/manage: "true"`.
//! ApplicationSets also wait for Argo CD's applicationset controller
//! (`argocd-applicationset-controller`) to be gone: both write the
//! Applications they generate.
//!
//! Until both watches have listed once, the state is unknown and litecd
//! stands down.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::{Api, DynamicObject};
use kube::runtime::watcher::{self, Event};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::app::Ctx;

/// The annotation that lets litecd reconcile an Application (or
/// ApplicationSet) while Argo CD's controller runs.
pub const MANAGE: &str = "litecd.dev/manage";

const APP_CONTROLLER: &str = "argocd-application-controller";
const APPSET_CONTROLLER: &str = "argocd-applicationset-controller";
const NAME_LABEL: &str = "app.kubernetes.io/name";

/// The workload kinds Argo CD's controllers run as.
const KINDS: [&str; 2] = ["StatefulSet", "Deployment"];

/// What litecd knows about Argo CD's controllers.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Status {
    /// Both watches have listed once.
    pub known: bool,
    /// The application controller's running workloads (`kind/name: n
    /// replicas`), none when it is gone.
    pub controller: Option<String>,
    /// The same for the applicationset controller.
    pub appset_controller: Option<String>,
}

impl Status {
    /// Whether an Application may be acted on.
    pub fn may_act_on_app(&self, app: &Value) -> bool {
        self.apps_clear() || managed(app)
    }

    /// Whether every Application may be acted on (Argo CD's application
    /// controller is gone).
    pub fn apps_clear(&self) -> bool {
        self.known && self.controller.is_none()
    }

    /// Whether an ApplicationSet may be acted on.
    pub fn may_act_on_appset(&self, set: &Value) -> bool {
        (self.known && self.controller.is_none() && self.appset_controller.is_none())
            || managed(set)
    }

    /// Whether everything is litecd's (nothing of Argo CD's runs).
    pub fn clear(&self) -> bool {
        self.known && self.controller.is_none() && self.appset_controller.is_none()
    }

    /// One line for the log.
    pub fn describe(&self) -> String {
        if !self.known {
            return "Argo CD's controllers not seen yet: standing down".into();
        }
        match (&self.controller, &self.appset_controller) {
            (None, None) => {
                "no Argo CD application or applicationset controller running: litecd reconciles every app".into()
            }
            (Some(c), a) => format!(
                "Argo CD's application controller is running ({c}{}): standing down, only apps annotated {MANAGE}=\"true\" are reconciled",
                a.as_ref().map(|a| format!("; applicationset controller: {a}")).unwrap_or_default()
            ),
            (None, Some(a)) => format!(
                "Argo CD's application controller is gone: taking over Applications; its applicationset controller is running ({a}): ApplicationSets left to it unless annotated {MANAGE}=\"true\""
            ),
        }
    }
}

/// Whether an object carries `litecd.dev/manage: "true"`.
pub fn managed(obj: &Value) -> bool {
    obj["metadata"]["annotations"][MANAGE].as_str() == Some("true")
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Component {
    Controller,
    AppSetController,
}

/// Which of Argo CD's controllers a workload is, by name or by
/// `app.kubernetes.io/name`.
fn component(obj: &Value) -> Option<Component> {
    let name = obj["metadata"]["name"].as_str();
    let label = obj["metadata"]["labels"][NAME_LABEL].as_str();
    let is = |n: &str| name == Some(n) || label == Some(n);
    if is(APP_CONTROLLER) {
        Some(Component::Controller)
    } else if is(APPSET_CONTROLLER) {
        Some(Component::AppSetController)
    } else {
        None
    }
}

/// The replicas a workload is asked for or has, 0 when it is scaled down
/// and its pods are gone. spec.replicas defaults to 1.
fn replicas(obj: &Value) -> i64 {
    let spec = obj["spec"]["replicas"].as_i64().unwrap_or(1);
    let status = obj["status"]["replicas"].as_i64().unwrap_or(0);
    let ready = obj["status"]["readyReplicas"].as_i64().unwrap_or(0);
    spec.max(status).max(ready)
}

/// The status the workloads seen give.
fn status_of<'a>(known: bool, workloads: impl IntoIterator<Item = (&'a str, &'a Value)>) -> Status {
    let mut s = Status {
        known,
        ..Status::default()
    };
    for (kind, w) in workloads {
        let Some(c) = component(w) else { continue };
        let n = replicas(w);
        if n <= 0 {
            continue;
        }
        let line = format!(
            "{}/{}: {n} replicas, {} ready",
            kind.to_lowercase(),
            w["metadata"]["name"].as_str().unwrap_or_default(),
            w["status"]["readyReplicas"].as_i64().unwrap_or(0)
        );
        let slot = match c {
            Component::Controller => &mut s.controller,
            Component::AppSetController => &mut s.appset_controller,
        };
        *slot = Some(match slot.take() {
            Some(prev) => format!("{prev}, {line}"),
            None => line,
        });
    }
    s
}

/// The coexistence state, shared by every part of litecd that writes.
pub struct Guard {
    tx: watch::Sender<Status>,
}

impl Default for Guard {
    fn default() -> Self {
        Guard {
            tx: watch::channel(Status::default()).0,
        }
    }
}

impl Guard {
    pub fn status(&self) -> Status {
        self.tx.borrow().clone()
    }

    pub fn may_act_on_app(&self, app: &Value) -> bool {
        self.tx.borrow().may_act_on_app(app)
    }

    pub fn may_act_on_appset(&self, set: &Value) -> bool {
        self.tx.borrow().may_act_on_appset(set)
    }

    /// Changes of the state, to wake whoever waits on it.
    pub fn subscribe(&self) -> watch::Receiver<Status> {
        self.tx.subscribe()
    }

    /// Record a new state; true when it changed (and was logged).
    fn set(&self, s: Status) -> bool {
        let changed = self.tx.send_if_modified(|cur| {
            if *cur == s {
                return false;
            }
            *cur = s.clone();
            true
        });
        if changed {
            if s.clear() {
                info!("coexistence: {}", s.describe());
            } else {
                warn!("coexistence: {}", s.describe());
            }
        }
        changed
    }
}

/// Watch Argo CD's controller workloads in the controller's namespace and
/// keep `ctx.guard` up to date.
pub async fn run(ctx: Arc<Ctx>) {
    let (tx, mut rx) = mpsc::channel::<(&'static str, BTreeMap<String, Value>)>(16);
    for kind in KINDS {
        // Standing down meanwhile: the state stays unknown.
        let k = loop {
            match ctx.kube.kind("apps/v1", kind).await {
                Ok(k) => break k,
                Err(e) => {
                    warn!("coexistence: apps/v1 {kind}: {e:#}: standing down, retrying");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        };
        let api: Api<DynamicObject> =
            Api::namespaced_with(ctx.kube.client.clone(), &ctx.namespace, &k.api_resource());
        let tx = tx.clone();
        tokio::spawn(watch_kind(api, kind, tx));
    }
    drop(tx);
    let mut seen: BTreeMap<&'static str, BTreeMap<String, Value>> = BTreeMap::new();
    while let Some((kind, objs)) = rx.recv().await {
        seen.insert(kind, objs);
        let known = KINDS.iter().all(|k| seen.contains_key(k));
        let all = seen
            .iter()
            .flat_map(|(k, objs)| objs.values().map(move |o| (*k, o)));
        ctx.guard.set(status_of(known, all));
    }
}

/// One kind's workloads: the whole set after each change, once it has been
/// listed (a relist replaces it only when done, so it never looks empty in
/// between).
async fn watch_kind(
    api: Api<DynamicObject>,
    kind: &'static str,
    tx: mpsc::Sender<(&'static str, BTreeMap<String, Value>)>,
) {
    let mut live: BTreeMap<String, Value> = BTreeMap::new();
    let mut staging: Option<BTreeMap<String, Value>> = None;
    let mut listed = false;
    let mut s = watcher::watcher(api, watcher::Config::default()).boxed();
    while let Some(ev) = s.next().await {
        let to_value = |o: &DynamicObject| serde_json::to_value(o).unwrap_or_default();
        let name = |o: &DynamicObject| o.metadata.name.clone().unwrap_or_default();
        match ev {
            Ok(Event::Init) => {
                staging = Some(BTreeMap::new());
                continue;
            }
            Ok(Event::InitApply(o)) => {
                staging
                    .get_or_insert_with(BTreeMap::new)
                    .insert(name(&o), to_value(&o));
                continue;
            }
            Ok(Event::InitDone) => {
                live = staging.take().unwrap_or_default();
                listed = true;
            }
            Ok(Event::Apply(o)) => {
                live.insert(name(&o), to_value(&o));
            }
            Ok(Event::Delete(o)) => {
                live.remove(&name(&o));
            }
            Err(e) => {
                warn!("coexistence: watch {kind}s: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }
        if listed && tx.send((kind, live.clone())).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sts(name: &str, spec: i64, replicas: i64, ready: i64) -> Value {
        json!({"metadata": {"name": name, "labels": {}},
               "spec": {"replicas": spec}, "status": {"replicas": replicas, "readyReplicas": ready}})
    }

    fn app(annotations: Value) -> Value {
        json!({"metadata": {"name": "a", "annotations": annotations}})
    }

    #[test]
    fn unknown_stands_down() {
        let s = Status::default();
        assert!(!s.may_act_on_app(&app(json!({}))));
        assert!(!s.may_act_on_appset(&app(json!({}))));
        assert!(s.may_act_on_app(&app(json!({MANAGE: "true"}))));
    }

    #[test]
    fn running_controller_blocks_unannotated_apps() {
        let w = sts(APP_CONTROLLER, 1, 1, 1);
        let s = status_of(true, [("StatefulSet", &w)]);
        assert!(s.controller.is_some());
        assert!(!s.may_act_on_app(&app(json!({}))));
        assert!(!s.may_act_on_app(&app(json!({MANAGE: "false"}))));
        assert!(!s.may_act_on_app(&app(json!({MANAGE: true}))));
        assert!(s.may_act_on_app(&app(json!({MANAGE: "true"}))));
        assert!(!s.may_act_on_appset(&app(json!({}))));
        assert!(s.may_act_on_appset(&app(json!({MANAGE: "true"}))));
    }

    #[test]
    fn scaled_down_and_gone_lets_litecd_act() {
        let w = sts(APP_CONTROLLER, 0, 0, 0);
        let s = status_of(true, [("StatefulSet", &w)]);
        assert_eq!(s.controller, None);
        assert!(s.clear());
        assert!(s.may_act_on_app(&app(json!({}))));
        assert!(s.may_act_on_appset(&app(json!({}))));
        // No Argo CD at all.
        let s = status_of(true, []);
        assert!(s.may_act_on_app(&app(json!({}))));
    }

    #[test]
    fn starting_or_terminating_pods_count() {
        // Scaled up, pod not Ready yet: it may already reconcile.
        let w = sts(APP_CONTROLLER, 1, 0, 0);
        assert!(status_of(true, [("StatefulSet", &w)]).controller.is_some());
        // Scaled down, pod still terminating.
        let w = sts(APP_CONTROLLER, 0, 1, 0);
        assert!(status_of(true, [("StatefulSet", &w)]).controller.is_some());
        let w = sts(APP_CONTROLLER, 0, 0, 1);
        assert!(status_of(true, [("StatefulSet", &w)]).controller.is_some());
        // spec.replicas left out means 1.
        let w = json!({"metadata": {"name": APP_CONTROLLER}, "spec": {}});
        assert!(status_of(true, [("StatefulSet", &w)]).controller.is_some());
    }

    #[test]
    fn found_by_label_and_as_deployment() {
        let w = json!({"metadata": {"name": "argo-cd-controller", "labels": {NAME_LABEL: APP_CONTROLLER}},
                       "spec": {"replicas": 2}, "status": {"replicas": 2, "readyReplicas": 2}});
        let s = status_of(true, [("Deployment", &w)]);
        assert_eq!(
            s.controller.as_deref(),
            Some("deployment/argo-cd-controller: 2 replicas, 2 ready")
        );
        // Others in the namespace do not count.
        let others = [
            sts("litecd", 1, 1, 1),
            sts("argocd-server", 1, 1, 1),
            sts("argocd-redis", 1, 1, 1),
        ];
        assert!(status_of(true, others.iter().map(|o| ("Deployment", o))).clear());
    }

    #[test]
    fn appset_controller_holds_appsets_only() {
        let c = sts(APP_CONTROLLER, 0, 0, 0);
        let a = sts(APPSET_CONTROLLER, 1, 1, 1);
        let s = status_of(true, [("StatefulSet", &c), ("Deployment", &a)]);
        assert!(s.may_act_on_app(&app(json!({}))));
        assert!(!s.may_act_on_appset(&app(json!({}))));
        assert!(s.may_act_on_appset(&app(json!({MANAGE: "true"}))));
        assert!(!s.clear());
    }

    #[test]
    fn guard_logs_only_changes() {
        let g = Guard::default();
        assert!(!g.may_act_on_app(&app(json!({}))));
        let up = status_of(true, [("StatefulSet", &sts(APP_CONTROLLER, 1, 1, 1))]);
        assert!(g.set(up.clone()));
        assert!(!g.set(up));
        assert!(!g.may_act_on_app(&app(json!({}))));
        assert!(g.set(status_of(
            true,
            [("StatefulSet", &sts(APP_CONTROLLER, 0, 0, 0))]
        )));
        assert!(g.may_act_on_app(&app(json!({}))));
    }
}
