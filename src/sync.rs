//! A sync operation, as gitops-engine runs one: a dry run of every task,
//! then waves in order, each applied and, before the next, healthy.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};

use crate::app::{self, Comparison, Ctx, Desired, Key, ResourceState};
use crate::health::Status;
use crate::kubeapi::{ApiError, Kind};

/// Pause between waves (Argo CD's ARGOCD_SYNC_WAVE_DELAY).
pub const WAVE_DELAY: Duration = Duration::from_secs(2);

/// Helm's install order, which Argo CD uses within a wave.
const KIND_ORDER: [&str; 35] = [
    "Namespace",
    "NetworkPolicy",
    "ResourceQuota",
    "LimitRange",
    "PodSecurityPolicy",
    "PodDisruptionBudget",
    "ServiceAccount",
    "Secret",
    "SecretList",
    "ConfigMap",
    "StorageClass",
    "PersistentVolume",
    "PersistentVolumeClaim",
    "CustomResourceDefinition",
    "ClusterRole",
    "ClusterRoleList",
    "ClusterRoleBinding",
    "ClusterRoleBindingList",
    "Role",
    "RoleList",
    "RoleBinding",
    "RoleBindingList",
    "Service",
    "DaemonSet",
    "Pod",
    "ReplicationController",
    "ReplicaSet",
    "Deployment",
    "HorizontalPodAutoscaler",
    "StatefulSet",
    "Job",
    "CronJob",
    "IngressClass",
    "Ingress",
    "APIService",
];

fn kind_rank(kind: &str) -> usize {
    KIND_ORDER
        .iter()
        .position(|k| *k == kind)
        .unwrap_or(KIND_ORDER.len())
}

/// One task's outcome, an entry of syncResult.resources.
#[derive(Clone, Debug)]
pub struct TaskResult {
    pub key: Key,
    pub version: String,
    pub status: &'static str,
    pub message: String,
    pub hook_phase: &'static str,
    pub sync_phase: &'static str,
    pub images: Vec<String>,
}

impl TaskResult {
    pub fn to_json(&self) -> Value {
        let mut r = json!({
            "group": self.key.group,
            "kind": self.key.kind,
            "name": self.key.name,
            "namespace": self.key.namespace,
            "version": self.version,
            "status": self.status,
            "message": self.message,
            "hookPhase": self.hook_phase,
            "syncPhase": self.sync_phase,
        });
        if !self.images.is_empty() {
            r["images"] = json!(self.images);
        }
        r
    }
}

/// What the operation asks for.
pub struct Request {
    pub prune: bool,
    pub options: Vec<String>,
    /// Selective sync: only these (group, kind, name).
    pub only: Option<Vec<(String, String, String)>>,
}

impl Request {
    pub fn from_operation(op: &Value) -> Self {
        let sync = &op["sync"];
        let only = sync["resources"].as_array().map(|rs| {
            rs.iter()
                .map(|r| {
                    (
                        r["group"].as_str().unwrap_or_default().to_string(),
                        r["kind"].as_str().unwrap_or_default().to_string(),
                        r["name"].as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect()
        });
        Request {
            prune: sync["prune"].as_bool().unwrap_or(false),
            options: app::app_options(&sync["syncOptions"]),
            only,
        }
    }

    fn has(&self, opt: &str) -> bool {
        self.options.iter().any(|o| o == opt)
    }

    fn wants(&self, k: &Key) -> bool {
        match &self.only {
            None => true,
            Some(list) => list
                .iter()
                .any(|(g, kind, n)| *g == k.group && *kind == k.kind && *n == k.name),
        }
    }
}

enum Task<'a> {
    Apply(&'a Desired, Option<&'a ResourceState>),
    Prune(&'a ResourceState),
}

impl Task<'_> {
    fn key(&self) -> &Key {
        match self {
            Task::Apply(d, _) => &d.key,
            Task::Prune(r) => &r.key,
        }
    }

    fn wave(&self) -> i64 {
        match self {
            Task::Apply(d, _) => d.wave,
            Task::Prune(r) => r.wave,
        }
    }
}

/// Progress reports while a sync waits between waves. `false`: the
/// operation is over (the app is gone, being deleted, or its operation
/// was removed).
pub trait Progress {
    fn waiting(
        &mut self,
        message: String,
        results: &[TaskResult],
    ) -> impl std::future::Future<Output = bool> + Send;
}

pub const TERMINATED: &str = "operation terminated";

/// The failure of a sync: the message and the results so far.
pub struct Failed {
    pub message: String,
    pub results: Vec<TaskResult>,
}

fn lower(kind: &str) -> String {
    kind.to_lowercase()
}

fn err_msg(e: &anyhow::Error) -> String {
    match e.downcast_ref::<ApiError>() {
        Some(a) => a.message.clone(),
        None => format!("{e:#}"),
    }
}

/// Run one attempt of a sync against a fresh comparison.
pub async fn run(
    ctx: &Ctx,
    app_obj: &Value,
    cmp: &Comparison,
    req: &Request,
    progress: &mut impl Progress,
) -> Result<Vec<TaskResult>, Failed> {
    let mut results = Vec::new();
    let dest_ns = app::destination_namespace(app_obj);

    // CreateNamespace=true: the destination namespace first.
    if req.has("CreateNamespace=true") && !dest_ns.is_empty() {
        let ns_kind = ctx.kube.known("v1", "Namespace").await;
        if let Some(k) = ns_kind {
            match ctx.kube.get(&k, None, &dest_ns).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    let mut ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": dest_ns}});
                    let meta = &app_obj["spec"]["syncPolicy"]["managedNamespaceMetadata"];
                    if meta.is_object() {
                        ns["metadata"]["labels"] = meta["labels"].clone();
                        ns["metadata"]["annotations"] = meta["annotations"].clone();
                    }
                    let key = Key {
                        group: String::new(),
                        kind: "Namespace".into(),
                        namespace: String::new(),
                        name: dest_ns.clone(),
                    };
                    match ctx.kube.create(&k, None, &ns, false).await {
                        Ok(_) => results.push(TaskResult {
                            key,
                            version: "v1".into(),
                            status: "Synced",
                            message: format!("namespace/{dest_ns} created"),
                            hook_phase: "Running",
                            sync_phase: "PreSync",
                            images: vec![],
                        }),
                        Err(e) => {
                            return Err(Failed {
                                message: format!(
                                    "one or more objects failed to apply, reason: {}",
                                    err_msg(&e)
                                ),
                                results,
                            });
                        }
                    }
                }
                Err(e) => {
                    return Err(Failed {
                        message: err_msg(&e),
                        results,
                    });
                }
            }
        }
    }

    let by_key = |k: &Key| cmp.resources.iter().find(|r| &r.key == k);
    let mut tasks: Vec<Task> = Vec::new();
    for d in cmp
        .desired
        .iter()
        .filter(|d| d.hook.is_none() && req.wants(&d.key))
    {
        tasks.push(Task::Apply(d, by_key(&d.key)));
    }
    for r in cmp
        .resources
        .iter()
        .filter(|r| r.requires_pruning && req.wants(&r.key))
    {
        tasks.push(Task::Prune(r));
    }
    tasks.sort_by(|a, b| {
        (a.wave(), kind_rank(&a.key().kind), &a.key().name).cmp(&(
            b.wave(),
            kind_rank(&b.key().kind),
            &b.key().name,
        ))
    });

    // Dry run: every kind known (or defined by a CRD of this sync), every
    // namespace there (or made by this sync).
    let creates_ns: BTreeSet<&str> = cmp
        .desired
        .iter()
        .filter(|d| d.key.kind == "Namespace")
        .map(|d| d.key.name.as_str())
        .collect();
    let defines_crd = |d: &Desired| {
        cmp.desired.iter().any(|c| {
            c.key.kind == "CustomResourceDefinition"
                && c.obj["spec"]["group"] == d.key.group.as_str()
                && c.obj["spec"]["names"]["kind"] == d.key.kind.as_str()
        })
    };
    let mut dry_failures = Vec::new();
    let ns_kind = ctx.kube.known("v1", "Namespace").await;
    let mut checked_ns = BTreeSet::new();
    for t in &tasks {
        let Task::Apply(d, _) = t else { continue };
        if d.kind.is_none() && !defines_crd(d) && !d.has_option("SkipDryRunOnMissingResource=true")
        {
            dry_failures.push((
                *d,
                format!(
                    "the server could not find the requested resource (kind {} in {})",
                    d.key.kind,
                    d.obj["apiVersion"].as_str().unwrap_or_default()
                ),
            ));
            continue;
        }
        if let Some(k) = &d.kind
            && k.namespaced
            && !d.key.namespace.is_empty()
            && !creates_ns.contains(d.key.namespace.as_str())
            && !(req.has("CreateNamespace=true") && d.key.namespace == dest_ns)
            && let Some(nk) = &ns_kind
        {
            let exists = if checked_ns.contains(&d.key.namespace) {
                true
            } else {
                matches!(ctx.kube.get(nk, None, &d.key.namespace).await, Ok(Some(_)))
            };
            if exists {
                checked_ns.insert(d.key.namespace.clone());
            } else {
                dry_failures.push((*d, format!("namespaces \"{}\" not found", d.key.namespace)));
            }
        }
    }
    if !dry_failures.is_empty() {
        let message = format!(
            "one or more objects failed to apply, reason: {}",
            dry_failures[0].1
        );
        for (d, msg) in dry_failures {
            results.push(TaskResult {
                key: d.key.clone(),
                version: d.version.clone(),
                status: "SyncFailed",
                message: msg,
                hook_phase: "Failed",
                sync_phase: "Sync",
                images: workload_images(&d.obj),
            });
        }
        return Err(Failed { message, results });
    }

    let waves: Vec<i64> = {
        let mut w: Vec<i64> = tasks.iter().map(Task::wave).collect();
        w.dedup();
        w
    };
    for (i, wave) in waves.iter().enumerate() {
        let mut failed = None;
        let mut applied: Vec<&Desired> = Vec::new();
        for t in tasks.iter().filter(|t| t.wave() == *wave) {
            match t {
                Task::Apply(d, state) => {
                    let r = apply(ctx, d, *state, req).await;
                    if r.status == "SyncFailed" && failed.is_none() {
                        failed = Some(r.message.clone());
                    }
                    if r.status != "SyncFailed" {
                        applied.push(d);
                    }
                    results.push(r);
                }
                Task::Prune(r) => {
                    let res = prune(ctx, r, req).await;
                    if res.status == "SyncFailed" && failed.is_none() {
                        failed = Some(res.message.clone());
                    }
                    results.push(res);
                }
            }
        }
        if let Some(msg) = failed {
            return Err(Failed {
                message: format!(
                    "one or more synchronization tasks completed unsuccessfully, reason: {msg}"
                ),
                results,
            });
        }
        if i + 1 < waves.len() {
            if !wait_healthy(ctx, &applied, &results, progress).await {
                return Err(Failed {
                    message: TERMINATED.into(),
                    results,
                });
            }
            tokio::time::sleep(WAVE_DELAY).await;
        }
    }
    Ok(results)
}

fn workload_images(obj: &Value) -> Vec<String> {
    let spec = match obj["kind"].as_str() {
        Some("Pod") => &obj["spec"],
        Some("CronJob") => &obj["spec"]["jobTemplate"]["spec"]["template"]["spec"],
        Some("Deployment" | "StatefulSet" | "DaemonSet" | "ReplicaSet" | "Job") => {
            &obj["spec"]["template"]["spec"]
        }
        _ => return vec![],
    };
    spec["containers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["image"].as_str().map(str::to_string))
        .collect()
}

async fn apply(ctx: &Ctx, d: &Desired, state: Option<&ResourceState>, req: &Request) -> TaskResult {
    let mut r = TaskResult {
        key: d.key.clone(),
        version: d.version.clone(),
        status: "Synced",
        message: String::new(),
        hook_phase: "Running",
        sync_phase: "Sync",
        images: workload_images(&d.obj),
    };
    let kind: Kind = match &d.kind {
        Some(k) => k.clone(),
        None => match ctx
            .kube
            .kind(
                d.obj["apiVersion"].as_str().unwrap_or_default(),
                &d.key.kind,
            )
            .await
        {
            Ok(k) => k,
            Err(e) => {
                r.status = "SyncFailed";
                r.hook_phase = "Failed";
                r.message = err_msg(&e);
                return r;
            }
        },
    };
    let ns = (!d.key.namespace.is_empty()).then_some(d.key.namespace.as_str());
    let live = state.and_then(|s| s.live.as_ref());
    let what = format!("{}/{}", lower(&kind.kind), d.key.name);
    let replace = d.has_option("Replace=true") || req.has("Replace=true");
    let ssa = d.has_option("ServerSideApply=true") || req.has("ServerSideApply=true");
    let result = if replace {
        match live {
            Some(l) => {
                let mut o = d.obj.clone();
                o["metadata"]["resourceVersion"] = l["metadata"]["resourceVersion"].clone();
                ctx.kube
                    .replace(&kind, ns, &d.key.name, &o)
                    .await
                    .map(|_| format!("{what} replaced"))
            }
            None => ctx
                .kube
                .create(&kind, ns, &d.obj, false)
                .await
                .map(|_| format!("{what} created")),
        }
    } else if ssa {
        ctx.kube
            .apply(&kind, ns, &d.key.name, &d.obj, false)
            .await
            .map(|_| format!("{what} serverside-applied"))
    } else {
        let obj = app::with_last_applied(&d.obj);
        let verb = match (live, state.map(|s| s.synced)) {
            (None, _) => "created",
            (Some(_), Some(true)) => "unchanged",
            _ => "configured",
        };
        ctx.kube
            .apply(&kind, ns, &d.key.name, &obj, false)
            .await
            .map(|_| format!("{what} {verb}"))
    };
    match result {
        Ok(m) => r.message = m,
        Err(e) => {
            r.status = "SyncFailed";
            r.hook_phase = "Failed";
            r.message = err_msg(&e);
        }
    }
    r
}

async fn prune(ctx: &Ctx, s: &ResourceState, req: &Request) -> TaskResult {
    let live = s.live.as_ref();
    let mut r = TaskResult {
        key: s.key.clone(),
        version: s.version.clone(),
        status: "PruneSkipped",
        message: String::new(),
        hook_phase: "Succeeded",
        sync_phase: "Sync",
        images: vec![],
    };
    let no_prune = live.is_some_and(|l| app::options_of(l).iter().any(|o| o == "Prune=false"));
    if no_prune {
        r.message = "ignored (no prune)".into();
        return r;
    }
    if !req.prune {
        r.message = "ignored (requires pruning)".into();
        return r;
    }
    let api = if s.key.group.is_empty() {
        s.version.clone()
    } else {
        format!("{}/{}", s.key.group, s.version)
    };
    let propagation = req
        .options
        .iter()
        .find_map(|o| o.strip_prefix("PrunePropagationPolicy="))
        .map(|p| match p {
            "background" => "Background",
            "orphan" => "Orphan",
            _ => "Foreground",
        })
        .unwrap_or("Foreground");
    let res = match ctx.kube.kind(&api, &s.key.kind).await {
        Ok(k) => {
            let ns = (!s.key.namespace.is_empty()).then_some(s.key.namespace.as_str());
            ctx.kube.delete(&k, ns, &s.key.name, propagation).await
        }
        Err(e) => Err(e),
    };
    match res {
        Ok(()) => {
            r.status = "Pruned";
            r.message = "pruned".into();
        }
        Err(e) => {
            r.status = "SyncFailed";
            r.hook_phase = "Failed";
            r.message = err_msg(&e);
        }
    }
    r
}

/// Wait until every resource of a wave is healthy (no check counts as
/// healthy), reporting what it waits for.
async fn wait_healthy(
    ctx: &Ctx,
    applied: &[&Desired],
    results: &[TaskResult],
    progress: &mut impl Progress,
) -> bool {
    loop {
        let custom = ctx.settings.read().await.health.clone();
        let mut waiting = None;
        for d in applied {
            let live = app::live_of(ctx, d).await.ok().flatten();
            let ok = match &live {
                None => false,
                Some(l) => match crate::health::assess(l, &custom) {
                    Ok(Some(h)) => h.status == Status::Healthy,
                    Ok(None) => true,
                    Err(_) => false,
                },
            };
            if !ok {
                waiting = Some(d.key.display());
                break;
            }
        }
        let Some(w) = waiting else { return true };
        if !progress
            .waiting(format!("waiting for healthy state of {w}"), results)
            .await
        {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order() {
        assert!(kind_rank("Namespace") < kind_rank("ConfigMap"));
        assert!(kind_rank("ConfigMap") < kind_rank("Deployment"));
        assert!(kind_rank("CustomResourceDefinition") < kind_rank("Service"));
        assert_eq!(kind_rank("Application"), KIND_ORDER.len());
    }

    #[test]
    fn selective_sync() {
        let req = Request::from_operation(
            &json!({"sync": {"prune": true, "resources": [{"group": "apps", "kind": "Deployment", "name": "web"}]}}),
        );
        let k = |kind: &str| Key {
            group: if kind == "Deployment" {
                "apps".into()
            } else {
                "".into()
            },
            kind: kind.into(),
            namespace: "n".into(),
            name: "web".into(),
        };
        assert!(req.wants(&k("Deployment")));
        assert!(!req.wants(&k("Service")));
        assert!(req.prune);
    }
}
