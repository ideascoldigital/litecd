//! A sync operation, as gitops-engine runs one: a dry run of every task,
//! then waves in order, each applied and, before the next, healthy.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};

use crate::app::{self, Comparison, Ctx, Desired, Key, ResourceState};
use crate::health::Status;
use crate::kubeapi::{ApiError, Kind, Kube};

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
    /// For a hook: PreSync, Sync, PostSync or SyncFail.
    pub hook_type: Option<String>,
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
        if let Some(h) = &self.hook_type {
            r["hookType"] = json!(h);
        }
        r
    }
}

/// What the operation asks for.
pub struct Request {
    pub prune: bool,
    /// ignoreDifferences, for RespectIgnoreDifferences=true.
    pub ignore: Vec<crate::ignore::Rule>,
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
            ignore: Vec::new(),
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
    PruneLast(&'a ResourceState),
}

impl Task<'_> {
    fn key(&self) -> &Key {
        match self {
            Task::Apply(d, _) => &d.key,
            Task::Prune(r) | Task::PruneLast(r) => &r.key,
        }
    }

    fn wave(&self) -> i64 {
        match self {
            Task::Apply(d, _) => d.wave,
            Task::Prune(r) => r.wave,
            // PruneLast: after every wave.
            Task::PruneLast(_) => i64::MAX,
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

/// Why a sync stopped where it was: Argo CD's application controller came
/// back (docs/spec.md, Coexistence). Nothing more is written, not even the
/// SyncFail hooks or the cleanup of the hooks it created.
pub const STOOD_DOWN: &str = "stopped: Argo CD's application controller is running";

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
) -> Result<Done, Failed> {
    // The destination cluster's API; Applications and settings stay on ctx.
    let kube = &cmp.kube;
    let mut results = Vec::new();
    let dest_ns = app::destination_namespace(app_obj);
    // Checked before every write: the guard can turn while a sync waits.
    let stood_down = || !ctx.guard.may_act_on_app(app_obj);
    if stood_down() {
        return Err(Failed {
            message: STOOD_DOWN.to_string(),
            results,
        });
    }

    // FailOnSharedResource=true: nothing is applied when another app
    // manages one of these resources.
    if req.has("FailOnSharedResource=true")
        && let Some(m) = cmp.resources.iter().find_map(|r| r.shared_message(app_obj))
    {
        return Err(Failed {
            message: format!("Shared resource found: {m}"),
            results,
        });
    }

    // CreateNamespace=true: the destination namespace first.
    if req.has("CreateNamespace=true") && !dest_ns.is_empty() {
        let ns_kind = kube.known("v1", "Namespace").await;
        if let Some(k) = ns_kind {
            match kube.get(&k, None, &dest_ns).await {
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
                    match kube.create(&k, None, &ns, false).await {
                        Ok(_) => results.push(TaskResult {
                            key,
                            version: "v1".into(),
                            status: "Synced",
                            message: format!("namespace/{dest_ns} created"),
                            hook_phase: "Running",
                            sync_phase: "PreSync",
                            images: vec![],
                            hook_type: None,
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
        let last = req.has("PruneLast=true")
            || r.live
                .as_ref()
                .is_some_and(|l| app::options_of(l).iter().any(|o| o == "PruneLast=true"));
        tasks.push(if last {
            Task::PruneLast(r)
        } else {
            Task::Prune(r)
        });
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
    let ns_kind = kube.known("v1", "Namespace").await;
    let mut checked_ns = BTreeSet::new();
    let hooks: Vec<&Desired> = cmp.desired.iter().filter(|d| d.hook.is_some()).collect();
    let applies = tasks.iter().filter_map(|t| match t {
        Task::Apply(d, _) => Some(*d),
        Task::Prune(_) | Task::PruneLast(_) => None,
    });
    for d in applies.chain(hooks.iter().copied()) {
        if d.kind.is_none() && !defines_crd(d) && !d.has_option("SkipDryRunOnMissingResource=true")
        {
            dry_failures.push((
                d,
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
                matches!(kube.get(nk, None, &d.key.namespace).await, Ok(Some(_)))
            };
            if exists {
                checked_ns.insert(d.key.namespace.clone());
            } else {
                dry_failures.push((d, format!("namespaces \"{}\" not found", d.key.namespace)));
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
                hook_type: d.hook.clone(),
            });
        }
        return Err(Failed { message, results });
    }

    let phase_hooks = |phase: &str| -> Vec<&Desired> {
        let mut h: Vec<&Desired> = hooks
            .iter()
            .copied()
            .filter(|d| d.hook.as_deref() == Some(phase))
            .collect();
        h.sort_by(|a, b| {
            (a.wave, kind_rank(&a.key.kind), &a.key.name).cmp(&(
                b.wave,
                kind_rank(&b.key.kind),
                &b.key.name,
            ))
        });
        h
    };
    let mut created: Vec<Hook> = Vec::new();
    let mut waited_last = false;
    let outcome = async {
        // PreSync hooks, each to its end.
        let pre = phase_hooks("PreSync");
        if !pre.is_empty() {
            if stood_down() {
                return Err(STOOD_DOWN.to_string());
            }
            run_hooks(
                ctx,
                kube,
                &pre,
                "PreSync",
                &mut results,
                &mut created,
                progress,
            )
            .await?;
            waited_last = true;
        }
        // Sync: the resources in waves, with the Sync hooks of each wave.
        let sync_hooks = phase_hooks("Sync");
        let mut waves: Vec<i64> = tasks
            .iter()
            .map(Task::wave)
            .chain(sync_hooks.iter().map(|d| d.wave))
            .collect();
        waves.sort();
        waves.dedup();
        let mut all_applied: Vec<&Desired> = Vec::new();
        for (i, wave) in waves.iter().enumerate() {
            let mut failed = None;
            let mut applied: Vec<&Desired> = Vec::new();
            waited_last = false;
            for t in tasks.iter().filter(|t| t.wave() == *wave) {
                if stood_down() {
                    return Err(STOOD_DOWN.to_string());
                }
                match t {
                    // ApplyOutOfSyncOnly: what is in sync is left alone.
                    Task::Apply(_, Some(state))
                        if req.has("ApplyOutOfSyncOnly=true") && state.synced => {}
                    Task::Apply(d, state) => {
                        let r = apply(kube, d, *state, req).await;
                        if r.status == "SyncFailed" && failed.is_none() {
                            failed = Some(r.message.clone());
                        }
                        if r.status != "SyncFailed" && !app::is_self(app_obj, &d.key) {
                            applied.push(d);
                        }
                        results.push(r);
                    }
                    Task::Prune(r) | Task::PruneLast(r) => {
                        let res = prune(kube, r, req).await;
                        if res.status == "SyncFailed" && failed.is_none() {
                            failed = Some(res.message.clone());
                        }
                        results.push(res);
                    }
                }
            }
            if let Some(msg) = failed {
                return Err(format!(
                    "one or more synchronization tasks completed unsuccessfully, reason: {msg}"
                ));
            }
            let wave_hooks: Vec<&Desired> = sync_hooks
                .iter()
                .copied()
                .filter(|d| d.wave == *wave)
                .collect();
            if !wave_hooks.is_empty() {
                if stood_down() {
                    return Err(STOOD_DOWN.to_string());
                }
                run_hooks(
                    ctx,
                    kube,
                    &wave_hooks,
                    "Sync",
                    &mut results,
                    &mut created,
                    progress,
                )
                .await?;
                waited_last = true;
            }
            all_applied.extend(applied.iter().copied());
            if i + 1 < waves.len() {
                if !wait_healthy(ctx, kube, &applied, &results, progress).await {
                    return Err(TERMINATED.to_string());
                }
                tokio::time::sleep(WAVE_DELAY).await;
            }
        }
        // PostSync hooks once everything synced is healthy.
        let post = phase_hooks("PostSync");
        if !post.is_empty() {
            if !wait_healthy(ctx, kube, &all_applied, &results, progress).await {
                return Err(TERMINATED.to_string());
            }
            if stood_down() {
                return Err(STOOD_DOWN.to_string());
            }
            run_hooks(
                ctx,
                kube,
                &post,
                "PostSync",
                &mut results,
                &mut created,
                progress,
            )
            .await?;
            waited_last = true;
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => {
            cleanup_hooks(kube, &created, true).await;
            Ok(Done {
                results,
                no_more_tasks: waited_last,
            })
        }
        Err(message) if message == STOOD_DOWN => Err(Failed { message, results }),
        Err(message) => {
            if message != TERMINATED {
                let fail = phase_hooks("SyncFail");
                if !fail.is_empty() {
                    let _ = run_hooks(
                        ctx,
                        kube,
                        &fail,
                        "SyncFail",
                        &mut results,
                        &mut created,
                        progress,
                    )
                    .await;
                }
            }
            cleanup_hooks(kube, &created, false).await;
            Err(Failed { message, results })
        }
    }
}

/// A successful sync: its results, and whether it ended waiting for hooks
/// (Argo CD then says "no more tasks" instead of "all tasks run").
pub struct Done {
    pub results: Vec<TaskResult>,
    pub no_more_tasks: bool,
}

/// A hook created by this sync, for its delete policy.
struct Hook {
    kind: Kind,
    namespace: String,
    name: String,
    policies: Vec<String>,
    succeeded: bool,
}

/// Create a phase's hooks and wait for each to complete (healthy: it
/// succeeded; degraded: it failed). Err: why the phase failed.
async fn run_hooks(
    ctx: &Ctx,
    kube: &Kube,
    hooks: &[&Desired],
    phase: &'static str,
    results: &mut Vec<TaskResult>,
    created: &mut Vec<Hook>,
    progress: &mut impl Progress,
) -> Result<(), String> {
    let mut started = Vec::new();
    for d in hooks {
        let mut r = TaskResult {
            key: d.key.clone(),
            version: d.version.clone(),
            status: "Synced",
            message: String::new(),
            hook_phase: "Running",
            sync_phase: phase,
            images: workload_images(&d.obj),
            hook_type: Some(phase.to_string()),
        };
        let kind = match &d.kind {
            Some(k) => k.clone(),
            None => match kube
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
                    results.push(r);
                    return Err(format!(
                        "one or more synchronization tasks completed unsuccessfully, reason: {}",
                        err_msg(&e)
                    ));
                }
            },
        };
        let ns = (!d.key.namespace.is_empty()).then_some(d.key.namespace.as_str());
        let policies = if d.delete_policies.is_empty() {
            vec!["BeforeHookCreation".to_string()]
        } else {
            d.delete_policies.clone()
        };
        if !d.key.name.is_empty() && policies.iter().any(|p| p == "BeforeHookCreation") {
            let _ = kube.delete(&kind, ns, &d.key.name, "Foreground").await;
            for _ in 0..120 {
                if !matches!(kube.get(&kind, ns, &d.key.name).await, Ok(Some(_))) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        let name = match kube.create(&kind, ns, &d.obj, false).await {
            Ok(obj) => obj["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            // Already there and kept (no BeforeHookCreation): that one runs.
            Err(e) if e.downcast_ref::<ApiError>().is_some_and(|a| a.code == 409) => {
                d.key.name.clone()
            }
            Err(e) => {
                r.status = "SyncFailed";
                r.hook_phase = "Failed";
                r.message = err_msg(&e);
                results.push(r);
                return Err(format!(
                    "one or more synchronization tasks completed unsuccessfully, reason: {}",
                    err_msg(&e)
                ));
            }
        };
        r.key.name = name.clone();
        r.message = format!("{}/{name} created", lower(&kind.kind));
        created.push(Hook {
            kind: kind.clone(),
            namespace: d.key.namespace.clone(),
            name: name.clone(),
            policies,
            succeeded: false,
        });
        results.push(r);
        started.push((
            results.len() - 1,
            created.len() - 1,
            kind,
            d.key.namespace.clone(),
            name,
        ));
    }
    let custom = ctx.settings.read().await.health.clone();
    let mut pending: Vec<usize> = (0..started.len()).collect();
    let mut failure = None;
    while !pending.is_empty() {
        let mut still = Vec::new();
        for i in pending {
            let (ri, ci, kind, ns, name) = &started[i];
            let nsr = (!ns.is_empty()).then_some(ns.as_str());
            let live = kube.get(kind, nsr, name).await.ok().flatten();
            let done = match &live {
                // Gone already (a TTL, say): it ran.
                None => Some((true, String::new())),
                Some(l) => match crate::health::assess(l, &custom) {
                    Ok(None) => Some((true, String::new())),
                    Ok(Some(h)) if h.status == Status::Healthy => Some((true, h.message)),
                    Ok(Some(h)) if h.status == Status::Degraded => Some((false, h.message)),
                    _ => None,
                },
            };
            match done {
                Some((ok, msg)) => {
                    let r = &mut results[*ri];
                    r.hook_phase = if ok { "Succeeded" } else { "Failed" };
                    if !msg.is_empty() {
                        r.message = msg.clone();
                    }
                    created[*ci].succeeded = ok;
                    if !ok && failure.is_none() {
                        failure = Some(msg);
                    }
                }
                None => still.push(i),
            }
        }
        pending = still;
        if let Some(&i) = pending.first() {
            let (_, _, kind, _, name) = &started[i];
            let what = format!("{}/{}/{name}", kind.group, kind.kind);
            if !progress
                .waiting(format!("waiting for completion of hook {what}"), results)
                .await
            {
                return Err(TERMINATED.to_string());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    match failure {
        Some(m) => Err(format!(
            "one or more synchronization tasks completed unsuccessfully, reason: {m}"
        )),
        None => Ok(()),
    }
}

/// Delete the hooks whose policy asks for it.
async fn cleanup_hooks(kube: &Kube, created: &[Hook], _sync_ok: bool) {
    for h in created {
        let delete = (h.succeeded && h.policies.iter().any(|p| p == "HookSucceeded"))
            || (!h.succeeded && h.policies.iter().any(|p| p == "HookFailed"));
        if delete {
            let ns = (!h.namespace.is_empty()).then_some(h.namespace.as_str());
            let _ = kube.delete(&h.kind, ns, &h.name, "Background").await;
        }
    }
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

async fn apply(
    kube: &Kube,
    d: &Desired,
    state: Option<&ResourceState>,
    req: &Request,
) -> TaskResult {
    let mut r = TaskResult {
        key: d.key.clone(),
        version: d.version.clone(),
        status: "Synced",
        message: String::new(),
        hook_phase: "Running",
        sync_phase: "Sync",
        images: workload_images(&d.obj),
        hook_type: None,
    };
    let kind: Kind = match &d.kind {
        Some(k) => k.clone(),
        None => match kube
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
    // RespectIgnoreDifferences: what the comparison ignores keeps its live value.
    let desired = match live {
        Some(l) if req.has("RespectIgnoreDifferences=true") && !req.ignore.is_empty() => {
            let paths = crate::ignore::ignored(&req.ignore, &d.key, &d.obj, Some(l));
            crate::ignore::respect(&d.obj, l, &paths)
        }
        _ => d.obj.clone(),
    };
    let d = &Desired {
        obj: desired,
        ..d.clone()
    };
    let ssa = d.has_option("ServerSideApply=true") || req.has("ServerSideApply=true");
    let result = if replace {
        match live {
            Some(l) => {
                let mut o = d.obj.clone();
                o["metadata"]["resourceVersion"] = l["metadata"]["resourceVersion"].clone();
                kube.replace(&kind, ns, &d.key.name, &o)
                    .await
                    .map(|_| format!("{what} replaced"))
            }
            None => kube
                .create(&kind, ns, &d.obj, false)
                .await
                .map(|_| format!("{what} created")),
        }
    } else if ssa {
        kube.apply(&kind, ns, &d.key.name, &d.obj, false)
            .await
            .map(|_| format!("{what} serverside-applied"))
    } else if live.is_some() && state.is_some_and(|s| s.synced) {
        // As kubectl apply: nothing to change, nothing written.
        Ok(format!("{what} unchanged"))
    } else {
        let obj = app::with_last_applied(&d.obj);
        let verb = if live.is_none() {
            "created"
        } else {
            "configured"
        };
        kube.apply(&kind, ns, &d.key.name, &obj, false)
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

async fn prune(kube: &Kube, s: &ResourceState, req: &Request) -> TaskResult {
    let live = s.live.as_ref();
    let mut r = TaskResult {
        key: s.key.clone(),
        version: s.version.clone(),
        status: "PruneSkipped",
        message: String::new(),
        hook_phase: "Succeeded",
        sync_phase: "Sync",
        images: vec![],
        hook_type: None,
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
    let res = match kube.kind(&api, &s.key.kind).await {
        Ok(k) => {
            let ns = (!s.key.namespace.is_empty()).then_some(s.key.namespace.as_str());
            kube.delete(&k, ns, &s.key.name, propagation).await
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
    kube: &Kube,
    applied: &[&Desired],
    results: &[TaskResult],
    progress: &mut impl Progress,
) -> bool {
    loop {
        let custom = ctx.settings.read().await.health.clone();
        let mut waiting = None;
        for d in applied {
            let live = app::live_of(kube, d).await.ok().flatten();
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
