//! Resource health as Argo CD assesses it (gitops-engine's checks for
//! built-in kinds) plus Lua checks from argocd-cm.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    Healthy,
    Suspended,
    Progressing,
    Missing,
    Degraded,
    Unknown,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Healthy => "Healthy",
            Status::Suspended => "Suspended",
            Status::Progressing => "Progressing",
            Status::Missing => "Missing",
            Status::Degraded => "Degraded",
            Status::Unknown => "Unknown",
        }
    }

    pub fn parse(s: &str) -> Status {
        match s {
            "Healthy" => Status::Healthy,
            "Suspended" => Status::Suspended,
            "Progressing" => Status::Progressing,
            "Missing" => Status::Missing,
            "Degraded" => Status::Degraded,
            _ => Status::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Health {
    pub status: Status,
    pub message: String,
}

fn h(status: Status, message: impl Into<String>) -> Option<Health> {
    Some(Health {
        status,
        message: message.into(),
    })
}

/// Lua health scripts by "group/Kind" (core group: "/Kind").
#[derive(Clone, Debug, Default)]
pub struct Customizations {
    pub scripts: HashMap<String, String>,
}

impl Customizations {
    /// From argocd-cm's data: `resource.customizations.health.<group>_<Kind>`
    /// keys and the older `resource.customizations` YAML.
    pub fn from_config(data: &serde_json::Map<String, Value>) -> Self {
        let mut scripts = HashMap::new();
        for (k, v) in data {
            let Some(script) = v.as_str() else { continue };
            if let Some(gk) = k.strip_prefix("resource.customizations.health.") {
                let (group, kind) = gk.rsplit_once('_').unwrap_or(("", gk));
                scripts.insert(format!("{group}/{kind}"), script.to_string());
            }
        }
        if let Some(text) = data.get("resource.customizations").and_then(Value::as_str)
            && let Ok(Value::Object(m)) = serde_yaml::from_str::<Value>(text)
        {
            for (gk, c) in m {
                if let Some(script) = c["health.lua"].as_str() {
                    let key = if gk.contains('/') {
                        gk.clone()
                    } else {
                        format!("/{gk}")
                    };
                    scripts.entry(key).or_insert_with(|| script.to_string());
                }
            }
        }
        Customizations { scripts }
    }
}

fn int(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0)
}

fn condition<'a>(obj: &'a Value, ty: &str) -> Option<&'a Value> {
    obj["status"]["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == ty)
}

/// The health of a live object, `None` for kinds without a health check.
pub fn assess(obj: &Value, custom: &Customizations) -> Result<Option<Health>> {
    let api = obj["apiVersion"].as_str().unwrap_or_default();
    let (group, _) = crate::kubeapi::split_api_version(api);
    let kind = obj["kind"].as_str().unwrap_or_default();
    if obj["metadata"]["annotations"]["argocd.argoproj.io/ignore-healthcheck"] == "true" {
        return Ok(None);
    }
    // As Argo CD looks: argocd-cm's scripts, then its bundled ones (both
    // with wildcards), then the built-in checks.
    if let Some(script) = custom_script(custom, group, kind).or_else(|| bundled(group, kind)) {
        return lua(script, obj).map(Some);
    }
    Ok(builtin(group, kind, obj))
}

mod bundled_scripts {
    include!(concat!(env!("OUT_DIR"), "/bundled_health.rs"));
}

fn wild(pattern: &str, text: &str) -> bool {
    pattern == text || crate::app::glob_match(pattern, text)
}

fn custom_script<'a>(custom: &'a Customizations, group: &str, kind: &str) -> Option<&'a str> {
    if let Some(s) = custom.scripts.get(&format!("{group}/{kind}")) {
        return Some(s);
    }
    custom
        .scripts
        .iter()
        .find(|(k, _)| {
            k.contains('*')
                && k.split_once('/')
                    .is_some_and(|(g, kk)| wild(g, group) && wild(kk, kind))
        })
        .map(|(_, s)| s.as_str())
}

/// Argo CD's resource_customizations health scripts (vendor/argo-cd).
fn bundled(group: &str, kind: &str) -> Option<&'static str> {
    let all = bundled_scripts::BUNDLED;
    all.iter()
        .find(|(g, k, _)| *g == group && *k == kind)
        .or_else(|| {
            all.iter().find(|(g, k, _)| {
                (g.contains('*') || k.contains('*')) && wild(g, group) && wild(k, kind)
            })
        })
        .map(|(_, _, s)| *s)
}

fn builtin(group: &str, kind: &str, o: &Value) -> Option<Health> {
    let s = &o["status"];
    let generation = int(&o["metadata"]["generation"]);
    let observed = int(&s["observedGeneration"]);
    match (group, kind) {
        ("apps" | "extensions", "Deployment") => {
            if o["spec"]["paused"].as_bool() == Some(true) {
                return h(Status::Suspended, "Deployment is paused");
            }
            if generation <= observed {
                let name = o["metadata"]["name"].as_str().unwrap_or_default();
                let replicas = o["spec"]["replicas"].as_i64();
                if condition(o, "Progressing")
                    .is_some_and(|c| c["reason"] == "ProgressDeadlineExceeded")
                {
                    return h(
                        Status::Degraded,
                        format!("Deployment {name:?} exceeded its progress deadline"),
                    );
                }
                if let Some(r) = replicas
                    && int(&s["updatedReplicas"]) < r
                {
                    return h(
                        Status::Progressing,
                        format!(
                            "Waiting for rollout to finish: {} out of {r} new replicas have been updated...",
                            int(&s["updatedReplicas"])
                        ),
                    );
                }
                if int(&s["replicas"]) > int(&s["updatedReplicas"]) {
                    return h(
                        Status::Progressing,
                        format!(
                            "Waiting for rollout to finish: {} old replicas are pending termination...",
                            int(&s["replicas"]) - int(&s["updatedReplicas"])
                        ),
                    );
                }
                if int(&s["availableReplicas"]) < int(&s["updatedReplicas"]) {
                    return h(
                        Status::Progressing,
                        format!(
                            "Waiting for rollout to finish: {} of {} updated replicas are available...",
                            int(&s["availableReplicas"]),
                            int(&s["updatedReplicas"])
                        ),
                    );
                }
                return h(Status::Healthy, "");
            }
            h(
                Status::Progressing,
                "Waiting for rollout to finish: observed deployment generation less than desired generation",
            )
        }
        ("apps", "StatefulSet") => {
            if observed == 0 || generation > observed {
                return h(
                    Status::Progressing,
                    "Waiting for statefulset spec update to be observed...",
                );
            }
            if let Some(r) = o["spec"]["replicas"].as_i64()
                && int(&s["readyReplicas"]) < r
            {
                return h(
                    Status::Progressing,
                    format!(
                        "Waiting for {} pods to be ready...",
                        r - int(&s["readyReplicas"])
                    ),
                );
            }
            let strategy = &o["spec"]["updateStrategy"];
            if strategy["type"] == "RollingUpdate"
                && let Some(p) = strategy["rollingUpdate"]["partition"].as_i64()
                && let Some(r) = o["spec"]["replicas"].as_i64()
                && int(&s["updatedReplicas"]) < r - p
            {
                return h(
                    Status::Progressing,
                    format!(
                        "Waiting for partitioned roll out to finish: {} out of {} new pods have been updated...",
                        int(&s["updatedReplicas"]),
                        r - p
                    ),
                );
            }
            if strategy["type"] == "OnDelete" {
                return h(
                    Status::Healthy,
                    format!("statefulset has {} ready pods", int(&s["readyReplicas"])),
                );
            }
            if s["updateRevision"] != s["currentRevision"] {
                return h(
                    Status::Progressing,
                    format!(
                        "waiting for statefulset rolling update to complete {} pods at revision {}...",
                        int(&s["updatedReplicas"]),
                        s["updateRevision"].as_str().unwrap_or_default()
                    ),
                );
            }
            h(
                Status::Healthy,
                format!(
                    "statefulset rolling update complete {} pods at revision {}...",
                    int(&s["currentReplicas"]),
                    s["currentRevision"].as_str().unwrap_or_default()
                ),
            )
        }
        ("apps" | "extensions", "DaemonSet") => {
            if generation > observed {
                return h(
                    Status::Progressing,
                    "Waiting for rollout to finish: observed daemon set generation less than desired generation",
                );
            }
            if o["spec"]["updateStrategy"]["type"] == "OnDelete" {
                return h(
                    Status::Healthy,
                    format!(
                        "daemon set {} out of {} new pods have been updated",
                        int(&s["updatedNumberScheduled"]),
                        int(&s["desiredNumberScheduled"])
                    ),
                );
            }
            if int(&s["updatedNumberScheduled"]) < int(&s["desiredNumberScheduled"]) {
                return h(
                    Status::Progressing,
                    format!(
                        "Waiting for daemon set {:?} rollout to finish: {} out of {} new pods have been updated...",
                        o["metadata"]["name"].as_str().unwrap_or_default(),
                        int(&s["updatedNumberScheduled"]),
                        int(&s["desiredNumberScheduled"])
                    ),
                );
            }
            if int(&s["numberAvailable"]) < int(&s["desiredNumberScheduled"]) {
                return h(
                    Status::Progressing,
                    format!(
                        "Waiting for daemon set {:?} rollout to finish: {} of {} updated pods are available...",
                        o["metadata"]["name"].as_str().unwrap_or_default(),
                        int(&s["numberAvailable"]),
                        int(&s["desiredNumberScheduled"])
                    ),
                );
            }
            h(Status::Healthy, "")
        }
        ("apps" | "extensions", "ReplicaSet") => {
            if generation > observed {
                return h(
                    Status::Progressing,
                    "Waiting for rollout to finish: observed replica set generation less than desired generation",
                );
            }
            if let Some(c) = condition(o, "ReplicaFailure").filter(|c| c["status"] == "True") {
                return h(Status::Degraded, c["message"].as_str().unwrap_or_default());
            }
            if let Some(r) = o["spec"]["replicas"].as_i64()
                && int(&s["availableReplicas"]) < r
            {
                return h(
                    Status::Progressing,
                    format!(
                        "Waiting for rollout to finish: {} out of {r} new replicas are available...",
                        int(&s["availableReplicas"])
                    ),
                );
            }
            h(Status::Healthy, "")
        }
        ("batch", "Job") => {
            let (mut complete, mut failed, mut suspended) = (false, false, false);
            let mut message = String::new();
            for c in s["conditions"].as_array().into_iter().flatten() {
                if c["status"] != "True" {
                    continue;
                }
                match c["type"].as_str() {
                    Some("Failed") | Some("FailureTarget") => {
                        failed = true;
                        message = c["message"].as_str().unwrap_or_default().to_string();
                    }
                    Some("Complete") | Some("SuccessCriteriaMet") => {
                        complete = true;
                        message = c["message"].as_str().unwrap_or_default().to_string();
                    }
                    Some("Suspended") => {
                        suspended = true;
                        message = c["message"].as_str().unwrap_or_default().to_string();
                    }
                    _ => {}
                }
            }
            if failed {
                h(Status::Degraded, message)
            } else if complete {
                h(Status::Healthy, message)
            } else if suspended {
                h(Status::Suspended, message)
            } else {
                h(Status::Progressing, message)
            }
        }
        ("", "Pod") => pod(o),
        ("", "PersistentVolumeClaim") => match s["phase"].as_str() {
            Some("Bound") => h(Status::Healthy, ""),
            Some("Lost") => h(Status::Degraded, ""),
            _ => h(Status::Progressing, ""),
        },
        ("", "Service") => {
            if o["spec"]["type"] == "LoadBalancer"
                && s["loadBalancer"]["ingress"]
                    .as_array()
                    .is_none_or(|a| a.is_empty())
            {
                return h(Status::Progressing, "");
            }
            h(Status::Healthy, "")
        }
        ("networking.k8s.io" | "extensions", "Ingress") => {
            if s["loadBalancer"]["ingress"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
            {
                h(Status::Healthy, "")
            } else {
                h(Status::Progressing, "")
            }
        }
        ("apiregistration.k8s.io", "APIService") => match condition(o, "Available") {
            Some(c) if c["status"] == "True" => {
                h(Status::Healthy, c["message"].as_str().unwrap_or_default())
            }
            Some(c) => h(
                Status::Progressing,
                format!(
                    "{}: {}",
                    c["reason"].as_str().unwrap_or_default(),
                    c["message"].as_str().unwrap_or_default()
                ),
            ),
            None => h(Status::Progressing, "Waiting to be processed"),
        },
        _ => None,
    }
}

const POD_DEGRADED: [&str; 6] = [
    "ErrImagePull",
    "ImagePullBackOff",
    "CrashLoopBackOff",
    "CreateContainerConfigError",
    "InvalidImageName",
    "CreateContainerError",
];

fn pod(o: &Value) -> Option<Health> {
    let s = &o["status"];
    let statuses = || {
        s["initContainerStatuses"]
            .as_array()
            .into_iter()
            .flatten()
            .chain(s["containerStatuses"].as_array().into_iter().flatten())
    };
    for c in statuses() {
        let reason = c["state"]["waiting"]["reason"].as_str().unwrap_or_default();
        if POD_DEGRADED.contains(&reason) {
            return h(
                Status::Degraded,
                c["state"]["waiting"]["message"].as_str().unwrap_or(reason),
            );
        }
    }
    match s["phase"].as_str() {
        Some("Succeeded") => h(Status::Healthy, s["message"].as_str().unwrap_or_default()),
        Some("Failed") => h(Status::Degraded, s["message"].as_str().unwrap_or_default()),
        Some("Running") => {
            let always = o["spec"]["restartPolicy"]
                .as_str()
                .is_none_or(|p| p == "Always");
            if always && condition(o, "Ready").is_some_and(|c| c["status"] == "True") {
                h(Status::Healthy, s["message"].as_str().unwrap_or_default())
            } else {
                h(
                    Status::Progressing,
                    s["message"].as_str().unwrap_or_default(),
                )
            }
        }
        _ => h(
            Status::Progressing,
            s["message"].as_str().unwrap_or_default(),
        ),
    }
}

/// Run an Argo CD health script: `obj` is the object, the script returns a
/// table with `status` and `message`.
pub fn lua(script: &str, obj: &Value) -> Result<Health> {
    use mlua::{Lua, LuaSerdeExt, serde::ser::Options as SerializeOptions};
    let lua = Lua::new();
    let opts = SerializeOptions::new()
        .serialize_none_to_null(false)
        .serialize_unit_to_null(false);
    let o = lua.to_value_with(obj, opts).map_err(|e| anyhow!("{e}"))?;
    lua.globals().set("obj", o).map_err(|e| anyhow!("{e}"))?;
    let v: mlua::Value = lua
        .load(script)
        .set_name("health.lua")
        .eval()
        .map_err(|e| anyhow!("health script: {e}"))?;
    let t = v
        .as_table()
        .context("health script did not return a table")?;
    let status: String = t
        .get::<Option<String>>("status")
        .map_err(|e| anyhow!("{e}"))?
        .unwrap_or_default();
    let message: String = t
        .get::<Option<String>>("message")
        .map_err(|e| anyhow!("{e}"))?
        .unwrap_or_default();
    Ok(Health {
        status: Status::parse(&status),
        message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn st(o: Value) -> Option<Status> {
        assess(&o, &Customizations::default())
            .unwrap()
            .map(|h| h.status)
    }

    #[test]
    fn deployments() {
        let mut d = json!({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web", "generation": 1},
            "spec": {"replicas": 1}, "status": {"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1, "availableReplicas": 1}});
        assert_eq!(st(d.clone()), Some(Status::Healthy));
        d["status"]["availableReplicas"] = json!(0);
        assert_eq!(st(d.clone()), Some(Status::Progressing));
        d["status"]["conditions"] = json!([{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}]);
        assert_eq!(st(d.clone()), Some(Status::Degraded));
        d["metadata"]["generation"] = json!(2);
        assert_eq!(st(d), Some(Status::Progressing));
    }

    #[test]
    fn jobs_and_kinds_without_checks() {
        let j = json!({"apiVersion": "batch/v1", "kind": "Job", "status": {"conditions": [{"type": "Failed", "status": "True", "message": "BackoffLimitExceeded"}]}});
        assert_eq!(st(j), Some(Status::Degraded));
        let j = json!({"apiVersion": "batch/v1", "kind": "Job", "status": {}});
        assert_eq!(st(j), Some(Status::Progressing));
        assert_eq!(st(json!({"apiVersion": "v1", "kind": "ConfigMap"})), None);
        assert_eq!(
            st(json!({"apiVersion": "argoproj.io/v1alpha1", "kind": "Application"})),
            None
        );
    }

    #[test]
    fn bundled_library() {
        assert!(bundled_scripts::BUNDLED.len() > 200);
        let cert = |status: &str| {
            json!({"apiVersion": "cert-manager.io/v1", "kind": "Certificate",
                "status": {"conditions": [{"type": "Ready", "status": status, "message": "m"}]}})
        };
        assert_eq!(st(cert("True")), Some(Status::Healthy));
        assert_eq!(st(cert("False")), Some(Status::Degraded));
        // A wildcard directory: _.crossplane.io/_
        assert!(bundled("aws.crossplane.io", "Bucket").is_some());
        let crd = json!({"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": {"name": "x.example.com"}, "status": {"conditions": [
            {"type": "NamesAccepted", "status": "True"}, {"type": "Established", "status": "True"}]}});
        assert_eq!(st(crd), Some(Status::Healthy));
    }

    #[test]
    fn lua_from_argocd_cm() {
        let data = json!({"resource.customizations.health.argoproj.io_Application": r#"
hs = {}
hs.status = "Progressing"
hs.message = ""
if obj.status ~= nil and obj.status.health ~= nil then
  hs.status = obj.status.health.status
  if obj.status.health.message ~= nil then
    hs.message = obj.status.health.message
  end
  if hs.status == "Healthy" and (obj.status.sync == nil or obj.status.sync.status ~= "Synced") then
    hs.status = "Progressing"
    hs.message = "waiting for the Application to sync"
  end
end
return hs
"#});
        let c = Customizations::from_config(data.as_object().unwrap());
        let app = |sync: &str, health: &str| json!({"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "status": {"sync": {"status": sync}, "health": {"status": health}}});
        let got = |o: Value| assess(&o, &c).unwrap().unwrap();
        assert_eq!(got(app("Synced", "Healthy")).status, Status::Healthy);
        let h = got(app("OutOfSync", "Healthy"));
        assert_eq!(
            (h.status, h.message.as_str()),
            (Status::Progressing, "waiting for the Application to sync")
        );
        assert_eq!(
            got(json!({"apiVersion": "argoproj.io/v1alpha1", "kind": "Application"})).status,
            Status::Progressing
        );
        assert!(Status::Degraded > Status::Missing && Status::Missing > Status::Progressing);
    }
}
