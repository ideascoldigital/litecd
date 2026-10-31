//! DEPRECATED compatibility mode: the resource tree Argo CD's web UI shows.
//!
//! Argo CD's UI (argocd-server) does not compute an Application's resource
//! tree (Deployment → ReplicaSets → Pods, with health, images and pod info):
//! it reads the one Argo CD's application controller keeps in Redis. With
//! litecd in that controller's place the tree is empty. This module writes
//! it, so clusters that keep argocd-server keep a working UI. It exists
//! only for that coexistence: it is coupled to Argo CD's internal cache
//! format (key names, cache version, compression, JSON shape), which Argo
//! CD may change in any release, and it will be removed once litecd has
//! its own view, computed on demand from the Kubernetes API. See
//! docs/spec.md, "Deprecated compatibility".
//!
//! Compiled only with the `argocd-ui-compat` Cargo feature and active only
//! with `--argocd-ui-cache`. Nothing else in litecd depends on it: the
//! controller hands it each comparison ([`update`]) and the end of each app
//! ([`forget`]); both do nothing when the mode is off.
//!
//! What argocd-server reads (Argo CD v2.6 to v3.5, util/cache): the key
//! `app|resources-tree|<instance>|<CacheVersion>` plus `.gz` when Redis
//! compression is gzip (the default), holding the JSON of an
//! `ApplicationTree` (gzipped), set with a TTL (the controller's
//! appStateCacheExpiration, 1 h); beside it
//! `app|managed-resources|<instance>|...`, the managed resources as a JSON
//! `[]ResourceDiff` (desired, live, normalized and predicted live objects as
//! JSON strings, Secret values masked), which the UI reads when a resource
//! is opened; a publish on `app|managed-resources|<instance>|<CacheVersion>`
//! tells a UI that streams the tree to read both again. litecd writes the tree only when it changed
//! (and again before the TTL runs out), and only for apps it reconciled
//! recently, so it never overwrites the tree of an app Argo CD's own
//! controller owns again.
//!
//! The format is trusted only when it is known: argocd-server's image is a
//! version this module was checked against (or the cache version is given)
//! and its compression setting agrees, and no tree key in Redis has another
//! cache version or compression. Otherwise nothing is written.
//!
//! Children come from watches of the child kinds only (ReplicaSets and
//! Pods; Jobs under a CronJob), only in the namespaces the apps deploy
//! resources of a parent kind to. Not written: orphaned nodes, hosts (the
//! UI's node view), ingress and Application node info, hooks among the
//! managed resources, scheme defaults in normalized live objects.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, Event};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::resp::{Address, Conn, Reply};
use crate::app::{Comparison, Ctx};
use crate::health::{self, Health};
use crate::kubeapi::Kube;

/// The flags of the compatibility mode.
#[derive(clap::Args, Clone, Debug)]
pub struct Flags {
    /// DEPRECATED compatibility mode, off by default: write each
    /// Application's resource tree to the Redis of Argo CD's web UI
    /// (argocd-server), e.g. redis://argocd-redis-ha-haproxy:6379, so its
    /// resource view works while litecd replaces Argo CD's application
    /// controller. Coupled to Argo CD's internal cache format; removed once
    /// litecd has its own view
    #[arg(long, env = "LITECD_ARGOCD_UI_CACHE", default_value = "")]
    pub argocd_ui_cache: String,
    /// The Secret (in --namespace) whose key `auth` holds the Redis
    /// password; empty: none (a password in the URL wins)
    #[arg(
        long,
        env = "LITECD_ARGOCD_UI_CACHE_SECRET",
        default_value = "argocd-redis"
    )]
    pub argocd_ui_cache_secret: String,
    /// Argo CD's cache version (common.CacheVersion); default: the one of
    /// the argocd-server version found, nothing written when that version
    /// is unknown
    #[arg(long, env = "LITECD_ARGOCD_UI_CACHE_VERSION")]
    pub argocd_ui_cache_version: Option<String>,
    /// argocd-server's Redis compression (gzip or none); default: read from
    /// its Deployment. Nothing is written when the two disagree
    #[arg(long, env = "LITECD_ARGOCD_UI_CACHE_COMPRESSION")]
    pub argocd_ui_cache_compression: Option<String>,
    /// How long a tree lives in Redis (rewritten at half of it); Argo CD's
    /// default appStateCacheExpiration
    #[arg(long, env = "LITECD_ARGOCD_UI_CACHE_TTL", default_value = "1h")]
    pub argocd_ui_cache_ttl: String,
}

/// The tree key's format.
#[derive(Clone, Debug, PartialEq)]
pub struct Format {
    pub cache_version: String,
    pub gzip: bool,
}

/// Argo CD's common.CacheVersion, by the argocd-server versions this module
/// was checked against (it has been "1.8.3" since v2.0; gzip compression
/// and the `.gz` suffix came in v2.6).
fn known_cache_version(major: u32, minor: u32) -> Option<&'static str> {
    match (major, minor) {
        (2, 6..) | (3, 0..=5) => Some("1.8.3"),
        _ => None,
    }
}

pub fn tree_key(instance: &str, f: &Format) -> String {
    let gz = if f.gzip { ".gz" } else { "" };
    format!("app|resources-tree|{instance}|{}{gz}", f.cache_version)
}

/// The managed resources (desired and live state of each, for the UI's
/// resource details and Diff tab): `app|managed-resources|<instance>|...`.
pub fn managed_key(instance: &str, f: &Format) -> String {
    let gz = if f.gzip { ".gz" } else { "" };
    format!("app|managed-resources|{instance}|{}{gz}", f.cache_version)
}

/// Where argocd-server listens for a tree that changed.
pub fn notify_channel(instance: &str, f: &Format) -> String {
    format!("app|managed-resources|{instance}|{}", f.cache_version)
}

// --- Argo CD's ApplicationTree, as encoding/json writes it ----------------

fn is_zero(n: &i64) -> bool {
    *n == 0
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ref {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InfoItem {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HealthStatus {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[serde(flatten)]
    pub r: Ref,
    #[serde(rename = "parentRefs", default, skip_serializing_if = "Vec::is_empty")]
    pub parent_refs: Vec<Ref>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<InfoItem>,
    #[serde(
        rename = "networkingInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub networking_info: Option<Value>,
    #[serde(
        rename = "resourceVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_version: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<HealthStatus>,
    #[serde(rename = "createdAt", default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

impl Node {
    fn full_name(&self) -> String {
        let r = &self.r;
        format!("{}/{}/{}/{}", r.group, r.kind, r.namespace, r.name)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Tree {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<Node>,
    #[serde(
        rename = "orphanedNodes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub orphaned_nodes: Vec<Node>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<Value>,
    #[serde(rename = "shardsCount", default, skip_serializing_if = "is_zero")]
    pub shards_count: i64,
}

/// The value argocd-server reads: the JSON (with encoding/json's trailing
/// newline), gzipped when the format says so. go-redis/cache stores a
/// []byte as it is, without its own header.
pub fn encode<T: Serialize>(value: &T, f: &Format) -> Result<Vec<u8>> {
    let mut json = serde_json::to_vec(value)?;
    json.push(b'\n');
    if !f.gzip {
        return Ok(json);
    }
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&json)?;
    Ok(gz.finish()?)
}

pub fn decode(data: &[u8], f: &Format) -> Result<Tree> {
    decode_as(data, f).context("not an ApplicationTree")
}

pub fn decode_managed(data: &[u8], f: &Format) -> Result<Vec<ResourceDiff>> {
    decode_as(data, f).context("not a list of ResourceDiffs")
}

fn decode_as<T: serde::de::DeserializeOwned>(data: &[u8], f: &Format) -> Result<T> {
    let mut json = Vec::new();
    if f.gzip {
        flate2::read::GzDecoder::new(data)
            .read_to_end(&mut json)
            .context("not gzip")?;
    } else {
        json = data.to_vec();
    }
    Ok(serde_json::from_slice(&json)?)
}

// --- managed resources (Argo CD's []ResourceDiff) --------------------------

fn is_false(b: &bool) -> bool {
    !*b
}

/// One managed resource as Argo CD's controller caches it: the objects as
/// JSON strings ("null" when absent), Secret values masked.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceDiff {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(
        rename = "targetState",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub target_state: String,
    #[serde(
        rename = "liveState",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub live_state: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub diff: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub hook: bool,
    #[serde(
        rename = "normalizedLiveState",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub normalized_live_state: String,
    #[serde(
        rename = "predictedLiveState",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub predicted_live_state: String,
    #[serde(
        rename = "resourceVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_version: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub modified: bool,
}

impl ResourceDiff {
    fn full_name(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.group, self.kind, self.namespace, self.name
        )
    }
}

const MASK: &str = "++++++++";
const LAST_APPLIED: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// A Secret's stringData folded into data (base64), as the API server and
/// Argo CD's NormalizeSecret do.
fn normalize_secret(obj: &mut Value) {
    if obj["kind"] != "Secret" {
        return;
    }
    let Some(map) = obj.as_object_mut() else {
        return;
    };
    let Some(Value::Object(sd)) = map.remove("stringData") else {
        return;
    };
    use base64::Engine;
    let data = map.entry("data").or_insert_with(|| json!({}));
    if !data.is_object() {
        *data = json!({});
    }
    for (k, v) in sd {
        let text = v
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| v.to_string());
        data[k] = Value::String(base64::engine::general_purpose::STANDARD.encode(text));
    }
}

/// Argo CD's HideSecretData: every value of a Secret's data, in the
/// desired object, the live one and their last-applied annotations,
/// becomes `++++++++`; for one key, each different value a longer run
/// (`++++++++++++`, ...), so a diff still shows which values differ.
pub fn hide_secret_data(target: &mut Value, live: &mut Value) {
    let la = |o: &Value| {
        o["metadata"]["annotations"][LAST_APPLIED]
            .as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
    };
    let mut target_la = la(target);
    let mut live_la = la(live);
    let mut objs: Vec<&mut Value> = vec![target, live];
    if let Some(t) = target_la.as_mut() {
        objs.push(t);
    }
    if let Some(l) = live_la.as_mut() {
        objs.push(l);
    }
    for o in objs.iter_mut() {
        normalize_secret(o);
    }
    let keys: BTreeSet<String> = objs
        .iter()
        .filter_map(|o| o["data"].as_object())
        .flat_map(|m| m.keys().cloned())
        .collect();
    for k in keys {
        let mut next = MASK.to_string();
        let mut seen: HashMap<String, String> = HashMap::new();
        for o in objs.iter_mut() {
            let Some(v) = o.get_mut("data").and_then(|d| d.get_mut(&k)) else {
                continue;
            };
            let val = v
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| v.to_string());
            let r = seen
                .entry(val)
                .or_insert_with(|| {
                    let r = next.clone();
                    next.push_str("++++");
                    r
                })
                .clone();
            *v = Value::String(r);
        }
    }
    drop(objs);
    for (o, l) in [(target, target_la), (live, live_la)] {
        if let Some(l) = l {
            o["metadata"]["annotations"][LAST_APPLIED] = Value::String(l.to_string());
        } else if o["metadata"]["annotations"][LAST_APPLIED].is_string() {
            // Not JSON: hidden whole.
            o["metadata"]["annotations"][LAST_APPLIED] = Value::String(MASK.into());
        }
    }
}

/// The live object as Argo CD's diff normalizes it: no status, no
/// creationTimestamp.
fn normalized_live(live: &Value) -> Value {
    let mut v = live.clone();
    if let Some(m) = v.as_object_mut() {
        m.remove("status");
    }
    if let Some(m) = v["metadata"].as_object_mut() {
        m.remove("creationTimestamp");
    }
    v
}

/// `b`'s fields over `a`'s: what the live object becomes once the desired
/// one is applied, near enough for the UI's Diff tab.
fn overlay(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut out = x.clone();
            for (k, v) in y {
                let merged = match x.get(k) {
                    Some(av) => overlay(av, v),
                    None => v.clone(),
                };
                out.insert(k.clone(), merged);
            }
            Value::Object(out)
        }
        (_, b) => b.clone(),
    }
}

/// The managed resources of a comparison as Argo CD's controller caches
/// them, sorted by group/kind/namespace/name.
pub fn managed_diffs(cmp: &Comparison) -> Vec<ResourceDiff> {
    let mut out: Vec<ResourceDiff> = cmp
        .resources
        .iter()
        .map(|r| {
            let mut target = cmp
                .desired
                .iter()
                .find(|d| d.key == r.key)
                .map(|d| d.obj.clone())
                .unwrap_or(Value::Null);
            let mut live = r.live.clone().unwrap_or(Value::Null);
            if r.key.kind == "Secret" && r.key.group.is_empty() {
                hide_secret_data(&mut target, &mut live);
            }
            let normalized = if live.is_null() {
                Value::Null
            } else {
                normalized_live(&live)
            };
            let predicted = match (&target, r.synced) {
                (Value::Null, _) => Value::Null,
                (_, true) => normalized.clone(),
                (t, false) if normalized.is_null() => t.clone(),
                (t, false) => overlay(&normalized, t),
            };
            ResourceDiff {
                group: r.key.group.clone(),
                kind: r.key.kind.clone(),
                namespace: r.key.namespace.clone(),
                name: r.key.name.clone(),
                target_state: target.to_string(),
                live_state: live.to_string(),
                normalized_live_state: normalized.to_string(),
                predicted_live_state: predicted.to_string(),
                resource_version: live["metadata"]["resourceVersion"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                modified: !r.synced,
                ..Default::default()
            }
        })
        .collect();
    out.sort_by_key(ResourceDiff::full_name);
    out
}

// --- nodes -------------------------------------------------------------

fn split_api_version(av: &str) -> (String, String) {
    match av.split_once('/') {
        Some((g, v)) => (g.into(), v.into()),
        None => (String::new(), av.into()),
    }
}

fn health_status(h: &Health) -> HealthStatus {
    HealthStatus {
        status: h.status.as_str().into(),
        message: h.message.clone(),
    }
}

/// A node for a live object, as Argo CD's cluster cache makes it
/// (controller/cache: asResourceNode, populateNodeInfo).
pub fn node_of(obj: &Value, health: Option<&Health>) -> Node {
    let meta = &obj["metadata"];
    let s = |v: &Value| v.as_str().unwrap_or_default().to_string();
    let (group, version) = split_api_version(obj["apiVersion"].as_str().unwrap_or_default());
    let namespace = s(&meta["namespace"]);
    let parent_refs = meta["ownerReferences"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|o| {
            let (group, version) = split_api_version(o["apiVersion"].as_str().unwrap_or_default());
            Ref {
                group,
                version,
                kind: s(&o["kind"]),
                namespace: namespace.clone(),
                name: s(&o["name"]),
                uid: s(&o["uid"]),
            }
        })
        .collect();
    let mut n = Node {
        r: Ref {
            group: group.clone(),
            version,
            kind: s(&obj["kind"]),
            namespace,
            name: s(&meta["name"]),
            uid: s(&meta["uid"]),
        },
        parent_refs,
        resource_version: s(&meta["resourceVersion"]),
        health: health.map(health_status),
        created_at: meta["creationTimestamp"].as_str().map(str::to_string),
        ..Default::default()
    };
    let revision = [
        "deployment.kubernetes.io/revision",
        "rollout.argoproj.io/revision",
    ]
    .iter()
    .find_map(|a| meta["annotations"][a].as_str())
    .map(|t| t.parse::<i64>().unwrap_or(0))
    .or_else(|| obj["revision"].as_i64())
    .unwrap_or(0);
    if revision > 0 {
        n.info.push(InfoItem {
            name: "Revision".into(),
            value: format!("Rev:{revision}"),
        });
    }
    let urls = external_urls(obj);
    match (group.as_str(), n.r.kind.as_str()) {
        ("", "Pod") => pod_info(obj, &mut n, urls),
        ("", "Service") => {
            let mut ni = json!({});
            if let Some(sel) = obj["spec"]["selector"]
                .as_object()
                .filter(|m| !m.is_empty())
            {
                ni["targetLabels"] = Value::Object(sel.clone());
            }
            if obj["spec"]["type"] == "LoadBalancer" {
                let ingress: Vec<Value> = obj["status"]["loadBalancer"]["ingress"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|i| {
                        if let Some(h) = i["hostname"].as_str() {
                            Some(json!({"hostname": h}))
                        } else {
                            i["ip"].as_str().map(|ip| json!({"ip": ip}))
                        }
                    })
                    .collect();
                if !ingress.is_empty() {
                    ni["ingress"] = Value::Array(ingress);
                }
            }
            if !urls.is_empty() {
                ni["externalURLs"] = json!(urls);
            }
            n.networking_info = Some(ni);
        }
        _ if !urls.is_empty() => n.networking_info = Some(json!({"externalURLs": urls})),
        _ => {}
    }
    n
}

/// `link.argocd.argoproj.io/*` annotations: a URL or `title|url`, http(s)
/// only.
fn external_urls(obj: &Value) -> Vec<String> {
    let Some(a) = obj["metadata"]["annotations"].as_object() else {
        return Vec::new();
    };
    a.iter()
        .filter(|(k, _)| k.starts_with("link.argocd.argoproj.io/"))
        .filter_map(|(_, v)| v.as_str())
        .filter(|v| {
            let url = v.split_once('|').map_or(*v, |(_, u)| u);
            url.starts_with("http://") || url.starts_with("https://")
        })
        .map(str::to_string)
        .collect()
}

fn arr(v: &Value) -> &[Value] {
    v.as_array().map(Vec::as_slice).unwrap_or_default()
}

fn str_of(v: &Value) -> &str {
    v.as_str().unwrap_or_default()
}

/// Argo CD's populatePodInfo: the reason kubectl shows, node, containers,
/// restarts, requests, images and labels.
fn pod_info(pod: &Value, n: &mut Node, urls: Vec<String>) {
    let (spec, status) = (&pod["spec"], &pod["status"]);
    let containers = arr(&spec["containers"]);
    let inits = arr(&spec["initContainers"]);
    let restartable = |c: &Value| c["restartPolicy"] == "Always";
    let total = containers.len() + inits.iter().filter(|c| restartable(c)).count();
    let mut ready = 0;
    let mut restarts = 0;
    let phase = str_of(&status["phase"]);
    let mut reason = match str_of(&status["reason"]) {
        "" => phase.to_string(),
        r => r.to_string(),
    };
    let images: BTreeSet<String> = inits
        .iter()
        .chain(containers)
        .filter_map(|c| c["image"].as_str())
        .map(str::to_string)
        .collect();
    n.images = images.into_iter().collect();
    let conditions = arr(&status["conditions"]);
    if conditions
        .iter()
        .any(|c| c["type"] == "PodScheduled" && c["reason"] == "SchedulingGated")
    {
        reason = "SchedulingGated".into();
    }
    let mut initializing = false;
    for (i, cs) in arr(&status["initContainerStatuses"]).iter().enumerate() {
        restarts += cs["restartCount"].as_i64().unwrap_or(0);
        let (term, waiting) = (&cs["state"]["terminated"], &cs["state"]["waiting"]);
        let is_restartable = inits
            .iter()
            .find(|c| c["name"] == cs["name"])
            .is_some_and(restartable);
        if term.is_object() && term["exitCode"].as_i64().unwrap_or(0) == 0 {
            continue;
        }
        if is_restartable && cs["started"] == true {
            if cs["ready"] == true {
                ready += 1;
            }
            continue;
        }
        if term.is_object() {
            reason = match str_of(&term["reason"]) {
                "" => match term["signal"].as_i64().unwrap_or(0) {
                    0 => format!("Init:ExitCode:{}", term["exitCode"].as_i64().unwrap_or(0)),
                    s => format!("Init:Signal:{s}"),
                },
                r => format!("Init:{r}"),
            };
        } else if !matches!(str_of(&waiting["reason"]), "" | "PodInitializing") {
            reason = format!("Init:{}", str_of(&waiting["reason"]));
        } else {
            reason = format!("Init:{i}/{}", inits.len());
        }
        initializing = true;
        break;
    }
    let initialized = conditions
        .iter()
        .any(|c| c["type"] == "Initialized" && c["status"] == "True");
    if !initializing || initialized {
        let mut running = false;
        for cs in arr(&status["containerStatuses"]).iter().rev() {
            restarts += cs["restartCount"].as_i64().unwrap_or(0);
            let (term, waiting) = (&cs["state"]["terminated"], &cs["state"]["waiting"]);
            if !str_of(&waiting["reason"]).is_empty() {
                reason = str_of(&waiting["reason"]).into();
            } else if term.is_object() && !str_of(&term["reason"]).is_empty() {
                reason = str_of(&term["reason"]).into();
            } else if term.is_object() {
                reason = match term["signal"].as_i64().unwrap_or(0) {
                    0 => format!("ExitCode:{}", term["exitCode"].as_i64().unwrap_or(0)),
                    s => format!("Signal:{s}"),
                };
            } else if cs["ready"] == true && cs["state"]["running"].is_object() {
                running = true;
                ready += 1;
            }
        }
        if reason == "Completed" && running {
            reason = "Running".into();
        }
    }
    let terminal = matches!(phase, "Succeeded" | "Failed");
    if pod["metadata"]["deletionTimestamp"].is_string() {
        if status["reason"] == "NodeLost" {
            reason = "Unknown".into();
        } else if !terminal {
            reason = "Terminating".into();
        }
    }
    let mut item = |name: &str, value: String| {
        n.info.push(InfoItem {
            name: name.into(),
            value,
        })
    };
    if !reason.is_empty() {
        item("Status Reason", reason);
    }
    item("Node", str_of(&spec["nodeName"]).into());
    item("Containers", format!("{ready}/{total}"));
    if restarts > 0 {
        item("Restart Count", restarts.to_string());
    }
    if !terminal {
        let (cpu, mem) = pod_requests(spec);
        item("cpu", cpu.to_string());
        item("memory", mem.to_string());
    }
    let mut ni = json!({});
    if let Some(l) = pod["metadata"]["labels"]
        .as_object()
        .filter(|m| !m.is_empty())
    {
        ni["labels"] = Value::Object(l.clone());
    }
    if !urls.is_empty() {
        ni["externalURLs"] = json!(urls);
    }
    n.networking_info = Some(ni);
}

/// A pod's CPU and memory requests in milli-units (Kubernetes'
/// PodRequests: containers, sidecars, the largest init step, overhead).
fn pod_requests(spec: &Value) -> (i128, i128) {
    let req = |r: &Value| {
        (
            r["cpu"].as_str().and_then(milli).unwrap_or(0),
            r["memory"].as_str().and_then(milli).unwrap_or(0),
        )
    };
    let add = |a: (i128, i128), b: (i128, i128)| (a.0 + b.0, a.1 + b.1);
    let max = |a: (i128, i128), b: (i128, i128)| (a.0.max(b.0), a.1.max(b.1));
    let mut reqs = arr(&spec["containers"])
        .iter()
        .map(|c| req(&c["resources"]["requests"]))
        .fold((0, 0), add);
    let (mut sidecars, mut init) = ((0, 0), (0, 0));
    for c in arr(&spec["initContainers"]) {
        let r = req(&c["resources"]["requests"]);
        let step = if c["restartPolicy"] == "Always" {
            reqs = add(reqs, r);
            sidecars = add(sidecars, r);
            sidecars
        } else {
            add(r, sidecars)
        };
        init = max(init, step);
    }
    add(max(reqs, init), req(&spec["overhead"]))
}

/// A quantity in milli-units, rounded up (resource.Quantity.MilliValue).
pub fn milli(q: &str) -> Option<i128> {
    let q = q.trim();
    let split = q
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '+' || c == '-'))
        .unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let (int, frac) = num.split_once('.').unwrap_or((num, ""));
    let neg = int.starts_with('-');
    let int = int.trim_start_matches(['+', '-']);
    let mantissa: i128 = format!("{int}{frac}").parse().ok()?;
    // value = mantissa * num / den
    let (mut num, mut den): (i128, i128) = (1, 10i128.checked_pow(frac.len() as u32)?);
    let p10 = |e: u32| 10i128.checked_pow(e);
    match suffix {
        "" => {}
        "m" => den *= 1000,
        "k" => num *= 1000,
        "M" => num *= p10(6)?,
        "G" => num *= p10(9)?,
        "T" => num *= p10(12)?,
        "P" => num *= p10(15)?,
        "E" => num *= p10(18)?,
        "Ki" => num *= 1 << 10,
        "Mi" => num *= 1 << 20,
        "Gi" => num *= 1 << 30,
        "Ti" => num *= 1 << 40,
        "Pi" => num *= 1 << 50,
        "Ei" => num *= 1 << 60,
        s if s.starts_with(['e', 'E']) => {
            let e: i32 = s[1..].parse().ok()?;
            if e >= 0 {
                num *= p10(e as u32)?;
            } else {
                den *= p10(e.unsigned_abs())?;
            }
        }
        _ => return None,
    }
    let v = mantissa.checked_mul(num)?.checked_mul(1000)?;
    let m = (v + den - 1) / den;
    Some(if neg { -m } else { m })
}

/// A child: a live object of a child kind, as a node, and the uids of its
/// owners.
#[derive(Clone, Debug, PartialEq)]
pub struct Child {
    pub kind: &'static str,
    pub node: Node,
    pub owners: Vec<String>,
}

impl Child {
    /// A watched object: the items of a list (and so of a watcher's first
    /// listing) carry no apiVersion and kind, so they come from the watched
    /// resource.
    pub fn of(kind: &'static str, obj: &Value, custom: &health::Customizations) -> Child {
        let mut obj = obj.clone();
        let ar = api_resource(kind);
        obj["apiVersion"] = Value::String(ar.api_version.clone());
        obj["kind"] = Value::String(ar.kind.clone());
        let h = health::assess(&obj, custom).ok().flatten();
        let node = node_of(&obj, h.as_ref());
        let owners = node.parent_refs.iter().map(|r| r.uid.clone()).collect();
        Child { kind, node, owners }
    }
}

/// An app's tree: its managed resources, then every child whose owner is
/// in the tree (ownerReferences, to any depth), sorted as Argo CD's
/// ApplicationTree.Normalize sorts them.
pub fn build(managed: &[Node], children: &[&Child]) -> Tree {
    let mut nodes: Vec<Node> = managed.to_vec();
    let mut have: HashSet<&str> = managed
        .iter()
        .map(|n| n.r.uid.as_str())
        .filter(|u| !u.is_empty())
        .collect();
    let mut added = vec![false; children.len()];
    loop {
        let mut more = false;
        for (i, c) in children.iter().enumerate() {
            if added[i] || have.contains(c.node.r.uid.as_str()) {
                continue;
            }
            if c.owners.iter().any(|o| have.contains(o.as_str())) {
                added[i] = true;
                have.insert(&c.node.r.uid);
                nodes.push(c.node.clone());
                more = true;
            }
        }
        if !more {
            break;
        }
    }
    nodes.sort_by_key(Node::full_name);
    Tree {
        nodes,
        ..Default::default()
    }
}

/// The first node Argo CD's UI cannot show: one without kind or version
/// (group is empty for the core API).
pub fn incomplete(tree: &Tree) -> Option<&Node> {
    tree.nodes
        .iter()
        .chain(&tree.orphaned_nodes)
        .find(|n| n.r.kind.is_empty() || n.r.version.is_empty())
}

/// The managed resources of a comparison, as nodes; one that is not in the
/// cluster is Missing.
pub fn managed_nodes(cmp: &Comparison) -> Vec<Node> {
    cmp.resources
        .iter()
        .map(|r| match &r.live {
            Some(live) => {
                let mut n = node_of(live, r.health.as_ref());
                // A live object read from a list has no apiVersion or kind.
                if n.r.kind.is_empty() {
                    n.r.kind = r.key.kind.clone();
                    n.r.group = r.key.group.clone();
                }
                if n.r.version.is_empty() {
                    n.r.version = r.version.clone();
                }
                n
            }
            None => Node {
                r: Ref {
                    group: r.key.group.clone(),
                    version: r.version.clone(),
                    kind: r.key.kind.clone(),
                    namespace: r.key.namespace.clone(),
                    name: r.key.name.clone(),
                    uid: String::new(),
                },
                health: Some(HealthStatus {
                    status: "Missing".into(),
                    message: String::new(),
                }),
                ..Default::default()
            },
        })
        .collect()
}

/// The child kinds a managed kind needs watched.
fn child_kinds(group: &str, kind: &str) -> &'static [&'static str] {
    match (group, kind) {
        ("apps", "Deployment") | ("argoproj.io", "Rollout") => &["ReplicaSet", "Pod"],
        ("apps", "ReplicaSet" | "StatefulSet" | "DaemonSet") | ("batch", "Job") => &["Pod"],
        ("batch", "CronJob") => &["Job", "Pod"],
        _ => &[],
    }
}

fn api_resource(kind: &str) -> ApiResource {
    let (g, v) = match kind {
        "ReplicaSet" => ("apps", "v1"),
        "Job" => ("batch", "v1"),
        _ => ("", "v1"),
    };
    ApiResource::from_gvk(&GroupVersionKind::gvk(g, v, kind))
}

/// One child kind (ReplicaSet, Pod, Job) to watch in one namespace of one
/// cluster.
type WatchKey = (String, String, &'static str);
/// A namespace of a cluster.
type NsKey = (String, String);

struct App {
    server: String,
    managed: Vec<Node>,
    diffs: Vec<ResourceDiff>,
    updated: Instant,
    dirty: bool,
    /// The hash of the tree last written, and when.
    written: Option<(u64, Instant)>,
}

#[derive(Default)]
struct State {
    apps: HashMap<String, App>,
    /// Children by namespace, by uid.
    children: HashMap<NsKey, HashMap<String, Child>>,
    watches: HashMap<WatchKey, JoinHandle<()>>,
    /// Trees to delete: apps that are gone.
    gone: Vec<String>,
}

pub struct Ui {
    ctx: Arc<Ctx>,
    addr: Address,
    secret: String,
    version: Option<String>,
    compression: Option<bool>,
    ttl: Duration,
    state: Mutex<State>,
    wake: Notify,
}

static UI: OnceLock<Arc<Ui>> = OnceLock::new();

fn parse_compression(s: &str) -> Result<bool> {
    match s {
        "gzip" => Ok(true),
        "none" => Ok(false),
        _ => bail!("unknown Redis compression {s:?} (gzip or none)"),
    }
}

/// Start the mode when the flags ask for it; a bad flag stops litecd.
pub fn start(ctx: Arc<Ctx>, flags: &Flags) -> Result<()> {
    if flags.argocd_ui_cache.is_empty() {
        return Ok(());
    }
    let addr = Address::parse(&flags.argocd_ui_cache).context("--argocd-ui-cache")?;
    let ttl = crate::app::parse_duration(&flags.argocd_ui_cache_ttl)
        .filter(|d| *d >= Duration::from_secs(60))
        .ok_or_else(|| anyhow!("--argocd-ui-cache-ttl: a duration of 1m or more"))?;
    let compression = flags
        .argocd_ui_cache_compression
        .as_deref()
        .map(parse_compression)
        .transpose()
        .context("--argocd-ui-cache-compression")?;
    warn!(
        "DEPRECATED compatibility mode --argocd-ui-cache: writing resource trees to {}:{} for \
         Argo CD's web UI. It is coupled to Argo CD's internal cache format and will be removed \
         once litecd has its own view (docs/spec.md, Deprecated compatibility)",
        addr.host, addr.port
    );
    let ui = Arc::new(Ui {
        ctx,
        addr,
        secret: flags.argocd_ui_cache_secret.clone(),
        version: flags
            .argocd_ui_cache_version
            .clone()
            .filter(|v| !v.is_empty()),
        compression,
        ttl,
        state: Mutex::default(),
        wake: Notify::new(),
    });
    UI.set(ui.clone())
        .map_err(|_| anyhow!("argocd-ui-cache started twice"))?;
    tokio::spawn(ui.writer());
    Ok(())
}

/// The controller compared an app (it reconciles it: litecd owns it now).
pub async fn update(instance: &str, kube: &Kube, server: &str, cmp: &Comparison) {
    if let Some(ui) = UI.get() {
        ui.update(
            instance,
            kube,
            server,
            managed_nodes(cmp),
            managed_diffs(cmp),
        )
        .await;
    }
}

/// An app's task ended (the app is gone, or no longer reconciled here).
pub async fn forget(instance: &str) {
    if let Some(ui) = UI.get() {
        let mut st = ui.state.lock().await;
        if st
            .apps
            .remove(instance)
            .is_some_and(|a| a.written.is_some())
        {
            st.gone.push(instance.to_string());
        }
        ui.reconcile_watches(&mut st, None);
        drop(st);
        ui.wake.notify_one();
    }
}

impl Ui {
    async fn update(
        self: &Arc<Self>,
        instance: &str,
        kube: &Kube,
        server: &str,
        managed: Vec<Node>,
        diffs: Vec<ResourceDiff>,
    ) {
        let mut st = self.state.lock().await;
        let app = st.apps.entry(instance.to_string()).or_insert_with(|| App {
            server: server.to_string(),
            managed: Vec::new(),
            diffs: Vec::new(),
            updated: Instant::now(),
            dirty: true,
            written: None,
        });
        app.updated = Instant::now();
        if app.managed != managed || app.diffs != diffs || app.server != server {
            app.managed = managed;
            app.diffs = diffs;
            app.server = server.to_string();
            app.dirty = true;
        }
        self.reconcile_watches(&mut st, Some((server, kube)));
        drop(st);
        self.wake.notify_one();
    }

    /// Start the watches the apps need, stop the others (and drop what
    /// they saw). `new`: a cluster whose client is at hand, to start them.
    fn reconcile_watches(self: &Arc<Self>, st: &mut State, new: Option<(&str, &Kube)>) {
        let mut want: HashSet<WatchKey> = HashSet::new();
        for app in st.apps.values() {
            for n in &app.managed {
                if n.r.namespace.is_empty() {
                    continue;
                }
                for k in child_kinds(&n.r.group, &n.r.kind) {
                    want.insert((app.server.clone(), n.r.namespace.clone(), k));
                }
            }
        }
        let stale: Vec<WatchKey> = st
            .watches
            .keys()
            .filter(|k| !want.contains(*k))
            .cloned()
            .collect();
        for k in stale {
            if let Some(h) = st.watches.remove(&k) {
                h.abort();
            }
            let ns: NsKey = (k.0.clone(), k.1.clone());
            if let Some(c) = st.children.get_mut(&ns) {
                c.retain(|_, c| c.kind != k.2);
                if c.is_empty() {
                    st.children.remove(&ns);
                }
            }
            debug!("argocd-ui: stopped watching {} in {}", k.2, k.1);
        }
        let Some((server, kube)) = new else { return };
        for k in want {
            if k.0 == server && !st.watches.contains_key(&k) {
                debug!("argocd-ui: watching {} in {}", k.2, k.1);
                let h = tokio::spawn(self.clone().watch(kube.clone(), k.clone()));
                st.watches.insert(k, h);
            }
        }
    }

    async fn watch(self: Arc<Self>, kube: Kube, key: WatchKey) {
        let (server, ns, kind) = key;
        let nskey: NsKey = (server, ns.clone());
        let api: Api<DynamicObject> =
            Api::namespaced_with(kube.client.clone(), &ns, &api_resource(kind));
        let mut stream = watcher::watcher(api, watcher::Config::default()).boxed();
        let mut listing: Option<HashMap<String, Child>> = None;
        while let Some(ev) = stream.next().await {
            let ev = match ev {
                Ok(ev) => ev,
                Err(e) => {
                    warn!("argocd-ui: watch {kind} in {ns}: {e}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let custom = self.ctx.settings.read().await.health.clone();
            let child = |o: &DynamicObject| {
                serde_json::to_value(o)
                    .ok()
                    .map(|v| Child::of(kind, &v, &custom))
                    .filter(|c| !c.node.r.uid.is_empty())
            };
            let mut st = self.state.lock().await;
            let children = st.children.entry(nskey.clone()).or_default();
            match ev {
                Event::Init => listing = Some(HashMap::new()),
                Event::InitApply(o) => {
                    if let (Some(l), Some(c)) = (listing.as_mut(), child(&o)) {
                        l.insert(c.node.r.uid.clone(), c);
                    }
                    continue;
                }
                Event::InitDone => {
                    children.retain(|_, c| c.kind != kind);
                    children.extend(listing.take().unwrap_or_default());
                }
                Event::Apply(o) => {
                    if let Some(c) = child(&o) {
                        if children.get(&c.node.r.uid) == Some(&c) {
                            continue;
                        }
                        children.insert(c.node.r.uid.clone(), c);
                    }
                }
                Event::Delete(o) => {
                    if let Some(uid) = &o.metadata.uid {
                        children.remove(uid);
                    }
                }
            }
            for app in st.apps.values_mut() {
                if app.server == nskey.0 && app.managed.iter().any(|n| n.r.namespace == ns) {
                    app.dirty = true;
                }
            }
            drop(st);
            self.wake.notify_one();
        }
    }

    /// Whether the format argocd-server reads is known, and which.
    async fn check_format(&self, conn: &mut Conn) -> Result<Format> {
        let server = self.argocd_server().await?;
        let (detected_version, detected_gzip) = match &server {
            Some((image, gzip)) => {
                let v = image_version(image)
                    .and_then(|(ma, mi)| known_cache_version(ma, mi))
                    .map(str::to_string);
                (v, Some(*gzip))
            }
            None => (None, None),
        };
        let image = server.as_ref().map(|s| s.0.as_str()).unwrap_or("not found");
        let cache_version = match (&self.version, &detected_version) {
            (Some(a), Some(b)) if a != b => {
                bail!("--argocd-ui-cache-version {a}, but argocd-server ({image}) uses {b}")
            }
            (Some(v), _) | (None, Some(v)) => v.clone(),
            (None, None) => bail!(
                "argocd-server's cache version is unknown (image {image}): set \
                 --argocd-ui-cache-version if it is known to be right"
            ),
        };
        let gzip = match (self.compression, detected_gzip) {
            (Some(a), Some(b)) if a != b => bail!(
                "--argocd-ui-cache-compression {}, but argocd-server uses {}",
                if a { "gzip" } else { "none" },
                if b { "gzip" } else { "none" }
            ),
            (Some(g), _) | (None, Some(g)) => g,
            (None, None) => true,
        };
        let f = Format {
            cache_version,
            gzip,
        };
        // Trees already in Redis (Argo CD's controller's, or ours) must have
        // this format.
        let want = tree_key("", &f);
        let want_suffix = want.trim_start_matches("app|resources-tree|");
        for pattern in ["app|resources-tree|*", "app|managed-resources|*"] {
            let mut cursor = b"0".to_vec();
            let mut checked = false;
            for _ in 0..50 {
                let reply = conn
                    .call(&[
                        b"SCAN",
                        &cursor,
                        b"MATCH",
                        pattern.as_bytes(),
                        b"COUNT",
                        b"1000",
                    ])
                    .await?;
                let Reply::Array(parts) = reply else {
                    bail!("SCAN: unexpected reply")
                };
                let (Some(Reply::Bulk(Some(next))), Some(Reply::Array(keys))) =
                    (parts.first(), parts.get(1))
                else {
                    bail!("SCAN: unexpected reply");
                };
                for k in keys {
                    let Reply::Bulk(Some(k)) = k else { continue };
                    let k = String::from_utf8_lossy(k);
                    // A shard (`...|<n>|<version>`) or the tree itself.
                    if !k.ends_with(want_suffix) {
                        bail!("Redis holds {k}, not of the format {want_suffix:?}");
                    }
                    if !checked
                        && let Reply::Bulk(Some(v)) = conn.call(&[b"GET", k.as_bytes()]).await?
                    {
                        if pattern.starts_with("app|resources-tree|") {
                            decode(&v, &f).map(|_| ())
                        } else {
                            decode_managed(&v, &f).map(|_| ())
                        }
                        .with_context(|| format!("{k} cannot be read"))?;
                        checked = true;
                    }
                }
                cursor = next.clone();
                if cursor == b"0" {
                    break;
                }
            }
        }
        Ok(f)
    }

    /// argocd-server's image and whether it compresses (gzip): its
    /// Deployment's container env REDIS_COMPRESSION (or the
    /// argocd-cmd-params-cm key it names) or `--redis-compress` flag.
    async fn argocd_server(&self) -> Result<Option<(String, bool)>> {
        let kube = &self.ctx.kube;
        let ns = self.ctx.namespace.as_str();
        let Some(dk) = kube.known("apps/v1", "Deployment").await else {
            return Ok(None);
        };
        let mut found = kube.get(&dk, Some(ns), "argocd-server").await?;
        if found.is_none() {
            found = kube
                .list(&dk, Some(ns), Some("app.kubernetes.io/name=argocd-server"))
                .await?
                .into_iter()
                .next();
        }
        let Some(d) = found else { return Ok(None) };
        let containers = arr(&d["spec"]["template"]["spec"]["containers"]);
        let Some(c) = containers
            .iter()
            .find(|c| c["name"] == "server" || c["name"] == "argocd-server")
            .or(containers.first())
        else {
            return Ok(None);
        };
        let image = str_of(&c["image"]).to_string();
        let mut compression: Option<String> = None;
        let args: Vec<&str> = arr(&c["command"])
            .iter()
            .chain(arr(&c["args"]))
            .filter_map(Value::as_str)
            .collect();
        for (i, a) in args.iter().enumerate() {
            if let Some(v) = a.strip_prefix("--redis-compress=") {
                compression = Some(v.into());
            } else if *a == "--redis-compress" {
                compression = args.get(i + 1).map(|v| v.to_string());
            }
        }
        if compression.is_none()
            && let Some(e) = arr(&c["env"])
                .iter()
                .find(|e| e["name"] == "REDIS_COMPRESSION")
        {
            if let Some(v) = e["value"].as_str() {
                compression = Some(v.into());
            } else if let Some(r) = e["valueFrom"]["configMapKeyRef"].as_object() {
                let name = r.get("name").and_then(Value::as_str).unwrap_or_default();
                let key = r.get("key").and_then(Value::as_str).unwrap_or_default();
                if let Some(ck) = kube.known("v1", "ConfigMap").await
                    && let Some(cm) = kube.get(&ck, Some(ns), name).await?
                {
                    compression = cm["data"][key].as_str().map(str::to_string);
                }
            }
        }
        let gzip = parse_compression(compression.as_deref().unwrap_or("gzip"))?;
        Ok(Some((image, gzip)))
    }

    async fn connect(&self) -> Result<Conn> {
        let mut password = None;
        if self.addr.password.is_none() && !self.secret.is_empty() {
            let sk = self.ctx.kube.kind("v1", "Secret").await?;
            let s = self
                .ctx
                .kube
                .get(&sk, Some(&self.ctx.namespace), &self.secret)
                .await?
                .ok_or_else(|| anyhow!("Secret {} not found", self.secret))?;
            if let Some(b) = s["data"]["auth"].as_str() {
                use base64::Engine;
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(b)
                    .context("Secret key auth")?;
                password = Some(String::from_utf8(raw).context("Secret key auth")?);
            }
        }
        Conn::connect(&self.addr, password.as_deref()).await
    }

    /// Write the trees that changed, refresh the ones about to expire,
    /// delete those of apps that are gone; check the format every few
    /// minutes.
    async fn writer(self: Arc<Self>) {
        let mut conn: Option<Conn> = None;
        let mut format: Option<Format> = None;
        let mut checked: Option<Instant> = None;
        let mut last_error = String::new();
        loop {
            tokio::select! {
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(30)) => {},
            }
            // Coalesce a burst of pod events.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let r: Result<()> = async {
                if conn.is_none() {
                    conn = Some(self.connect().await?);
                }
                let c = conn.as_mut().expect("connected");
                if checked.is_none_or(|t| t.elapsed() >= Duration::from_secs(300)) {
                    checked = Some(Instant::now());
                    match self.check_format(c).await {
                        Ok(f) => {
                            if format.as_ref() != Some(&f) {
                                info!("argocd-ui: writing trees as {}", tree_key("<app>", &f));
                            }
                            format = Some(f);
                        }
                        Err(e) => {
                            if format.is_some() || last_error.is_empty() {
                                error!("argocd-ui: not writing resource trees: {e:#}");
                            }
                            format = None;
                            last_error = format!("{e:#}");
                        }
                    }
                }
                let Some(f) = &format else { return Ok(()) };
                self.flush(c, f).await
            }
            .await;
            if let Err(e) = r {
                let msg = format!("{e:#}");
                if msg != last_error {
                    warn!("argocd-ui: {msg}");
                }
                last_error = msg;
                conn = None;
                checked = None;
            }
        }
    }

    async fn flush(&self, c: &mut Conn, f: &Format) -> Result<()> {
        let reconcile = self.ctx.settings.read().await.reconcile;
        // An app litecd has not compared lately is not litecd's (Argo CD's
        // controller may own it again): its tree is left alone. While that
        // controller runs, only apps compared in the last interval (those
        // annotated litecd.dev/manage) are written.
        let recent = if self.ctx.guard.status().apps_clear() {
            (reconcile * 3).max(Duration::from_secs(300))
        } else {
            reconcile + Duration::from_secs(60)
        };
        let mut writes = Vec::new();
        let gone = {
            let mut st = self.state.lock().await;
            let st = &mut *st;
            for (instance, app) in st.apps.iter_mut() {
                let expiring = app
                    .written
                    .is_some_and(|(_, t)| t.elapsed() >= self.ttl / 2);
                if !(app.dirty || expiring) || app.updated.elapsed() > recent {
                    continue;
                }
                app.dirty = false;
                let empty = HashMap::new();
                let mut children: Vec<&Child> = Vec::new();
                let namespaces: BTreeSet<&str> =
                    app.managed.iter().map(|n| n.r.namespace.as_str()).collect();
                for ns in namespaces {
                    let k = (app.server.clone(), ns.to_string());
                    children.extend(st.children.get(&k).unwrap_or(&empty).values());
                }
                let tree = build(&app.managed, &children);
                // Argo CD's UI breaks on a node without kind or version:
                // such a tree is never written.
                if let Some(bad) = incomplete(&tree) {
                    warn!(
                        "argocd-ui: not writing the tree of {instance}: node {} has no kind or version",
                        bad.full_name()
                    );
                    continue;
                }
                if let Some(bad) = app
                    .diffs
                    .iter()
                    .find(|d| d.kind.is_empty() || d.name.is_empty())
                {
                    warn!(
                        "argocd-ui: not writing the managed resources of {instance}: {} has no kind or name",
                        bad.full_name()
                    );
                    continue;
                }
                let json = serde_json::to_vec(&tree)?;
                let mut h = std::collections::hash_map::DefaultHasher::new();
                json.hash(&mut h);
                serde_json::to_vec(&app.diffs)?.hash(&mut h);
                let hash = h.finish();
                if app.written.is_some_and(|(w, _)| w == hash) && !expiring {
                    continue;
                }
                writes.push((
                    instance.clone(),
                    hash,
                    encode(&tree, f)?,
                    encode(&app.diffs, f)?,
                ));
            }
            std::mem::take(&mut st.gone)
        };
        let px = self.ttl.as_millis().to_string();
        for (instance, hash, value, managed) in writes {
            // The managed resources first: the UI reads them on a tree's
            // notice.
            let mkey = managed_key(&instance, f);
            c.call(&[b"SET", mkey.as_bytes(), &managed, b"PX", px.as_bytes()])
                .await
                .with_context(|| format!("SET {mkey}"))?;
            let key = tree_key(&instance, f);
            c.call(&[b"SET", key.as_bytes(), &value, b"PX", px.as_bytes()])
                .await
                .with_context(|| format!("SET {key}"))?;
            c.call(&[b"PUBLISH", notify_channel(&instance, f).as_bytes(), b""])
                .await?;
            debug!("argocd-ui: wrote {key} ({} bytes)", value.len());
            if let Some(app) = self.state.lock().await.apps.get_mut(&instance) {
                app.written = Some((hash, Instant::now()));
            }
        }
        for instance in gone {
            // An app that came back meanwhile keeps its tree.
            if self.state.lock().await.apps.contains_key(&instance) {
                continue;
            }
            c.call(&[
                b"DEL",
                tree_key(&instance, f).as_bytes(),
                managed_key(&instance, f).as_bytes(),
            ])
            .await?;
        }
        Ok(())
    }
}

/// `vMAJOR.MINOR` from an image reference (`quay.io/argoproj/argocd:v3.5.5`,
/// with or without a digest).
pub fn image_version(image: &str) -> Option<(u32, u32)> {
    let image = image.split('@').next()?;
    let tag = image.rsplit_once(':')?.1;
    if tag.contains('/') {
        return None;
    }
    let mut parts = tag.trim_start_matches('v').split(['.', '-']);
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gz() -> Format {
        Format {
            cache_version: "1.8.3".into(),
            gzip: true,
        }
    }

    #[test]
    fn keys() {
        assert_eq!(
            tree_key("guestbook", &gz()),
            "app|resources-tree|guestbook|1.8.3.gz"
        );
        assert_eq!(
            tree_key(
                "team-a_web",
                &Format {
                    cache_version: "1.8.3".into(),
                    gzip: false
                }
            ),
            "app|resources-tree|team-a_web|1.8.3"
        );
        assert_eq!(
            notify_channel("guestbook", &gz()),
            "app|managed-resources|guestbook|1.8.3"
        );
    }

    #[test]
    fn versions() {
        assert_eq!(
            image_version("quay.io/argoproj/argocd:v3.5.5"),
            Some((3, 5))
        );
        assert_eq!(
            image_version("registry:5000/argocd:v2.14.1@sha256:abc"),
            Some((2, 14))
        );
        assert_eq!(image_version("argocd"), None);
        assert_eq!(image_version("registry:5000/argocd"), None);
        assert_eq!(known_cache_version(3, 5), Some("1.8.3"));
        assert_eq!(known_cache_version(2, 5), None);
        assert_eq!(known_cache_version(3, 6), None);
    }

    #[test]
    fn quantities() {
        assert_eq!(milli("100m"), Some(100));
        assert_eq!(milli("1"), Some(1000));
        assert_eq!(milli("0.5"), Some(500));
        assert_eq!(milli("64Mi"), Some(64 * 1024 * 1024 * 1000));
        assert_eq!(milli("1G"), Some(1_000_000_000_000));
        assert_eq!(milli("1e3"), Some(1_000_000));
        assert_eq!(milli("1.5k"), Some(1_500_000));
        assert_eq!(milli("0.0001"), Some(1));
        assert_eq!(milli("x"), None);
    }

    fn deployment() -> Value {
        json!({"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "n", "uid": "d1", "resourceVersion": "10",
                "creationTimestamp": "2026-01-01T00:00:00Z",
                "annotations": {"deployment.kubernetes.io/revision": "2"}}})
    }

    fn rs(uid: &str, owner: &str) -> Value {
        json!({"apiVersion": "apps/v1", "kind": "ReplicaSet",
            "metadata": {"name": format!("web-{uid}"), "namespace": "n", "uid": uid, "resourceVersion": "11",
                "annotations": {"deployment.kubernetes.io/revision": "2"},
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": owner, "controller": true}]},
            "spec": {"replicas": 1}, "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}})
    }

    fn pod(uid: &str, owner: &str) -> Value {
        json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": format!("web-{uid}"), "namespace": "n", "uid": uid, "resourceVersion": "12",
                "creationTimestamp": "2026-01-01T00:00:01Z", "labels": {"app": "web"},
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-r1", "uid": owner}]},
            "spec": {"nodeName": "node-1",
                "initContainers": [{"name": "proxy", "image": "proxy:1", "restartPolicy": "Always",
                    "resources": {"requests": {"cpu": "50m", "memory": "16Mi"}}}],
                "containers": [{"name": "web", "image": "nginx:1.27",
                    "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}}}]},
            "status": {"phase": "Running",
                "conditions": [{"type": "Initialized", "status": "True"}, {"type": "Ready", "status": "True"}],
                "initContainerStatuses": [{"name": "proxy", "started": true, "ready": true, "restartCount": 0,
                    "state": {"running": {}}}],
                "containerStatuses": [{"name": "web", "ready": true, "restartCount": 2,
                    "state": {"running": {"startedAt": "2026-01-01T00:00:02Z"}}}]}})
    }

    #[test]
    fn pod_nodes() {
        let n = node_of(&pod("p1", "r1"), None);
        let info: Vec<(&str, &str)> = n
            .info
            .iter()
            .map(|i| (i.name.as_str(), i.value.as_str()))
            .collect();
        assert_eq!(
            info,
            [
                ("Status Reason", "Running"),
                ("Node", "node-1"),
                ("Containers", "2/2"),
                ("Restart Count", "2"),
                ("cpu", "150"),
                ("memory", &(80u64 * 1024 * 1024 * 1000).to_string()),
            ]
        );
        assert_eq!(n.images, ["nginx:1.27", "proxy:1"]);
        assert_eq!(n.networking_info, Some(json!({"labels": {"app": "web"}})));
        assert_eq!(n.parent_refs[0].kind, "ReplicaSet");
        assert_eq!(n.parent_refs[0].namespace, "n");

        let mut p = pod("p2", "r1");
        p["status"]["containerStatuses"][0]["state"] =
            json!({"waiting": {"reason": "CrashLoopBackOff"}});
        p["status"]["containerStatuses"][0]["ready"] = json!(false);
        let n = node_of(&p, None);
        assert_eq!(n.info[0].value, "CrashLoopBackOff");
        assert_eq!(n.info[2].value, "1/2");

        p["status"]["phase"] = json!("Succeeded");
        p["status"]["containerStatuses"][0]["state"] =
            json!({"terminated": {"reason": "Completed", "exitCode": 0}});
        let n = node_of(&p, None);
        assert!(!n.info.iter().any(|i| i.name == "cpu"));
        assert_eq!(
            n.info[1],
            InfoItem {
                name: "Node".into(),
                value: "node-1".into()
            }
        );
    }

    #[test]
    fn trees_follow_owner_references() {
        let custom = health::Customizations::default();
        let managed = vec![
            node_of(&deployment(), None),
            node_of(
                &json!({"apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "web", "namespace": "n", "uid": "s1"},
                "spec": {"selector": {"app": "web"}}}),
                None,
            ),
        ];
        let children = [
            Child::of("ReplicaSet", &rs("r1", "d1"), &custom),
            Child::of("Pod", &pod("p1", "r1"), &custom),
            // Another app's.
            Child::of("ReplicaSet", &rs("r9", "other"), &custom),
            Child::of("Pod", &pod("p9", "r9"), &custom),
        ];
        let refs: Vec<&Child> = children.iter().rev().collect();
        let tree = build(&managed, &refs);
        let names: Vec<String> = tree.nodes.iter().map(Node::full_name).collect();
        assert_eq!(
            names,
            [
                "/Pod/n/web-p1",
                "/Service/n/web",
                "apps/Deployment/n/web",
                "apps/ReplicaSet/n/web-r1"
            ]
        );
        let rs = &tree.nodes[3];
        assert_eq!(
            rs.info,
            [InfoItem {
                name: "Revision".into(),
                value: "Rev:2".into()
            }]
        );
        assert_eq!(
            rs.health.as_ref().map(|h| h.status.as_str()),
            Some("Healthy")
        );
        assert_eq!(rs.parent_refs[0].uid, "d1");
        assert_eq!(
            tree.nodes[1].networking_info,
            Some(json!({"targetLabels": {"app": "web"}}))
        );
    }

    #[test]
    fn encoding_round_trips() {
        let custom = health::Customizations::default();
        let managed = vec![node_of(&deployment(), None)];
        let children = [Child::of("ReplicaSet", &rs("r1", "d1"), &custom)];
        let tree = build(&managed, &children.iter().collect::<Vec<_>>());
        for f in [
            gz(),
            Format {
                cache_version: "1.8.3".into(),
                gzip: false,
            },
        ] {
            let data = encode(&tree, &f).unwrap();
            assert_eq!(decode(&data, &f).unwrap(), tree);
        }
        let plain = encode(
            &tree,
            &Format {
                cache_version: "x".into(),
                gzip: false,
            },
        )
        .unwrap();
        assert!(plain.ends_with(b"}\n"));
        let v: Value = serde_json::from_slice(&plain).unwrap();
        assert_eq!(v["nodes"][0]["kind"], "Deployment");
        assert_eq!(v["nodes"][0]["createdAt"], "2026-01-01T00:00:00Z");
        assert!(v.get("shardsCount").is_none());
        assert!(decode(&plain, &gz()).is_err());
    }

    /// Off unless asked for: no flag, no mode; the controller's hooks do
    /// nothing (no watch started, no Redis connection).
    #[tokio::test]
    async fn off_by_default() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            flags: Flags,
        }
        // SAFETY: tests of this module read no LITECD_ARGOCD_UI_* variable
        // concurrently.
        unsafe { std::env::remove_var("LITECD_ARGOCD_UI_CACHE") };
        let cli = <Cli as clap::Parser>::parse_from(["litecd"]);
        assert!(cli.flags.argocd_ui_cache.is_empty());
        assert_eq!(cli.flags.argocd_ui_cache_secret, "argocd-redis");
        assert_eq!(cli.flags.argocd_ui_cache_ttl, "1h");
        forget("guestbook").await;
        assert!(UI.get().is_none());
    }

    /// Objects as a list or watch returns them: no apiVersion, no kind.
    #[test]
    fn watched_objects_get_their_kind() {
        let custom = health::Customizations::default();
        let strip = |mut v: Value| {
            let o = v.as_object_mut().unwrap();
            o.remove("apiVersion");
            o.remove("kind");
            v
        };
        let r = Child::of("ReplicaSet", &strip(rs("r1", "d1")), &custom);
        assert_eq!(
            (
                r.node.r.group.as_str(),
                r.node.r.version.as_str(),
                r.node.r.kind.as_str()
            ),
            ("apps", "v1", "ReplicaSet")
        );
        assert_eq!(
            r.node.health.as_ref().map(|h| h.status.as_str()),
            Some("Healthy")
        );
        let p = Child::of("Pod", &strip(pod("p1", "r1")), &custom);
        assert_eq!(
            (
                p.node.r.group.as_str(),
                p.node.r.version.as_str(),
                p.node.r.kind.as_str()
            ),
            ("", "v1", "Pod")
        );
        assert!(p.node.info.iter().any(|i| i.name == "Containers"));
        assert_eq!(p.node.parent_refs[0].kind, "ReplicaSet");
        assert_eq!(p.node.parent_refs[0].version, "v1");
        let j = Child::of(
            "Job",
            &json!({"metadata": {"name": "j", "namespace": "n", "uid": "j1"}}),
            &custom,
        );
        assert_eq!(
            (j.node.r.group.as_str(), j.node.r.kind.as_str()),
            ("batch", "Job")
        );

        let managed = vec![node_of(&deployment(), None)];
        let tree = build(&managed, &[&r, &p]);
        assert!(incomplete(&tree).is_none());
        let mut bad = tree.clone();
        bad.nodes[0].r.kind.clear();
        assert!(incomplete(&bad).is_some());
    }

    #[test]
    fn managed_keys() {
        assert_eq!(
            managed_key("guestbook", &gz()),
            "app|managed-resources|guestbook|1.8.3.gz"
        );
    }

    #[test]
    fn secrets_are_masked_as_argo_cd_masks_them() {
        let mut target = json!({"apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "s"},
            "stringData": {"password": "a"}, "data": {"token": "dA==", "same": "eA=="}});
        let mut live = json!({"apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "s", "annotations": {LAST_APPLIED:
                r#"{"apiVersion":"v1","kind":"Secret","data":{"password":"Yg==","same":"eA=="}}"#}},
            "data": {"password": "Yg==", "token": "dA==", "same": "eA=="}});
        hide_secret_data(&mut target, &mut live);
        assert!(target.get("stringData").is_none());
        // Equal values share a mask; a different one gets a longer one.
        assert_eq!(
            target["data"],
            json!({"password": "++++++++", "token": "++++++++", "same": "++++++++"})
        );
        assert_eq!(
            live["data"],
            json!({"password": "++++++++++++", "token": "++++++++", "same": "++++++++"})
        );
        let la: Value = serde_json::from_str(
            live["metadata"]["annotations"][LAST_APPLIED]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            la["data"],
            json!({"password": "++++++++++++", "same": "++++++++"})
        );
        let text = format!("{target}{live}");
        assert!(!text.contains("dA==") && !text.contains("Yg==") && !text.contains("eA=="));
    }

    /// The managed resources Argo CD v3.5.5's controller wrote for the
    /// compat case argocd_ui_tree (GET app|managed-resources|<app>|1.8.3.gz).
    #[test]
    fn decodes_argo_cd_managed_resources() {
        let data = include_bytes!("testdata/argocd-v3.5.5-managed-resources.json.gz");
        let items = decode_managed(data, &gz()).unwrap();
        let names: Vec<String> = items.iter().map(ResourceDiff::full_name).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        assert!(names.iter().any(|n| n.starts_with("apps/Deployment/")));
        for r in &items {
            assert!(!r.kind.is_empty() && !r.name.is_empty() && !r.resource_version.is_empty());
            let live: Value = serde_json::from_str(&r.live_state).unwrap();
            let target: Value = serde_json::from_str(&r.target_state).unwrap();
            assert_eq!(live["kind"], r.kind.as_str());
            assert_eq!(target["metadata"]["name"], r.name.as_str());
            // Argo CD's normalized live object: no status, no creationTimestamp
            // (plus scheme defaults on built-in kinds, which litecd leaves
            // out); ours is the same where there are no defaults.
            let normalized: Value = serde_json::from_str(&r.normalized_live_state).unwrap();
            assert!(normalized.get("status").is_none());
            assert!(normalized["metadata"].get("creationTimestamp").is_none());
            if matches!(r.kind.as_str(), "ConfigMap" | "Secret") {
                assert_eq!(normalized_live(&live), normalized, "{}", r.full_name());
            }
            if r.kind == "Secret" {
                assert_eq!(target["data"]["password"], MASK);
                assert_eq!(live["data"]["token"], MASK);
                // Masking what is masked changes nothing.
                let (mut t, mut l) = (target.clone(), live.clone());
                hide_secret_data(&mut t, &mut l);
                assert_eq!((t, l), (target.clone(), live.clone()));
            }
        }
        assert_eq!(
            decode_managed(&encode(&items, &gz()).unwrap(), &gz()).unwrap(),
            items
        );
    }

    #[test]
    fn managed_resource_states() {
        let live = json!({"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "c", "namespace": "n", "resourceVersion": "7", "creationTimestamp": "t"},
            "data": {"k": "live"}});
        let target = json!({"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "c", "namespace": "n"}, "data": {"k": "git"}});
        assert_eq!(
            overlay(&normalized_live(&live), &target),
            json!({"apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": "c", "namespace": "n", "resourceVersion": "7"}, "data": {"k": "git"}})
        );
        let r = ResourceDiff {
            kind: "ConfigMap".into(),
            name: "c".into(),
            target_state: "null".into(),
            modified: true,
            ..Default::default()
        };
        let v: Value = serde_json::from_slice(
            &encode(
                &vec![r],
                &Format {
                    cache_version: "x".into(),
                    gzip: false,
                },
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            v,
            json!([{"kind": "ConfigMap", "name": "c", "targetState": "null", "modified": true}])
        );
    }

    /// A tree Argo CD v3.5.5's application controller wrote to Redis (the
    /// compat case argocd_ui_tree, GET app|resources-tree|<app>|1.8.3.gz).
    #[test]
    fn decodes_argo_cd_trees() {
        let data = include_bytes!("testdata/argocd-v3.5.5-tree.json.gz");
        let tree = decode(data, &gz()).unwrap();
        let kinds: BTreeSet<&str> = tree.nodes.iter().map(|n| n.r.kind.as_str()).collect();
        assert!(
            kinds.contains("Deployment") && kinds.contains("ReplicaSet") && kinds.contains("Pod")
        );
        // Every node has a kind and a version, as the UI needs.
        assert!(incomplete(&tree).is_none());
        // Our encoding of it reads back the same.
        assert_eq!(decode(&encode(&tree, &gz()).unwrap(), &gz()).unwrap(), tree);
    }
}
