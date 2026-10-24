//! The Kubernetes API as litecd uses it: untyped JSON objects addressed by
//! group, version, kind, namespace and name, resolved through discovery.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use kube::Client;
use kube::discovery::{ApiResource, Discovery, Scope};
use serde_json::{Value, json};
use tokio::sync::RwLock;

/// A resource type as discovery reports it.
#[derive(Clone, Debug)]
pub struct Kind {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub namespaced: bool,
}

impl Kind {
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }

    /// The collection path, in `ns` when the kind is namespaced.
    pub fn path(&self, ns: Option<&str>) -> String {
        let base = if self.group.is_empty() {
            format!("/api/{}", self.version)
        } else {
            format!("/apis/{}/{}", self.group, self.version)
        };
        match (self.namespaced, ns) {
            (true, Some(ns)) => format!("{base}/namespaces/{ns}/{}", self.plural),
            _ => format!("{base}/{}", self.plural),
        }
    }

    pub fn api_resource(&self) -> ApiResource {
        ApiResource {
            group: self.group.clone(),
            version: self.version.clone(),
            api_version: self.api_version(),
            kind: self.kind.clone(),
            plural: self.plural.clone(),
        }
    }
}

/// An error answer of the API server.
#[derive(Debug)]
pub struct ApiError {
    pub code: u16,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ApiError {}

pub fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ApiError>().is_some_and(|e| e.code == 404)
}

#[derive(Clone)]
pub struct Kube {
    pub client: Client,
    kinds: Arc<RwLock<HashMap<(String, String), Kind>>>,
    api_versions: Arc<RwLock<Vec<String>>>,
    server_version: Arc<RwLock<String>>,
}

impl Kube {
    pub async fn new() -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("connecting to the Kubernetes API")?;
        let k = Kube {
            client,
            kinds: Arc::default(),
            api_versions: Arc::default(),
            server_version: Arc::default(),
        };
        k.rediscover().await?;
        Ok(k)
    }

    /// Reload discovery: after a CRD is created, say.
    pub async fn rediscover(&self) -> Result<()> {
        let d = Discovery::new(self.client.clone())
            .run()
            .await
            .context("API discovery")?;
        let mut kinds = HashMap::new();
        let mut versions = Vec::new();
        for g in d.groups() {
            for v in g.versions() {
                let gv = if g.name().is_empty() {
                    v.to_string()
                } else {
                    format!("{}/{v}", g.name())
                };
                versions.push(gv.clone());
                for (ar, caps) in g.versioned_resources(v) {
                    if ar.plural.contains('/') {
                        continue;
                    }
                    versions.push(format!("{gv}/{}", ar.kind));
                    kinds.insert(
                        (gv.clone(), ar.kind.clone()),
                        Kind {
                            group: ar.group.clone(),
                            version: ar.version.clone(),
                            kind: ar.kind.clone(),
                            plural: ar.plural.clone(),
                            namespaced: caps.scope == Scope::Namespaced,
                        },
                    );
                }
            }
        }
        versions.sort();
        versions.dedup();
        *self.kinds.write().await = kinds;
        *self.api_versions.write().await = versions;
        if let Ok(v) = self.client.apiserver_version().await {
            *self.server_version.write().await = v.git_version;
        }
        Ok(())
    }

    /// The kind for an apiVersion and kind, discovering again once when it
    /// is not known (a CRD applied a moment ago).
    pub async fn kind(&self, api_version: &str, kind: &str) -> Result<Kind> {
        let key = (api_version.to_string(), kind.to_string());
        if let Some(k) = self.kinds.read().await.get(&key) {
            return Ok(k.clone());
        }
        self.rediscover().await?;
        self.kinds.read().await.get(&key).cloned().ok_or_else(|| {
            anyhow!(ApiError {
                code: 404,
                message: format!("the server could not find the requested resource (kind {kind} in {api_version})"),
            })
        })
    }

    /// Whether a kind is known without discovering again.
    pub async fn known(&self, api_version: &str, kind: &str) -> Option<Kind> {
        self.kinds
            .read()
            .await
            .get(&(api_version.to_string(), kind.to_string()))
            .cloned()
    }

    /// Every group/version and group/version/Kind, for `helm template --api-versions`.
    pub async fn api_versions(&self) -> Vec<String> {
        self.api_versions.read().await.clone()
    }

    pub async fn server_version(&self) -> String {
        self.server_version.read().await.clone()
    }

    pub async fn request(
        &self,
        method: http::Method,
        path: &str,
        content_type: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        let body = body
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default();
        let req = http::Request::builder()
            .method(method)
            .uri(path)
            .header("Content-Type", content_type)
            .header("Accept", "application/json")
            .body(body)?;
        match self.client.request_text(req).await {
            Ok(text) if text.is_empty() => Ok(Value::Null),
            Ok(text) => Ok(serde_json::from_str(&text)
                .with_context(|| format!("decoding the answer of {path}"))?),
            Err(kube::Error::Api(s)) => bail!(ApiError {
                code: s.code,
                message: s.message.clone()
            }),
            Err(e) => Err(anyhow!(e).context(path.to_string())),
        }
    }

    pub async fn get(&self, kind: &Kind, ns: Option<&str>, name: &str) -> Result<Option<Value>> {
        let path = format!("{}/{name}", kind.path(ns));
        match self
            .request(http::Method::GET, &path, "application/json", None)
            .await
        {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn list(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        selector: Option<&str>,
    ) -> Result<Vec<Value>> {
        let mut path = kind.path(ns);
        if let Some(s) = selector {
            path = format!("{path}?labelSelector={}", urlencode(s));
        }
        let v = self
            .request(http::Method::GET, &path, "application/json", None)
            .await?;
        Ok(match v.get("items") {
            Some(Value::Array(items)) => items.clone(),
            _ => Vec::new(),
        })
    }

    pub async fn create(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        obj: &Value,
        dry_run: bool,
    ) -> Result<Value> {
        let mut path = kind.path(ns);
        if dry_run {
            path.push_str("?dryRun=All");
        }
        self.request(http::Method::POST, &path, "application/json", Some(obj))
            .await
    }

    pub async fn replace(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        name: &str,
        obj: &Value,
    ) -> Result<Value> {
        let path = format!("{}/{name}", kind.path(ns));
        self.request(http::Method::PUT, &path, "application/json", Some(obj))
            .await
    }

    /// Server-side apply with litecd's field manager, taking over conflicts.
    pub async fn apply(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        name: &str,
        obj: &Value,
        dry_run: bool,
    ) -> Result<Value> {
        let mut path = format!(
            "{}/{name}?fieldManager={FIELD_MANAGER}&force=true",
            kind.path(ns)
        );
        if dry_run {
            path.push_str("&dryRun=All");
        }
        self.request(
            http::Method::PATCH,
            &path,
            "application/apply-patch+yaml",
            Some(obj),
        )
        .await
    }

    pub async fn merge_patch(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        name: &str,
        patch: &Value,
    ) -> Result<Value> {
        let path = format!("{}/{name}?fieldManager={FIELD_MANAGER}", kind.path(ns));
        self.request(
            http::Method::PATCH,
            &path,
            "application/merge-patch+json",
            Some(patch),
        )
        .await
    }

    /// Merge patch of the status subresource.
    pub async fn merge_patch_status(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        name: &str,
        patch: &Value,
    ) -> Result<Value> {
        let path = format!(
            "{}/{name}/status?fieldManager={FIELD_MANAGER}",
            kind.path(ns)
        );
        self.request(
            http::Method::PATCH,
            &path,
            "application/merge-patch+json",
            Some(patch),
        )
        .await
    }

    pub async fn delete(
        &self,
        kind: &Kind,
        ns: Option<&str>,
        name: &str,
        propagation: &str,
    ) -> Result<()> {
        let path = format!("{}/{name}", kind.path(ns));
        let body =
            json!({"kind": "DeleteOptions", "apiVersion": "v1", "propagationPolicy": propagation});
        match self
            .request(http::Method::DELETE, &path, "application/json", Some(&body))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// The field manager Argo CD uses, so ownership carries over between the two.
pub const FIELD_MANAGER: &str = "argocd-controller";

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// apiVersion → (group, version).
pub fn split_api_version(av: &str) -> (&str, &str) {
    match av.split_once('/') {
        Some((g, v)) => (g, v),
        None => ("", av),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        let k = Kind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
            plural: "deployments".into(),
            namespaced: true,
        };
        assert_eq!(
            k.path(Some("web")),
            "/apis/apps/v1/namespaces/web/deployments"
        );
        let ns = Kind {
            group: "".into(),
            version: "v1".into(),
            kind: "Namespace".into(),
            plural: "namespaces".into(),
            namespaced: false,
        };
        assert_eq!(ns.path(Some("ignored")), "/api/v1/namespaces");
        assert_eq!(urlencode("a=b,c"), "a%3Db%2Cc");
        assert_eq!(split_api_version("v1"), ("", "v1"));
    }
}
