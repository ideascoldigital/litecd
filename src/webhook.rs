//! `/api/webhook`, where argocd-server takes pushes from Git providers:
//! the apps reading the pushed repository get a refresh, as Argo CD does it
//! (the `argocd.argoproj.io/refresh` annotation). GitHub, Gitea, Gogs and
//! GitLab payloads; `/healthz` for probes. Signatures are checked as Argo CD
//! does, against argocd-secret's `webhook.<provider>.secret`: a provider with
//! a secret set needs a valid signature (GitHub's `X-Hub-Signature-256`,
//! Gitea's or Gogs' HMAC, GitLab's token); one without takes any push.

use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::app::{Ctx, REFRESH};

const MAX_BODY: usize = 10 << 20;

/// The Git provider a push comes from, by its event header.
#[derive(Debug, PartialEq, Eq)]
enum Provider {
    GitHub,
    Gogs,
    GitLab,
    Unknown,
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn provider(headers: &[(String, String)]) -> Provider {
    if header(headers, "x-github-event").is_some() {
        Provider::GitHub
    } else if header(headers, "x-gitea-event").is_some()
        || header(headers, "x-gogs-event").is_some()
    {
        Provider::Gogs
    } else if header(headers, "x-gitlab-event").is_some() {
        Provider::GitLab
    } else {
        Provider::Unknown
    }
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Equal without an early exit on the first differing byte.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn hmac_ok(secret: &str, body: &[u8], signature_hex: Option<&str>) -> bool {
    let Some(sig) = signature_hex.and_then(unhex) else {
        return false;
    };
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    ring::hmac::verify(&key, body, &sig).is_ok()
}

/// Whether a push may be acted on: `Err` names why not. `secrets` are
/// argocd-secret's decoded data.
fn verify(
    headers: &[(String, String)],
    body: &[u8],
    secrets: &std::collections::HashMap<String, String>,
) -> std::result::Result<(), &'static str> {
    let secret = |k: &str| secrets.get(k).map(String::as_str).filter(|s| !s.is_empty());
    match provider(headers) {
        Provider::GitHub => match secret("webhook.github.secret") {
            None => Ok(()),
            Some(s) => {
                let sig =
                    header(headers, "x-hub-signature-256").and_then(|v| v.strip_prefix("sha256="));
                if hmac_ok(s, body, sig) {
                    Ok(())
                } else {
                    Err("GitHub signature does not match webhook.github.secret")
                }
            }
        },
        Provider::Gogs => match secret("webhook.gogs.secret") {
            None => Ok(()),
            Some(s) => {
                let sig = header(headers, "x-gitea-signature")
                    .or_else(|| header(headers, "x-gogs-signature"));
                if hmac_ok(s, body, sig) {
                    Ok(())
                } else {
                    Err("Gitea/Gogs signature does not match webhook.gogs.secret")
                }
            }
        },
        Provider::GitLab => match secret("webhook.gitlab.secret") {
            None => Ok(()),
            Some(s) => {
                let token = header(headers, "x-gitlab-token").unwrap_or_default();
                let same = ct_eq(token.as_bytes(), s.as_bytes());
                if same {
                    Ok(())
                } else {
                    Err("GitLab token does not match webhook.gitlab.secret")
                }
            }
        },
        // A payload without a known event header: refused once any secret is
        // set, so a secret cannot be sidestepped by leaving the header out.
        Provider::Unknown => {
            if [
                "webhook.github.secret",
                "webhook.gogs.secret",
                "webhook.gitlab.secret",
            ]
            .iter()
            .any(|k| secret(k).is_some())
            {
                Err("unknown Git provider and webhook secrets are set")
            } else {
                Ok(())
            }
        }
    }
}

/// argocd-secret's data, decoded; empty when it does not exist.
async fn webhook_secrets(ctx: &Ctx) -> Result<std::collections::HashMap<String, String>> {
    use base64::Engine;
    let kind = ctx.kube.kind("v1", "Secret").await?;
    let Some(secret) = ctx
        .kube
        .get(&kind, Some(&ctx.namespace), "argocd-secret")
        .await?
    else {
        return Ok(Default::default());
    };
    let mut out = std::collections::HashMap::new();
    if let Some(data) = secret["data"].as_object() {
        for (k, v) in data {
            if let Some(bytes) = v
                .as_str()
                .and_then(|v| base64::engine::general_purpose::STANDARD.decode(v).ok())
            {
                out.insert(
                    k.clone(),
                    String::from_utf8_lossy(&bytes).trim().to_string(),
                );
            }
        }
    }
    Ok(out)
}

/// A repository URL reduced to host and path: scheme, credentials, `.git`
/// and case do not matter.
pub fn normalize(url: &str) -> String {
    let mut u = url.trim().to_lowercase();
    if let Some((_, rest)) = u.split_once("://") {
        u = rest.to_string();
    }
    if let Some((creds, rest)) = u.split_once('@')
        && !creds.contains('/')
    {
        u = rest.to_string();
    }
    // scp-like ssh: git@host:owner/repo
    if !u.contains('/')
        || u.find(':').is_some_and(|c| {
            u.find('/').is_some_and(|s| c < s)
                && !u[c + 1..].starts_with(|ch: char| ch.is_ascii_digit())
        })
    {
        u = u.replacen(':', "/", 1);
    }
    u.trim_end_matches('/').trim_end_matches(".git").to_string()
}

/// The repository URLs a push payload names.
pub fn pushed_urls(payload: &Value) -> Vec<String> {
    let mut out = Vec::new();
    for (obj, keys) in [
        (
            &payload["repository"],
            &[
                "html_url",
                "clone_url",
                "ssh_url",
                "git_http_url",
                "git_ssh_url",
                "url",
            ][..],
        ),
        (
            &payload["project"],
            &[
                "web_url",
                "git_http_url",
                "git_ssh_url",
                "http_url",
                "ssh_url",
            ][..],
        ),
    ] {
        for k in keys {
            if let Some(u) = obj[*k].as_str().filter(|u| !u.is_empty()) {
                out.push(normalize(u));
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn sources(app: &Value) -> Vec<&Value> {
    match app["spec"]["sources"].as_array() {
        Some(s) => s.iter().collect(),
        None => vec![&app["spec"]["source"]],
    }
}

async fn refresh_matching(ctx: &Ctx, urls: &[String]) -> Result<usize> {
    let kind = ctx.kube.kind("argoproj.io/v1alpha1", "Application").await?;
    let mut n = 0;
    for app in ctx.kube.list(&kind, None, None).await? {
        let app_ns = app["metadata"]["namespace"].as_str().unwrap_or_default();
        if !ctx.app_namespace_allowed(app_ns).await {
            continue;
        }
        let hit = sources(&app).iter().any(|s| {
            s["repoURL"]
                .as_str()
                .is_some_and(|u| urls.contains(&normalize(u)))
        });
        if !hit {
            continue;
        }
        // Argo CD's controller owns it while it runs: no annotation write.
        if !ctx.guard.may_act_on_app(&app) {
            continue;
        }
        let name = app["metadata"]["name"].as_str().unwrap_or_default();
        let patch = json!({"metadata": {"annotations": {REFRESH: "normal"}}});
        if let Err(e) = ctx
            .kube
            .merge_patch(&kind, Some(app_ns), name, &patch)
            .await
        {
            warn!("webhook: refreshing {name}: {e}");
        } else {
            n += 1;
        }
    }
    Ok(n)
}

async fn handle(ctx: Arc<Ctx>, mut s: TcpStream) -> Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        if buf.len() > 64 << 10 {
            return Ok(());
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.lines();
    let mut first = lines.next().unwrap_or_default().split_whitespace();
    let method = first.next().unwrap_or_default().to_string();
    let path = first
        .next()
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let len = header(&headers, "content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0)
        .min(MAX_BODY);
    while buf.len() < head_end + len {
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = &buf[head_end..buf.len().min(head_end + len)];
    let (status, text) = match (method.as_str(), path.as_str()) {
        (_, "/healthz") => ("200 OK", "ok\n".to_string()),
        ("POST", "/api/webhook") => match webhook_secrets(&ctx)
            .await
            .map(|s| verify(&headers, body, &s))
        {
            Err(e) => (
                "500 Internal Server Error",
                format!("reading argocd-secret: {e:#}\n"),
            ),
            Ok(Err(why)) => {
                warn!("webhook: refused: {why}");
                ("401 Unauthorized", format!("{why}\n"))
            }
            Ok(Ok(())) => match serde_json::from_slice::<Value>(body) {
                Ok(payload) => {
                    let urls = pushed_urls(&payload);
                    match refresh_matching(&ctx, &urls).await {
                        Ok(n) => {
                            info!("webhook: push to {urls:?}, {n} app(s) refreshed");
                            ("200 OK", format!("{n} application(s) refreshed\n"))
                        }
                        Err(e) => ("500 Internal Server Error", format!("{e:#}\n")),
                    }
                }
                Err(e) => ("400 Bad Request", format!("{e}\n")),
            },
        },
        _ => ("404 Not Found", "not found\n".to_string()),
    };
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
        text.len()
    );
    s.write_all(resp.as_bytes()).await?;
    Ok(())
}

pub async fn serve(ctx: Arc<Ctx>, addr: &str) -> Result<()> {
    let l = TcpListener::bind(addr).await?;
    info!("webhooks on {addr}/api/webhook");
    loop {
        let (s, _) = l.accept().await?;
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(ctx, s).await {
                warn!("webhook: {e:#}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_normalize() {
        let a = normalize("http://gitea-http.gitea.svc.cluster.local:3000/lkadmin/gitops.git");
        assert_eq!(a, "gitea-http.gitea.svc.cluster.local:3000/lkadmin/gitops");
        assert_eq!(
            normalize("https://user:token@GitHub.com/me/Repo/"),
            "github.com/me/repo"
        );
        assert_eq!(
            normalize("git@github.com:me/repo.git"),
            "github.com/me/repo"
        );
        assert_eq!(
            normalize("ssh://git@github.com/me/repo.git"),
            "github.com/me/repo"
        );
    }

    #[test]
    fn payloads() {
        let gitea = json!({"ref": "refs/heads/main", "repository": {
            "html_url": "http://gitea.localhost/lkadmin/gitops",
            "clone_url": "http://gitea-http.gitea.svc.cluster.local:3000/lkadmin/gitops.git",
            "ssh_url": "git@gitea.localhost:lkadmin/gitops.git"}});
        let urls = pushed_urls(&gitea);
        assert!(
            urls.contains(&"gitea-http.gitea.svc.cluster.local:3000/lkadmin/gitops".to_string())
        );
        assert!(urls.contains(&"gitea.localhost/lkadmin/gitops".to_string()));
        let gitlab = json!({"project": {"git_http_url": "https://gitlab.com/g/p.git"}});
        assert_eq!(pushed_urls(&gitlab), ["gitlab.com/g/p"]);
    }

    fn sign(secret: &str, body: &[u8]) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
        ring::hmac::sign(&key, body)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn signatures() {
        let body = br#"{"ref":"refs/heads/main"}"#;
        let none = std::collections::HashMap::new();
        let secrets: std::collections::HashMap<String, String> = [
            ("webhook.github.secret", "gh"),
            ("webhook.gogs.secret", "gt"),
            ("webhook.gitlab.secret", "gl"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let github = |sig: &str| h(&[("X-GitHub-Event", "push"), ("X-Hub-Signature-256", sig)]);
        // No secret: as Argo CD, any push is taken.
        assert!(verify(&github("sha256=00"), body, &none).is_ok());
        assert!(verify(&h(&[]), body, &none).is_ok());
        // GitHub
        assert!(
            verify(
                &github(&format!("sha256={}", sign("gh", body))),
                body,
                &secrets
            )
            .is_ok()
        );
        assert!(
            verify(
                &github(&format!("sha256={}", sign("other", body))),
                body,
                &secrets
            )
            .is_err()
        );
        assert!(verify(&h(&[("X-GitHub-Event", "push")]), body, &secrets).is_err());
        assert!(
            verify(
                &github(&format!("sha256={}", sign("gh", b"tampered"))),
                body,
                &secrets
            )
            .is_err()
        );
        // Gitea (and Gogs)
        assert!(
            verify(
                &h(&[
                    ("X-Gitea-Event", "push"),
                    ("X-Gitea-Signature", &sign("gt", body))
                ]),
                body,
                &secrets
            )
            .is_ok()
        );
        assert!(
            verify(
                &h(&[
                    ("X-Gogs-Event", "push"),
                    ("X-Gogs-Signature", &sign("gt", body))
                ]),
                body,
                &secrets
            )
            .is_ok()
        );
        assert!(
            verify(
                &h(&[("X-Gitea-Event", "push"), ("X-Gitea-Signature", "zz")]),
                body,
                &secrets
            )
            .is_err()
        );
        // GitLab
        assert!(
            verify(
                &h(&[("X-Gitlab-Event", "Push Hook"), ("X-Gitlab-Token", "gl")]),
                body,
                &secrets
            )
            .is_ok()
        );
        assert!(
            verify(
                &h(&[("X-Gitlab-Event", "Push Hook"), ("X-Gitlab-Token", "nope")]),
                body,
                &secrets
            )
            .is_err()
        );
        // No event header once a secret is set
        assert!(verify(&h(&[]), body, &secrets).is_err());
    }
}
