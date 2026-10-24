//! `/api/webhook`, where argocd-server takes pushes from Git providers:
//! the apps reading the pushed repository get a refresh, as Argo CD does it
//! (the `argocd.argoproj.io/refresh` annotation). GitHub, Gitea, Gogs and
//! GitLab payloads; `/healthz` for probes.

use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::app::{Ctx, REFRESH};

const MAX_BODY: usize = 10 << 20;

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
    for app in ctx.kube.list(&kind, Some(&ctx.namespace), None).await? {
        let hit = sources(&app).iter().any(|s| {
            s["repoURL"]
                .as_str()
                .is_some_and(|u| urls.contains(&normalize(u)))
        });
        if !hit {
            continue;
        }
        let name = app["metadata"]["name"].as_str().unwrap_or_default();
        let patch = json!({"metadata": {"annotations": {REFRESH: "normal"}}});
        if let Err(e) = ctx
            .kube
            .merge_patch(&kind, Some(&ctx.namespace), name, &patch)
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
    let len = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
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
        ("POST", "/api/webhook") => match serde_json::from_slice::<Value>(body) {
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
}
