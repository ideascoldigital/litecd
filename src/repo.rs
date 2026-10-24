//! Sources: git repositories (the `git` CLI, as Argo CD's repo server uses)
//! and Helm charts (`helm pull`), cached under the cache directory.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tokio::sync::Mutex;

/// Credentials for a repository, from a `repository` or `repo-creds` Secret.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Creds {
    pub username: String,
    pub password: String,
}

pub struct Repos {
    dir: PathBuf,
    /// One lock per repository: git does not like two fetches at once.
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Resolved revisions: (url, targetRevision) → (sha, when).
    resolved: Mutex<HashMap<(String, String), (String, Instant)>>,
}

fn hash(s: &str) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

pub fn is_sha(rev: &str) -> bool {
    rev.len() == 40 && rev.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(crate) async fn run_command(cmd: &mut Command, what: &str) -> Result<Vec<u8>> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = tokio::time::timeout(Duration::from_secs(300), cmd.output())
        .await
        .with_context(|| format!("{what}: timed out"))?
        .with_context(|| format!("{what}: starting"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        bail!("{what}: {}", err.trim());
    }
    Ok(out.stdout)
}

impl Repos {
    pub fn new(dir: PathBuf) -> Self {
        Repos {
            dir,
            locks: Mutex::default(),
            resolved: Mutex::default(),
        }
    }

    async fn lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.locks
            .lock()
            .await
            .entry(key.to_string())
            .or_default()
            .clone()
    }

    fn git(&self, creds: Option<&Creds>) -> Command {
        let mut c = Command::new("git");
        c.env("GIT_TERMINAL_PROMPT", "0").env("GIT_ASKPASS", "true");
        if let Some(cr) = creds.filter(|c| !c.username.is_empty() || !c.password.is_empty()) {
            use base64::Engine;
            let token = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", cr.username, cr.password));
            c.arg("-c")
                .arg(format!("http.extraHeader=Authorization: Basic {token}"));
        }
        c
    }

    /// The commit `rev` (branch, tag, HEAD or a SHA) points at. Answers from
    /// a cache younger than `max_age`, unless `fresh`.
    pub async fn resolve(
        &self,
        url: &str,
        rev: &str,
        creds: Option<&Creds>,
        max_age: Duration,
        fresh: bool,
    ) -> Result<String> {
        if is_sha(rev) {
            return Ok(rev.to_lowercase());
        }
        let key = (url.to_string(), rev.to_string());
        if !fresh
            && let Some((sha, at)) = self.resolved.lock().await.get(&key)
            && at.elapsed() < max_age
        {
            return Ok(sha.clone());
        }
        let out = run_command(
            self.git(creds).args(["ls-remote", url]),
            &format!("git ls-remote {url}"),
        )
        .await?;
        let refs = String::from_utf8_lossy(&out);
        let want = if rev.is_empty() || rev == "HEAD" {
            "HEAD".to_string()
        } else {
            rev.to_string()
        };
        let mut found = None;
        for line in refs.lines() {
            let Some((sha, name)) = line.split_once('\t') else {
                continue;
            };
            let name = name.trim_end_matches("^{}");
            let hit = name == want
                || name == format!("refs/heads/{want}")
                || name == format!("refs/tags/{want}")
                || name == format!("refs/{want}");
            // A peeled tag (^{}) wins: it is the commit.
            if hit && (found.is_none() || line.ends_with("^{}")) {
                found = Some(sha.to_string());
            }
        }
        let Some(sha) = found else {
            bail!("unable to resolve '{rev}' to a commit SHA");
        };
        self.resolved
            .lock()
            .await
            .insert(key, (sha.clone(), Instant::now()));
        Ok(sha)
    }

    /// A directory holding the repository's tree at `sha`.
    pub async fn checkout(&self, url: &str, sha: &str, creds: Option<&Creds>) -> Result<PathBuf> {
        let id = hash(url);
        let tree = self.dir.join("trees").join(format!("{id}-{sha}"));
        if tree.join(".litecd-complete").exists() {
            return Ok(tree);
        }
        let lock = self.lock(url).await;
        let _g = lock.lock().await;
        if tree.join(".litecd-complete").exists() {
            return Ok(tree);
        }
        let bare = self.dir.join("git").join(&id);
        if !bare.join("HEAD").exists() {
            tokio::fs::create_dir_all(&bare).await?;
            run_command(
                Command::new("git")
                    .arg("init")
                    .arg("-q")
                    .arg("--bare")
                    .arg(&bare),
                "git init",
            )
            .await?;
        }
        let have = run_command(
            Command::new("git").arg("-C").arg(&bare).args([
                "cat-file",
                "-e",
                &format!("{sha}^{{commit}}"),
            ]),
            "git cat-file",
        )
        .await
        .is_ok();
        if !have {
            run_command(
                self.git(creds).arg("-C").arg(&bare).args([
                    "fetch",
                    "-q",
                    "--force",
                    "--prune",
                    url,
                    "+refs/heads/*:refs/heads/*",
                    "+refs/tags/*:refs/tags/*",
                ]),
                &format!("git fetch {url}"),
            )
            .await?;
            if run_command(
                Command::new("git").arg("-C").arg(&bare).args([
                    "cat-file",
                    "-e",
                    &format!("{sha}^{{commit}}"),
                ]),
                "git cat-file",
            )
            .await
            .is_err()
            {
                // Not on a branch or tag: ask for the commit itself.
                run_command(
                    self.git(creds)
                        .arg("-C")
                        .arg(&bare)
                        .args(["fetch", "-q", url, sha]),
                    &format!("git fetch {url} {sha}"),
                )
                .await?;
            }
        }
        let archive = run_command(
            Command::new("git")
                .arg("-C")
                .arg(&bare)
                .args(["archive", "--format=tar", sha]),
            "git archive",
        )
        .await?;
        let tmp = tree.with_extension("tmp");
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        tokio::fs::create_dir_all(&tmp).await?;
        let tmp2 = tmp.clone();
        tokio::task::spawn_blocking(move || tar::Archive::new(&archive[..]).unpack(&tmp2))
            .await??;
        tokio::fs::write(tmp.join(".litecd-complete"), b"").await?;
        let _ = tokio::fs::remove_dir_all(&tree).await;
        tokio::fs::rename(&tmp, &tree).await?;
        self.prune_trees(&id, sha).await;
        Ok(tree)
    }

    /// Keep the 3 newest trees of a repository.
    async fn prune_trees(&self, id: &str, keep: &str) {
        let Ok(mut rd) = tokio::fs::read_dir(self.dir.join("trees")).await else {
            return;
        };
        let mut trees = Vec::new();
        while let Ok(Some(e)) = rd.next_entry().await {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(&format!("{id}-"))
                && !name.ends_with(keep)
                && !name.ends_with(".tmp")
            {
                let at = e.metadata().await.and_then(|m| m.modified()).ok();
                trees.push((at, e.path()));
            }
        }
        trees.sort();
        while trees.len() > 2 {
            let (_, p) = trees.remove(0);
            let _ = tokio::fs::remove_dir_all(p).await;
        }
    }

    /// A Helm chart from a Helm repository (or an OCI registry), unpacked:
    /// the chart's directory.
    pub async fn chart(
        &self,
        repo: &str,
        chart: &str,
        version: &str,
        creds: Option<&Creds>,
    ) -> Result<PathBuf> {
        let key = format!("{repo}|{chart}|{version}");
        let dir = self.dir.join("charts").join(hash(&key));
        let chart_dir = dir.join(chart.rsplit('/').next().unwrap_or(chart));
        if dir.join(".litecd-complete").exists() {
            return Ok(chart_dir);
        }
        let lock = self.lock(&key).await;
        let _g = lock.lock().await;
        if dir.join(".litecd-complete").exists() {
            return Ok(chart_dir);
        }
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await?;
        let mut cmd = helm(&self.dir);
        if repo.starts_with("oci://") || !repo.contains("://") {
            let r = repo.trim_start_matches("oci://").trim_end_matches('/');
            cmd.arg("pull").arg(format!("oci://{r}/{chart}"));
        } else {
            cmd.args(["pull", chart, "--repo", repo]);
        }
        if !version.is_empty() && version != "*" {
            cmd.args(["--version", version]);
        }
        if let Some(c) = creds.filter(|c| !c.username.is_empty()) {
            cmd.args([
                "--username",
                &c.username,
                "--password",
                &c.password,
                "--pass-credentials",
            ]);
        }
        cmd.arg("--untar").arg("--untardir").arg(&dir);
        run_command(&mut cmd, &format!("helm pull {chart} from {repo}")).await?;
        tokio::fs::write(dir.join(".litecd-complete"), b"").await?;
        Ok(chart_dir)
    }

    pub fn cache_dir(&self) -> &Path {
        &self.dir
    }
}

/// `helm` with its caches and config under litecd's cache directory.
pub fn helm(cache: &Path) -> Command {
    let mut c = Command::new("helm");
    let h = cache.join("helm");
    c.env("HELM_CACHE_HOME", h.join("cache"))
        .env("HELM_CONFIG_HOME", h.join("config"))
        .env("HELM_DATA_HOME", h.join("data"));
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha_detection() {
        assert!(is_sha("54227dfefaca27d979cb41eaa7cb93210b8ee578"));
        assert!(!is_sha("main"));
        assert_ne!(hash("a"), hash("b"));
    }

    /// A local repository resolves branches and checks out trees.
    #[tokio::test]
    async fn resolve_and_checkout_local_repository() {
        let tmp = std::env::temp_dir().join(format!("litecd-repo-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let src = tmp.join("src");
        std::fs::create_dir_all(src.join("m")).unwrap();
        std::fs::write(src.join("m/cm.yaml"), "kind: ConfigMap\n").unwrap();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .arg("-C")
                .arg(&src)
                .args([
                    "-c",
                    "user.name=t",
                    "-c",
                    "user.email=t@t",
                    "-c",
                    "commit.gpgsign=false",
                ])
                .args(args)
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?}");
        };
        git(&["init", "-q", "-b", "main"]);
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "one"]);
        let url = format!("file://{}", src.display());
        let repos = Repos::new(tmp.join("cache"));
        let sha = repos
            .resolve(&url, "main", None, Duration::from_secs(60), true)
            .await
            .unwrap();
        assert!(is_sha(&sha));
        assert_eq!(
            repos
                .resolve(&url, "HEAD", None, Duration::from_secs(60), true)
                .await
                .unwrap(),
            sha
        );
        let tree = repos.checkout(&url, &sha, None).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(tree.join("m/cm.yaml")).unwrap(),
            "kind: ConfigMap\n"
        );
        assert!(
            repos
                .resolve(&url, "nope", None, Duration::from_secs(60), true)
                .await
                .is_err()
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
