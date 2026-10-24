//! litecd: GitOps continuous delivery that reads Argo CD's manifests
//! unchanged. See docs/spec.md for the plan.

mod app;
mod check;
mod controller;
mod diff;
mod health;
mod ignore;
mod kubeapi;
mod render;
mod repo;
mod sync;
mod webhook;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "litecd",
    version,
    about = "GitOps continuous delivery for Argo CD manifests, in one small binary"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Report which features your Argo CD manifests use and which litecd phase supports them
    Check {
        /// Files or directories with Application, AppProject and ApplicationSet manifests
        #[arg(short = 'f', long = "filename", required = true)]
        paths: Vec<PathBuf>,
        #[arg(short, long, value_enum, default_value_t = Output::Text)]
        output: Output,
    },
    /// Run the controller: reconcile the Applications of a namespace
    Controller {
        /// The namespace of the Applications, AppProjects, argocd-cm and repository Secrets
        #[arg(long, env = "ARGOCD_NAMESPACE", default_value = "argocd")]
        namespace: String,
        /// Where repositories, charts and rendered trees are cached
        #[arg(long, env = "LITECD_CACHE_DIR", default_value = "/tmp/litecd")]
        cache_dir: PathBuf,
        /// Where Git providers send pushes (`/api/webhook`, as argocd-server takes them); empty: none
        #[arg(long, env = "LITECD_LISTEN", default_value = "0.0.0.0:8080")]
        listen: String,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Output {
    Text,
    Json,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Controller {
            namespace,
            cache_dir,
            listen,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "litecd=info".into()),
                )
                .init();
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            rt.block_on(async move {
                let kube = kubeapi::Kube::new().await?;
                let cm = match kube.known("v1", "ConfigMap").await {
                    Some(k) => kube
                        .get(&k, Some(&namespace), "argocd-cm")
                        .await
                        .ok()
                        .flatten(),
                    None => None,
                };
                let settings = app::Settings::from_config(cm.as_ref());
                let repos = repo::Repos::new(cache_dir);
                let ctx = std::sync::Arc::new(app::Ctx::new(kube, repos, namespace, settings));
                if !listen.is_empty() {
                    let c = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = webhook::serve(c, &listen).await {
                            tracing::error!("webhooks: {e:#}");
                        }
                    });
                }
                // Exit at once on SIGTERM: an old pod that kept reconciling
                // during a rollout would fight the new one.
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::select! {
                    r = controller::Controller::new(ctx).run() => r,
                    _ = term.recv() => Ok(()),
                    _ = tokio::signal::ctrl_c() => Ok(()),
                }
            })?;
        }
        Command::Check { paths, output } => {
            let objects = check::scan(&paths)?;
            match output {
                Output::Text => print!("{}", check::report(&objects)),
                Output::Json => println!("{}", serde_json::to_string_pretty(&objects)?),
            }
        }
    }
    Ok(())
}
