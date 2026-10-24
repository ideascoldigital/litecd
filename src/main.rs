//! litecd: GitOps continuous delivery that reads Argo CD's manifests
//! unchanged. See docs/spec.md for the plan.

mod app;
mod check;
mod controller;
mod diff;
mod health;
mod kubeapi;
mod render;
mod repo;
mod sync;

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
                controller::Controller::new(ctx).run().await
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
