//! litecd: GitOps continuous delivery that reads Argo CD's manifests
//! unchanged. See docs/spec.md for the plan.

mod app;
mod appset;
mod check;
mod coexist;
mod compat;
mod controller;
mod diff;
mod gotemplate;
mod health;
mod ignore;
mod kubeapi;
mod render;
mod repo;
mod sync;
mod webhook;
mod windows;

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
        /// Namespaces besides --namespace whose Applications are reconciled
        /// (globs, `/regex/`), when argocd-cmd-params-cm has no
        /// `application.namespaces`
        #[arg(long, env = "ARGOCD_APPLICATION_NAMESPACES", default_value = "")]
        application_namespaces: String,
        /// Where Git providers send pushes (`/api/webhook`, as argocd-server takes them); empty: none
        #[arg(long, env = "LITECD_LISTEN", default_value = "0.0.0.0:8080")]
        listen: String,
        /// Compare every Application and log where litecd's status differs
        /// from the stored one, writing nothing: no status, sync, prune,
        /// finalizer, ApplicationSet or webhook refresh (safe beside a running Argo CD).
        /// Without it, litecd still stands down while Argo CD's application
        /// controller runs, except for Applications annotated
        /// `litecd.dev/manage: "true"`
        #[arg(long, env = "LITECD_OBSERVE")]
        observe: bool,
        /// DEPRECATED compatibility mode (feature argocd-ui-compat)
        #[cfg(feature = "argocd-ui-compat")]
        #[command(flatten)]
        argocd_ui: compat::argocd_ui::Flags,
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
            application_namespaces,
            observe,
            #[cfg(feature = "argocd-ui-compat")]
            argocd_ui,
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
                let mut ctx = app::Ctx::new(kube, repos, namespace, settings);
                ctx.observe = observe;
                let ctx = std::sync::Arc::new(ctx);
                if observe {
                    tracing::info!("observe mode: comparing only, nothing is written");
                }
                // DEPRECATED: resource trees for Argo CD's web UI (docs/spec.md,
                // Deprecated compatibility); a write, so not in observe mode.
                #[cfg(feature = "argocd-ui-compat")]
                if !observe {
                    compat::argocd_ui::start(ctx.clone(), &argocd_ui)?;
                }
                #[cfg(not(feature = "argocd-ui-compat"))]
                if std::env::var("LITECD_ARGOCD_UI_CACHE").is_ok_and(|v| !v.is_empty()) {
                    tracing::warn!(
                        "LITECD_ARGOCD_UI_CACHE is ignored: this build has no argocd-ui-compat \
                         feature (the image tagged -argocd-ui has it)"
                    );
                }
                *ctx.app_namespaces.write().await = app::split_list(&application_namespaces);
                // Nothing is written while Argo CD's controllers run
                // (docs/spec.md, Coexistence).
                if !observe {
                    tokio::spawn(coexist::run(ctx.clone()));
                }
                // A push refreshes apps through an annotation: a write.
                if !listen.is_empty() && !observe {
                    let c = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = webhook::serve(c, &listen).await {
                            tracing::error!("webhooks: {e:#}");
                        }
                    });
                }
                if !observe {
                    let c = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = appset::run(c).await {
                            tracing::error!("applicationsets: {e:#}");
                        }
                    });
                }
                // Exit at once on SIGTERM: an old pod that kept reconciling
                // during a rollout would fight the new one.
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::select! {
                    r = controller::Controller::new(ctx, application_namespaces).run() => r,
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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Cli;

    /// The deprecated Argo CD UI mode exists only in builds with its feature.
    #[test]
    fn argocd_ui_cache_only_with_its_feature() {
        let r = Cli::try_parse_from([
            "litecd",
            "controller",
            "--argocd-ui-cache",
            "redis://r:6379",
        ]);
        assert_eq!(r.is_ok(), cfg!(feature = "argocd-ui-compat"));
    }
}
