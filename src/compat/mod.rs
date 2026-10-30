//! Deprecated compatibility modes, each behind its own Cargo feature and
//! off by default. Nothing else in litecd depends on them.

#[cfg(feature = "argocd-ui-compat")]
pub mod argocd_ui;
#[cfg(feature = "argocd-ui-compat")]
mod resp;
