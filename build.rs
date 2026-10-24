//! Embeds Argo CD's bundled Lua health checks (vendor/argo-cd, Apache-2.0):
//! `$OUT_DIR/bundled_health.rs` holds (group, kind, script) for every
//! resource_customizations/<group>/<Kind>/health.lua; `_` in a directory
//! name is Argo CD's wildcard (`_.crossplane.io/_` = `*.crossplane.io/*`).

use std::fmt::Write as _;
use std::path::Path;

fn main() {
    let root = Path::new("vendor/argo-cd/resource_customizations");
    println!("cargo:rerun-if-changed={}", root.display());
    let mut entries = Vec::new();
    if let Ok(groups) = std::fs::read_dir(root) {
        for g in groups.flatten() {
            let Ok(kinds) = std::fs::read_dir(g.path()) else {
                continue;
            };
            for k in kinds.flatten() {
                let script = k.path().join("health.lua");
                if script.is_file() {
                    let group = g.file_name().to_string_lossy().replace('_', "*");
                    let kind = k.file_name().to_string_lossy().replace('_', "*");
                    entries.push((group, kind, std::fs::canonicalize(&script).unwrap()));
                }
            }
        }
    }
    entries.sort();
    let mut out = String::from("pub static BUNDLED: &[(&str, &str, &str)] = &[\n");
    for (g, k, p) in &entries {
        writeln!(
            out,
            "    ({g:?}, {k:?}, include_str!({:?})),",
            p.display().to_string()
        )
        .unwrap();
    }
    out.push_str("];\n");
    let dest = Path::new(&std::env::var("OUT_DIR").unwrap()).join("bundled_health.rs");
    std::fs::write(dest, out).unwrap();
}
