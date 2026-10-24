//! ignoreDifferences: fields left out of the comparison, from the app
//! (`spec.ignoreDifferences`) and argocd-cm
//! (`resource.customizations.ignoreDifferences.<group_Kind>` and `.all`).
//!
//! Each rule names fields three ways: JSON pointers, jq path expressions (a
//! subset: `.a.b`, `.a["b"]`, `.a[0]`, `.a[]`, `select(.k == "v")`, joined
//! with `|`) and field managers whose fields do not count. They become
//! concrete paths in the live object, removed from both sides before the
//! diff and, with RespectIgnoreDifferences=true, taken from the live object
//! when applying.

use serde_json::{Map, Value};

use crate::app::Key;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rule {
    pub group: String,
    pub kind: String,
    pub name: String,
    pub namespace: String,
    pub pointers: Vec<String>,
    pub jq: Vec<String>,
    pub managers: Vec<String>,
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

impl Rule {
    fn fields(v: &Value) -> (Vec<String>, Vec<String>, Vec<String>) {
        (
            strings(&v["jsonPointers"]),
            strings(&v["jqPathExpressions"]),
            strings(&v["managedFieldsManagers"]),
        )
    }

    pub fn from_spec(v: &Value) -> Rule {
        let (pointers, jq, managers) = Rule::fields(v);
        Rule {
            group: v["group"].as_str().unwrap_or_default().to_string(),
            kind: v["kind"].as_str().unwrap_or_default().to_string(),
            name: v["name"].as_str().unwrap_or_default().to_string(),
            namespace: v["namespace"].as_str().unwrap_or_default().to_string(),
            pointers,
            jq,
            managers,
        }
    }

    fn matches(&self, k: &Key) -> bool {
        let m = |want: &str, have: &str| want.is_empty() || want == "*" || want == have;
        m(&self.group, &k.group)
            && m(&self.kind, &k.kind)
            && m(&self.name, &k.name)
            && m(&self.namespace, &k.namespace)
    }
}

/// The rules argocd-cm gives: per kind and for every kind (`all`).
pub fn from_config(data: &Map<String, Value>) -> Vec<Rule> {
    let mut out = Vec::new();
    for (k, v) in data {
        let Some(gk) = k.strip_prefix("resource.customizations.ignoreDifferences.") else {
            continue;
        };
        let Some(text) = v.as_str() else { continue };
        let Ok(spec) = serde_yaml::from_str::<Value>(text) else {
            continue;
        };
        let (pointers, jq, managers) = Rule::fields(&spec);
        let (group, kind) = if gk == "all" {
            ("*".to_string(), "*".to_string())
        } else {
            let (g, k) = gk.rsplit_once('_').unwrap_or(("", gk));
            (g.to_string(), k.to_string())
        };
        out.push(Rule {
            group,
            kind,
            pointers,
            jq,
            managers,
            ..Rule::default()
        });
    }
    if let Some(text) = data.get("resource.customizations").and_then(Value::as_str)
        && let Ok(Value::Object(m)) = serde_yaml::from_str::<Value>(text)
    {
        for (gk, c) in m {
            if !c["ignoreDifferences"].is_string() {
                continue;
            }
            let Ok(spec) =
                serde_yaml::from_str::<Value>(c["ignoreDifferences"].as_str().unwrap_or_default())
            else {
                continue;
            };
            let (pointers, jq, managers) = Rule::fields(&spec);
            let (group, kind) = gk
                .split_once('/')
                .map(|(g, k)| (g.to_string(), k.to_string()))
                .unwrap_or((String::new(), gk.clone()));
            out.push(Rule {
                group,
                kind,
                pointers,
                jq,
                managers,
                ..Rule::default()
            });
        }
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Seg {
    Key(String),
    Index(usize),
}

pub type Path = Vec<Seg>;

fn get<'a>(v: &'a Value, p: &[Seg]) -> Option<&'a Value> {
    let mut cur = v;
    for s in p {
        cur = match (s, cur) {
            (Seg::Key(k), Value::Object(m)) => m.get(k)?,
            (Seg::Index(i), Value::Array(a)) => a.get(*i)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn remove(v: &mut Value, p: &[Seg]) {
    let Some((last, parent)) = p.split_last() else {
        return;
    };
    let mut cur = v;
    for s in parent {
        cur = match (s, cur) {
            (Seg::Key(k), Value::Object(m)) => match m.get_mut(k) {
                Some(x) => x,
                None => return,
            },
            (Seg::Index(i), Value::Array(a)) => match a.get_mut(*i) {
                Some(x) => x,
                None => return,
            },
            _ => return,
        };
    }
    match (last, cur) {
        (Seg::Key(k), Value::Object(m)) => {
            m.remove(k);
        }
        // Keep indexes stable for the other paths: blank the element.
        (Seg::Index(i), Value::Array(a)) if *i < a.len() => a[*i] = Value::Null,
        _ => {}
    }
}

fn set(v: &mut Value, p: &[Seg], value: Value) {
    let Some((last, parent)) = p.split_last() else {
        return;
    };
    let mut cur = v;
    for s in parent {
        cur = match s {
            Seg::Key(k) => {
                if !cur.is_object() {
                    *cur = Value::Object(Map::new());
                }
                cur.as_object_mut()
                    .unwrap()
                    .entry(k.clone())
                    .or_insert(Value::Object(Map::new()))
            }
            Seg::Index(i) => match cur.as_array_mut().and_then(|a| a.get_mut(*i)) {
                Some(x) => x,
                None => return,
            },
        };
    }
    match last {
        Seg::Key(k) => {
            if let Some(m) = cur.as_object_mut() {
                m.insert(k.clone(), value);
            }
        }
        Seg::Index(i) => {
            if let Some(x) = cur.as_array_mut().and_then(|a| a.get_mut(*i)) {
                *x = value;
            }
        }
    }
}

/// RFC 6901: "/spec/replicas", "~1" for "/", "~0" for "~".
pub fn pointer(p: &str, against: &Value) -> Option<Path> {
    let p = p.strip_prefix('/')?;
    let mut out = Vec::new();
    let mut cur = Some(against);
    for raw in p.split('/') {
        let s = raw.replace("~1", "/").replace("~0", "~");
        let seg = match (cur, s.parse::<usize>()) {
            (Some(Value::Array(_)), Ok(i)) => Seg::Index(i),
            _ => Seg::Key(s),
        };
        cur = cur.and_then(|c| get(c, std::slice::from_ref(&seg)));
        out.push(seg);
    }
    Some(out)
}

#[derive(Debug, PartialEq)]
enum Step {
    Key(String),
    Index(usize),
    Each,
    Select(Vec<Seg>, Value),
}

/// Parse the jq subset into steps; None when it is outside the subset.
fn parse_jq(expr: &str) -> Option<Vec<Step>> {
    let mut steps = Vec::new();
    for part in expr.split('|').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some(inner) = part
            .strip_prefix("select(")
            .and_then(|p| p.strip_suffix(')'))
        {
            let (lhs, rhs) = inner.split_once("==")?;
            let path = parse_path(lhs.trim())?
                .into_iter()
                .map(|s| match s {
                    Step::Key(k) => Some(Seg::Key(k)),
                    Step::Index(i) => Some(Seg::Index(i)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let value: Value = serde_json::from_str(rhs.trim()).ok()?;
            steps.push(Step::Select(path, value));
        } else {
            steps.extend(parse_path(part)?);
        }
    }
    Some(steps)
}

fn parse_path(p: &str) -> Option<Vec<Step>> {
    let mut out = Vec::new();
    let b = p.as_bytes();
    let mut i = 0;
    if p == "." {
        return Some(out);
    }
    while i < b.len() {
        match b[i] {
            b'.' => {
                i += 1;
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'-')
                {
                    i += 1;
                }
                if i > start {
                    out.push(Step::Key(p[start..i].to_string()));
                }
            }
            b'[' => {
                let end = p[i..].find(']')? + i;
                let inner = p[i + 1..end].trim();
                if inner.is_empty() {
                    out.push(Step::Each);
                } else if let Ok(n) = inner.parse::<usize>() {
                    out.push(Step::Index(n));
                } else {
                    let key: String = serde_json::from_str(inner).ok()?;
                    out.push(Step::Key(key));
                }
                i = end + 1;
                if i < b.len() && b[i] == b'?' {
                    i += 1;
                }
            }
            _ => return None,
        }
    }
    Some(out)
}

/// The concrete paths a jq expression points at in `v`.
pub fn jq_paths(expr: &str, v: &Value) -> Option<Vec<Path>> {
    let steps = parse_jq(expr)?;
    let mut paths: Vec<Path> = vec![vec![]];
    for step in &steps {
        let mut next = Vec::new();
        for p in paths {
            let Some(cur) = get(v, &p) else { continue };
            match step {
                Step::Key(k) => {
                    if cur.get(k).is_some() {
                        next.push([p.clone(), vec![Seg::Key(k.clone())]].concat());
                    }
                }
                Step::Index(i) => {
                    if cur.as_array().is_some_and(|a| *i < a.len()) {
                        next.push([p.clone(), vec![Seg::Index(*i)]].concat());
                    }
                }
                Step::Each => match cur {
                    Value::Array(a) => {
                        next.extend((0..a.len()).map(|i| [p.clone(), vec![Seg::Index(i)]].concat()))
                    }
                    Value::Object(m) => next.extend(
                        m.keys()
                            .map(|k| [p.clone(), vec![Seg::Key(k.clone())]].concat()),
                    ),
                    _ => {}
                },
                Step::Select(sub, want) => {
                    if get(cur, sub) == Some(want) {
                        next.push(p);
                    }
                }
            }
        }
        paths = next;
    }
    Some(paths.into_iter().filter(|p| !p.is_empty()).collect())
}

/// The paths a manager owns in `live`, from its managedFields (fieldsV1).
pub fn manager_paths(live: &Value, managers: &[String]) -> Vec<Path> {
    let mut out = Vec::new();
    for mf in live["metadata"]["managedFields"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let m = mf["manager"].as_str().unwrap_or_default();
        if !managers.iter().any(|x| x == m) {
            continue;
        }
        walk_fields(&mf["fieldsV1"], live, &mut vec![], &mut out);
    }
    out
}

fn walk_fields(fields: &Value, live: &Value, at: &mut Path, out: &mut Vec<Path>) {
    let Some(m) = fields.as_object() else { return };
    for (k, sub) in m {
        if k == "." {
            continue;
        }
        let Some(node) = get(live, at) else { return };
        let seg = if let Some(name) = k.strip_prefix("f:") {
            Some(Seg::Key(name.to_string()))
        } else if let Some(sel) = k.strip_prefix("k:") {
            let want: Value = serde_json::from_str(sel).unwrap_or(Value::Null);
            node.as_array()
                .and_then(|a| {
                    a.iter().position(|item| {
                        want.as_object()
                            .is_some_and(|w| w.iter().all(|(wk, wv)| item.get(wk) == Some(wv)))
                    })
                })
                .map(Seg::Index)
        } else if let Some(val) = k.strip_prefix("v:") {
            let want: Value = serde_json::from_str(val).unwrap_or(Value::Null);
            node.as_array()
                .and_then(|a| a.iter().position(|item| *item == want))
                .map(Seg::Index)
        } else if let Some(i) = k.strip_prefix("i:") {
            i.parse().ok().map(Seg::Index)
        } else {
            None
        };
        let Some(seg) = seg else { continue };
        at.push(seg);
        let leaf = sub.as_object().is_none_or(|s| s.keys().all(|x| x == "."));
        if leaf {
            if get(live, at).is_some() {
                out.push(at.clone());
            }
        } else {
            walk_fields(sub, live, at, out);
        }
        at.pop();
    }
}

/// The fields to ignore for one resource, as paths in `live` (or, without
/// a live object, in `desired`).
pub fn ignored(rules: &[Rule], k: &Key, desired: &Value, live: Option<&Value>) -> Vec<Path> {
    let against = live.unwrap_or(desired);
    let mut out = Vec::new();
    for r in rules.iter().filter(|r| r.matches(k)) {
        for p in &r.pointers {
            if let Some(path) = pointer(p, against) {
                out.push(path);
            }
        }
        for e in &r.jq {
            match jq_paths(e, against) {
                Some(ps) => out.extend(ps),
                None => tracing::warn!(
                    "ignoreDifferences: jq expression outside what litecd supports: {e}"
                ),
            }
            if live.is_some()
                && let Some(ps) = jq_paths(e, desired)
            {
                out.extend(ps);
            }
        }
        if let Some(l) = live {
            out.extend(manager_paths(l, &r.managers));
        }
    }
    // Longest first: removing a parent before its child would skip the child.
    out.sort_by(|a, b| b.len().cmp(&a.len()).then(b.cmp(a)));
    out.dedup();
    out
}

/// `v` without the ignored paths.
pub fn without(v: &Value, paths: &[Path]) -> Value {
    let mut v = v.clone();
    for p in paths {
        remove(&mut v, p);
    }
    v
}

/// RespectIgnoreDifferences: the desired object with the live values at the
/// ignored paths, so an apply leaves them as they are.
pub fn respect(desired: &Value, live: &Value, paths: &[Path]) -> Value {
    let mut d = desired.clone();
    for p in paths {
        match get(live, p) {
            Some(v) => set(&mut d, p, v.clone()),
            None => remove(&mut d, p),
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key() -> Key {
        Key {
            group: "apps".into(),
            kind: "Deployment".into(),
            namespace: "n".into(),
            name: "web".into(),
        }
    }

    fn deploy(replicas: i64, image: &str) -> Value {
        json!({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web", "annotations": {"a/b": "x"}},
            "spec": {"replicas": replicas, "template": {"spec": {"containers": [{"name": "m", "image": image}, {"name": "side", "image": "s"}]}}}})
    }

    #[test]
    fn pointers_and_respect() {
        let rules = vec![Rule {
            group: "apps".into(),
            kind: "Deployment".into(),
            pointers: vec!["/spec/replicas".into(), "/metadata/annotations/a~1b".into()],
            ..Rule::default()
        }];
        let live = deploy(3, "x");
        let paths = ignored(&rules, &key(), &deploy(1, "x"), Some(&live));
        assert_eq!(without(&deploy(1, "x"), &paths), without(&live, &paths));
        let applied = respect(&deploy(1, "x"), &live, &paths);
        assert_eq!(applied["spec"]["replicas"], 3);
        let other = Key {
            kind: "StatefulSet".into(),
            ..key()
        };
        assert!(ignored(&rules, &other, &deploy(1, "x"), Some(&live)).is_empty());
    }

    #[test]
    fn jq_subset() {
        let v = deploy(1, "x");
        let p = jq_paths(
            r#".spec.template.spec.containers[] | select(.name == "m") | .image"#,
            &v,
        )
        .unwrap();
        assert_eq!(
            p,
            vec![vec![
                Seg::Key("spec".into()),
                Seg::Key("template".into()),
                Seg::Key("spec".into()),
                Seg::Key("containers".into()),
                Seg::Index(0),
                Seg::Key("image".into())
            ]]
        );
        assert_eq!(
            jq_paths(r#".metadata.annotations["a/b"]"#, &v)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            jq_paths(".spec.template.spec.containers[1].image", &v)
                .unwrap()
                .len(),
            1
        );
        assert!(jq_paths(".spec | to_entries", &v).is_none());
    }

    #[test]
    fn managed_fields() {
        let mut live = deploy(3, "x");
        live["metadata"]["managedFields"] = json!([
            {"manager": "kube-controller-manager", "fieldsV1": {"f:spec": {"f:replicas": {}}}},
            {"manager": "kubectl", "fieldsV1": {"f:spec": {"f:template": {"f:spec": {"f:containers": {"k:{\"name\":\"side\"}": {".": {}, "f:image": {}}}}}}}}
        ]);
        let p = manager_paths(&live, &["kube-controller-manager".into()]);
        assert_eq!(
            p,
            vec![vec![Seg::Key("spec".into()), Seg::Key("replicas".into())]]
        );
        let p = manager_paths(&live, &["kubectl".into()]);
        assert_eq!(p[0].last(), Some(&Seg::Key("image".into())));
        assert_eq!(p[0][4], Seg::Index(1));
    }

    #[test]
    fn config_rules() {
        let data = json!({"resource.customizations.ignoreDifferences.apps_Deployment": "jsonPointers:\n- /spec/replicas\n",
                          "resource.customizations.ignoreDifferences.all": "managedFieldsManagers:\n- kube-controller-manager\n"});
        let rules = from_config(data.as_object().unwrap());
        assert_eq!(rules.len(), 2);
        assert!(
            rules
                .iter()
                .any(|r| r.kind == "Deployment" && r.pointers == ["/spec/replicas"])
        );
        assert!(rules.iter().any(|r| r.kind == "*" && r.matches(&key())));
    }
}
