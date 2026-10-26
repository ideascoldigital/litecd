//! Comparing what git wants with what the cluster has.
//!
//! Like kubectl's three-way apply: the live object is in sync when every
//! field of the desired object has the same value live (the API server's
//! defaults and other managers' fields do not count), and no field the last
//! apply set (`last-applied-configuration`) has been dropped from git but is
//! still there. When values differ only in form (a quantity, an int written
//! as a string), the controller asks the API server with a server-side apply
//! dry run.

use serde_json::Value;

pub const LAST_APPLIED: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// Whether every field of `desired` is in `live` with the same value.
pub fn subset(desired: &Value, live: &Value) -> bool {
    subset_at(desired, live, 0)
}

fn empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Object(m) => m.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

fn subset_at(desired: &Value, live: &Value, depth: usize) -> bool {
    match (desired, live) {
        (Value::Object(d), Value::Object(l)) => d.iter().all(|(k, dv)| {
            // Top-level status is the cluster's, never git's.
            if depth == 0 && k == "status" {
                return true;
            }
            if depth == 1 && k == "annotations" {
                return annotations_subset(dv, l.get(k).unwrap_or(&Value::Null));
            }
            match l.get(k) {
                Some(lv) => subset_at(dv, lv, depth + 1),
                None => empty(dv),
            }
        }),
        (Value::Array(d), Value::Array(l)) => {
            d.len() == l.len() && d.iter().zip(l).all(|(a, b)| subset_at(a, b, depth + 1))
        }
        (Value::Null, _) => true,
        (d, Value::Null) => empty(d),
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (d, l) => d == l,
    }
}

/// Annotations, without last-applied-configuration: its JSON is compared by
/// meaning, through `stale`, not as a string.
fn annotations_subset(desired: &Value, live: &Value) -> bool {
    let Some(d) = desired.as_object() else {
        return true;
    };
    d.iter()
        .all(|(k, v)| k == LAST_APPLIED || live.get(k) == Some(v))
}

/// Whether a field the last apply set is gone from `desired` but still live:
/// then the next apply removes it, so the object is out of sync.
pub fn stale(last_applied: &Value, desired: &Value, live: &Value) -> bool {
    stale_at(last_applied, desired, live, 0)
}

fn stale_at(last: &Value, desired: &Value, live: &Value, depth: usize) -> bool {
    let (Value::Object(la), Value::Object(l)) = (last, live) else {
        return false;
    };
    let d = desired.as_object();
    la.iter().any(|(k, lav)| {
        if depth == 0 && k == "status" {
            return false;
        }
        if depth == 1 && k == "annotations" {
            let ld = d.and_then(|d| d.get(k));
            return lav.as_object().into_iter().flatten().any(|(ak, _)| {
                ak != LAST_APPLIED
                    && ld.and_then(|x| x.get(ak)).is_none()
                    && l.get(k).and_then(|x| x.get(ak)).is_some()
            });
        }
        match (d.and_then(|d| d.get(k)), l.get(k)) {
            (None, Some(lv)) => !empty(lv) && !empty(lav),
            (Some(dv), Some(lv)) => stale_at(lav, dv, lv, depth + 1),
            _ => false,
        }
    })
}

/// The object without what the server keeps for itself, to compare a dry
/// run's answer with the live object.
pub fn strip_volatile(v: &Value) -> Value {
    let mut v = v.clone();
    if let Some(m) = v.get_mut("metadata").and_then(Value::as_object_mut) {
        for k in [
            "managedFields",
            "resourceVersion",
            "generation",
            "uid",
            "creationTimestamp",
            "selfLink",
        ] {
            m.remove(k);
        }
        if let Some(a) = m.get_mut("annotations").and_then(Value::as_object_mut) {
            a.remove(LAST_APPLIED);
            if a.is_empty() {
                m.remove("annotations");
            }
        }
    }
    if let Some(m) = v.as_object_mut() {
        m.remove("status");
    }
    v
}

/// JSON merge patch (RFC 7386) that turns `old` into `new`.
pub fn merge_patch(old: &Value, new: &Value) -> Option<Value> {
    match (old, new) {
        (Value::Object(o), Value::Object(n)) => {
            let mut p = serde_json::Map::new();
            for (k, nv) in n {
                match o.get(k) {
                    Some(ov) => {
                        if let Some(sub) = merge_patch(ov, nv) {
                            p.insert(k.clone(), sub);
                        }
                    }
                    None => {
                        p.insert(k.clone(), nv.clone());
                    }
                }
            }
            for k in o.keys() {
                if !n.contains_key(k) {
                    p.insert(k.clone(), Value::Null);
                }
            }
            if p.is_empty() {
                None
            } else {
                Some(Value::Object(p))
            }
        }
        (o, n) if o == n => None,
        (_, n) => Some(n.clone()),
    }
}

/// `live` cut to the fields of `shape`: what `subset` compared, so a diff
/// of it with the desired object shows only what is out of sync. Lists are
/// cut item by item; items `shape` lacks are kept whole.
pub fn project(shape: &Value, live: &Value) -> Value {
    project_at(shape, live, 0)
}

fn project_at(shape: &Value, live: &Value, depth: usize) -> Value {
    match (shape, live) {
        (Value::Object(s), Value::Object(l)) => Value::Object(
            s.iter()
                .filter(|(k, _)| !(depth == 0 && *k == "status"))
                .filter_map(|(k, sv)| {
                    let lv = l.get(k)?;
                    let v = if depth == 1 && k == "annotations" {
                        let mut a = project_at(sv, lv, depth + 1);
                        if let Some(m) = a.as_object_mut() {
                            m.remove(LAST_APPLIED);
                        }
                        a
                    } else {
                        project_at(sv, lv, depth + 1)
                    };
                    Some((k.clone(), v))
                })
                .collect(),
        ),
        (Value::Array(s), Value::Array(l)) => Value::Array(
            l.iter()
                .enumerate()
                .map(|(i, lv)| match s.get(i) {
                    Some(sv) => project_at(sv, lv, depth + 1),
                    None => lv.clone(),
                })
                .collect(),
        ),
        (_, l) => l.clone(),
    }
}

/// `b` merged into `a`, `a` winning: the fields a diff shows.
pub fn union(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut out = x.clone();
            for (k, v) in y {
                let merged = match x.get(k) {
                    Some(av) => union(av, v),
                    None => v.clone(),
                };
                out.insert(k.clone(), merged);
            }
            Value::Object(out)
        }
        (Value::Null, b) => b.clone(),
        (a, _) => a.clone(),
    }
}

/// A Secret's values hidden in a diff: the same mask when equal, marked
/// apart when they differ (as Argo CD's `++++++++`).
pub fn mask_secrets(desired: &mut Value, live: &mut Value) {
    if desired["kind"] != "Secret" && live["kind"] != "Secret" {
        return;
    }
    for field in ["data", "stringData"] {
        let keys: std::collections::BTreeSet<String> = [&*desired, &*live]
            .iter()
            .filter_map(|o| o[field].as_object())
            .flat_map(|m| m.keys().cloned())
            .collect();
        for k in keys {
            let same = desired[field].get(&k) == live[field].get(&k);
            for (o, mark) in [(&mut *desired, "+++++++++"), (&mut *live, "++++++++")] {
                if let Some(v) = o.get_mut(field).and_then(|m| m.get_mut(&k)) {
                    *v = Value::String(if same { "++++++++" } else { mark }.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn deploy(replicas: i64) -> Value {
        json!({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web", "annotations": {"a": "1"}},
            "spec": {"replicas": replicas, "template": {"spec": {"containers": [{"name": "m", "image": "x"}]}}}})
    }

    #[test]
    fn defaults_do_not_count_values_do() {
        let mut live = deploy(1);
        live["spec"]["progressDeadlineSeconds"] = json!(600);
        live["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] =
            json!("IfNotPresent");
        live["metadata"]["annotations"][LAST_APPLIED] = json!("{...}");
        live["status"] = json!({"replicas": 1});
        assert!(subset(&deploy(1), &live));
        assert!(!subset(&deploy(3), &live));
        let mut d = deploy(1);
        d["spec"]["template"]["spec"]["containers"] =
            json!([{"name": "m", "image": "x"}, {"name": "n", "image": "y"}]);
        assert!(!subset(&d, &live));
        // An empty map in git and none live are the same.
        let mut d = deploy(1);
        d["spec"]["strategy"] = json!({});
        assert!(subset(&d, &live));
    }

    #[test]
    fn dropped_fields_are_stale() {
        let mut last = deploy(1);
        last["spec"]["minReadySeconds"] = json!(5);
        last["metadata"]["annotations"]["gone"] = json!("x");
        let mut live = last.clone();
        live["spec"]["progressDeadlineSeconds"] = json!(600);
        assert!(stale(&last, &deploy(1), &live));
        assert!(!stale(&deploy(1), &deploy(1), &live));
        let mut only_ann = deploy(1);
        only_ann["metadata"]["annotations"]["gone"] = json!("x");
        let mut live2 = only_ann.clone();
        live2["spec"]["x"] = json!(1);
        assert!(stale(&only_ann, &deploy(1), &live2));
    }

    #[test]
    fn merge_patches() {
        let old = json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4});
        let new = json!({"a": 1, "b": {"c": 5}, "f": 6});
        assert_eq!(
            merge_patch(&old, &new),
            Some(json!({"b": {"c": 5, "d": null}, "e": null, "f": 6}))
        );
        assert_eq!(merge_patch(&old, &old), None);
    }

    #[test]
    fn project_keeps_only_what_git_sets() {
        let mut live = deploy(1);
        live["spec"]["progressDeadlineSeconds"] = json!(600);
        live["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] = json!("Always");
        live["metadata"]["annotations"][LAST_APPLIED] = json!("{...}");
        live["metadata"]["uid"] = json!("u");
        live["status"] = json!({"replicas": 1});
        let p = project(&deploy(3), &live);
        assert_eq!(p, deploy(1));
        // A field the last apply set, dropped from git, shows too.
        let mut last = deploy(1);
        last["spec"]["minReadySeconds"] = json!(5);
        live["spec"]["minReadySeconds"] = json!(5);
        let p = project(&union(&deploy(3), &last), &live);
        assert_eq!(p["spec"]["minReadySeconds"], 5);
        // Extra list items stay whole.
        let mut more = deploy(1);
        more["spec"]["template"]["spec"]["containers"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "side", "image": "y"}));
        let p = project(&deploy(1), &more);
        assert_eq!(
            p["spec"]["template"]["spec"]["containers"][1]["name"],
            "side"
        );
    }

    #[test]
    fn secrets_are_masked_apart_when_they_differ() {
        let secret = |a: &str, b: &str| json!({"kind": "Secret", "data": {"a": a, "b": b}});
        let (mut d, mut l) = (secret("x", "y"), secret("x", "z"));
        mask_secrets(&mut d, &mut l);
        assert_eq!(d["data"]["a"], l["data"]["a"]);
        assert_ne!(d["data"]["b"], l["data"]["b"]);
        assert!(!d.to_string().contains('y') && !l.to_string().contains('z'));
        let mut missing = Value::Null;
        mask_secrets(&mut d, &mut missing);
        assert!(missing.is_null());
    }
}
