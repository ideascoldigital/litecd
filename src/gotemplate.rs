//! Go templates for ApplicationSets with goTemplate: true (gtmpl, Go's
//! text/template in Rust) and the sprig functions people use in them.

use std::collections::HashMap;

use anyhow::{Result, bail};
use gtmpl::{Context, Func, FuncError, Template, Value as G};
use serde_json::Value;

fn to_g(v: &Value) -> G {
    match v {
        Value::Null => G::Nil,
        Value::Bool(b) => G::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                G::from(i)
            } else if let Some(u) = n.as_u64() {
                G::from(u)
            } else {
                G::from(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => G::String(s.clone()),
        Value::Array(a) => G::Array(a.iter().map(to_g).collect()),
        Value::Object(m) => G::Map(
            m.iter()
                .map(|(k, v)| (k.clone(), to_g(v)))
                .collect::<HashMap<_, _>>(),
        ),
    }
}

fn to_json(v: &G) -> Value {
    match v {
        G::Nil | G::NoValue => Value::Null,
        G::Bool(b) => Value::Bool(*b),
        G::String(s) => Value::String(s.clone()),
        G::Number(n) => serde_json::from_str(&n.to_string()).unwrap_or(Value::Null),
        G::Array(a) => Value::Array(a.iter().map(to_json).collect()),
        G::Map(m) | G::Object(m) => {
            Value::Object(m.iter().map(|(k, v)| (k.clone(), to_json(v))).collect())
        }
        G::Function(_) => Value::Null,
    }
}

fn s(v: &G) -> String {
    match v {
        G::String(s) => s.clone(),
        G::Nil | G::NoValue => String::new(),
        other => other.to_string(),
    }
}

fn arg<'a>(args: &'a [G], i: usize, name: &str) -> Result<&'a G, FuncError> {
    args.get(i)
        .ok_or_else(|| FuncError::Generic(format!("{name}: missing argument")))
}

macro_rules! str1 {
    ($name:ident, $f:expr) => {
        fn $name(args: &[G]) -> Result<G, FuncError> {
            let f: fn(String) -> String = $f;
            Ok(G::String(f(s(arg(args, 0, stringify!($name))?))))
        }
    };
}

str1!(upper, |x| x.to_uppercase());
str1!(lower, |x| x.to_lowercase());
str1!(trim, |x| x.trim().to_string());
str1!(title, |x| {
    x.split(' ')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
});
str1!(quote, |x| format!("{x:?}"));
str1!(squote, |x| format!("'{x}'"));
str1!(b64enc, |x| {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(x)
});

fn to_string(args: &[G]) -> Result<G, FuncError> {
    Ok(G::String(s(arg(args, 0, "toString")?)))
}

// Sprig's argument order: the piped value comes last.
fn replace(args: &[G]) -> Result<G, FuncError> {
    Ok(G::String(
        s(arg(args, 2, "replace")?).replace(&s(&args[0]), &s(&args[1])),
    ))
}

fn trim_prefix(args: &[G]) -> Result<G, FuncError> {
    let x = s(arg(args, 1, "trimPrefix")?);
    Ok(G::String(
        x.strip_prefix(&s(&args[0])).unwrap_or(&x).to_string(),
    ))
}

fn trim_suffix(args: &[G]) -> Result<G, FuncError> {
    let x = s(arg(args, 1, "trimSuffix")?);
    Ok(G::String(
        x.strip_suffix(&s(&args[0])).unwrap_or(&x).to_string(),
    ))
}

fn contains(args: &[G]) -> Result<G, FuncError> {
    Ok(G::Bool(s(arg(args, 1, "contains")?).contains(&s(&args[0]))))
}

fn has_prefix(args: &[G]) -> Result<G, FuncError> {
    Ok(G::Bool(
        s(arg(args, 1, "hasPrefix")?).starts_with(&s(&args[0])),
    ))
}

fn has_suffix(args: &[G]) -> Result<G, FuncError> {
    Ok(G::Bool(
        s(arg(args, 1, "hasSuffix")?).ends_with(&s(&args[0])),
    ))
}

fn trunc(args: &[G]) -> Result<G, FuncError> {
    let n: i64 = s(arg(args, 0, "trunc")?)
        .parse()
        .map_err(|_| FuncError::Generic("trunc: not a number".into()))?;
    let x = s(arg(args, 1, "trunc")?);
    let chars: Vec<char> = x.chars().collect();
    let out: String = if n >= 0 {
        chars.iter().take(n as usize).collect()
    } else {
        chars
            .iter()
            .skip(chars.len().saturating_sub((-n) as usize))
            .collect()
    };
    Ok(G::String(out))
}

fn default(args: &[G]) -> Result<G, FuncError> {
    let d = arg(args, 0, "default")?.clone();
    match args.get(1) {
        None | Some(G::Nil) | Some(G::NoValue) => Ok(d),
        Some(G::String(x)) if x.is_empty() => Ok(d),
        Some(G::Bool(false)) => Ok(d),
        Some(v) => Ok(v.clone()),
    }
}

fn to_json_fn(args: &[G]) -> Result<G, FuncError> {
    Ok(G::String(
        serde_json::to_string(&to_json(arg(args, 0, "toJson")?)).unwrap_or_default(),
    ))
}

fn join(args: &[G]) -> Result<G, FuncError> {
    let sep = s(arg(args, 0, "join")?);
    match arg(args, 1, "join")? {
        G::Array(a) => Ok(G::String(a.iter().map(s).collect::<Vec<_>>().join(&sep))),
        other => Ok(G::String(s(other))),
    }
}

/// Render one template string with the parameters as `.`.
pub fn render(text: &str, params: &Value, missing_error: bool) -> Result<String> {
    if !text.contains("{{") {
        return Ok(text.to_string());
    }
    let mut t = Template::default();
    let funcs: [(&str, Func); 20] = [
        ("upper", upper),
        ("lower", lower),
        ("trim", trim),
        ("title", title),
        ("quote", quote),
        ("squote", squote),
        ("b64enc", b64enc),
        ("toString", to_string),
        ("replace", replace),
        ("trimPrefix", trim_prefix),
        ("trimSuffix", trim_suffix),
        ("contains", contains),
        ("hasPrefix", has_prefix),
        ("hasSuffix", has_suffix),
        ("trunc", trunc),
        ("default", default),
        ("toJson", to_json_fn),
        ("join", join),
        ("toRawJson", to_json_fn),
        ("lower", lower),
    ];
    for (name, f) in funcs {
        t.add_func(name, f);
    }
    t.parse(text)
        .map_err(|e| anyhow::anyhow!("template: {e}"))?;
    let out = t
        .render(&Context::from(to_g(params)))
        .map_err(|e| anyhow::anyhow!("template: {e}"))?;
    if missing_error && out.contains("<no value>") {
        bail!("failed to execute go template {text}: map has no entry for key");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn what_the_corpus_uses() {
        let p = json!({"name": "q", "env": "prod", "replicas": 3, "path": {"basename": "x"}});
        assert_eq!(render("{{ .env | upper }}", &p, true).unwrap(), "PROD");
        assert_eq!(
            render(r#"{{ printf "%s-%s" .name .env }}"#, &p, true).unwrap(),
            "q-prod"
        );
        assert_eq!(
            render(
                r#"{{ if eq .env "prod" }}production{{ else }}other{{ end }}"#,
                &p,
                true
            )
            .unwrap(),
            "production"
        );
        assert_eq!(render("{{ .replicas }}", &p, true).unwrap(), "3");
        assert_eq!(render("go-{{ .path.basename }}", &p, true).unwrap(), "go-x");
        assert!(render("{{ .missing }}", &p, true).is_err());
        assert_eq!(
            render(r#"{{ .missing | default "d" }}"#, &p, false).unwrap(),
            "d"
        );
        assert_eq!(
            render(r#"{{ "a-b" | replace "-" "_" }}"#, &p, false).unwrap(),
            "a_b"
        );
        assert_eq!(render("plain", &p, true).unwrap(), "plain");
    }
}
