//! AppProject sync windows (`spec.syncWindows`), as Argo CD reads them: the
//! windows that match an app (by application, cluster or namespace globs, OR
//! unless `andOperator`), which of them are active now (a cron schedule in
//! the window's time zone plus a duration), and whether a sync may run:
//! an active deny blocks (a manual sync passes when every active deny has
//! `manualSync`), an active allow lets it through, and allow windows that
//! are all inactive block like a deny. `syncOverrun` lets an operation that
//! started while syncing was allowed finish.

use chrono::{
    DateTime, Datelike, Duration as TimeDelta, NaiveDateTime, Offset, TimeZone, Timelike, Utc,
};
use serde_json::Value;

use crate::app::glob_match;

/// What Argo CD writes in a sync it holds back.
pub const BLOCKED: &str = "Sync operation blocked by sync window";

/// The windows of `project` that match `app`.
pub fn matching<'a>(project: &'a Value, app: &Value) -> Vec<&'a Value> {
    let name = app["metadata"]["name"].as_str().unwrap_or_default();
    let dest = &app["spec"]["destination"];
    let dname = dest["name"].as_str().unwrap_or_default();
    let dserver = dest["server"].as_str().unwrap_or_default();
    let dns = dest["namespace"].as_str().unwrap_or_default();
    let list = |w: &'a Value, k: &str| -> Vec<&'a str> {
        w[k].as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect()
    };
    let mut out = Vec::new();
    for w in project["spec"]["syncWindows"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let and = w["andOperator"].as_bool().unwrap_or(false);
        let mut matched = false;
        let mut set = false;
        let apps = list(w, "applications");
        if !apps.is_empty() {
            set = true;
            matched = apps.iter().any(|a| glob_match(a, name));
        }
        if and && set && !matched {
            continue;
        } else if !and && matched {
            out.push(w);
            continue;
        }
        let clusters = list(w, "clusters");
        if !clusters.is_empty() {
            set = true;
            matched = clusters.iter().any(|c| {
                (!dname.is_empty() && glob_match(c, dname))
                    || (!dserver.is_empty() && glob_match(c, dserver))
            });
        }
        if and && set && !matched {
            continue;
        } else if !and && matched {
            out.push(w);
            continue;
        }
        let namespaces = list(w, "namespaces");
        if !namespaces.is_empty() {
            matched = namespaces.iter().any(|n| glob_match(n, dns));
        }
        if matched {
            out.push(w);
        }
    }
    out
}

/// Whether the matching `windows` let a sync run at `now`; `started` is when
/// the operation began (for `syncOverrun`). Err: a window that does not
/// parse, which blocks.
pub fn can_sync(
    windows: &[&Value],
    manual: bool,
    started: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<bool, String> {
    if windows.is_empty() {
        return Ok(true);
    }
    let active = active(windows, now)?;
    let denies: Vec<&&Value> = active.iter().filter(|w| w["kind"] == "deny").collect();
    if !denies.is_empty() {
        if manual && denies.iter().all(|w| flag(w, "manualSync")) {
            return Ok(true);
        }
        // Overrun: every deny window allows it and the sync was allowed
        // when it started.
        if let Some(t) = started
            && windows
                .iter()
                .filter(|w| w["kind"] == "deny")
                .all(|w| flag(w, "syncOverrun"))
            && can_sync_at(windows, manual, t)?
        {
            return Ok(true);
        }
        return Ok(false);
    }
    if active.iter().any(|w| w["kind"] == "allow") {
        return Ok(true);
    }
    let inactive = inactive_allows(windows, now)?;
    if !inactive.is_empty() {
        if manual && inactive.iter().all(|w| flag(w, "manualSync")) {
            return Ok(true);
        }
        if let Some(t) = started
            && inactive.iter().all(|w| flag(w, "syncOverrun"))
            && can_sync_at(windows, manual, t)?
        {
            return Ok(true);
        }
        return Ok(false);
    }
    Ok(true)
}

/// can_sync without overrun, at a past time.
fn can_sync_at(windows: &[&Value], manual: bool, t: DateTime<Utc>) -> Result<bool, String> {
    let active = active(windows, t)?;
    let denies: Vec<&&Value> = active.iter().filter(|w| w["kind"] == "deny").collect();
    if !denies.is_empty() {
        return Ok(manual && denies.iter().all(|w| flag(w, "manualSync")));
    }
    if active.iter().any(|w| w["kind"] == "allow") {
        return Ok(true);
    }
    let inactive = inactive_allows(windows, t)?;
    if !inactive.is_empty() {
        return Ok(manual && inactive.iter().all(|w| flag(w, "manualSync")));
    }
    Ok(true)
}

fn flag(w: &Value, k: &str) -> bool {
    w[k].as_bool().unwrap_or(false)
}

fn active<'a>(windows: &[&'a Value], now: DateTime<Utc>) -> Result<Vec<&'a Value>, String> {
    let mut out = Vec::new();
    for w in windows {
        if is_active(w, now)? {
            out.push(*w);
        }
    }
    Ok(out)
}

fn inactive_allows<'a>(
    windows: &[&'a Value],
    now: DateTime<Utc>,
) -> Result<Vec<&'a Value>, String> {
    let mut out = Vec::new();
    for w in windows.iter().filter(|w| w["kind"] == "allow") {
        if !is_active(w, now)? {
            out.push(*w);
        }
    }
    Ok(out)
}

/// A window is active when its schedule fired within the last `duration`,
/// counted on the wall clock of its time zone (Argo CD shifts by the zone's
/// current offset).
pub fn is_active(w: &Value, now: DateTime<Utc>) -> Result<bool, String> {
    let schedule = w["schedule"].as_str().unwrap_or_default();
    let cron = Cron::parse(schedule)
        .map_err(|e| format!("invalid sync windows: cannot parse schedule '{schedule}': {e}"))?;
    let duration = w["duration"].as_str().unwrap_or_default();
    let d = crate::app::parse_duration(duration)
        .ok_or_else(|| format!("invalid sync windows: cannot parse duration '{duration}'"))?;
    let d = TimeDelta::from_std(d).unwrap_or(TimeDelta::MAX);
    let local = (now + offset(w["timeZone"].as_str().unwrap_or_default(), now)).naive_utc();
    Ok(local
        .checked_sub_signed(d)
        .and_then(|from| cron.next(from))
        .is_some_and(|next| next < local))
}

fn offset(tz: &str, now: DateTime<Utc>) -> TimeDelta {
    if tz.is_empty() || tz == "UTC" {
        return TimeDelta::zero();
    }
    match tz.parse::<chrono_tz::Tz>() {
        Ok(z) => TimeDelta::seconds(
            z.offset_from_utc_datetime(&now.naive_utc())
                .fix()
                .local_minus_utc() as i64,
        ),
        Err(_) => {
            tracing::warn!("Invalid time zone {tz} specified. Using UTC as default time zone");
            TimeDelta::zero()
        }
    }
}

/// A five-field cron schedule (minute hour day-of-month month day-of-week),
/// the syntax Argo CD's parser takes: `*`, `?`, lists, ranges, steps,
/// month and weekday names.
#[derive(Debug, PartialEq)]
pub struct Cron {
    minute: u64,
    hour: u64,
    dom: u64,
    month: u64,
    dow: u64,
    /// Day of month or day of week was `*`/`?`: then both must match,
    /// else either.
    day_star: bool,
}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

impl Cron {
    pub fn parse(s: &str) -> Result<Cron, String> {
        let f: Vec<&str> = s.split_whitespace().collect();
        if f.len() != 5 {
            return Err(format!(
                "expected exactly 5 fields, found {}: [{s}]",
                f.len()
            ));
        }
        let (minute, _) = field(f[0], 0, 59, &[])?;
        let (hour, _) = field(f[1], 0, 23, &[])?;
        let (dom, dom_star) = field(f[2], 1, 31, &[])?;
        let (month, _) = field(f[3], 1, 12, &MONTHS)?;
        let (dow, dow_star) = field(f[4], 0, 6, &DAYS)?;
        Ok(Cron {
            minute,
            hour,
            dom,
            month,
            dow,
            day_star: dom_star || dow_star,
        })
    }

    fn day_matches(&self, t: &NaiveDateTime) -> bool {
        let d = self.dom & (1 << t.day()) != 0;
        let w = self.dow & (1 << t.weekday().num_days_from_sunday()) != 0;
        if self.day_star { d && w } else { d || w }
    }

    /// The first time after `t` the schedule fires (whole minutes).
    pub fn next(&self, t: NaiveDateTime) -> Option<NaiveDateTime> {
        let mut t = t.with_second(0)?.with_nanosecond(0)? + TimeDelta::minutes(1);
        let limit = t.year() + 5;
        'wrap: loop {
            if t.year() > limit {
                return None;
            }
            while self.month & (1 << t.month()) == 0 {
                let (y, m) = if t.month() == 12 {
                    (t.year() + 1, 1)
                } else {
                    (t.year(), t.month() + 1)
                };
                t = chrono::NaiveDate::from_ymd_opt(y, m, 1)?.and_hms_opt(0, 0, 0)?;
                if t.month() == 1 {
                    continue 'wrap;
                }
            }
            while !self.day_matches(&t) {
                let m = t.month();
                t = (t.date() + TimeDelta::days(1)).and_hms_opt(0, 0, 0)?;
                if t.month() != m {
                    continue 'wrap;
                }
            }
            while self.hour & (1 << t.hour()) == 0 {
                let d = t.day();
                t = t.with_minute(0)? + TimeDelta::hours(1);
                if t.day() != d {
                    continue 'wrap;
                }
            }
            while self.minute & (1 << t.minute()) == 0 {
                let h = t.hour();
                t += TimeDelta::minutes(1);
                if t.hour() != h {
                    continue 'wrap;
                }
            }
            return Some(t);
        }
    }
}

/// One field: a bit per allowed value, and whether it was `*` or `?`.
fn field(s: &str, min: u32, max: u32, names: &[&str]) -> Result<(u64, bool), String> {
    let mut bits = 0u64;
    let mut star = false;
    for part in s.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, st)) => (r, Some(st)),
            None => (part, None),
        };
        let value = |v: &str| -> Result<u32, String> {
            if let Some(i) = names.iter().position(|n| n.eq_ignore_ascii_case(v)) {
                return Ok(i as u32 + if min == 1 { 1 } else { 0 });
            }
            v.parse::<u32>()
                .map_err(|_| format!("failed to parse int from {v}"))
        };
        let (mut lo, mut hi, mut this_star) = if range == "*" || range == "?" {
            (min, max, true)
        } else if let Some((a, b)) = range.split_once('-') {
            (value(a)?, value(b)?, false)
        } else {
            let v = value(range)?;
            (v, v, false)
        };
        let step = match step {
            None => 1,
            Some(st) => {
                let n: u32 = st
                    .parse()
                    .map_err(|_| format!("failed to parse int from {st}"))?;
                // "N/step" is N through the maximum.
                if !range.contains('-') && !this_star {
                    hi = max;
                }
                if n > 1 {
                    this_star = false;
                }
                n
            }
        };
        if step == 0 {
            return Err(format!("step of range should be a positive number: {part}"));
        }
        if lo < min {
            return Err(format!(
                "beginning of range ({lo}) below minimum ({min}): {part}"
            ));
        }
        if hi > max {
            return Err(format!("end of range ({hi}) above maximum ({max}): {part}"));
        }
        if lo > hi {
            return Err(format!(
                "beginning of range ({lo}) beyond end of range ({hi}): {part}"
            ));
        }
        while lo <= hi {
            bits |= 1 << lo;
            lo += step;
        }
        star |= this_star;
    }
    Ok((bits, star))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn naive(s: &str) -> NaiveDateTime {
        at(s).naive_utc()
    }

    #[test]
    fn cron_next() {
        let c = Cron::parse("30 2 * * *").unwrap();
        assert_eq!(
            c.next(naive("2026-10-24T01:00:00Z")),
            Some(naive("2026-10-24T02:30:00Z"))
        );
        assert_eq!(
            c.next(naive("2026-10-24T02:30:00Z")),
            Some(naive("2026-10-25T02:30:00Z"))
        );
        let c = Cron::parse("0 0 1 1 *").unwrap();
        assert_eq!(
            c.next(naive("2026-10-24T01:00:00Z")),
            Some(naive("2027-01-01T00:00:00Z"))
        );
        let c = Cron::parse("*/15 9-17 * * mon-fri").unwrap();
        // 2026-10-24 is a Saturday.
        assert_eq!(
            c.next(naive("2026-10-24T12:00:00Z")),
            Some(naive("2026-10-26T09:00:00Z"))
        );
        assert_eq!(
            c.next(naive("2026-10-26T09:01:00Z")),
            Some(naive("2026-10-26T09:15:00Z"))
        );
        // Day of month or day of week when neither is `*`.
        let c = Cron::parse("0 0 1 * 1").unwrap();
        assert_eq!(
            c.next(naive("2026-10-24T00:00:00Z")),
            Some(naive("2026-10-26T00:00:00Z"))
        );
        assert!(Cron::parse("* * *").is_err());
        assert!(Cron::parse("60 * * * *").is_err());
        assert!(
            Cron::parse("0 0 30 2 *")
                .unwrap()
                .next(naive("2026-01-01T00:00:00Z"))
                .is_none()
        );
    }

    #[test]
    fn active_windows() {
        let w = json!({"kind": "deny", "schedule": "0 22 * * *", "duration": "2h"});
        assert!(is_active(&w, at("2026-10-24T23:00:00Z")).unwrap());
        assert!(!is_active(&w, at("2026-10-24T21:59:00Z")).unwrap());
        assert!(!is_active(&w, at("2026-10-25T00:00:00Z")).unwrap());
        let w = json!({"kind": "deny", "schedule": "0 22 * * *", "duration": "2h", "timeZone": "Etc/GMT+5"});
        // 22:00 at UTC-5 is 03:00 UTC.
        assert!(is_active(&w, at("2026-10-25T04:00:00Z")).unwrap());
        assert!(!is_active(&w, at("2026-10-24T23:00:00Z")).unwrap());
        assert!(
            is_active(
                &json!({"schedule": "nope", "duration": "1h"}),
                at("2026-10-24T00:00:00Z")
            )
            .is_err()
        );
    }

    #[test]
    fn matching_and_can_sync() {
        let proj = json!({"spec": {"syncWindows": [
            {"kind": "deny", "schedule": "* * * * *", "duration": "1h", "applications": ["web-*"]},
            {"kind": "deny", "schedule": "* * * * *", "duration": "1h", "applications": ["db"], "manualSync": true},
            {"kind": "allow", "schedule": "0 0 1 1 *", "duration": "1m", "namespaces": ["closed"]},
            {"kind": "deny", "schedule": "* * * * *", "duration": "1h", "applications": ["x"], "namespaces": ["nowhere"], "andOperator": true},
        ]}});
        let app = |name: &str, ns: &str| {
            json!({"metadata": {"name": name},
            "spec": {"destination": {"server": "https://kubernetes.default.svc", "namespace": ns}}})
        };
        let now = at("2026-10-24T12:00:00Z");
        let w = matching(&proj, &app("web-1", "a"));
        assert_eq!(w.len(), 1);
        assert_eq!(can_sync(&w, false, None, now), Ok(false));
        assert_eq!(can_sync(&w, true, None, now), Ok(false));
        let w = matching(&proj, &app("db", "a"));
        assert_eq!(can_sync(&w, false, None, now), Ok(false));
        assert_eq!(can_sync(&w, true, None, now), Ok(true));
        let w = matching(&proj, &app("other", "closed"));
        assert_eq!(can_sync(&w, false, None, now), Ok(false));
        assert!(matching(&proj, &app("x", "a")).is_empty());
        assert_eq!(can_sync(&[], false, None, now), Ok(true));
        // Overrun: started before the deny window opened.
        let proj = json!({"spec": {"syncWindows": [
            {"kind": "deny", "schedule": "0 12 * * *", "duration": "1h", "applications": ["*"], "syncOverrun": true}]}});
        let w = matching(&proj, &app("a", "a"));
        assert_eq!(
            can_sync(
                &w,
                false,
                Some(at("2026-10-24T11:50:00Z")),
                at("2026-10-24T12:10:00Z")
            ),
            Ok(true)
        );
        assert_eq!(
            can_sync(
                &w,
                false,
                Some(at("2026-10-24T12:05:00Z")),
                at("2026-10-24T12:10:00Z")
            ),
            Ok(false)
        );
    }
}
