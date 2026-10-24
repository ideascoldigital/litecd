"""report.py <argocd.json> <litecd.json>: the bench's markdown table."""

import json
import statistics
import sys


def med(xs):
    return round(statistics.median(xs), 2)


def rng(xs):
    return f"{min(xs):g}–{max(xs):g}"


def main():
    runs = {}
    for p in sys.argv[1:]:
        r = json.load(open(p))
        runs[r["target"]] = r
    a, l = runs["argocd"], runs["litecd"]
    rows = [
        ("Memory, no apps (working set)", f"{a['idle_no_apps']['total']['working_set_mib']} MiB", f"{l['idle_no_apps']['total']['working_set_mib']} MiB"),
        ("Memory, 20 apps synced, idle", f"{a['idle_20_apps']['total']['working_set_mib']} MiB", f"{l['idle_20_apps']['total']['working_set_mib']} MiB"),
        ("… anonymous (heap) of that", f"{a['idle_20_apps']['total']['anon_mib']} MiB", f"{l['idle_20_apps']['total']['anon_mib']} MiB"),
        ("Memory, peak during the burst", f"{a['burst']['peak_working_set_mib']} MiB", f"{l['burst']['peak_working_set_mib']} MiB"),
        ("CPU idle, no apps (% of one CPU)", f"{a['idle_no_apps']['total']['cpu_percent']} %", f"{l['idle_no_apps']['total']['cpu_percent']} %"),
        ("CPU idle, 20 apps", f"{a['idle_20_apps']['total']['cpu_percent']} %", f"{l['idle_20_apps']['total']['cpu_percent']} %"),
        ("Install → ready", f"{a['install_seconds']} s", f"{l['install_seconds']} s"),
        ("App created → Synced + Healthy (median, range)", f"{med(a['first'])} s ({rng(a['first'])})", f"{med(l['first'])} s ({rng(l['first'])})"),
        ("Commit + refresh → Synced on it", f"{med(a['change'])} s ({rng(a['change'])})", f"{med(l['change'])} s ({rng(l['change'])})"),
        ("Deployment deleted → healed, Healthy", f"{med(a['drift'])} s ({rng(a['drift'])})", f"{med(l['drift'])} s ({rng(l['drift'])})"),
        (f"{a['burst']['apps']} apps at once → all Synced + Healthy", f"{a['burst']['seconds']} s", f"{l['burst']['seconds']} s"),
    ]
    print("| | Argo CD | litecd |\n|---|---|---|")
    for r in rows:
        print(f"| {r[0]} | {r[1]} | {r[2]} |")
    print()
    for t, r in (("Argo CD", a), ("litecd", l)):
        parts = ", ".join(f"{k.split('/')[0].rsplit('-', 2)[0]}/{k.split('/')[1]} {v['working_set_mib']} MiB"
                          for k, v in sorted(r["idle_20_apps"]["containers"].items()))
        print(f"- {t}, 20 apps idle, per container: {parts}")


if __name__ == "__main__":
    main()
