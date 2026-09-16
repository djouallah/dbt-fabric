"""Render the GitHub Actions job summary dashboard.

Invoked by the workflow's always() summary step as
    python .github/scripts/summary.py >> "$GITHUB_STEP_SUMMARY"
All inputs arrive via environment variables (timings T_*, GITHUB_*, JOB_STATUS,
WS_NAME/LH_*, PAGE_URL) and the dbt artifacts under dbt/target + /tmp snapshots.
"""

import json
import os
from collections import Counter


def fmt(s):
    s = int(s or 0)
    return f"{s}s" if s < 60 else f"{s//60}m {s%60:02d}s"


def env(k, default=''):
    return os.environ.get(k, default) or default


phases = [
    ('🏗️', 'Provision Lakehouse', env('T_LAKEHOUSE', '0')),
    ('🔨', 'dbt run',             env('T_DBT_RUN',   '0')),
    ('✅', 'dbt test',            env('T_DBT_TEST',  '0')),
    ('🚀', 'Deploy Fabric',       env('T_DEPLOY',    '0')),
]
total = sum(int(p[2] or 0) for p in phases)

# Merge run + retry snapshots: dbt retry overwrites run_results.json
# with only the reran nodes, so snapshot after each attempt and let the
# summary merge them (later attempts override earlier per unique_id) to
# recover the final status of every model. Keep only model nodes (drop
# on-run hooks/operations) so the count matches docs.
def load_results(path):
    try:
        return json.load(open(path))['results']
    except FileNotFoundError:
        return []


merged = {}
for p in ('/tmp/rr_0.json', '/tmp/rr_1.json', '/tmp/rr_2.json'):
    for r in load_results(p):
        if r['unique_id'].startswith('model.'):
            merged[r['unique_id']] = r
rs = list(merged.values())
rstat = Counter(x['status'] for x in rs)
rslow = sorted(rs, key=lambda x: -x.get('execution_time', 0))[:3]

try:
    ts = json.load(open('/tmp/test_results.json'))['results']
    tstat = Counter(x['status'] for x in ts)
    ttotal = len(ts)
except FileNotFoundError:
    tstat, ttotal = Counter(), 0

items = []
if env('DEPLOY_SKIPPED') != 'true':
    try:
        for f in sorted(os.listdir('fabric_items')):
            if '.' in f:
                name, typ = f.rsplit('.', 1)
                items.append((typ, name))
    except FileNotFoundError:
        pass

ws        = env('WS_NAME', '?')
lh        = env('LH_NAME', '?')
ws_id     = env('WS_ID')
lh_id     = env('LH_ID')
lh_action = env('LH_ACTION', '?')
sha       = env('GITHUB_SHA')[:7]
branch    = env('GITHUB_REF_NAME', '?')
actor     = env('GITHUB_ACTOR', '?')
page_url  = env('PAGE_URL')
repo      = env('GITHUB_REPOSITORY')
server    = env('GITHUB_SERVER_URL', 'https://github.com')
run_id    = env('GITHUB_RUN_ID')

# ===== Header =====
print("# 🎯 Pipeline Summary")
print()
status = env('JOB_STATUS', 'unknown').lower()
color = {'success': 'brightgreen', 'failure': 'red'}.get(status, 'lightgrey')
print(f"![pipeline](https://img.shields.io/badge/pipeline-{status}-{color}?style=for-the-badge&logo=githubactions)", end=' ')
print(f"![workspace](https://img.shields.io/badge/workspace-{ws}-0078d4?style=for-the-badge&logo=microsoft)", end=' ')
print(f"![branch](https://img.shields.io/badge/branch-{branch}-7719aa?style=for-the-badge)", end=' ')
print(f"![sha](https://img.shields.io/badge/sha-{sha}-grey?style=for-the-badge&logo=github)", end=' ')
print(f"![duration](https://img.shields.io/badge/total-{fmt(total).replace(' ', '_')}-blue?style=for-the-badge)")
print()

# ===== 4-card layout: Timings | Fabric on top, dbt full-width below =====
print("<table>")

# Row 1: Timings | Fabric (Lakehouse details + deployed items)
print('<tr><td width="50%" valign="top">\n')
print("### ⏱️ Phase Timings\n")
print("| Phase | Duration |")
print("| --- | ---: |")
for emoji, name, secs in phases:
    print(f"| {emoji} {name} | {fmt(secs)} |")
print(f"| **Total** | **{fmt(total)}** |")
print('\n</td><td width="50%" valign="top">\n')
print("### 🚀 Fabric\n")
print("| Item | Name |")
print("| :---: | --- |")
print(f"| 🏢 Workspace | `{ws}` |")
print(f"| 🏠 Lakehouse | `{lh}` _({lh_action})_ |")
if env('DEPLOY_SKIPPED') == 'true':
    print(f"\n_Deploy skipped — data-only run (deploy happens on push/dispatch to `main`)_")
else:
    emojis = {
        'Lakehouse': '🏠', 'Notebook': '📓', 'SemanticModel': '📊',
        'DataPipeline': '🔄', 'VariableLibrary': '📚',
        'Warehouse': '🏭', 'Report': '📈',
    }
    for typ, name in items:
        print(f"| {emojis.get(typ, '📦')} {typ} | `{name}` |")
print('\n</td></tr>')

# Row 2: dbt (run · test · docs) — full width, three sub-columns
ok = rstat.get('success', 0); err = rstat.get('error', 0); skip = rstat.get('skipped', 0)
tp = tstat.get('pass', 0); tf = tstat.get('fail', 0); tw = tstat.get('warn', 0); te = tstat.get('error', 0)
print('<tr><td colspan="2" valign="top">\n')
print("### 🔨 dbt\n")
print('<table><tr><td valign="top">\n')
# run
print(f"**🔨 run · {len(rs)} models · {fmt(env('T_DBT_RUN', '0'))}**\n")
print("| Status | Count |")
print("| --- | ---: |")
print(f"| ✅ Success | **{ok}** |")
print(f"| ❌ Error | **{err}** |")
print(f"| ⏭️ Skipped | **{skip}** |")
if rslow:
    print("\n**🐢 Slowest**\n")
    for s in rslow:
        print(f"- `{s['unique_id'].split('.')[-1]}` {s.get('execution_time', 0):.1f}s")
print('\n</td><td valign="top">\n')
# test
print(f"**✅ test · {ttotal} tests · {fmt(env('T_DBT_TEST', '0'))}**\n")
print("| Result | Count |")
print("| --- | ---: |")
print(f"| ✅ Pass | **{tp}** |")
print(f"| ❌ Fail | **{tf}** |")
print(f"| ⚠️ Warn | **{tw}** |")
print(f"| 💥 Error | **{te}** |")
print('\n</td></tr></table>')
if page_url:
    print(f"\n🔗 [Live dashboard]({page_url})")
print('\n</td></tr>')
print("</table>\n")

# ===== Mermaid below =====
print("## 🛤️ Pipeline\n")
print("```mermaid")
print("flowchart LR")
print("  A[🏗️ Lakehouse] --> B[🔨 dbt run]")
print("  B --> C[✅ dbt test]")
print("  C --> D[🚀 Deploy Fabric]")
print("```")
print()
print("---")
print(f'<sub>Pipeline run by <b>@{actor}</b> · commit <a href="{server}/{repo}/commit/{env("GITHUB_SHA")}"><code>{sha}</code></a> · <a href="{server}/{repo}/actions/runs/{run_id}">view run</a></sub>')
