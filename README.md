from __future__ import annotations

import html
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import pendulum
import requests
from requests.auth import HTTPBasicAuth

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Configure through Airflow environment variables / secrets.
# Example: AIRFLOW_API_BASE_URL=https://airflow.example.ru/api/v1
API_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", "").rstrip("/")
API_USERNAME = os.getenv("AIRFLOW_API_USERNAME", "")
API_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD", "")
# Path to PEM CA bundle (or corporate root CA). Do not set to "false".
API_CA_BUNDLE = os.getenv("AIRFLOW_API_CA_BUNDLE", "")
REPORT_DIR = Path(os.getenv("AIRFLOW_REPORT_DIR", "/opt/airflow/reports"))
REPORT_NAME = "dag_schedule_dashboard.html"
TIMEZONE = os.getenv("AIRFLOW_REPORT_TIMEZONE", "Europe/Moscow")
HISTORY_DAYS = int(os.getenv("DAG_DASHBOARD_HISTORY_DAYS", "7"))
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)


def api_session() -> requests.Session:
    if not API_BASE_URL:
        raise ValueError("Set AIRFLOW_API_BASE_URL, e.g. https://airflow.example.ru/api/v1")
    if not API_USERNAME or not API_PASSWORD:
        raise ValueError("Set AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD using a secret/env var")

    session = requests.Session()
    session.auth = HTTPBasicAuth(API_USERNAME, API_PASSWORD)
    session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
    # Empty means use the operating system's trusted CA store.
    # A path means verify the server certificate against that CA bundle.
    session.verify = API_CA_BUNDLE if API_CA_BUNDLE else True
    return session


def get_json(session: requests.Session, endpoint: str, params: dict | None = None) -> dict:
    url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def get_all_dags(session: requests.Session) -> list[dict]:
    result = []
    offset = 0
    while True:
        payload = get_json(
            session,
            "dags",
            params={"limit": PAGE_SIZE, "offset": offset, "only_active": "false"},
        )
        batch = payload.get("dags", [])
        result.extend(batch)
        total = payload.get("total_entries", len(result))
        offset += len(batch)
        if not batch or offset >= total:
            break
    return result


def get_runs(session: requests.Session, dag_id: str, since_iso: str) -> list[dict]:
    result = []
    offset = 0
    safe_dag_id = quote(dag_id, safe="")
    while True:
        payload = get_json(
            session,
            f"dags/{safe_dag_id}/dagRuns",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "order_by": "-start_date",
                "start_date_gte": since_iso,
            },
        )
        batch = payload.get("dag_runs", [])
        result.extend(batch)
        total = payload.get("total_entries", len(result))
        offset += len(batch)
        if not batch or offset >= total:
            break
    return result


def parse_dt(value):
    if not value:
        return None
    try:
        parsed = pendulum.parse(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.in_timezone(TIMEZONE)
    except Exception:
        return None


def fmt_dt(value) -> str:
    dt = parse_dt(value) if isinstance(value, str) else value
    return dt.format("YYYY-MM-DD HH:mm:ss") if dt else "â"


def duration_seconds(run: dict) -> float | None:
    start = parse_dt(run.get("start_date"))
    end = parse_dt(run.get("end_date"))
    if start and end:
        return max(0.0, (end - start).total_seconds())
    return None


def fmt_duration(seconds) -> str:
    if seconds is None:
        return "Ð²ÑÐ¿Ð¾Ð»Ð½ÑÐµÑÑÑ / Ð½ÐµÑ Ð´Ð°Ð½Ð½ÑÑ"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours} Ñ {minutes} Ð¼Ð¸Ð½"
    if minutes:
        return f"{minutes} Ð¼Ð¸Ð½ {secs} ÑÐµÐº"
    return f"{secs} ÑÐµÐº"


def find_overlaps(runs: list[dict]) -> list[tuple[dict, dict]]:
    """Detect actual runs whose execution intervals overlap."""
    normalized = []
    for run in runs:
        start = parse_dt(run.get("start_date"))
        end = parse_dt(run.get("end_date")) or pendulum.now(TIMEZONE)
        if start:
            normalized.append((start, end, run))
    normalized.sort(key=lambda item: item[0])

    conflicts = []
    active = []
    for start, end, run in normalized:
        active = [item for item in active if item[1] > start]
        for other_start, other_end, other_run in active:
            if run.get("dag_id") != other_run.get("dag_id"):
                conflicts.append((other_run, run))
        active.append((start, end, run))
    return conflicts


def make_timeline_svg(runs: list[dict], max_rows: int = 35) -> str:
    """Self-contained SVG timeline; no external JS/chart CDN required."""
    valid = []
    now = pendulum.now(TIMEZONE)
    for run in runs:
        start = parse_dt(run.get("start_date"))
        if not start:
            continue
        end = parse_dt(run.get("end_date")) or now
        if end < start:
            continue
        valid.append((start, end, run))
    valid.sort(key=lambda item: item[0], reverse=True)
    valid = valid[:max_rows]

    width, label_width, right_pad, row_h, top = 1100, 280, 25, 27, 38
    chart_width = width - label_width - right_pad
    if not valid:
        return '<p class="muted">ÐÐ° Ð²ÑÐ±ÑÐ°Ð½Ð½ÑÐ¹ Ð¿ÐµÑÐ¸Ð¾Ð´ Ð½ÐµÑ Ð·Ð°Ð¿ÑÑÐºÐ¾Ð² Ñ Ð¸Ð·Ð²ÐµÑÑÐ½ÑÐ¼ Ð²ÑÐµÐ¼ÐµÐ½ÐµÐ¼ ÑÑÐ°ÑÑÐ°.</p>'

    min_t = min(item[0] for item in valid)
    max_t = max(max(item[1], item[0] + timedelta(seconds=1)) for item in valid)
    span = max((max_t - min_t).total_seconds(), 1)
    height = top + row_h * len(valid) + 35

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="ÐÐµÐ½ÑÐ° Ð·Ð°Ð¿ÑÑÐºÐ¾Ð² DAG" '
        'style="width:100%;height:auto;min-width:700px">',
        '<style>.axis{fill:#667085;font:11px sans-serif}.daglabel{fill:#202939;font:12px sans-serif}'
        '.bar{fill:#4776d0}.running{fill:#e7a23b}.grid{stroke:#e5e7eb;stroke-width:1}'
        '.bartext{fill:white;font:10px sans-serif}</style>',
    ]
    for tick in range(5):
        x = label_width + chart_width * tick / 4
        tick_time = min_t + (max_t - min_t) * tick / 4
        parts.append(f'<line class="grid" x1="{x:.1f}" y1="24" x2="{x:.1f}" y2="{height-22}"/>')
        parts.append(f'<text class="axis" x="{x:.1f}" y="16" text-anchor="middle">{tick_time.format("MM-DD HH:mm")}</text>')

    for index, (start, end, run) in enumerate(valid):
        y = top + index * row_h
        x1 = label_width + (start - min_t).total_seconds() / span * chart_width
        x2 = label_width + (end - min_t).total_seconds() / span * chart_width
        bar_w = max(3, x2 - x1)
        state = str(run.get("state") or "").lower()
        cls = "running" if state in ("running", "queued") or not run.get("end_date") else "bar"
        dag_id = html.escape(str(run.get("dag_id", "")))
        run_id = html.escape(str(run.get("dag_run_id", "")))
        title = html.escape(f"{dag_id} | {run_id} | {state or 'unknown'}")
        parts.append(f'<text class="daglabel" x="8" y="{y+15}">{dag_id[:38]}</text>')
        parts.append(f'<rect class="{cls}" x="{x1:.1f}" y="{y+2}" width="{bar_w:.1f}" height="17" rx="3"><title>{title}</title></rect>')
        if bar_w > 70:
            parts.append(f'<text class="bartext" x="{x1+5:.1f}" y="{y+14}">{html.escape(state or "unknown")}</text>')
    parts.append(f'<text class="axis" x="{label_width}" y="{height-5}">ÐÐµÑÐ¸Ð¾Ð´: {min_t.format("YYYY-MM-DD HH:mm")} â {max_t.format("YYYY-MM-DD HH:mm")} ({TIMEZONE})</text>')
    parts.append("</svg>")
    return "".join(parts)


def build_html(dags: list[dict], runs: list[dict], errors: list[str]) -> str:
    conflicts = find_overlaps(runs)
    active_count = sum(1 for dag in dags if dag.get("is_active"))
    paused_count = sum(1 for dag in dags if dag.get("is_paused"))
    running_count = sum(1 for run in runs if run.get("state") == "running")
    failed_count = sum(1 for run in runs if run.get("state") == "failed")

    def esc(value):
        return html.escape(str(value if value not in (None, "") else "â"))

    dag_rows = []
    for dag in sorted(dags, key=lambda d: str(d.get("dag_id", "")).lower()):
        dag_id = dag.get("dag_id", "")
        latest = next((r for r in runs if r.get("dag_id") == dag_id), None)
        schedule = dag.get("timetable_description") or dag.get("schedule_interval") or dag.get("schedule") or "â"
        dag_rows.append(
            "<tr>"
            f"<td>{esc(dag_id)}</td>"
            f"<td><span class='pill {'ok' if dag.get('is_active') else 'off'}'>{'ÐÐ°' if dag.get('is_active') else 'ÐÐµÑ'}</span></td>"
            f"<td><span class='pill {'off' if dag.get('is_paused') else 'ok'}'>{'ÐÐ°' if dag.get('is_paused') else 'ÐÐµÑ'}</span></td>"
            f"<td>{esc(schedule)}</td>"
            f"<td>{esc(fmt_dt(latest.get('start_date')) if latest else 'ÐÐµÑ Ð·Ð°Ð¿ÑÑÐºÐ¾Ð² Ð² Ð¿ÐµÑÐ¸Ð¾Ð´Ðµ')}</td>"
            f"<td>{esc(latest.get('state') if latest else 'â')}</td>"
            "</tr>"
        )

    conflict_rows = []
    for first, second in conflicts:
        first_start = parse_dt(first.get("start_date"))
        first_end = parse_dt(first.get("end_date")) or pendulum.now(TIMEZONE)
        second_start = parse_dt(second.get("start_date"))
        second_end = parse_dt(second.get("end_date")) or pendulum.now(TIMEZONE)
        overlap_start = max(first_start, second_start)
        overlap_end = min(first_end, second_end)
        overlap = max(0, (overlap_end - overlap_start).total_seconds())
        conflict_rows.append(
            "<tr>"
            f"<td>{esc(first.get('dag_id'))}</td><td>{esc(second.get('dag_id'))}</td>"
            f"<td>{esc(fmt_dt(overlap_start))}</td><td>{esc(fmt_duration(overlap))}</td>"
            "</tr>"
        )

    error_block = ""
    if errors:
        error_block = "<section class='panel'><h2>ÐÑÐ¸Ð±ÐºÐ¸ Ð¿Ð¾Ð»ÑÑÐµÐ½Ð¸Ñ Ð´Ð°Ð½Ð½ÑÑ</h2><ul>" + "".join(
            f"<li>{esc(err)}</li>" for err in errors
        ) + "</ul></section>"

    generated = pendulum.now(TIMEZONE).format("YYYY-MM-DD HH:mm:ss")
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Airflow â ÑÐ°ÑÐ¿Ð¸ÑÐ°Ð½Ð¸Ðµ Ð¸ ÐºÐ¾Ð½ÑÐ»Ð¸ÐºÑÑ DAG</title>
<style>
:root{{--bg:#f4f6fa;--panel:#fff;--text:#172033;--muted:#667085;--line:#e5e7eb;--blue:#4776d0}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Arial,sans-serif}}
header{{padding:26px 30px;background:#172b4d;color:white}}header h1{{margin:0 0 6px;font-size:25px}}header p{{margin:0;color:#d7e2f4}}
main{{padding:24px;max-width:1600px;margin:auto}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:20px}}
.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:18px;box-shadow:0 2px 8px #17203308}}
.metric{{font-size:28px;font-weight:700;margin-top:4px}}.muted{{color:var(--muted)}}h2{{font-size:18px;margin:0 0 14px}}.panel{{margin-bottom:18px;overflow:hidden}}
.tablewrap{{overflow:auto}}table{{width:100%;border-collapse:collapse;white-space:nowrap}}th,td{{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line)}}th{{background:#f8fafc;color:#475467;font-size:12px;position:sticky;top:0}}
.pill{{padding:3px 8px;border-radius:99px;font-size:12px}}.ok{{background:#dcfce7;color:#166534}}.off{{background:#fee2e2;color:#991b1b}}
.notice{{padding:12px 14px;background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;margin-bottom:18px}}
footer{{color:var(--muted);padding:8px 0 24px}}@media(max-width:600px){{main{{padding:12px}}header{{padding:20px}}}}
</style></head><body>
<header><h1>Airflow: ÑÐ°ÑÐ¿Ð¸ÑÐ°Ð½Ð¸Ðµ Ð¸ ÐºÐ¾Ð½ÑÐ»Ð¸ÐºÑÑ DAG</h1>
<p>ÐÑÑÐ¾ÑÐ¸Ñ Ð·Ð° {HISTORY_DAYS} Ð´Ð½. Â· ÑÐ°ÑÐ¾Ð²Ð¾Ð¹ Ð¿Ð¾ÑÑ {esc(TIMEZONE)} Â· ÑÑÐ¾ÑÐ¼Ð¸ÑÐ¾Ð²Ð°Ð½Ð¾ {esc(generated)}</p></header>
<main>
<div class="cards">
<div class="card"><div class="muted">ÐÑÐµÐ³Ð¾ DAG</div><div class="metric">{len(dags)}</div></div>
<div class="card"><div class="muted">ÐÐºÑÐ¸Ð²Ð½ÑÐµ</div><div class="metric">{active_count}</div></div>
<div class="card"><div class="muted">ÐÑÐ¸Ð¾ÑÑÐ°Ð½Ð¾Ð²Ð»ÐµÐ½Ð½ÑÐµ</div><div class="metric">{paused_count}</div></div>
<div class="card"><div class="muted">ÐÐ°Ð¿ÑÑÐºÐ¸ Ð² Ð¸ÑÑÐ¾ÑÐ¸Ð¸</div><div class="metric">{len(runs)}</div></div>
<div class="card"><div class="muted">Ð¡ÐµÐ¹ÑÐ°Ñ Ð²ÑÐ¿Ð¾Ð»Ð½ÑÑÑÑÑ</div><div class="metric">{running_count}</div></div>
<div class="card"><div class="muted">ÐÐµÑÑÐ¿ÐµÑÐ½ÑÐµ Ð·Ð°Ð¿ÑÑÐºÐ¸</div><div class="metric">{failed_count}</div></div>
<div class="card"><div class="muted">ÐÐ°ÑÑ Ð¿ÐµÑÐµÑÐµÑÐµÐ½Ð¸Ð¹</div><div class="metric">{len(conflicts)}</div></div>
</div>
<div class="notice"><b>ÐÐ°Ðº ÑÐ¸ÑÐ°ÑÑÑÑ ÐºÐ¾Ð½ÑÐ»Ð¸ÐºÑÑ:</b> ÑÑÐ¾ ÑÐ°ÐºÑÐ¸ÑÐµÑÐºÐ¾Ðµ Ð¿ÐµÑÐµÑÐµÑÐµÐ½Ð¸Ðµ Ð¸Ð½ÑÐµÑÐ²Ð°Ð»Ð¾Ð² Ð²ÑÐ¿Ð¾Ð»Ð½ÐµÐ½Ð¸Ñ Ð´Ð²ÑÑ ÑÐ°Ð·Ð½ÑÑ DAG Ð² Ð¸ÑÑÐ¾ÑÐ¸Ð¸. Ð¡Ð¾Ð²Ð¿Ð°Ð´ÐµÐ½Ð¸Ðµ Ð²ÑÐµÐ¼ÐµÐ½Ð¸ ÑÑÐ°ÑÑÐ° ÑÐ°Ð¼Ð¾ Ð¿Ð¾ ÑÐµÐ±Ðµ Ð½Ðµ Ð´Ð¾ÐºÐ°Ð·ÑÐ²Ð°ÐµÑ ÐºÐ¾Ð½ÑÐ»Ð¸ÐºÑ ÑÐµÑÑÑÑÐ¾Ð²; Ð´Ð»Ñ Ð¾ÑÐµÐ½ÐºÐ¸ Ð½Ð°Ð³ÑÑÐ·ÐºÐ¸ Ð½ÑÐ¶Ð½Ñ Ð´Ð¾Ð¿Ð¾Ð»Ð½Ð¸ÑÐµÐ»ÑÐ½ÑÐµ Ð´Ð°Ð½Ð½ÑÐµ Ð¾ Ð¿ÑÐ»Ð°Ñ, ÑÐ»Ð¾ÑÐ°Ñ Ð¸ ÑÐµÑÑÑÑÐ°Ñ.</div>
<section class="panel"><h2>ÐÑÐ°ÑÐ¸Ðº ÑÐ°ÐºÑÐ¸ÑÐµÑÐºÐ¸Ñ Ð·Ð°Ð¿ÑÑÐºÐ¾Ð²</h2><p class="muted">ÐÐ¾Ð»Ð¾ÑÑ Ð¿Ð¾ÐºÐ°Ð·ÑÐ²Ð°ÑÑ Ð¸Ð½ÑÐµÑÐ²Ð°Ð» Ð¾Ñ ÑÑÐ°ÑÑÐ° Ð´Ð¾ Ð·Ð°Ð²ÐµÑÑÐµÐ½Ð¸Ñ. ÐÑÐ°Ð½Ð¶ÐµÐ²ÑÐ¹ â Ð²ÑÐ¿Ð¾Ð»Ð½ÑÐµÑÑÑ ÑÐµÐ¹ÑÐ°Ñ Ð¸Ð»Ð¸ Ð¾Ð¶Ð¸Ð´Ð°ÐµÑ Ð·Ð°Ð¿ÑÑÐºÐ°.</p>
<div style="overflow-x:auto">{make_timeline_svg(runs)}</div></section>
<section class="panel"><h2>ÐÐ¾Ð½ÑÐ»Ð¸ÐºÑÑ â Ð¾Ð´Ð½Ð¾Ð²ÑÐµÐ¼ÐµÐ½Ð½Ð¾Ðµ Ð²ÑÐ¿Ð¾Ð»Ð½ÐµÐ½Ð¸Ðµ ÑÐ°Ð·Ð½ÑÑ DAG</h2>
<div class="tablewrap"><table><thead><tr><th>DAG 1</th><th>DAG 2</th><th>ÐÐ°ÑÐ°Ð»Ð¾ Ð¿ÐµÑÐµÑÐµÑÐµÐ½Ð¸Ñ</th><th>ÐÐ»Ð¸ÑÐµÐ»ÑÐ½Ð¾ÑÑÑ Ð¿ÐµÑÐµÑÐµÑÐµÐ½Ð¸Ñ</th></tr></thead>
<tbody>{''.join(conflict_rows) if conflict_rows else '<tr><td colspan="4">ÐÐµÑÐµÑÐµÑÐµÐ½Ð¸Ð¹ Ð·Ð° Ð²ÑÐ±ÑÐ°Ð½Ð½ÑÐ¹ Ð¿ÐµÑÐ¸Ð¾Ð´ Ð½Ðµ Ð¾Ð±Ð½Ð°ÑÑÐ¶ÐµÐ½Ð¾.</td></tr>'}</tbody></table></div></section>
<section class="panel"><h2>Ð¡Ð¾ÑÑÐ¾ÑÐ½Ð¸Ðµ DAG Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½ÐµÐµ Ð²ÑÐ¿Ð¾Ð»Ð½ÐµÐ½Ð¸Ðµ Ð² Ð¿ÐµÑÐ¸Ð¾Ð´Ðµ</h2>
<div class="tablewrap"><table><thead><tr><th>DAG ID</th><th>ÐÐºÑÐ¸Ð²ÐµÐ½</th><th>ÐÑÐ¸Ð¾ÑÑÐ°Ð½Ð¾Ð²Ð»ÐµÐ½</th><th>Ð Ð°ÑÐ¿Ð¸ÑÐ°Ð½Ð¸Ðµ</th><th>ÐÐ¾ÑÐ»ÐµÐ´Ð½Ð¸Ð¹ ÑÑÐ°ÑÑ</th><th>Ð¡Ð¾ÑÑÐ¾ÑÐ½Ð¸Ðµ</th></tr></thead>
<tbody>{''.join(dag_rows) if dag_rows else '<tr><td colspan="6">Ð¡Ð¿Ð¸ÑÐ¾Ðº DAG Ð¿ÑÑÑ.</td></tr>'}</tbody></table></div></section>
{error_block}
<footer>ÐÑÑÐ¾ÑÐ½Ð¸Ðº: Airflow Stable REST API. ÐÐ°ÑÐ±Ð¾ÑÐ´ ÑÐ¾Ð»ÑÐºÐ¾ ÑÐ¸ÑÐ°ÐµÑ API Ð¸ Ð½Ðµ Ð¿Ð¾Ð´ÐºÐ»ÑÑÐ°ÐµÑÑÑ Ð½Ð°Ð¿ÑÑÐ¼ÑÑ Ðº Ð¼ÐµÑÐ°Ð±Ð°Ð·Ðµ PostgreSQL.</footer>
</main></body></html>"""


def create_dashboard():
    session = api_session()
    errors = []
    dags = get_all_dags(session)
    since = pendulum.now("UTC").subtract(days=HISTORY_DAYS).to_iso8601_string()
    all_runs = []
    for dag in dags:
        dag_id = dag.get("dag_id")
        if not dag_id:
            continue
        try:
            all_runs.extend(get_runs(session, dag_id, since))
        except requests.RequestException as exc:
            message = f"{dag_id}: {type(exc).__name__}: {exc}"
            logger.warning("Could not fetch DAG runs: %s", message)
            errors.append(message)

    # De-duplicate in case the API returned a run twice.
    unique = {}
    for run in all_runs:
        key = (run.get("dag_id"), run.get("dag_run_id"))
        unique[key] = run
    all_runs = list(unique.values())

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output = REPORT_DIR / REPORT_NAME
    output.write_text(build_html(dags, all_runs, errors), encoding="utf-8")
    logger.info("Airflow DAG dashboard written to %s", output)


with DAG(
    dag_id="airflow_dag_schedule_dashboard",
    description="HTML dashboard for DAG schedule, run history and overlapping executions via Airflow REST API",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["monitoring", "report", "api"],
) as dag:
    PythonOperator(
        task_id="build_html_dashboard",
        python_callable=create_dashboard,
    )












 
from __future__ import annotations

import logging
import os
import tempfile
import zipfile

from datetime import date, datetime, timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Moscow"
RETENTION_DAYS = 30

DAGS_FOLDER = Path(
    conf.get("core", "dags_folder")
).resolve()

BACKUP_FOLDER = Path(
    os.environ.get(
        "AIRFLOW_DAG_BACKUP_DIR",
        "/opt/airflow/dag_backups",
    )
).resolve()

EXCLUDED_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".git",
    ".venv",
    "venv",
}

EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def is_inside(path: Path, parent: Path) -> bool:
    """Проверяет, находится ли путь внутри parent или совпадает с ним."""
    try:
        return os.path.commonpath(
            [str(path.resolve()), str(parent.resolve())]
        ) == str(parent.resolve())
    except (ValueError, OSError):
        return False


def collect_files() -> list[Path]:
    """Собирает файлы DAG с сохранением структуры каталогов."""
    if not DAGS_FOLDER.is_dir():
        raise RuntimeError(
            f"Папка DAG не существует: {DAGS_FOLDER}"
        )

    if DAGS_FOLDER == BACKUP_FOLDER:
        raise RuntimeError(
            "Папка бэкапа не должна совпадать с папкой DAG"
        )

    files = []

    for root, dirs, filenames in os.walk(
        DAGS_FOLDER,
        followlinks=False,
    ):
        root_path = Path(root)

        dirs[:] = sorted(
            d for d in dirs
            if d not in EXCLUDED_DIRS
            and not (root_path / d).is_symlink()
            and not is_inside(root_path / d, BACKUP_FOLDER)
        )

        for filename in sorted(filenames):
            path = root_path / filename

            if path.is_symlink():
                continue

            if path.suffix.lower() in EXCLUDED_SUFFIXES:
                continue

            if not path.is_file():
                continue

            files.append(path)

    return sorted(files)


def cleanup_old_backups(today: date) -> None:
    """Удаляет только папки резервных копий старше срока хранения."""
    cutoff = today - timedelta(days=RETENTION_DAYS)

    for folder in BACKUP_FOLDER.iterdir():
        if not folder.is_dir() or folder.is_symlink():
            continue

        try:
            folder_date = datetime.strptime(
                folder.name, "%Y-%m-%d"
            ).date()
        except ValueError:
            # Не трогаем папки с другими именами.
            continue

        if folder_date >= cutoff:
            continue

        try:
            # Удаляем только ожидаемую структуру:
            # дата / dags_backup.zip
            archive = folder / "dags_backup.zip"

            if not archive.is_file() or archive.is_symlink():
                logger.warning(
                    "Пропуск старой папки с неизвестным содержимым: %s",
                    folder,
                )
                continue

            for item in folder.iterdir():
                if item.is_file() and not item.is_symlink():
                    item.unlink()
                else:
                    logger.warning(
                        "Неизвестный объект в папке бэкапа: %s",
                        item,
                    )

            # Удаляем папку только если она стала пустой.
            folder.rmdir()
            logger.info("Удалена старая копия: %s", folder)

        except OSError:
            logger.exception(
                "Не удалось удалить старую копию: %s", folder
            )


def backup_dags() -> None:
    now = pendulum.now(TIMEZONE)
    today = now.format("YYYY-MM-DD")

    BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

    if is_inside(BACKUP_FOLDER, DAGS_FOLDER):
        logger.warning(
            "Папка бэкапа находится внутри папки DAG. "
            "Она будет исключена из архива."
        )

    daily_folder = BACKUP_FOLDER / today
    daily_folder.mkdir(parents=True, exist_ok=True)

    archive_path = daily_folder / "dags_backup.zip"
    temp_path = None

    files = collect_files()

    if not files:
        raise RuntimeError(
            f"Нет файлов для резервного копирования: {DAGS_FOLDER}"
        )

    try:
        # Временный архив создаётся в той же папке,
        # чтобы os.replace мог атомарно заменить файл.
        with tempfile.NamedTemporaryFile(
            prefix=".dags_backup_",
            suffix=".tmp",
            dir=daily_folder,
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

        with zipfile.ZipFile(
            temp_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            for path in files:
                relative_path = path.relative_to(DAGS_FOLDER)

                archive.write(
                    path,
                    arcname=relative_path.as_posix(),
                )

        # Проверяем целостность ZIP и наличие файлов.
        with zipfile.ZipFile(temp_path, "r") as archive:
            members = archive.namelist()

            if not members:
                raise RuntimeError("Создан пустой ZIP-архив")

            bad_file = archive.testzip()

            if bad_file is not None:
                raise RuntimeError(
                    f"Ошибка целостности архива: {bad_file}"
                )

        # Основной архив заменяется только после проверки.
        os.replace(temp_path, archive_path)
        temp_path = None

        logger.info(
            "Бэкап успешно создан: %s",
            archive_path,
        )
        logger.info("Количество файлов: %d", len(files))
        logger.info(
            "Размер архива: %d байт",
            archive_path.stat().st_size,
        )

    finally:
        # Убираем временный файл при ошибке.
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                logger.exception(
                    "Не удалось удалить временный архив: %s",
                    temp_path,
                )

    # Ротация выполняется только после успешного бэкапа.
    cleanup_old_backups(now.date())


with DAG(
    dag_id="dags_backup_to_zip",
    description="Ежедневный проверяемый бэкап DAG-файлов в ZIP",
    start_date=pendulum.datetime(
        2026, 1, 1, tz=TIMEZONE
    ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["maintenance", "backup"],
) as dag:

    PythonOperator(
        task_id="backup_dags_to_zip",
        python_callable=backup_dags,
    )