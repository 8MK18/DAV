# 
from __future__ import annotations

import html
import json
import logging
import os
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState

# ============================================================
# CONFIGURATION
# ============================================================

REPORT_DIR = Path(
    os.environ.get("AIRFLOW_REPORT_DIR", "/opt/airflow/reports")
)
REPORT_FILE = REPORT_DIR / "dag_health_report.html"

ANALYSIS_DAYS = int(os.environ.get("DAG_REPORT_DAYS", "30"))
LOCAL_TIMEZONE = os.environ.get("AIRFLOW_REPORT_TIMEZONE", "Europe/Moscow")

logger = logging.getLogger(__name__)


def safe_duration(start, end):
    """Return duration in seconds, or None."""
    if not start or not end:
        return None

    try:
        return max(0.0, (end - start).total_seconds())
    except (TypeError, ValueError):
        return None


def fmt_duration(seconds):
    if seconds is None:
        return "—"

    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours} ч {minutes} мин"
    if minutes:
        return f"{minutes} мин {seconds} с"
    return f"{seconds} с"


def fmt_datetime(value, timezone):
    if value is None:
        return "—"

    try:
        return pendulum.instance(value).in_timezone(timezone).format(
            "YYYY-MM-DD HH:mm:ss"
        )
    except Exception:
        return str(value)


def percentile(values, percent):
    if not values:
        return None

    values = sorted(values)
    index = round((len(values) - 1) * percent)
    return values[index]


def collect_airflow_data():
    """Read Airflow metadata without changing DAGs or task states."""
    now = pendulum.now("UTC")
    cutoff = now.subtract(days=ANALYSIS_DAYS)

    dags = defaultdict(
        lambda: {
            "runs": [],
            "tasks": [],
            "success": 0,
            "failed": 0,
            "running": 0,
            "queued": 0,
            "durations": [],
            "task_durations": [],
            "schedule": "Не определено",
            "last_success": None,
            "latest_run": None,
            "missing_days": [],
            "recommendations": [],
        }
    )

    # Read-only metadata queries. Use a bounded analysis window.
    with create_session() as session:
        runs = (
            session.query(DagRun)
            .filter(
                DagRun.start_date >= cutoff,
            )
            .order_by(DagRun.start_date.desc())
            .all()
        )

        # Include runs with no start_date where possible, for example
        # queued or scheduled runs. Restrict by logical/execution date.
        try:
            extra_runs = (
                session.query(DagRun)
                .filter(
                    DagRun.start_date.is_(None),
                    DagRun.execution_date >= cutoff,
                )
                .all()
            )
            known_ids = {getattr(r, "id", None) for r in runs}
            runs.extend(
                r for r in extra_runs
                if getattr(r, "id", None) not in known_ids
            )
        except Exception:
            logger.warning("Could not retrieve unstarted DAG runs")

        for run in runs:
            dag_id = run.dag_id
            item = dags[dag_id]

            state = str(run.state or "unknown").lower()
            start = run.start_date
            end = run.end_date
            duration = safe_duration(start, end)

            logical_date = (
                getattr(run, "logical_date", None)
                or getattr(run, "execution_date", None)
            )

            run_info = {
                "dag_id": dag_id,
                "run_id": getattr(run, "run_id", "unknown"),
                "state": state,
                "start": fmt_datetime(start, LOCAL_TIMEZONE),
                "end": fmt_datetime(end, LOCAL_TIMEZONE),
                "logical_date": fmt_datetime(
                    logical_date, LOCAL_TIMEZONE
                ),
                "duration": duration,
            }
            item["runs"].append(run_info)

            if state == str(DagRunState.SUCCESS).lower():
                item["success"] += 1
                if (
                    item["last_success"] is None
                    or (
                        start is not None
                        and start > item["last_success"]
                    )
                ):
                    item["last_success"] = start

            elif state == str(DagRunState.FAILED).lower():
                item["failed"] += 1
            elif state == str(DagRunState.RUNNING).lower():
                item["running"] += 1
            elif state in ("queued", "scheduled"):
                item["queued"] += 1

            if duration is not None:
                item["durations"].append(duration)

            if (
                item["latest_run"] is None
                or (
                    start is not None
                    and start > item["latest_run"]
                )
            ):
                item["latest_run"] = start

        # Task instances from the analysis window.
        task_rows = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.start_date >= cutoff,
            )
            .order_by(TaskInstance.start_date.desc())
            .all()
        )

        for ti in task_rows:
            dag_id = ti.dag_id
            item = dags[dag_id]

            start = ti.start_date
            end = ti.end_date
            duration = safe_duration(start, end)
            state = str(ti.state or "unknown").lower()

            task_info = {
                "dag_id": dag_id,
                "task_id": ti.task_id,
                "run_id": getattr(ti, "run_id", ""),
                "state": state,
                "start_obj": start,
                "end_obj": end,
                "start": fmt_datetime(start, LOCAL_TIMEZONE),
                "end": fmt_datetime(end, LOCAL_TIMEZONE),
                "duration": duration,
            }
            item["tasks"].append(task_info)

            if duration is not None:
                item["task_durations"].append(duration)

    # Sort data and derive statistics.
    for dag_id, item in dags.items():
        item["runs"].sort(
            key=lambda x: x["start"], reverse=True
        )
        item["tasks"].sort(
            key=lambda x: x["duration"] or 0, reverse=True
        )

        # These metadata-only heuristics do not prove that a run
        # was missed; the schedule itself must be checked separately.
        durations = item["durations"]
        if durations:
            item["avg_duration"] = sum(durations) / len(durations)
            item["p95_duration"] = percentile(durations, 0.95)
        else:
            item["avg_duration"] = None
            item["p95_duration"] = None

        failures = item["failed"]
        total = len(item["runs"])

        if failures:
            item["recommendations"].append(
                f"Проверить ошибки: {failures} неуспешных запусков."
            )

        if item["avg_duration"] and item["p95_duration"]:
            if item["p95_duration"] > item["avg_duration"] * 2:
                item["recommendations"].append(
                    "Большой разброс длительности: изучить медленные запуски."
                )

        if total == 0:
            item["recommendations"].append(
                "Нет запусков за выбранный период; проверить расписание, "
                "активность DAG и доступность метаданных."
            )

    # Find overlapping task intervals using a sweep-line event list.
    events = []
    for dag_id, item in dags.items():
        for task in item["tasks"]:
            start = task["start_obj"]
            end = task["end_obj"]

            if start and end and end >= start:
                events.append((start, 1, dag_id, task["task_id"]))
                events.append((end, -1, dag_id, task["task_id"]))

    events.sort(key=lambda x: (x[0], x[1]))

    active = set()
    concurrency = []
    for timestamp, change, dag_id, task_id in events:
        key = (dag_id, task_id, timestamp)
        if change == 1:
            active.add(key)
            concurrency.append(
                {
                    "time": fmt_datetime(timestamp, LOCAL_TIMEZONE),
                    "concurrent_tasks": len(active),
                    "dag_id": dag_id,
                    "task_id": task_id,
                }
            )
        else:
            active.discard(key)

    concurrency.sort(
        key=lambda x: x["concurrent_tasks"], reverse=True
    )

    # Hourly load proxy, based on observed task start times.
    hourly_load = defaultdict(int)
    for _, item in dags.items():
        for task in item["tasks"]:
            start = task["start_obj"]
            if start:
                local_start = pendulum.instance(start).in_timezone(
                    LOCAL_TIMEZONE
                )
                hourly_load[local_start.format("HH:00")] += 1

    peak_hours = sorted(
        hourly_load.items(), key=lambda x: x[1], reverse=True
    )

    return {
        "generated_at": fmt_datetime(now, LOCAL_TIMEZONE),
        "cutoff": fmt_datetime(cutoff, LOCAL_TIMEZONE),
        "timezone": LOCAL_TIMEZONE,
        "days": ANALYSIS_DAYS,
        "dags": dict(dags),
        "concurrency": concurrency[:100],
        "hourly_load": peak_hours,
    }


def build_html(data):
    """Build a self-contained HTML dashboard."""
    dags = data["dags"]

    total_runs = sum(len(x["runs"]) for x in dags.values())
    total_failed = sum(x["failed"] for x in dags.values())
    total_tasks = sum(len(x["tasks"]) for x in dags.values())

    def esc(value):
        return html.escape(str(value if value is not None else "—"))

    summary_rows = []
    detail_sections = []

    for dag_id, item in sorted(dags.items()):
        run_count = len(item["runs"])
        success = item["success"]
        failed = item["failed"]
        avg = fmt_duration(item.get("avg_duration"))
        p95 = fmt_duration(item.get("p95_duration"))
        last_success = fmt_datetime(
            item["last_success"], data["timezone"]
        )
        last_run = fmt_datetime(
            item["latest_run"], data["timezone"]
        )

        status = "OK" if run_count and not failed else (
            "Ошибки" if failed else "Нет запусков"
        )

        summary_rows.append(
            "<tr>"
            f"<td>{esc(dag_id)}</td>"
            f"<td>{esc(status)}</td>"
            f"<td>{run_count}</td>"
            f"<td>{success}</td>"
            f"<td>{failed}</td>"
            f"<td>{esc(last_run)}</td>"
            f"<td>{esc(last_success)}</td>"
            f"<td>{esc(avg)}</td>"
            f"<td>{esc(p95)}</td>"
            "</tr>"
        )

        task_rows = []
        for task in item["tasks"][:30]:
            task_rows.append(
                "<tr>"
                f"<td>{esc(task['task_id'])}</td>"
                f"<td>{esc(task['state'])}</td>"
                f"<td>{esc(task['start'])}</td>"
                f"<td>{esc(task['end'])}</td>"
                f"<td>{esc(fmt_duration(task['duration']))}</td>"
                "</tr>"
            )

        run_rows = []
        for run in item["runs"][:20]:
            run_rows.append(
                "<tr>"
                f"<td>{esc(run['run_id'])}</td>"
                f"<td>{esc(run['state'])}</td>"
                f"<td>{esc(run['logical_date'])}</td>"
                f"<td>{esc(run['start'])}</td>"
                f"<td>{esc(run['end'])}</td>"
                f"<td>{esc(fmt_duration(run['duration']))}</td>"
                "</tr>"
            )

        recommendations = "".join(
            f"<li>{esc(rec)}</li>"
            for rec in item["recommendations"]
        ) or "<li>Явных проблем по доступным метаданным не выявлено.</li>"

        detail_sections.append(
            f"""
            <section class="dag-card">
              <h3>{esc(dag_id)}</h3>
              <p>Статус: <b>{esc(status)}</b> |
                 Успешно: {success} |
                 Ошибки: {failed} |
                 Запусков: {run_count}</p>
              <p>Последний успешный: {esc(last_success)} |
                 Средняя длительность: {esc(avg)} |
                 P95: {esc(p95)}</p>
              <h4>Рекомендации</h4>
              <ul>{recommendations}</ul>
              <details>
                <summary>История запусков (до 20)</summary>
                <div class="table-wrap"><table>
                  <tr><th>Run ID</th><th>Состояние</th>
                  <th>Логическая дата</th><th>Старт</th>
                  <th>Завершение</th><th>Длительность</th></tr>
                  {''.join(run_rows)}
                </table></div>
              </details>
              <details>
                <summary>Задачи (до 30 самых длительных)</summary>
                <div class="table-wrap"><table>
                  <tr><th>Задача</th><th>Состояние</th>
                  <th>Старт</th><th>Завершение</th>
                  <th>Длительность</th></tr>
                  {''.join(task_rows)}
                </table></div>
              </details>
            </section>
            """
        )

    hourly_rows = "".join(
        f"<tr><td>{esc(hour)}</td><td>{count}</td></tr>"
        for hour, count in data["hourly_load"]
    )

    concurrency_rows = "".join(
        "<tr>"
        f"<td>{esc(row['time'])}</td>"
        f"<td>{row['concurrent_tasks']}</td>"
        f"<td>{esc(row['dag_id'])}</td>"
        f"<td>{esc(row['task_id'])}</td>"
        "</tr>"
        for row in data["concurrency"][:30]
    )

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Airflow DAG Health Report</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #202938; }}
h1, h2, h3 {{ color: #183153; }}
.cards {{ display: flex; flex-wrap: wrap; gap: 12px; }}
.metric {{ border: 1px solid #d8dee8; border-radius: 8px;
           padding: 16px; min-width: 140px; }}
.metric strong {{ display: block; font-size: 25px; margin-top: 8px; }}
input {{ padding: 10px; width: min(420px, 90%); margin: 12px 0; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0 24px; }}
th, td {{ border: 1px solid #d8dee8; padding: 8px; text-align: left; }}
th {{ background: #eef2f7; }}
tr:nth-child(even) {{ background: #f8fafc; }}
.table-wrap {{ overflow-x: auto; }}
.dag-card {{ border: 1px solid #d8dee8; border-radius: 8px;
             padding: 16px; margin: 14px 0; }}
summary {{ cursor: pointer; font-weight: bold; padding: 8px 0; }}
.note {{ background: #fff8e6; padding: 12px; border-radius: 6px; }}
</style>
</head>
<body>
<h1>Airflow — анализ DAG-ов</h1>
<p>Сформировано: <b>{esc(data['generated_at'])}</b> |
Период: {esc(data['cutoff'])} — {esc(data['generated_at'])} |
Часовой пояс: {esc(data['timezone'])}</p>

<div class="cards">
  <div class="metric">DAG-ов<strong>{len(dags)}</strong></div>
  <div class="metric">Запусков<strong>{total_runs}</strong></div>
  <div class="metric">Ошибок DAG<strong>{total_failed}</strong></div>
  <div class="metric">Задач в выборке<strong>{total_tasks}</strong></div>
</div>

<p class="note">
Нагрузка CPU/RAM не измеряется напрямую. Здесь используется прокси-оценка:
число задач, их длительность и пересечения интервалов выполнения.
Количество запусков по часу — это количество стартов задач, а не процент
загрузки процессора. Пропущенные запуски нельзя достоверно вычислить без
сопоставления истории с расписанием каждого DAG.
</p>

<h2>Сводка по DAG-ам</h2>
<input id="search" placeholder="Фильтр по DAG ID..." oninput="filterRows()">
<div class="table-wrap">
<table id="summary">
<tr><th>DAG ID</th><th>Статус по истории</th><th>Запусков</th>
<th>Успешно</th><th>Ошибки</th><th>Последний старт</th>
<th>Последний успех</th><th>Среднее</th><th>P95</th></tr>
{''.join(summary_rows)}
</table>
</div>

<h2>Пиковые часы старта задач</h2>
<p>Число стартовавших задач по локальному часу за выбранный период.</p>
<div class="table-wrap"><table>
<tr><th>Час</th><th>Количество стартов</th></tr>
{hourly_rows}
</table></div>

<h2>Наибольшая наблюдаемая параллельность</h2>
<p>События пересечения задач, вычисленные по сохранённым интервалам start/end.
Это оценка по истории, а не точный снимок текущей загрузки.</p>
<div class="table-wrap"><table>
<tr><th>Время</th><th>Активных задач по оценке</th>
<th>DAG</th><th>Задача</th></tr>
{concurrency_rows}
</table></div>

<h2>Детальный анализ</h2>
{''.join(detail_sections)}

<script>
function filterRows() {{
  const q = document.getElementById('search').value.toLowerCase();
  document.querySelectorAll('#summary tr').forEach((row, i) => {{
    if (i === 0) return;
    row.style.display = row.cells[0].textContent.toLowerCase().includes(q)
      ? '' : 'none';
  }});
}}
</script>
</body>
</html>"""


def generate_report():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    data = collect_airflow_data()
    report_html = build_html(data)

    temp_file = REPORT_FILE.with_suffix(".html.tmp")
    temp_file.write_text(report_html, encoding="utf-8")
    temp_file.replace(REPORT_FILE)

    logger.info("HTML report generated: %s", REPORT_FILE)
    logger.info(
        "DAGs: %s; runs: %s; analysis days: %s",
        len(data["dags"]),
        sum(len(x["runs"]) for x in data["dags"].values()),
        ANALYSIS_DAYS,
    )


with DAG(
    dag_id="daily_dag_health_html_report",
    description="Daily analysis of DAG runs, durations, errors and concurrency",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TIMEZONE),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["monitoring", "report", "health"],
) as dag:

    create_html_report = PythonOperator(
        task_id="create_html_report",
        python_callable=generate_report,
        execution_timeout=timedelta(minutes=15),
    )
