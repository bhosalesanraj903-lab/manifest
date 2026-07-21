"""Hourly carrier pipeline: ingest -> normalize -> alert (R1/R2).

R2 hardening:
- dagrun_timeout of 15 min stands in for the removed Airflow-3 SLA feature
  (ADR-001 decision 3): a run exceeding 15 min is failed, which triggers the
  Slack callback.
- on_failure_callback posts to Slack through the same R1 sender used for
  exception alerts, so operators have one notification channel.
- normalize pushes run metrics to the Prometheus pushgateway (PUSHGATEWAY_URL).

The repo is mounted at /opt/airflow/manifest inside the container; tasks shell
out to the same modules used locally so there is one pipeline implementation.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/manifest"
sys.path.insert(0, REPO)


def notify_failure(context) -> None:
    from alerting.notify import send_slack
    import os
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    ti = context.get("task_instance")
    msg = (f":x: carrier_normalize FAILED — task `{ti.task_id}` "
           f"run {context.get('run_id')} — check Airflow logs")
    if url:
        send_slack(msg, url)
    else:
        print(f"[log-only] {msg}")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="carrier_normalize",
    description="Ingest carrier feed to bronze, normalize to silver, alert on exceptions",
    schedule="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=15),
    default_args=default_args,
    tags=["manifest", "phase-1"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"cd {REPO} && python -m pipelines.ingest",
    )
    normalize = BashOperator(
        task_id="normalize",
        bash_command=f"cd {REPO} && python -m pipelines.normalize",
        env={"PUSHGATEWAY_URL": "http://pushgateway:9091"},
        append_env=True,
    )
    alert = BashOperator(
        task_id="alert",
        bash_command=f"cd {REPO} && python -m alerting.notify",
        append_env=True,
    )

    ingest >> normalize >> alert
