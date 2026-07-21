"""R5: hourly AIS dwell/congestion aggregation over today's bronze positions."""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/manifest"
sys.path.insert(0, REPO)

with DAG(
    dag_id="ais_dwell",
    description="Aggregate AIS anchor dwell into port_congestion / vessel_dwell / dim_vessel",
    schedule="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["manifest", "phase-2"],
) as dag:
    BashOperator(
        task_id="dwell",
        bash_command=f"cd {REPO} && python -m ais.dwell",
        env={"PUSHGATEWAY_URL": "http://pushgateway:9091"},
        append_env=True,
    )
