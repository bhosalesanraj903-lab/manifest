"""R8: daily weather/disruption enrichment -> bronze -> port_conditions."""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/manifest"
sys.path.insert(0, REPO)

with DAG(
    dag_id="port_enrichment",
    description="Poll NWS / Open-Meteo / GDELT and build silver port_conditions",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["manifest", "phase-3"],
) as dag:
    pollers = [
        BashOperator(task_id=f"poll_{src}",
                     bash_command=f"cd {REPO} && python -m pipelines.enrich --source {src}")
        for src in ("nws", "open_meteo", "gdelt")
    ]
    conditions = BashOperator(
        task_id="build_conditions",
        bash_command=f"cd {REPO} && python -m pipelines.enrich --source conditions",
    )
    pollers >> conditions
