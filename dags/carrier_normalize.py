"""Hourly carrier pipeline: ingest landing files to bronze, then normalize to
silver. R1 adds the alert task; R2 adds the 15-min deadline + failure callback
and Prometheus metrics push.

The repo is mounted at /opt/airflow/manifest inside the Airflow container
(see docker-compose.yml); tasks shell out to the same modules used locally so
there is exactly one implementation of the pipeline.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/manifest"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="carrier_normalize",
    description="Ingest carrier feed to bronze, normalize to silver, detect exceptions",
    schedule="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
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
    )

    ingest >> normalize
