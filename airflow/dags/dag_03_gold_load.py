from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

GOLD_JOB_ID = 698505861011490   # replace with your actual Job ID

default_args = {
    "owner":            "gridwatch360",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_03_gold_load",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gridwatch360", "gold"],
) as dag:

    run_gold = DatabricksRunNowOperator(
        task_id="run_gold_job",
        databricks_conn_id="databricks_default",
        job_id=GOLD_JOB_ID,
    )

    run_gold