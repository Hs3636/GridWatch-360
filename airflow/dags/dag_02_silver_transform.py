from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

SILVER_JOB_ID = 127696362434975   # replace with your actual Job ID

default_args = {
    "owner":            "gridwatch360",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_02_silver_transform",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gridwatch360", "silver"],
) as dag:

    run_silver = DatabricksRunNowOperator(
        task_id="run_silver_job",
        databricks_conn_id="databricks_default",
        job_id=SILVER_JOB_ID,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="dag_03_gold_load",
        wait_for_completion=True,
    )

    run_silver >> trigger_gold