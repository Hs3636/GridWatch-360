from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

BRONZE_JOB_ID = 254934091008916   # replace with your actual Job ID

default_args = {
    "owner":            "gridwatch360",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_01_bronze_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["gridwatch360", "bronze"],
) as dag:

    run_bronze = DatabricksRunNowOperator(
        task_id="run_bronze_job",
        databricks_conn_id="databricks_default",
        job_id=BRONZE_JOB_ID,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="dag_02_silver_transform",
        wait_for_completion=True,
    )

    run_bronze >> trigger_silver

