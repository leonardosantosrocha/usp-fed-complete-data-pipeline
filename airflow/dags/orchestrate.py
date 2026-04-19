from airflow import DAG
from datetime import datetime
from tasks import wait_db_task, dbt_tasks, ingestion_task


with DAG(
    dag_id="lab_final",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion"],
) as dag:

    wait_db_task() >> ingestion_task() >> dbt_tasks()