from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import time
import psycopg2


def wait_for_postgres(
    host=os.getenv("POSTGRES_HOST", "postgres-app"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    database=os.getenv("POSTGRES_DB", "incomes_db"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "admin"),
    timeout=60,
    interval=3,
):
    start_time = time.time()

    while True:
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
            )
            conn.close()
            print("PostgreSQL disponível")
            return
        except Exception as e:
            if time.time() - start_time > timeout:
                raise Exception(f"Timeout ao conectar no PostgreSQL: {e}")
            print("Aguardando PostgreSQL...")
            time.sleep(interval)


with DAG(
    dag_id="orchestrate_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion"],
) as dag:

    wait_db = PythonOperator(
        task_id="wait_for_postgres",
        python_callable=wait_for_postgres,
    )

    ingestion_task = BashOperator(
        task_id="run_ingestion",
        bash_command="python /opt/app/src/ingestion/ingestion.py",
    )

    wait_db >> ingestion_task