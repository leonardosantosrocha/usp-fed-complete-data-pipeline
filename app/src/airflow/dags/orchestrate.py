from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import time
import psycopg2


def wait_for_postgres(
    host="postgres-app",
    port=5432,
    database="incomes_db",
    user="admin",
    password="admin",
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
        bash_command="python /opt/airflow/app/src/ingestion/ingestion.py",
    )

    wait_db >> ingestion_task