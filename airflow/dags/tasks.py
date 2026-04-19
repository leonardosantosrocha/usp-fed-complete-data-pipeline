import os
import time
import psycopg2
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


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


def wait_db_task():
    return PythonOperator(
        task_id="wait_for_postgres",
        python_callable=wait_for_postgres,
    )

def ingestion_task():
    return BashOperator(
        task_id="run_ingestion",
        bash_command="python /opt/app/src/ingestion/ingestion.py",
    )

@task_group(group_id='dbt_tasks')
def dbt_tasks():
    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command="dbt deps --project-dir /opt/app/src/incomes_dbt --profiles-dir /opt/app/src/incomes_dbt &&\
            dbt seed --project-dir /opt/app/src/incomes_dbt --profiles-dir /opt/app/src/incomes_dbt && \
            dbt run --project-dir /opt/app/src/incomes_dbt --profiles-dir /opt/app/src/incomes_dbt",
    )

    dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command="dbt test --project-dir /opt/app/src/incomes_dbt --profiles-dir /opt/app/src/incomes_dbt",
    )

    dbt_docs = BashOperator(
        task_id="run_dbt_docs",
        bash_command="dbt docs generate \
            --project-dir /opt/app/src/incomes_dbt \
            --profiles-dir /opt/app/src/incomes_dbt &&\
            echo 'DBT docs gerados'",
    )

    dbt_run >> dbt_test >> dbt_docs

def run_gx():
    return BashOperator(
        task_id="run_great_expectations",
        bash_command="python /opt/app/src/quality/quality.py",
    )



def alert_on_failure():
    ## TODO: Implementar envio de email usando SMTP ou outro serviço de notificação
    print("Tarefa falhou! Enviando alerta...")
    print("Email enviado para equipe de dados: Tarefa falhou no Airflow.")

def fail_alert():
    return PythonOperator(
    task_id="alert_on_failure",
    python_callable=alert_on_failure,
    trigger_rule="one_failed",
    )