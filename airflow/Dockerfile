FROM apache/airflow:2.9.3

USER root

# Dependências de sistema
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean

USER airflow

# Instala dependências Python
RUN pip install --no-cache-dir -r /opt/app/src/ingestion/requirements.txt && \
    pip install --no-cache-dir -r /opt/app/src/quality/requirements.txt && \
    pip install --no-cache-dir -r /opt/app/src/incomes_dbt/requirements.txt

USER root

# Corrige logs (evita PermissionError)
RUN mkdir -p /opt/airflow/logs && \
    mkdir -p /opt/airflow/logs/scheduler && \
    chown -R airflow:0 /opt/airflow && \
    chmod -R 777 /opt/airflow && \

USER airflow

RUN airflow db migrate && \
    airflow users create --username admin --password admin --firstname admin --lastname user --role Admin --email admin@email.com
    
# PYTHONPATH correto e resiliente
ENV PYTHONPATH="/opt/app:${PYTHONPATH:-}"
