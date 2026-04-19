FROM apache/airflow:2.9.3

USER root

# Dependências de sistema
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean

USER airflow

COPY ./app /opt/app

# Instala dependências Python
RUN pip install --no-cache-dir -r /opt/app/src/ingestion/requirements.txt && \
    pip install --no-cache-dir -r /opt/app/src/quality/requirements.txt && \
    pip install --no-cache-dir -r /opt/app/src/incomes_dbt/requirements.txt

USER root

RUN mkdir -p /opt/airflow/logs/scheduler /opt/airflow/dags /opt/airflow/plugins && \
    chown -R airflow:0 /opt/airflow && \
    chmod -R 775 /opt/airflow

USER airflow

# PYTHONPATH correto e resiliente
ENV PYTHONPATH="/opt/app:${PYTHONPATH:-}"