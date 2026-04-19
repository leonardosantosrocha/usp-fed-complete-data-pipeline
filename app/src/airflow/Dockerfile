FROM apache/airflow:2.9.3

USER root

# Dependências de sistema
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean

# Código da aplicação
COPY app /opt/airflow/app

# Requirements
COPY app/src/ingestion/requirements.txt /requirements_ingestion.txt
COPY app/src/quality/requirements.txt /requirements_quality.txt
COPY app/src/incomes_dbt/requirements.txt /requirements_dbt.txt

USER airflow

# Instala dependências Python
RUN pip install --no-cache-dir -r /requirements_ingestion.txt && \
    pip install --no-cache-dir -r /requirements_quality.txt && \
    pip install --no-cache-dir -r /requirements_dbt.txt

USER root

# Corrige logs (evita PermissionError)
RUN mkdir -p /opt/airflow/logs && \
    chown -R airflow:0 /opt/airflow/logs

# PYTHONPATH correto e resiliente
ENV PYTHONPATH="/opt/airflow/app:${PYTHONPATH:-}"

USER airflow