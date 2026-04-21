FROM apache/airflow:2.9.3

USER root

# Dependências de sistema
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cria estrutura necessária com permissões corretas
RUN mkdir -p /opt/app /opt/app/data \
    && chown -R airflow:0 /opt/app \
    && chmod -R 775 /opt/app

USER airflow

# Copia apenas requirements primeiro (melhora cache)
COPY ./app/src/ingestion/requirements.txt /requirements_ingestion.txt
COPY ./app/src/quality/requirements.txt /requirements_quality.txt
COPY ./app/src/incomes_dbt/requirements.txt /requirements_dbt.txt

RUN pip install --no-cache-dir -r /requirements_ingestion.txt && \
    pip install --no-cache-dir -r /requirements_quality.txt && \
    pip install --no-cache-dir -r /requirements_dbt.txt

# Copia aplicação
COPY ./app /opt/app

USER root

# Ajusta permissões finais (sem 777 global)
RUN chown -R airflow:0 /opt/app /opt/airflow && \
    chmod -R 775 /opt/app /opt/airflow

USER airflow

ENV PYTHONPATH="/opt/app:${PYTHONPATH:-}"