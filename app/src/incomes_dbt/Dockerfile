FROM python:3.11-slim

WORKDIR /app/dbt

RUN apt-get update && \
    apt-get install -y libpq-dev gcc make git && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-postgres

CMD bash -c "\
    dbt debug --profiles-dir /app/dbt && \
    dbt deps --profiles-dir /app/dbt && \
    dbt run --profiles-dir /app/dbt && \
    dbt test --profiles-dir /app/dbt"