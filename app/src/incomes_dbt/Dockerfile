FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN apt-get update && apt-get install -y libpq-dev gcc make && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir dbt-postgres

COPY packages.yml .
COPY dbt_project.yml .
COPY requirements.txt .
RUN dbt deps

COPY . .

CMD ["dbt", "run"]