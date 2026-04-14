FROM python:3.11-slim

WORKDIR /app

COPY src/ingestion/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ingestion/ingestion.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "ingestion.py"]