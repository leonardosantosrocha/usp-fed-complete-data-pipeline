FROM python:3.11-slim

WORKDIR /app/src/quality

COPY src/quality/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/quality/quality.py .

CMD ["python", "quality.py"]