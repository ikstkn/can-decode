FROM python:3.9-slim

# Установка декодера (бинарник под Linux)
COPY mdf2parquet_decode /usr/local/bin/mdf2parquet_decode
RUN chmod +x /usr/local/bin/mdf2parquet_decode

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY orchestrator.py /app/orchestrator.py

WORKDIR /app

CMD ["python", "orchestrator.py"]