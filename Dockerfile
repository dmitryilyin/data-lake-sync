FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_lake_sync.py .

CMD ["python", "data_lake_sync.py"]
