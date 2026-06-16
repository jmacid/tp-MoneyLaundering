FROM python:3-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt pika

COPY . .

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "-m", "gateway.main"]