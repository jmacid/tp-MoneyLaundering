FROM python:3.12-slim

RUN pip3 install pika

WORKDIR /app

COPY . .

ENTRYPOINT ["python3", "-m", "gateway.main"]
