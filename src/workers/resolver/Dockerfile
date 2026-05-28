FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app

COPY src/ ./src/

ENTRYPOINT ["python3", "src/workers/resolvers/bank_resolver.py"]