FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app

COPY common ./common
COPY workers/resolver .


ENTRYPOINT ["python3", "-m", "bank_resolver"]