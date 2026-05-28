FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY operations/aggregators/average .

ENTRYPOINT ["python3", "-m", "average_aggregator_main"]
