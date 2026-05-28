FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY operations/aggregators .

ENTRYPOINT ["python3", "-m", "payment_method_aggregator_main"]
