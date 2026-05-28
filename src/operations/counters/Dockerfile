FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY operations/counters .

ENTRYPOINT ["python3", "-m", "payment_method_counter_main"]
