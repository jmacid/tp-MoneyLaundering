FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY operations/evaluators .

ENTRYPOINT ["python3", "-m", "threshold_evaluator_main"]
