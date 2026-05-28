FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY operations/average .

ENTRYPOINT ["python3", "-m", "average_calculator_main"]
