FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY shared ./shared
COPY gateway .

ENTRYPOINT ["python3", "main.py"]
