FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY operations/scatter_gather ./operations/scatter_gather

ENTRYPOINT ["python3", "-m", "operations.scatter_gather.scatter_gather_detector_main"]