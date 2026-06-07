FROM python:3-alpine

WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "-u", "coordinator/coordinator.py"]