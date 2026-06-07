FROM python:3-alpine

WORKDIR /app

# Copiamos las carpetas
COPY common ./src/common
COPY client ./src/client

# 💡 SOLUCIÓN DEFINITIVA: Añadimos la ruta de message_protocol al PYTHONPATH
ENV PYTHONPATH=/app:/app/src:/app/src/common/message_protocol

ENTRYPOINT ["python3", "src/client/main.py"]