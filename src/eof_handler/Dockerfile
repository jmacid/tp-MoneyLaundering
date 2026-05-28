FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt --break-system-packages
CMD ["python", "-m", "eof_handler.eof_handler"]