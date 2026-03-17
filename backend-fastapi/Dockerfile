FROM python:3.11-slim

WORKDIR /app

COPY app/ app/
RUN pip install --no-cache-dir fastapi uvicorn psycopg2-binary

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
