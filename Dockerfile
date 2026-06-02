FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas redis numpy

COPY . .

CMD ["python", "app.py"]