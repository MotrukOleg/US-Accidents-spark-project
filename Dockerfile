FROM python:3.9-slim-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

RUN pip install --no-cache-dir pyspark==3.2.0

WORKDIR /app

COPY . /app

CMD ["python", "main.py"]