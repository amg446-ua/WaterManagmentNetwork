FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

COPY src/WM_Front.py .
COPY src/WM_Proceso.py .
COPY src/index.html .
COPY src/*.java ./src/
COPY lib/*.jar ./lib/

ENV PYTHONUNBUFFERED=1

RUN javac -cp "lib/*" -d out src/*.java

RUN pip install --no-cache-dir flask
