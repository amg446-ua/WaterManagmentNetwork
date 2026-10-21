FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

COPY WM_Central/WM_Front.py .
COPY WM_Central/WM_Proceso.py .
COPY WM_Central/index.html .
COPY WM_Central/*.java ./src/
COPY WM_Central/lib/*.jar ./lib/

ENV PYTHONUNBUFFERED=1

RUN javac -cp "lib/*" -d out src/*.java

RUN pip install --no-cache-dir flask