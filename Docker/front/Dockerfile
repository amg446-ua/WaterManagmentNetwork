FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY src/WM_Front.py .
COPY src/WM_Proceso.py .
COPY src/index.html .
COPY out ./out
COPY lib ./lib

RUN pip install --no-cache-dir flask

ENTRYPOINT ["python3", "WM_Front.py"]