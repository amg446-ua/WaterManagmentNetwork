FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY /src/WM_WS_E.py .

RUN pip install --no-cache-dir kafka-python

ENTRYPOINT ["python3", "WM_WS_E.py"]