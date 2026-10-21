FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY WM_WS/WM_WS_M.py .

RUN pip install --no-cache-dir kafka-python-ng

ENTRYPOINT ["python3", "WM_WS_M.py"]