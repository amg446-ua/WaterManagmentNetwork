FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY src/WM_FO.py .
COPY src/WM_FO_Frontend.py .
COPY src/fo.html .
COPY src/WM_Proceso.py .

RUN pip install --no-cache-dir kafka-python flask

ENTRYPOINT ["python3"]

CMD ["WM_FO.py"]