FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY WM_FO/WM_FO.py .
COPY WM_FO/WM_FO_Frontend.py .
COPY WM_FO/fo.html .
COPY WM_FO/WM_Proceso.py .

RUN pip install --no-cache-dir kafka-python-ng flask

ENTRYPOINT ["python3"]

CMD ["WM_FO.py"]