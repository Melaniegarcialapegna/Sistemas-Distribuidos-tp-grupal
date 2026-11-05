FROM python:3.12-alpine

RUN pip3 install pika

COPY common ./common
COPY year_filter .

ENTRYPOINT ["python3", "main.py"]