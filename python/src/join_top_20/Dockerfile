FROM python:3.12-alpine

RUN pip3 install pika

COPY common ./common
COPY join_top_20 .

ENTRYPOINT ["python3", "main.py"]