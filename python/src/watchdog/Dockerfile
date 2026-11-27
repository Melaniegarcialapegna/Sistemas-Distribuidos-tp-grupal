FROM python:3.12-alpine

RUN pip3 install pika docker

COPY common ./common
COPY watchdog .

ENTRYPOINT ["python3", "main.py"]
