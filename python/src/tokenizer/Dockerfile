FROM python:3.12-alpine

RUN pip3 install pika
RUN pip3 install nltk
RUN python3 -m nltk.downloader stopwords

COPY common ./common
COPY tokenizer .

ENTRYPOINT ["python3", "main.py"]