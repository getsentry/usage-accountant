FROM python:3.8-alpine

RUN apt-get update && apt-get install -y git && apt-get clean

WORKDIR /app

RUN git clone https://github.com/getsentry/usage-accountant.git .
