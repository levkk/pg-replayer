FROM postgres:latest

RUN apt-get update && apt-get install build-essential libpq-dev -y

COPY . /app

WORKDIR /app

RUN make
