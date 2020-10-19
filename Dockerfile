FROM postgres:latest

RUN apt-get update && apt-get install build-essential libpq-dev sudo valgrind -y

COPY . /app

COPY ./tests/pktlog /tmp/pktlog

WORKDIR /app

RUN make

ENV POSTGRES_PASSWORD=root
ENV DATABASE_URL=postgres://postgres:root@localhost:5432/postgres

COPY ./docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
