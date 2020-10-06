FROM postgres:latest

RUN apt-get update && apt-get install build-essential libpq-dev sudo -y

COPY . /app

WORKDIR /app

RUN make

ENV POSTGRES_PASSWORD=root
ENV DATABASE_URL=postgres://postgres:root@localhost:5432/postgres

COPY ./docker_entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
