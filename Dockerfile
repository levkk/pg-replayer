FROM postgres:latest

RUN apt-get update && apt-get install build-essential libpq-dev sudo valgrind git libevent-dev -y

RUN apt-get install libssl-dev pkg-config libtool python pandoc procps -y

WORKDIR /tmp

RUN git clone https://github.com/instacart/pgbouncer && \
    cd pgbouncer && \
    git submodule init && \
    git submodule update && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install

RUN mkdir /var/log/pgbouncer && chown postgres:postgres /var/log/pgbouncer

COPY . /app

# COPY ./tests/pktlog /tmp/pktlog

COPY ./tests /tests

WORKDIR /app

RUN make

ENV POSTGRES_PASSWORD=root
ENV DATABASE_URL=postgres://postgres:root@localhost:5432/mirror

COPY ./docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
