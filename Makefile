INCLUDE=$(shell pg_config --includedir)
LIB=$(shell pg_config --libdir)
OPT=-lpq -std=c99 -pthread
FILES=src/helpers.c src/parameter.c src/statement.c src/postgres.c src/client.c
CMD=gcc -I include -I $(INCLUDE) -L $(LIB) $(FILES) $(OPT) -Wall


debug:
	$(CMD) -g src/main.c -o player

release:
	$(CMD) -O2 src/main.c -o player

test:
	$(CMD) src/test.c -g -o test

install:
	cp player /usr/bin/replayer
