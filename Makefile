all:
	gcc -I include -I $(shell pg_config --includedir) -L $(shell pg_config --libdir) -o player src/helpers.c src/parameter.c src/statement.c src/main.c -lpq -std=c99
