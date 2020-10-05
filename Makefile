all:
	gcc -I include -o player src/helpers.c src/parameter.c src/statement.c src/main.c -lpq -std=c99
