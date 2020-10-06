INCLUDE=$(shell pg_config --includedir)
LIB=$(shell pg_config --libdir)
OPT=-lpq -std=c99
FILES=src/helpers.c src/parameter.c src/statement.c src/main.c
OUT=player
CMD=gcc -I include -I $(INCLUDE) -L $(LIB) -o $(OUT) $(FILES) $(OPT)


debug:
	$(CMD)

release:
	$(CMD) -O2
