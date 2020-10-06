#!/bin/bash

#
# Run the tests.
#

# Change me to fit your local env
export DATABASE_URL="postgres://postgres:root@localhost:5432/postgres"

# Required for a "smooth" test
psql $DATABASE_URL -c "CREATE TABLE IF NOT EXISTS users (id BIGSERIAL);"

# Test file
export PACKET_FILE="$(pwd)/tests/testdata.bin"

if [[ ! -d .git ]]; then
	echo "Run me from the root of the repository."
	exit 1
fi

if [[ ! -f ./player ]]; then
	echo "Run make to build the program first."
	exit 1
fi

echo "Using $PACKET_FILE for test data."

if which valrind; then
	valgrind --leak-check=full \
	         --show-leak-kinds=all \
	         --track-origins=yes \
	         --verbose \
	         ./player
else
	./player
fi
