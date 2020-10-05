#!/bin/bash

#
# Run the tests.
#

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

./player
