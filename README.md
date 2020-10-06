# pg-replayer

A tool that reads packets from a dump file and replays logical statements against a database of your choice. The packets have to be `\x19`-separated in the input file.

This can be thought of as logical replication for your PG clients.

## Why
It's hard to deploy new databases without benchmarks & the only benchmarks that really matter are your production traffic. This will simulate it, with some exceptions.

## Supported packets
1. `Q`: execute a query, simple and works
2. `P`: prepared statement, supported
3. `B`: bind params to the prepared statement, supported
4. `E`: execute the prepared statement, supported

## Not supported yet

1. Copy sub-protocol

## Installation

1. Make sure you have `libpq-dev` (Linux) or `brew install postgresql` (Mac OS).
2. `make`

This will produce the binary `player` in the root directory of this repository.

## Tests

1. Make sure you have a PostgreSQL DB running locally.
2. Compile the binary.
3. From the root of the repository, run `bash tests/run_tests.sh`.

Any problems, check out the script, it should be obvious what's going on.
