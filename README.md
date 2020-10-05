# pg-replayer

A tool that reads Postgres packets from a file and replays them against a database of your choice. The packets have to be `\x19` separated in the input file.

## Why
It's hard to deploy new databases without benchmarks & the only benchmarks that really matter are your production traffic. This will simulate it.

## Installation

1. Make sure you have `libpq-dev` (Linux) or `brew install postgresql` (Mac OS).
2. `make`

This will produce the binary `player` in the root directory of this repository.

## Tests

1. Make sure you have a PostgreSQL DB running locally.
2. Compile the binary.
3. From the root of the repository, run `bash tests/run_tests.sh`.

Any problems, check out the script, it should be obvious what's going on.
