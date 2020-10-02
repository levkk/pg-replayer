"""Replay some queries stored in a file."""

import fcntl
import psycopg2
import os
import string
import random
import binascii

# The mirror DB
DATABASE_URL = os.environ.get("DATABASE_URL")

# The queries dumped by PgBouncer
QUERY_FILE_PATH = os.environ.get("QUERY_FILE_PATH", "/tmp/pgbouncer_queries.bin")

# Line separator for the file above (hex)
LINE_SEPARATOR = "19" # EM lol


def setup():
    """Make sure we got all the configs & setup a connection to the mirror server.

    Will exit the script if any config is missing or will throw errors from psycopg2 if there are any.
    """
    if DATABASE_URL is None:
        print("DATABASE_URL is not set.")
        exit(1)

    if not os.path.exists(QUERY_FILE_PATH):
        print("{} does not exist.".format(QUERY_FILE_PATH))
        exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    return conn, cur


def random_string(n):
    """Generates a random string. Yup."""
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))


def parse_file(file):
    """Parse the file and execute any queries, aka the main loop."""
    with open(file, "rb") as f:
        # Grab the whole file and split the lines
        data = f.read()
        lines = data.split(binascii.unhexlify(LINE_SEPARATOR))

        # State machine
        query = None
        params = []
        prev_state = None

        # Parse the lines
        for line in lines:
            # debug(line)
            # EOF
            if len(line) == 0:
                execute(query, params)
                break

            # Parse the line a.k.a. packet
            # 1. type Q/P/B (Qexec, prepared statement, or bind param)
            # 2. Len of the packet
            # 3. The data of the packet
            type_ = chr(line[0])
            len_ = line[1:5]
            data = line[5:]

            # Query or prepared statement
            if type_ in ["Q", "P"]:
                # We have a complete query, let's go
                if prev_state in ["Q", "B"]:
                    execute(query, params)
                query = data

            # Save the parameter
            elif type_ == "B":
                params.append(data)

            # Execute
            elif type_ == "E":
                execute(query, params)

            # Save the state
            prev_state = type_
        if query is not None:
            execute(query, params)


def debug(line):
    print("Parsing: ", "".join(list(map(lambda x: chr(x), list(line)))))

def execute(query, params):
    """Execute the query with params if any."""
    # Dummy
    pass
    print("Executing: ", "".join(list(map(lambda x: chr(x), list(query)))))


def main():
    """Main loop. Poll."""
    # while True:
    tmp_file = "{}.{}".format(QUERY_FILE_PATH, random_string(10))

    # "Copy" the file
    with open("{}.lock".format(QUERY_FILE_PATH), "wb") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        os.rename(QUERY_FILE_PATH, tmp_file)

        # Touch new file
        open(QUERY_FILE_PATH, "a").close()
    parse_file(tmp_file)
    # Done



if __name__ == '__main__':
    main()




