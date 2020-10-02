import os
import binascii
from pgreplayer import *

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pgbouncer_queries.bin")
EOL = binascii.unhexlify(LINE_SEPARATOR)

def generate_test_file():
    with open(PATH, "wb") as f:
        f.write(b"Q2SELECT * FROM users" + EOL)
        f.write(b"P2SELECT * FROM users WHERE field = $1" + EOL)
        f.write(b"B3234" + EOL)
        f.write(b"Q5SELECT * FROM users LIMIT 1" + EOL)


def test_write_file():
    generate_test_file()
    parse_file(PATH)
