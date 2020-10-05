/*
  Postgres queries parser and replayer.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>

#include "helpers.h"
#include "statement.h"
#include "parameter.h"

#define DELIMETER '\x19' /* EM */
#define DEBUG 1

void hexDump(const char *, const void *, const int);

/*
 * Convert between 4 bytes of network data and a 32 bit integer.
 */
uint32_t parse_uint32(char *data) {
  unsigned a, b, c, d;
  a = data[0];
  b = data[1];
  c = data[2];
  d = data[3];

  uint32_t result = (a << 24) | (b << 16) | (c << 8) | d;
  return result;
}

/*
 * Convert between 2 bytes of network data and a 16 bit integer.
 */
uint16_t parse_uint16(char *data) {
  unsigned a, b;
  a = data[0];
  b = data[1];
  return (a << 8) | (b);
}

/*
 * Will execute a preparted statement.
 */
int pexec(char *query, char **params, size_t param_len) {
  if (DEBUG) {
    printf("Preparted statement: %s\n", query);

    int i;
    for (i = 0; i < param_len; i++) {
      printf("Param: %s\n", params[i]);
    }
  }

  /* TODO: execute the query */
  return 0;
}

/*
 * Will execute a query.
 */
int exec(char *query) {
  //printf("Query: %s\n", query);
  return 0;
}

int main() {
  FILE *f;
  char *line, *it;
  size_t line_len;
  ssize_t nread;
  int i;

  char *fname = getenv("PACKET_FILE");

  if (strlen(fname) == 0) {
    f = fopen("/tmp/pktlog", "r");
  }
  else {
    f = fopen(fname, "r");
  }

  if (f == NULL) {
    printf("Could not open packet log.");
    exit(1);
  }

  struct PStatement *stmt = NULL;

  while ((nread = getdelim(&line, &line_len, DELIMETER, f)) != -1) {
    /* Place the iterator at the beginning. */
    it = line;

    /* Parse the tag and move forward */
    char tag = *it;
    it += 1;

    /* Parse the len of the packet and move forward. */
    uint32_t len = parse_uint32(it);
    it += 4;

    /* Simple query, 'Q' packet */
    if (tag == 'Q') {
      exec(it);
    }

    /* Prepared statement, 'P' packet */
    else if (tag == 'P') {
      /*
       * str stmt
       * str query
       */
      char *stmt_name = it;
      char *query = it + strlen(stmt_name) + 1; /* +1 for the NULL character. */

      stmt = pstatement_init(query);
    }

    /* Bind parameter(s), 'B' packet */
    else if (tag == 'B') {
      char *portal = it; /* Portal, can be empty */
      it += strlen(portal) + 1; /* Skip it for now */

      char *statement = it; /* Statement name, if any  */
      it += strlen(statement) + 1; /* Also not using it for now */

      uint16_t nf = parse_uint16(it); /* number of formats used */
      it = it + 2; /* Parsed it, now move forward */

      /* Parse each format */
      for (i = 0; i < nf; i++) {
        uint16_t fmt = parse_uint16(it);
        it += 2;
      }

      /* Number of parameters */
      uint16_t np = parse_uint16(it);
      it = it + 2; /* move iterator forward 2 bytes */

      /* Store the parameters */

      /* Copy over the params */
      for (i = 0; i < np; i++) {
        int32_t plen = (int32_t)parse_uint32(it); /* Parameter length */
        it += 4; /* 4 bytes */

        struct Parameter *parameter = parameter_init(plen, it);
        it += plen;

        pstatement_add_param(stmt, parameter);
      }
    }

    /* Execute the prepared statement, 'E' packet */
    else if (tag == 'E') {
      pstatement_debug(stmt);
      pstatement_free(stmt);
    }

    /* Clear the line buffer */
    memset(line, 0, line_len);
  }

  free(line);
  fclose(f);
}
