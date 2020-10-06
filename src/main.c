/*
  Postgres queries parser and replayer.
*/

#define _GNU_SOURCE

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>
#include <libpq-fe.h>
#include <time.h>

#define DELIMETER '\x19' /* EM */
#define DEBUG 0

#include "helpers.h"
#include "statement.h"
#include "parameter.h"

/*
 * Check the result of our query.
 */
int check_result(PGresult *res, PGconn *conn, char *query) {
  int code = PQresultStatus(res);

  switch(code) {
    case PGRES_TUPLES_OK:
    case PGRES_COMMAND_OK: {
      if (DEBUG)
        printf("[Postgres] Executed: %s\n", query);
      break;
    }
    default: {
      char *err = PQerrorMessage(conn);
      printf("[%d] Error: %s\n", code, err);
      break;
    }
  }

  PQclear(res);

  return code;
}

/*
 * Will execute a preparted statement against the connection.
 */
void pexec(struct PStatement *stmt, PGconn *conn) {
  int i;
  const char *params[stmt->np];

  for (i = 0; i < stmt->np; i++) {
    params[i] = stmt->params[i]->value;
  }

  /* Send the prepared statement over. */
  PGresult *res = PQexecParams(
    conn,
    stmt->query,
    stmt->np,
    NULL,
    params,
    NULL,
    NULL,
    0
  );

  check_result(res, conn, stmt->query);
}

/* Cleanly exit the Postgres connection and abort. */
void do_exit(PGconn *conn) {
  PQfinish(conn);
  exit(1);
}

/*
 * Will execute a simple query.
 */
void exec(char *query, PGconn *conn) {
  PGresult *res = PQexec(conn, query);
  check_result(res, conn, query);
}

/*
 * Protect against buffer overruns.
 */
void move_it(char **it, size_t offset, char *buf, size_t len) {
  *it += offset;

  if (DEBUG >= 2) {
    printf("[Debug] Moved %p by %lu. Total: %lu\n", *it, offset, len);
  }

  /* Make sure we didn't go to far */
  assert(*it + offset < buf + len);
}

/*
 * Entrypoint.
 */
int main() {
  FILE *f;
  char *line, *it;
  size_t line_len;
  ssize_t nread;
  int i, q_sent = 0;
  clock_t start = clock(), end;

  char *fname = getenv("PACKET_FILE");

  if (fname == NULL) {
    f = fopen("/tmp/pktlog", "r");
  }
  else {
    f = fopen(fname, "r");
  }

  if (f == NULL) {
    printf("Could not open packet log.");
    exit(1);
  }

  int libpq_version = PQlibVersion();

  if (DEBUG) {
    printf("libpq version: %d\n", libpq_version);
  }

  char *pg_conn = getenv("DATABASE_URL");

  if (pg_conn == NULL) {
    fprintf(stderr, "DATABASE_URL environment variable is required but not set.\n");
    exit(1);
  }

  PGconn *conn = PQconnectdb(pg_conn);

  if (PQstatus(conn) == CONNECTION_BAD) {
      fprintf(stderr, "Connection to database failed: %s\n",
        PQerrorMessage(conn));
      do_exit(conn);
  }

  struct PStatement *stmt = NULL;

  while ((nread = getdelim(&line, &line_len, DELIMETER, f)) > 0) {
    /* Not enough data to be a valid line. */
    if (line_len < 5) {
      continue;
    }

    /* Place the iterator at the beginning. */
    it = line;

    /* Parse the tag and move forward */
    char tag = *it;
    move_it(&it, 1, line, line_len);

    /* Parse the len of the packet and move forward. */
    uint32_t len = parse_uint32(it);
    move_it(&it, 4, line, line_len);

    /* Simple query, 'Q' packet */
    if (tag == 'Q') {
      exec(it, conn);
      q_sent += 1;
    }

    /* Prepared statement, 'P' packet */
    else if (tag == 'P') {
      /*
       * str stmt
       * str query
       */
      char *stmt_name = it;
      char *query = it + strlen(stmt_name) + 1; /* +1 for the NULL character. */

      if (stmt != NULL) {
        printf("Statement is not flushed, packets out of order. \n");
        pstatement_free(stmt);
        exit(1);
      }

      stmt = pstatement_init(query);
    }

    /* Bind parameter(s), 'B' packet */
    else if (tag == 'B') {
      char *portal = it; /* Portal, can be empty */
      move_it(&it, strlen(portal) + 1, line, line_len); /* Skip it for now */

      char *statement = it; /* Statement name, if any  */
      move_it(&it, strlen(statement) + 1, line, line_len); /* Also not using it for now */

      uint16_t nf = parse_uint16(it); /* number of formats used */
      move_it(&it, 2, line, line_len); /* Parsed it, now move forward */

      /* Parse each format */
      for (i = 0; i < nf; i++) {
        uint16_t fmt = parse_uint16(it);
        move_it(&it, 2, line, line_len);
      }

      /* Number of parameters */
      uint16_t np = parse_uint16(it);
      move_it(&it, 2, line, line_len); /* move iterator forward 2 bytes */

      /* Store the parameters */

      /* Copy over the params */
      for (i = 0; i < np; i++) {
        int32_t plen = (int32_t)parse_uint32(it); /* Parameter length */
        move_it(&it, 4, line, line_len); /* 4 bytes */

        struct Parameter *parameter = parameter_init(plen, it);
        move_it(&it, plen, line, line_len);

        pstatement_add_param(stmt, parameter);
      }
    }

    /* Execute the prepared statement, 'E' packet */
    else if (tag == 'E') {
      pexec(stmt, conn);
      if (DEBUG) {
        pstatement_debug(stmt);
      }
      pstatement_free(stmt);
      stmt = NULL;
      q_sent += 1;
    }

    /* Clear the line buffer */
    memset(line, 0, line_len);
  }

  free(line);
  fclose(f);

  end = clock();

  printf("Sent %d queries in %f seconds.\n", q_sent, (double)((end - start)) / CLOCKS_PER_SEC);

  do_exit(conn);
}
