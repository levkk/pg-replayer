/*
 * Postgres pooler.
 */

#include <assert.h>
#include <libpq-fe.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

#include "replayer.h"

#define POOL_SIZE 10

enum CONN_STATES {
  CONN_FREE = 0,
  CONN_BUSY
};

/*
 * Multiplex connections.
 */
static PGconn **conns = NULL;
static uint32_t *conn_clients = NULL;
static size_t conn_idx = 0;

static int postgres_conn();
static int check_result(PGresult *res, PGconn *conn, char *query);
static int ignore_transction_blocks(char *stmt);


/*
 * Init the pool.
 */
int postgres_init() {
  assert(conns == NULL);
  assert(conn_clients == NULL);

  int i;
  char *database_url = getenv("DATABASE_URL");

  if (!database_url) {
    printf("No DATABASE_URL is set but is required.\n");
    return -1;
  }

  conns = malloc(POOL_SIZE * sizeof(PGconn *));
  conn_clients = malloc(POOL_SIZE * sizeof(uint32_t));

  for (i = 0; i < POOL_SIZE; i++) {
    PGconn *conn = PQconnectdb(database_url);

    if (PQstatus(conn) == CONNECTION_BAD) {
      fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
      PQfinish(conn);
      return -1;
    }

    conns[i] = conn;
    conn_clients[i] = 0;
  }

  return 0;
}

/*
 * Transactional check.
 */
PGconn *postgres_conn_trans(char *stmt) {
  size_t idx = conn_idx++ % POOL_SIZE;
  if (DEBUG)
    printf("[Postgres] Using %lu connection from the pool.\n", idx);
  return conns[idx];
}

/*
 * "Async" PQexec
 */
int postgres_exec(char *stmt) {
  /* Skip transactional indicators */
  if (ignore_transction_blocks(stmt)) {
    return 0;
  }

  PGconn *conn = postgres_conn_trans(stmt);
  

  if (DEBUG) {
    printf("[Postgres] Executing %s\n", stmt);
  }

  PGresult *res = PQexec(conn, stmt);
  return check_result(res, conn, stmt);
}

/*
 * "Async" prepared statement execution.
 */
int postgres_pexec(char *stmt, const char **params, size_t nparams) {
  /* Skip transactional indicators */
  if (ignore_transction_blocks(stmt)) {
    return 0;
  }

  PGconn *conn = postgres_conn_trans(stmt);

  if (DEBUG) {
    printf("[Postgres] Executing %s\n", stmt);
  }

  PQexecParams(
    conn,
    stmt,
    nparams,
    NULL,
    params,
    NULL,
    NULL,
    0
  );

  /* return check_result(res, conn, stmt); */
  return 0;
}

static int ignore_transction_blocks(char *stmt) {
  if (strstr(stmt, "BEGIN") == stmt) {
    return 1;
  }

  if (strstr(stmt, "END") == stmt) {
    return 1;
  }

  if (strstr(stmt, "COMMIT") == stmt) {
    return 1;
  }

  /* Oh well */
  if (strstr(stmt, "ROLLBACK") == stmt) {
    return 1;
  }

  return 0;
}


/*
 * Get conn from pool.
 * TODO: switch to libpq-events
 */
static int postgres_conn(uint32_t client_id) {
  return POOL_SIZE % client_id;
  /*
  int i;

loop:
  for(i = 0; i < POOL_SIZE; i++) {
    PGconn *conn = conns[i];

    PQconsumeInput(conn);
    if (PQtransactionStatus(conn) != PQTRANS_ACTIVE) {
      if (DEBUG)
        printf("[Postgres] Connection %d is available.\n", i);
      return i;
    }
  }

  if (DEBUG)
    printf("[Postgres] No connections available. Waiting 5ms.\n");

  usleep(SECOND * 0.005);
  goto loop;  */
}

/*
 * Check the result of our query.
 */
static int check_result(PGresult *res, PGconn *conn, char *query) {
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
 * Shutdown pool
 */
void postgres_free() {
  int i;
  for (i = 0; i < POOL_SIZE; i++) {
    PQfinish(conns[i]);
  }
  free(conns);
  free(conn_clients);
  conns = NULL;
  conn_clients = NULL;
}
