/*
 * Postgres pooler.
 *
 * BUG: There is a small memory leak hiding in Postgres connection initialization.
 * Since that happens only once in the application life time, it has not been found yet
 * by this developer.
 */

#define _GNU_SOURCE

#include <assert.h>
#include <libpq-fe.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include "replayer.h"
#include "statement.h"
#include "helpers.h"

#define POOL_SIZE 10
/*
 * Multiplex connections.
 */
static PGconn **conns = NULL;

static pthread_t threads[POOL_SIZE];
static int thread_ids[POOL_SIZE];
static int pipes[2] = { 0 };

static int ignore_transction_blocks(char *stmt);

static void *postgres_worker(void *arg);
static int postgres_pexec(struct PStatement *stmt, PGconn *conn);

/*
 * Initialize the pool.
 */
int postgres_init(void) {
  assert(conns == NULL);

  int i;
  char *database_url = getenv("DATABASE_URL");

  if (!database_url) {
    log_info("No DATABASE_URL is set but is required.");
    return -1;
  }

  conns = malloc(POOL_SIZE * sizeof(PGconn *));
  if (pipe(pipes) == -1) {
    log_info("pipe\n");
    exit(1);
  }

  for (i = 0; i < POOL_SIZE; i++) {
    PGconn *conn = PQconnectdb(database_url);

    if (PQstatus(conn) == CONNECTION_BAD) {
      log_info("Connection to database failed: %s", PQerrorMessage(conn));
      PQfinish(conn);
      return -1;
    }

    conns[i] = conn;

    /* Initialize the threads & signals */
    thread_ids[i] = i;

    pthread_create(&threads[i], NULL, postgres_worker, &thread_ids[i]);
  }

  return 0;
}

/*
 * The worker.
 */
static void *postgres_worker(void *arg) {
  int id = *(int*)arg;
  size_t nread = 0;
  PGconn *conn = conns[id];

  log_info("Worker %d ready", id);

  while(1) {

    /* Get the work from the main thread */
    struct PStatement *stmt;

    nread = read(pipes[0], &stmt, sizeof(struct PStatement*));

    if (nread != sizeof(struct PStatement*)) {
      log_info("partial read");
      abort(); /* No partial reads on 8 bytes of data, but if that happens, blow up */
    }

    if (DEBUG)
      log_info("[%d] Got work", id);

    assert(stmt != NULL);

    /* Execute query in thread */
    postgres_pexec(stmt, conn);

    /* Clean up */
    pstatement_free(stmt);
  }

  return NULL;
}

/*
 * Add work to the queue.
 */
void postgres_assign(struct PStatement *stmt) {
  /* Let the kernel handle the scheduling */
  write(pipes[1], &stmt, sizeof(struct PStatement*));
}


/*
 * Prepared statement execution.
 */
static int postgres_pexec(struct PStatement *stmt, PGconn *conn) {
  int i;
  const char *params[stmt->np];

  for (i = 0; i < stmt->np; i++) {
    params[i] = stmt->params[i]->value;
  }

  /* Skip transactional indicators for now, we can't guarantee per-client connections yet. */
  if (ignore_transction_blocks(stmt->query)) {
    return 0;
  }

  if (DEBUG) {
    log_info("[Postgres][%u] Executing %s", stmt->client_id, stmt->query);
  }

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

  if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK) {
    log_info("[Postgres] Status: %s", PQresStatus(PQresultStatus(res)));
    log_info("[Postgres] Error: %s", PQerrorMessage(conn));
  }

  PQclear(res);

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
 * Shutdown pool.
 */
void postgres_free(void) {
  int i;

  for (i = 0; i < POOL_SIZE; i++) {
    /* Kill */
    pthread_cancel(threads[i]);

    /* Clean up */
    PQfinish(conns[i]);
  }

  free(conns);
  conns = NULL;
}
