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

#define POOL_SIZE 20
/*
 * Multiplex connections.
 */
static PGconn *conns[POOL_SIZE] = { NULL };
static pthread_t threads[POOL_SIZE];
static int thread_ids[POOL_SIZE];
static int pipes[2] = { 0 };
static uint64_t ok = 0, not_ok = 0, ignored = 0;

static int ignore_transction_blocks(char *stmt);

static void *postgres_worker(void *arg);
static void postgres_pexec(struct PStatement *stmt, PGconn *conn);

/*
 * Initialize the pool.
 */
int postgres_init(void) {
  int i;
  char *database_url = getenv("DATABASE_URL");

  if (!database_url) {
    log_info("No DATABASE_URL is set but is required.");
    return -1;
  }

  if (pipe(pipes) == -1) {
    log_info("pipe\n");
    exit(1);
  }

  log_info("Creating a pool of %d connections", POOL_SIZE);

  for (i = 0; i < POOL_SIZE; i++) {
    assert(conns[i] == NULL);

    PGconn *conn = PQconnectdb(database_url);

    if (PQstatus(conn) == CONNECTION_BAD) {
      log_info("Connection to database failed: %s", PQerrorMessage(conn));
      PQfinish(conn);
      return -1;
    }

    conns[i] = conn;

    /* Initialize the threads */
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

    /* Wait for work from the main thread */
    struct PStatement *stmt;
    nread = read(pipes[0], &stmt, sizeof(stmt));

    if (nread != sizeof(stmt)) {
      log_info("[%d] partial read", id);
      abort(); /* No partial reads on 8 bytes of data, but if that happens, blow up */
    }

    if (stmt == NULL) {
      log_info("[%d] Null pointer in work queue", id);
      continue;
    }

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
  write(pipes[1], &stmt, sizeof(stmt));
}


/*
 * Prepared statement execution.
 */
static void postgres_pexec(struct PStatement *stmt, PGconn *conn) {
  int i;
  const char *params[stmt->np];

  for (i = 0; i < stmt->np; i++) {
    params[i] = stmt->params[i]->value;
  }

  /* Skip transactional indicators for now, we can't guarantee per-client connections yet. */
  if (ignore_transction_blocks(stmt->query)) {
    __atomic_add_fetch(&ignored, 1, __ATOMIC_SEQ_CST);
    return;
  }

  if (DEBUG) {
    log_info("[Postgres][%u] Executing %s", stmt->client_id, stmt->query);
  }

  /* Check connection status */
  switch(PQtransactionStatus(conn)) {
    case PQTRANS_INTRANS:
    case PQTRANS_INERROR: {
      /* Abort any in-progress transactions, a BEGIN statement sneaked through.
        TODO: Absolutely remove this once we support transactions!
      */
      if (DEBUG)
        log_info("[Postgres] Rolling back transaction in progress");

      PQclear(PQexec(conn, "ROLLBACK"));
      break;
    }
    default:
      break;
  }

  PQexec(conn, "BEGIN");
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
  PQexec(conn, "COMMIT");

  switch (PQresultStatus(res)) {
    case PGRES_TUPLES_OK:
    case PGRES_COMMAND_OK: {
      __atomic_add_fetch(&ok, 1, __ATOMIC_SEQ_CST);
      break;
    }
    default: {
      __atomic_add_fetch(&not_ok, 1, __ATOMIC_SEQ_CST);
      log_info("[Postgres][%llu] %s | %s | %s", stmt->client_id, PQresStatus(PQresultStatus(res)), stmt->query, PQerrorMessage(conn));
    }
  }

  PQclear(res);
}

static int ignore_transction_blocks(char *stmt) {
  if (strstr(stmt, "BEGIN") != NULL) {
    return 1;
  }

  /* Not sure about this one */
  if (strstr(stmt, "END") == stmt) {
    return 1;
  }

  if (strstr(stmt, "COMMIT") != NULL) {
    return 1;
  }

  /* Oh well */
  if (strstr(stmt, "ROLLBACK") != NULL) {
    return 1;
  }

  /* No copy, since we don't record the 'd' packets.
   * Space is intentional because all copy statements
   * will be "COPY table_name" or "COPY FROM"
  */
  if (strstr(stmt, "COPY ") != NULL) {
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
    /* Kill, best effort, we don't really clean up! Main thread will exit immediately. */
    pthread_cancel(threads[i]);

    /* Clean up */
    PQfinish(conns[i]);
    conns[i] = NULL;
  }
}

/*
 * Show some stats. They are not exact, since this is multi-threaded.
 */
void postgres_stats(void) {
  uint64_t l_ok = 0, l_not_ok = 0, l_ignored = 0;
  double success_rate = 0.0;

  /* Load stats */
  __atomic_load(&ok, &l_ok, __ATOMIC_SEQ_CST);
  __atomic_load(&not_ok, &l_not_ok, __ATOMIC_SEQ_CST);
  __atomic_load(&ignored, &l_ignored, __ATOMIC_SEQ_CST);

  success_rate = (double)l_ok / ((double)l_ok + (double)l_not_ok);

  log_info("[Postgres][Statistics] Success rate: %.4f; OK: %llu; Error: %llu; Ignored: %llu.", success_rate, l_ok, l_not_ok, l_ignored);

  /* Reset stats */
  __atomic_store_n(&ok, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&not_ok, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&ignored, 0, __ATOMIC_SEQ_CST);
}
