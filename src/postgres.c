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

#define POOL_SIZE 40
#define MAX_STATEMENTS_PER_TRANSACTION 10

/*
 * Multiplex connections.
 */
static PGconn *conns[POOL_SIZE] = { NULL };
static pthread_mutex_t locks[POOL_SIZE];
static size_t stmt_cnt[POOL_SIZE] = { 0 };

static pthread_t threads[POOL_SIZE];
static int thread_ids[POOL_SIZE];
static int pipes[2] = { 0 };
static uint64_t ok = 0,
                not_ok = 0,
                ignored = 0,
                scheduled = 0,
                consumed = 0,
                cut_short = 0,
                aborted = 0;

enum {
  STATEMENT_EXECUTE = 0,
  STATEMENT_TRANSACTION_START,
  STATEMENT_TRANSACTION_END,
  STATEMENT_IGNORE
};

static int ignore_statement(char *stmt);

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

    /* Protect against deadlocks */
    PQclear(PQexec(conn, "SET statement_timeout = 5000"));

    conns[i] = conn;

    /* Initialize the threads */
    thread_ids[i] = i;

    if (pthread_mutex_init(&locks[i], NULL)) {
      log_info("Error initializing mutex %d", i);
      abort();
    }

    if (pthread_create(&threads[i], NULL, postgres_worker, &thread_ids[i])) {
      log_info("Error creating thread %d", i);
    }
  }

  return 0;
}

/*
 * The worker.
 */
static void *postgres_worker(void *arg) {
  int id = *(int*)arg;
  size_t nread = 0;
  PGconn *conn = NULL;

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

    __atomic_add_fetch(&consumed, 1, __ATOMIC_SEQ_CST);

    if (DEBUG >= 2)
      log_info("[Postgres] Waiting on connection %lu to become available", stmt->client_id % POOL_SIZE);

    /* Bind a connection to a client and execute query in thread */
    pthread_mutex_lock(&locks[stmt->client_id % POOL_SIZE]);

    if (DEBUG >= 2)
      log_info("[Postgres] Executing on connection %lu", stmt->client_id % POOL_SIZE);

    conn = conns[stmt->client_id % POOL_SIZE];
    postgres_pexec(stmt, conn);

    /* Release  and clean up */
    pthread_mutex_unlock(&locks[stmt->client_id % POOL_SIZE]);
    pstatement_free(stmt);
  }

  return NULL;
}

/*
 * Add work to the queue.
 */
void postgres_assign(struct PStatement *stmt) {
  /* Let the kernel handle the scheduling */
  if (write(pipes[1], &stmt, sizeof(stmt)) == sizeof(stmt)) {
    __atomic_add_fetch(&scheduled, 1, __ATOMIC_SEQ_CST);

    if (DEBUG)
      log_info("[Postgres] Assigned %lu", stmt->client_id);
  }

  /* Probably should abort() here since the worker thread will. */
  else {
    log_info("[Postgres][%u] Partial write to pipe");
  }
}


/*
 * Prepared statement execution.
 */
static void postgres_pexec(struct PStatement *stmt, PGconn *conn) {
  int i;
  const char *params[stmt->np];
  size_t conn_idx = stmt->client_id % POOL_SIZE;

  for (i = 0; i < stmt->np; i++) {
    params[i] = stmt->params[i]->value;
  }

  /* Skip transactional indicators for now, we can't guarantee per-client connections yet. */
  if (ignore_statement(stmt->query) == STATEMENT_IGNORE) {
    __atomic_add_fetch(&ignored, 1, __ATOMIC_SEQ_CST);
    if (DEBUG)
      log_info("[Postgres] Ignoring %s", stmt->query);
    return;
  }

  if (DEBUG) {
    log_info("[Postgres][%u] Executing %s", stmt->client_id, stmt->query);
  }

  /* Check connection status */
  switch(PQtransactionStatus(conn)) {

    case PQTRANS_INTRANS: {
      /* We likely missed a COMMIT, let's not drag this longer than we have to */
      stmt_cnt[conn_idx]++; /* Not a race because it's 1 conn/thread */
      if (stmt_cnt[conn_idx] > MAX_STATEMENTS_PER_TRANSACTION) {
        PQclear(PQexec(conn, "COMMIT"));
        __atomic_add_fetch(&cut_short, 1, __ATOMIC_SEQ_CST);
        stmt_cnt[conn_idx] = 0;
      }
      break;
    }

    /* Abort any in-progress transactions, some error sneaked through */
    case PQTRANS_INERROR: {
      if (DEBUG)
        log_info("[Postgres] Rolling back transaction in progress");

      PQclear(PQexec(conn, "ROLLBACK"));
      __atomic_add_fetch(&aborted, 1, __ATOMIC_SEQ_CST);
      break;
    }

    default:
      break;
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

static int ignore_statement(char *stmt) {
  if (strstr(stmt, "BEGIN") != NULL) {
    return STATEMENT_TRANSACTION_START;
  }

  /* Not sure about this one */
  if (strstr(stmt, "END") == stmt) {
    return STATEMENT_IGNORE;
  }

  if (strstr(stmt, "COMMIT") != NULL) {
    return STATEMENT_TRANSACTION_END;
  }

  /* Oh well */
  if (strstr(stmt, "ROLLBACK") != NULL) {
    return STATEMENT_TRANSACTION_END;
  }

  /* No copy, since we don't record the 'd' packets.
   * Space is intentional because all copy statements
   * will be "COPY table_name" or "COPY FROM"
  */
  if (strstr(stmt, "COPY ") != NULL) {
    return STATEMENT_IGNORE;
  }

  return STATEMENT_EXECUTE;
}

/*
 * Shutdown pool.
 */
void postgres_free(void) {
  int i;

  for (i = 0; i < POOL_SIZE; i++) {
    /* Tell the threads to stop */
    pthread_cancel(threads[i]);

    /* Clean up */
    PQfinish(conns[i]);
    pthread_mutex_destroy(&locks[i]);
    conns[i] = NULL;
  }
}

/*
 * Show some stats. They are not exact, since this is multi-threaded.
 */
void postgres_stats(void) {
  uint64_t l_ok = 0,
           l_not_ok = 0,
           l_ignored = 0,
           l_consumed = 0,
           l_scheduled = 0,
           l_aborted = 0,
           l_cut_short = 0,
           queue_overflow = 0;

  double success_rate = 0.0;

  /* Load stats */
  __atomic_load(&ok, &l_ok, __ATOMIC_SEQ_CST);
  __atomic_load(&not_ok, &l_not_ok, __ATOMIC_SEQ_CST);
  __atomic_load(&ignored, &l_ignored, __ATOMIC_SEQ_CST);
  __atomic_load(&scheduled, &l_scheduled,__ATOMIC_SEQ_CST);
  __atomic_load(&consumed, &l_consumed, __ATOMIC_SEQ_CST);
  __atomic_load(&aborted, &l_aborted, __ATOMIC_SEQ_CST);
  __atomic_load(&cut_short, &l_cut_short, __ATOMIC_SEQ_CST);

  /* Reset stats, race-prone but that's ok */
  __atomic_store_n(&ok, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&not_ok, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&ignored, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&scheduled,0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&consumed, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&aborted, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&cut_short, 0, __ATOMIC_SEQ_CST);

  success_rate = (double)l_ok / ((double)l_ok + (double)l_not_ok);
  queue_overflow = l_scheduled - l_consumed;

  log_info("[Postgres][Statistics] Success rate: %.4f; OK: %llu; Error: %llu; Ignored: %llu; Scheduled: %llu; Consumed: %llu; Aborted: %llu, Cut short: %llu", success_rate, l_ok, l_not_ok, l_ignored, l_scheduled, l_consumed, l_aborted, l_cut_short);

}
