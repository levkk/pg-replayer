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

#define POOL_SIZE 10
/*
 * Multiplex connections.
 */
static PGconn **conns = NULL;

static pthread_t threads[POOL_SIZE];
static int thread_ids[POOL_SIZE];
static volatile struct PStatement *work_queue[POOL_SIZE];
static int pipes[2] = { 0 };
static pthread_cond_t conditionals[POOL_SIZE];
static pthread_mutex_t signals[POOL_SIZE];
static int shutdown = 0;

static int ignore_transction_blocks(char *stmt);

static void *postgres_worker(void *arg);
static int postgres_pexec(volatile struct PStatement *stmt, PGconn *conn);

/*
 * Initialize the pool.
 */
int postgres_init() {
  assert(conns == NULL);

  int i;
  char *database_url = getenv("DATABASE_URL");

  if (!database_url) {
    printf("No DATABASE_URL is set but is required.\n");
    return -1;
  }

  conns = malloc(POOL_SIZE * sizeof(PGconn *));
  if (pipe(pipes) == -1) {
    printf("pipe\n");
    exit(1);
  }

  for (i = 0; i < POOL_SIZE; i++) {
    PGconn *conn = PQconnectdb(database_url);

    if (PQstatus(conn) == CONNECTION_BAD) {
      fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
      PQfinish(conn);
      return -1;
    }

    conns[i] = conn;

    /* Initialize the threads & signals */
    thread_ids[i] = i;

    if (pthread_mutex_init(&signals[i], NULL)) {
      printf("pthread_mutex_init\n");
      exit(1);
    }

    if (pthread_cond_init(&conditionals[i], NULL)) {
      printf("pthread_cond_init\n");
      exit(1);
    }

    /* No work yet, pause the worker. */
    // pthread_mutex_lock(&signals[i]);

    if (pthread_create(&threads[i], NULL, postgres_worker, &thread_ids[i])) {
      printf("pthread_create\n");
      exit(1);
    }
  }

  return 0;
}

/*
 * The worker.
 */
static void *postgres_worker(void *arg) {
  int id = *(int*)arg;
  PGconn *conn = conns[id];

  while(1) {
    /* Pause until work is given */
    // pthread_mutex_lock(&signals[id]);

    struct PStatement *stmt;
    read(pipes[0], &stmt, sizeof(struct PStatement*));
    printf("Worker %d read it\n", id);

    /* Shutdown, skip clean up for now */
    if (shutdown) {
      printf("[%d] Shut down worker\n", id);
      return NULL;
    }

    if (DEBUG)
      printf("[%d] Got work\n", id);

    assert(stmt != NULL);

    /* Execute query in thread */
    postgres_pexec(stmt, conn);

    /* Clean up */
    pstatement_free(stmt);
    // work_queue[id] = NULL;

    /* Ready for more work */
  }

  return NULL;
}

/*
 * Assign work, polling workers for availability.
 */
void postgres_assign(struct PStatement *stmt) {
  // int i;
  write(pipes[1], stmt, sizeof(stmt));
  // while (1) {
  //   for (i = 0; i < POOL_SIZE; i++) {
  //     /* Worker has no work, give it to it */
  //     if (work_queue[i] == NULL) {
  //       work_queue[i] = stmt;

  //       /* Signal the worker to start working */
  //       // pthread_mutex_unlock(&signals[i]);
  //       return;
  //     }
  //   }
  //   usleep(0.001 * SECOND);
  // }
}

/*
 * Pause all workers to help with context switching overhead.
 */
void postgres_pause(void) {
  int i;
  for (i = 0; i < POOL_SIZE; i++) {
    pthread_mutex_trylock(&signals[i]);
  }
}

/*
 * Prepared statement execution.
 */
static int postgres_pexec(volatile struct PStatement *stmt, PGconn *conn) {
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
    printf("[Postgres][%u] Executing %s\n", stmt->client_id, stmt->query);
  }

  PQclear(PQexecParams(
    conn,
    stmt->query,
    stmt->np,
    NULL,
    params,
    NULL,
    NULL,
    0
  ));

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
 * Check the result of our query.
 *
 * Don't need it for now.
 */
/*
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
*/

/*
 * Shutdown pool.
 */
void postgres_free() {
  int i;

  /* Signal the workers to shut down */
  shutdown = 1;
  for (i = 0; i < POOL_SIZE; i++) {
    pthread_mutex_unlock(&signals[i]);
    pthread_join(threads[i], NULL);
    PQfinish(conns[i]);
  }

  free(conns);
  conns = NULL;
}
