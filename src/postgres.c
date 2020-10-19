/*
 * Postgres pooler.
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
static uint32_t *conn_clients = NULL;
// static size_t conn_idx = 0;

static pthread_t threads[POOL_SIZE];
static int thread_ids[POOL_SIZE];
static struct PStatement *work_queue[POOL_SIZE];
static pthread_mutex_t signals[POOL_SIZE];
static int shutdown = 0;

// static int postgres_conn();
// static int check_result(PGresult *res, PGconn *conn, char *query);
static int ignore_transction_blocks(char *stmt);

static void *postgres_worker(void *arg);
static int postgres_pexec(struct PStatement *stmt, PGconn *conn);

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

    /* Initialize the threads & signals */
    thread_ids[i] = i;

    if (pthread_mutex_init(&signals[i], NULL)) {
      printf("pthread_mutex_init\n");
      exit(1);
    }

    /* No work yet */
    pthread_mutex_lock(&signals[i]);

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
    pthread_mutex_lock(&signals[id]);
    if (shutdown) {
      printf("[%d] Shut down worker\n", id);
      return NULL;
    }
    if (DEBUG)
      printf("[%d] Got work\n", id);

    /* Interrupted */
    assert(work_queue[id] != NULL);

    postgres_pexec(work_queue[id], conn);

    /* clean up */
    pstatement_free(work_queue[id]);
    work_queue[id] = NULL;
    /* ready for more work */
  }

  return NULL;
}

/*
 * Assign work, in a polling way.
 */
void postgres_assign(struct PStatement *stmt) {
  int i;
  while (1) {
    for (i = 0; i < POOL_SIZE; i++) {
      if (work_queue[i] == NULL) {
        work_queue[i] = stmt;
        pthread_mutex_unlock(&signals[i]);
        return;
      }
    }
    usleep(0.001 * SECOND);
  }
}

/*
 * Pause all workers.
 */
void postgres_pause(void) {
  int i;
  for (i = 0; i < POOL_SIZE; i++) {
    pthread_mutex_trylock(&signals[i]);
  }
}

// /*
//  * Transactional check.
//  */
// PGconn *postgres_conn_trans(char *stmt) {
//   size_t idx = conn_idx++ % POOL_SIZE;
//   if (DEBUG)
//     printf("[Postgres] Using %lu connection from the pool.\n", idx);
//   return conns[idx];
// }

// /*
//  * "Async" PQexec
//  */
// int postgres_exec(char *stmt) {
//    Skip transactional indicators 
//   if (ignore_transction_blocks(stmt)) {
//     return 0;
//   }

//   PGconn *conn = postgres_conn_trans(stmt);
  

//   if (DEBUG) {
//     printf("[Postgres] Executing %s\n", stmt);
//   }

//   PGresult *res = PQexec(conn, stmt);
//   return check_result(res, conn, stmt);
// }

/*
 * "Async" prepared statement execution.
 */
static int postgres_pexec(struct PStatement *stmt, PGconn *conn) {
  int i;
  const char *params[stmt->np];

  for (i = 0; i < stmt->np; i++) {
    params[i] = stmt->params[i]->value;
  }

  /* Skip transactional indicators */
  if (ignore_transction_blocks(stmt->query)) {
    return 0;
  }

  if (DEBUG) {
    printf("[Postgres][%u] Executing %s\n", stmt->client_id, stmt->query);
  }

  PQexecParams(
    conn,
    stmt->query,
    stmt->np,
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
 * Check the result of our query.
 */
// static int check_result(PGresult *res, PGconn *conn, char *query) {
//   int code = PQresultStatus(res);

//   switch(code) {
//     case PGRES_TUPLES_OK:
//     case PGRES_COMMAND_OK: {
//       if (DEBUG)
//         printf("[Postgres] Executed: %s\n", query);
//       break;
//     }
//     default: {
//       char *err = PQerrorMessage(conn);
//       printf("[%d] Error: %s\n", code, err);
//       break;
//     }
//   }

//   PQclear(res);

//   return code;
// }

/*
 * Shutdown pool
 */
void postgres_free() {
  int i;
  shutdown = 1;
  for (i = 0; i < POOL_SIZE; i++) {
    pthread_mutex_unlock(&signals[i]);
    pthread_join(threads[i], NULL);
    PQfinish(conns[i]);
  }
  free(conns);
  free(conn_clients);
  conns = NULL;
  conn_clients = NULL;
}
