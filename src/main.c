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
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/time.h>

#include "replayer.h"

/*
 * Separator between packets.
 */
#define DELIMETER '\x19' /* EM */

#include "helpers.h"
#include "statement.h"
#include "parameter.h"
#include "postgres.h"
#include "list.h"

/*
 * Will execute a preparted statement against the connection.
 */
void pexec(struct PStatement *stmt) {
  assert(stmt != NULL);

  int i;
  const char *params[stmt->np];

  for (i = 0; i < stmt->np; i++) {
    params[i] = stmt->params[i]->value;
  }

  /* Send the prepared statement over. */
  /* The pooler will check result eventually */
  if (DEBUG) {
    printf("Executing query for client %u.\n", stmt->client_id);
  }
  postgres_pexec(stmt->query, params, stmt->np);
}

/*
 * Will execute a simple query.
 */
void exec(char *query) {
  postgres_exec(query);
}

/*
 * Move the iterator foward while protecting against buffer overruns.
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
 * Rotate packet logfile so the bouncer can log some more.
 */
int rotate_logfile(char *new_fn, const char *fn) {
  int res;
  char lock_fn[strlen(fn) + 6];

  /* lock file for concurrent log file access */
  sprintf(lock_fn, "%s.lock", fn);

  /* rotate log file to this */
  sprintf(new_fn, "%s.1", fn);

  /* Get exclusive lock & rotate */
  FILE *fd = fopen(lock_fn, "w");

  /* Try again if we can't get a lock */
  if (flock(fileno(fd), LOCK_EX) == EWOULDBLOCK) {
    printf("[Rotation] Could not get lock on %s: %s\n", lock_fn, strerror(errno));
    return 1;
  }

  /* Rotate */
  if ((res = rename(fn, new_fn))) {
    printf("[Rotation] Could not rename %s: %s\n", fn, strerror(errno));
    goto unlock;
  }

  /* Touch the logfile to create an empty one */
  FILE *f = fopen(fn, "w");
  if (f) {
    fclose(f);
  }

unlock:
  flock(fileno(fd), LOCK_UN);
  fclose(fd);
  return res;
}

struct List *pstatement_find(struct List *pstatements, uint32_t client_id) {
  struct List *it = pstatements;
  while (it != NULL) {
    struct PStatement *pstatement = (struct PStatement*)it->value;
    assert(pstatement != NULL);
    if (pstatement->client_id)
      return it;
    it = list_next(it);
  }
  return NULL;
}

/*
 * Main loop:
 *   - rotate log file
 *   - read log file and replay packets against mirror DB
 */

int main_loop() {
  FILE *f;
  char *line = NULL, *it, *env_f_name = getenv("PACKET_FILE");
  size_t line_len;
  ssize_t nread;
  int i, q_sent = 0;
  struct timeval start, end;

  gettimeofday(&start, NULL);

  /*
   * Log file
   */
  char fname[512];
  char new_fn[514];

  if (env_f_name == NULL) {
    sprintf(fname, "/tmp/pktlog");
  }

  else {
    sprintf(fname, "%s", env_f_name);
  }

  if (rotate_logfile(new_fn, fname)) {
    return 1;
  }

  f = fopen(new_fn, "r");

  if (f == NULL) {
    printf("[Main] Could not open packet log.");
    return 1;
  }

  struct List *pstatements = list_init();

  while ((nread = getdelim(&line, &line_len, DELIMETER, f)) > 0) {
    /* Not enough data to be a valid line. */

    if (line_len < 5) {
      continue;
    }

    /* Place the iterator at the beginning. */
    it = line;

    uint32_t client_id = parse_uint32(it);
    move_it(&it, 4, line, line_len);

    /* Parse the tag and move forward */
    char tag = *it;
    move_it(&it, 1, line, line_len);

    /* Parse the len of the packet and move forward. */
    uint32_t len = parse_uint32(it);
    move_it(&it, 4, line, line_len);

    /* Simple query, 'Q' packet */
    if (tag == 'Q') {
      exec(it);
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

      struct PStatement *stmt = pstatement_init(query, client_id);
      list_add(pstatements, stmt);
    }

    /* Bind parameter(s), 'B' packet */
    else if (tag == 'B') {
      /* Find the statement this bind belongs to */
      struct List *node = pstatement_find(pstatements, client_id);

      if (node == NULL) {
        if (DEBUG)
          printf("[Main] Out of order Bind packet for client %d. Dropping.\n", client_id);
        continue;
      }

      struct PStatement *stmt = (struct PStatement*)node->value;

      /* Parse the packet */

      char *portal = it; /* Portal, can be empty */
      move_it(&it, strlen(portal) + 1, line, line_len);
      // move_it(&it, strlen(portal) + 1, line, line_len); /* Skip it for now */

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

      /* Save the params */
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
      struct List *node = pstatement_find(pstatements, client_id);
      if (node == NULL) {
        if (DEBUG)
          printf("[Main] Out of order E packet for client %d. Dropping. \n", client_id);
        continue;
      }

      struct PStatement *stmt = (struct PStatement*)node->value;
      pexec(stmt);
      if (DEBUG) {
        pstatement_debug(stmt);
      }
      list_remove(pstatements, node);
      pstatement_free(stmt);
      stmt = NULL;
      q_sent += 1;
    }

    else {
      printf("Unsupported tag: %c\n",  tag);
      hexDump("line", line, line_len);
    }

    /* Clear the line buffer */
    memset(line, 0, line_len);
  }

  free(line);
  fclose(f);

  printf("Orphaned queries: %lu.\n", list_len(pstatements));
  list_free(pstatements);

  gettimeofday(&end, NULL);

  printf("Sent %d queries in %f seconds.\n", q_sent, (double)((end.tv_sec - start.tv_sec)));

  return 0;
}

/*
 * Clean up everything if clean shut down.
 */
void cleanup(int signo) {
  postgres_free();

  printf("Exiting. Bye!\n");
  exit(0);
}

/*
 * Entrypoint.
 */
int main() {
  if (DEBUG) {
    printf("libpq version: %d\n", PQlibVersion());
  }

  if (postgres_init()) {
    printf("Postgres pool failed to initialize.\n");
    exit(1);
  }

  if (signal(SIGINT, cleanup) == SIG_ERR) {
    printf("Can't catch signals, so no clean up will be done on shutdown.\n");
  }

  while(1) {
    main_loop();
    usleep(SECOND * 0.1); /* Sleep for .1 of a second */
  }
}
