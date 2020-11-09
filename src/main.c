/*
  Postgres queries parser and replayer.
*/

#define VERSION 1.4

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

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
 * I know this is bad since this is actually a valid SQL UTF-8 character.
 * TODO: Implement "getdelim" with 16-bit or 32-bit delimiters.
 */
static const char DELIMITER = '~';
#define LIST_SIZE 4096

#include "helpers.h"
#include "statement.h"
#include "parameter.h"
#include "postgres.h"

/* Throttle logging */
static int erred = 0;

/* Stats */
static size_t q_sent = 0, q_dropped = 0, lines_read = 0, lines_dropped = 0;
static double total_seconds = 0;

/* Show extra info in logs. Used across the code base. */
int DEBUG = 0;
static struct PStatement *list[4098] = { NULL };

/* Safe iterator move.
 *
 * Usually indicates a corrupt packet in the log file.
 */
#define MOVE_IT(it, offset, buf, len) do { \
  if (it + offset > buf + len) { \
    goto next_line; \
  } \
  it += offset; \
  } while (0);

/*
 * Will execute a preparted statement against a connection in the pool.
 */
void pexec(struct PStatement *stmt) {
  assert(stmt != NULL);

  postgres_assign(stmt);
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
    log_info("[Rotation] Could not get lock on %s: %s", lock_fn, strerror(errno));
    return 1;
  }

  /* Rotate */
  if ((res = rename(fn, new_fn))) {
    if (!erred) { /* log errors only once per occurence */
      log_info("[Rotation] Could not rename %s: %s", fn, strerror(errno));
      erred = 1;
    }
  }
  else
    erred = 0;

  flock(fileno(fd), LOCK_UN);
  fclose(fd);
  return res;
}

/*
 * Find the prepared statement in our linked list.
 */
struct PStatement *pstatement_find(uint32_t client_id) {
  int i;
  for (i = 0; i < LIST_SIZE; i++) {
    if (list[i] != NULL) {
      if (list[i]->client_id == client_id) {
        struct PStatement *stmt = list[i];
        list[i] = NULL;
        return stmt;
      }
    }
  }
  return NULL;
}

/*
 * Add the prepared statement into our linked list.
 */
int pstatement_add(struct PStatement *stmt) {
  int i;
  for (i = 0; i < LIST_SIZE; i++) {
    if (list[i] == NULL) {
      list[i] = stmt;
      return 0;
    }
  }

  return 1; /* list full */
}

/*
 * Main loop:
 *   - rotate log file
 *   - read log file and replay packets against mirror DB
 */
int main_loop() {
  FILE *f;
  char *line = NULL, *it, *env_f_name;
  size_t line_len = 0;
  ssize_t nread;
  int i, len;
  struct timeval start, end;
  double seconds;

  /* Start the benchmark */
  gettimeofday(&start, NULL);

  /*
   * Log file
   */
  char fname[512];
  char new_fn[514];

  if ((env_f_name = getenv("PACKET_FILE")) == NULL) {
    /* By default it's in /tmp */
    sprintf(fname, "/tmp/pktlog");
  }

  else {
    sprintf(fname, "%s", env_f_name);
  }

  /* Can't go forward unless we can rotate the file. */
  if (rotate_logfile(new_fn, fname)) {
    return 1;
  }

  f = fopen(new_fn, "r");

  if (f == NULL) {
    log_info("[Main] Could not open packet log");
    return 1;
  }

  while ((nread = getdelim(&line, &line_len, DELIMITER, f)) > 0) {
    /* Not enough data to be a valid line.
     *
     * 5 bytes would have the tag (char) & packet length (32-bit int)
     */
    if (nread < 5) {
      lines_dropped++;
      continue;
    }

    /* Remove the delimiter */
    if (line[nread - 1] == DELIMITER)
      line[nread - 1] = '\0';

    /* Place the iterator at the beginning. */
    it = line;

    uint32_t client_id = parse_uint32(it);
    MOVE_IT(it, sizeof(uint32_t), line, nread);

    /* Parse the tag and move forward */
    char tag = *it;
    MOVE_IT(it, 1, line, nread);

    /* Parse the len of the packet and move forward. */
    /* uint32_t len = parse_uint32(it); */
    MOVE_IT(it, sizeof(uint32_t), line, nread);

    /* Simple query, 'Q' packet */
    if (tag == 'Q') {
      struct PStatement *stmt = pstatement_init(it, client_id);
      pexec(stmt);
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
      if (pstatement_add(stmt)) {
        q_dropped++;
        if (DEBUG)
          log_info("[Main] List full, dropping statement");
        pstatement_free(stmt);
      }
    }

    /* Bind parameter(s), 'B' packet */
    else if (tag == 'B') {
      /* Find the statement this bind belongs to */
      struct PStatement *stmt = pstatement_find(client_id);

      if (stmt == NULL) {
        q_dropped++;
        if (DEBUG)
          log_info("[Main] Dropping out of order Bind packet for client %d", client_id);
        goto next_line;
      }

      /* Parse the packet */

      char *portal = it; /* Portal, can be empty */
      MOVE_IT(it, strlen(portal) + 1, line, nread);
      /* MOVE_IT(it, strlen(portal) + 1, line, line_len); */ /* Skip it for now */

      char *statement = it; /* Statement name, if any  */
      MOVE_IT(it, strlen(statement) + 1, line, nread); /* Also not using it for now */

      uint16_t nf = parse_uint16(it); /* number of formats used */
      MOVE_IT(it, 2, line, nread); /* Parsed it, now move forward */

      /* Parse each format */
      for (i = 0; i < nf; i++) {
        /* uint16_t fmt = parse_uint16(it); */
        MOVE_IT(it, 2, line, nread);
      }

      /* Number of parameters */
      uint16_t np = parse_uint16(it);
      MOVE_IT(it, 2, line, nread); /* move iterator forward 2 bytes */

      /* Save the params */
      for (i = 0; i < np; i++) {
        int32_t plen = (int32_t)parse_uint32(it); /* Parameter length */
        MOVE_IT(it, 4, line, nread); /* 4 bytes */

        struct Parameter *parameter = parameter_init(plen, it);

        pstatement_add_param(stmt, parameter);
        MOVE_IT(it, plen, line, nread);
      }

      pstatement_add(stmt); /* Add back to list */
    }

    /* Execute the prepared statement, 'E' packet */
    else if (tag == 'E') {
      struct PStatement *stmt = pstatement_find(client_id);
      if (stmt == NULL) {
        q_dropped++;
        if (DEBUG)
          log_info("[Main] Dropping out of order E packet for client %d", client_id);
        goto next_line;
      }

      pexec(stmt);

      if (DEBUG)
        pstatement_debug(stmt);

      /* The worker will deallocate this object */
      stmt = NULL;
      q_sent++;
    }

    else {
      lines_dropped++;
      /* BUG: fix corruption in the packet log file.
       * This still might happen, but logs too much.
       * This is due to a poor choice in delimiter.
       */
    }

    /* Clear the line buffer */
  next_line:
    memset(line, 0, nread);
    lines_read++;
  }

  /* Let getdelim re-allocate memory */
  free(line);
  line = NULL;

  /* Close the packet log file we just read and remove it */
  fclose(f);
  unlink(new_fn);

  /* Clean up any orphaned statements.
   *
   * They can become orphaned because packets are out-of-order in the packet log file
   * or have not been logged at all.
   */
  len = 0;
  for (i = 0; i < LIST_SIZE; i++) {
    if (list[i] != NULL) {
      pstatement_free(list[i]);
      list[i] = NULL;
      len++;
    }
  }

  if (len > 0)
    log_info("Orphaned queries: %lu", len);

  /* Benchmark how we did */
  gettimeofday(&end, NULL);

  seconds = (end.tv_sec - start.tv_sec) * 1e6;
  seconds = (seconds + end.tv_usec - start.tv_usec) * 1e-6;
  total_seconds += seconds;

  /* Only log when enough queries went through, otherwise we would log too much */
  if (q_sent > 2048) {
    log_info("[Main][Statistics] Sent %lu queries; dropped %lu out-of-order packets; read %lu lines; corrupted %lu lines; time %.2f seconds", q_sent, q_dropped, lines_read, lines_dropped, total_seconds);
    postgres_stats();
    q_sent = 0;
    q_dropped = 0;
    total_seconds = 0;
    lines_read = 0;
    lines_dropped = 0;
  }

  return 0;
}

/*
 * Clean up everything if clean shut down.
 */
void cleanup(int signo) {
  postgres_free();

  log_info("Exiting. Bye!");
  exit(0);
}

/*
 * Entrypoint.
 */
int main() {
  log_info("PGReplayer %.2f started. Waiting for packets", VERSION);
  char *debug = getenv("DEBUG");
  if (debug != NULL) {
    DEBUG = atoi(debug);
  }

  if (DEBUG) {
    log_info("libpq version: %d", PQlibVersion());
  }

  if (postgres_init()) {
    log_info("Postgres pool failed to initialize");
    exit(1);
  }

  if (signal(SIGINT, cleanup) == SIG_ERR) {
    log_info("Can't catch signals, so no clean up will be done on shutdown");
  }

  while(1) {
    main_loop();
    usleep(SECOND * 0.1); /* Sleep for .1 of a second */
  }
}
