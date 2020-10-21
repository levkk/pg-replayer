#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <stdarg.h>
#include <string.h>

#include "replayer.h"


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

/* https://stackoverflow.com/questions/7775991/how-to-get-hexdump-of-a-structure-data */
void hexDump (const char * desc, const void * addr, const int len) {
    int i;
    unsigned char buff[17];
    const unsigned char * pc = (const unsigned char *)addr;

    /* Output description if given. */

    if (desc != NULL)
        printf ("%s:\n", desc);

    /* Length checks. */

    if (len == 0) {
        printf("  ZERO LENGTH\n");
        return;
    }
    else if (len < 0) {
        printf("  NEGATIVE LENGTH: %d\n", len);
        return;
    }

    /* Process every byte in the data. */

    for (i = 0; i < len; i++) {
        /* Multiple of 16 means new line (with line offset). */

        if ((i % 16) == 0) {
            /* Don't print ASCII buffer for the "zeroth" line. */

            if (i != 0)
                printf ("  %s\n", buff);

            /* Output the offset. */

            printf ("  %04x ", i);
        }

        /* Now the hex code for the specific character. */
        printf (" %02x", pc[i]);

        /* And buffer a printable ASCII character for later. */

        if ((pc[i] < 0x20) || (pc[i] > 0x7e)) /* isprint() may be better. */
            buff[i % 16] = '.';
        else
            buff[i % 16] = pc[i];
        buff[(i % 16) + 1] = '\0';
    }

    /* Pad out last line if not exactly 16 characters. */

    while ((i % 16) != 0) {
        printf ("   ");
        i++;
    }

    /* And print the final ASCII buffer. */

    printf ("  %s\n", buff);
}

void free_safe(void *ptr, const char *called_from) {
  if (ptr != NULL) {
    if (DEBUG >= 2) {
      printf("[Debug] Dealocated %p from %s\n", ptr, called_from);
    }
    free(ptr);
    ptr = NULL;
  }
}

void gen_random(char *s, const int len) {
  static const char alphanum[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  int i;

  for (i = 0; i < len; ++i) {
    s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  s[len] = 0;
}

/* Log to stdout */
void log_info(const char *fmt, ...) {
  char buf[2048]; /* Max log line length = 2048 chars */
  int date_len = strlen("2020-01-01 00:00:00 +hhmm");
  char timebuf[date_len+1];
  struct tm *local;
  time_t now = time(NULL);
  local = localtime(&now);

  memset(buf, 0, sizeof(buf));
  memset(timebuf, 0, sizeof(timebuf));

  strftime(timebuf, sizeof(timebuf), "%F %T %z", local);

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  printf("%s INFO %s\n", timebuf, buf);
}
