#ifndef HELPERS_H
#define HELPERS_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

/*
 * Move iterator and check that we didn't go too far
 */
#define MOVE_IT(a, b, c, d) do { \
	a += b; \
	assert(a + b < c + d); \
	} while (0);


/* https://stackoverflow.com/questions/7775991/how-to-get-hexdump-of-a-structure-data */
void hexDump(const char * desc, const void * addr, const int len);

/*
 * Helper to free a pointer and set it to null, safely.
 */
void free_safe(void *ptr, const char*);

/*
 * Convert between 4 bytes of network data and a 32 bit integer.
 */
uint32_t parse_uint32(char *data);

/*
 * Convert between 2 bytes of network data and a 16 bit integer.
 */
uint16_t parse_uint16(char *data);

/*
 * random string
 */
void gen_random(char *s, const int len);

#endif
