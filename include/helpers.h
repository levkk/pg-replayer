#ifndef HELPERS_H
#define HELPERS_H

#include <stdlib.h>
#include <stdio.h>

// https://stackoverflow.com/questions/7775991/how-to-get-hexdump-of-a-structure-data
void hexDump(const char * desc, const void * addr, const int len);

/*
 * Helper to free a pointer and set it to null, safely.
 */
void free_safe(void *ptr);

#endif
