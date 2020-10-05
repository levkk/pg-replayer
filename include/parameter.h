#ifndef PARAMETER_H
#define PARAMETER_H

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

struct Parameter {
  int32_t len;
  char *value;
};

struct Parameter *parameter_init(int32_t len, char *value);
void parameter_debug(struct Parameter *param);
void parameter_free(struct Parameter *param);

#endif
