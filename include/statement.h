/*
 * Prepared statement.
 */

#include <stdint.h>

#include "parameter.h"

struct PStatement {
	char *query;
	struct Parameter **params;
	uint16_t np;
	uint16_t sp;
};

struct PStatement *pstatement_init(char *query);
void pstatement_add_param(struct PStatement *stmt, struct Parameter *param);
void pstatement_debug(struct PStatement *stmt);
void pstatement_free(struct PStatement *stmt);
