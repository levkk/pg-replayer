/*
 * Prepared statement.
 */

#include <stdint.h>

#include "parameter.h"

struct PStatement {
	uint32_t client_id;
	char *query;
	struct Parameter **params;
	uint16_t np;
	uint16_t sp;
};

struct PStatement *pstatement_init(char *query, uint32_t client_id);
void pstatement_add_param(struct PStatement *stmt, struct Parameter *param);
void pstatement_debug(struct PStatement *stmt);
void pstatement_free(struct PStatement *stmt);
