/*
 * Prepared statement.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "statement.h"
#include "helpers.h"

#define PARAM_PREALLOC 5

/*
 * Initialize.
 */
struct PStatement *pstatement_init(char *query, uint32_t client_id) {
	struct PStatement *stmt = malloc(sizeof(struct PStatement));
	size_t len = strlen(query);
	stmt->query = malloc(len + 1);
	memcpy(stmt->query, query, len + 1);
	stmt->query[len] = '\0';

	stmt->params = malloc(PARAM_PREALLOC * sizeof(struct Parameter *));
	stmt->sp = PARAM_PREALLOC;
	stmt->np = 0;
	stmt->client_id = client_id;

	return stmt;
}

/*
 * Add a parameter to the statement.
 */
void pstatement_add_param(struct PStatement *stmt, struct Parameter *param) {
	if (stmt->sp == stmt->np) {
		uint16_t new_sp = stmt->sp + PARAM_PREALLOC;
		stmt->params = realloc(stmt->params, new_sp * sizeof(struct Parameter *));
		stmt->sp = new_sp;
	}
	stmt->params[stmt->np] = param;

	stmt->np++;
}

/*
 * Debug.
 */
void pstatement_debug(struct PStatement *stmt) {
	assert (stmt != NULL);

	int i;

	// for (i = 0; i < strlen(stmt->query); i++) {
	// 	if (stmt->query[i] == '\n') {
	// 		stmt->query[i] = ' ';
	// 	}
	// }

	printf("[Debug]: PStatement(query=[%s])\n", stmt->query);

	for (i = 0; i < stmt->np; i++) {
		parameter_debug(stmt->params[i]);
	}
}

/*
 * Free
 */
void pstatement_free(struct PStatement *stmt) {
	int i;
	for (i = 0; i < stmt->np; i++) {
		parameter_free(stmt->params[i]);
	}
	free_safe(stmt->params, "pstatement_free");
	free_safe(stmt->query, "pstatement_free");
	free_safe(stmt, "pstatement_free");
}
