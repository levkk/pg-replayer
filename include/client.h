#ifndef CLIENT_H
#define CLIENT_H
/*
 *
 */
#include <stdint.h>
#include "statement.h"

#define STATEMENT_LIMIT 10


struct Client {
	struct PStatement **queue;
	uint32_t client_id;
	int has_begin;
	int has_commit;
};

struct Client *client_init(struct PStatement *stmt);
int client_add_stmt(struct Client *client, struct PStatement *stmt);
int client_queue_full(struct Client *client);
void client_free(struct Client*);

#endif
