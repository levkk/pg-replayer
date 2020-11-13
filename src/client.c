/*
 * Client
 */
#include <string.h>
#include "client.h"
#include "helpers.h"

static int transaction_start(struct PStatement*);
static int transaction_commit(struct PStatement *stmt);

struct Client *client_init(struct PStatement *stmt) {
	struct Client *client = malloc(sizeof(struct Client));
	client->queue = malloc(sizeof(struct PStatement*) * STATEMENT_LIMIT);
	memset(client->queue, 0, sizeof(struct PStatement*) * STATEMENT_LIMIT);
	client->client_id = stmt->client_id;
	client->queue[0] = stmt;
	client->has_begin = transaction_start(stmt);
	client->has_commit = 0;

	return client;
}

int client_add_stmt(struct Client *client, struct PStatement *stmt) {
	int i;
	for (i = 0; i < STATEMENT_LIMIT; i++) {
		if (client->queue[i] != NULL) {
			client->queue[i] = stmt;
			return 0;
		}
	}

	client->has_commit = transaction_commit(stmt);

	return 1; /* queue full */
}

void client_free(struct Client *client) {
	int i;
	for (i = 0; i < STATEMENT_LIMIT; i++) {
		if (client->queue[i] != NULL) {
			pstatement_free(client->queue[i]);
		}
	}

	free_safe(client->queue, "client_free");
	free_safe(client, "client_free");
}

int client_queue_full(struct Client *client) {
	if (client->queue[STATEMENT_LIMIT-1] != NULL) {
		return 1;
	}

	return 0;
}

static int transaction_start(struct PStatement *stmt) {
	if (strstr(stmt->query, "BEGIN") != NULL) {
		return 1;
	}

	return 0;
}

static int transaction_commit(struct PStatement *stmt) {
	if (
		strstr(stmt->query, "COMMIT") != NULL || 
		strstr(stmt->query, "ROLLBACK") != NULL ||
		strstr(stmt->query, "END"))
	{
		return 1;
	}

	return 0;
}
