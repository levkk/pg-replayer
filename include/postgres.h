/*
 * Postgres pooler.
 */
int postgres_init(void);
void postgres_assign(struct PStatement*);
void postgres_free(void);
void postgres_stats(void);
