/*
 * Postgres pooler.
 */
int postgres_init();
void postgres_assign(struct PStatement*);
void postgres_pause(void);
void postgres_free();
