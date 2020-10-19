/*
 * Postgres pooler.
 */
int postgres_init();
void postgres_assign(struct PStatement*);
void postgres_pause(void);
int postgres_pexec(char *stmt, const char **params, size_t nparams);
int postgres_exec(char *stmt);
void postgres_free();
