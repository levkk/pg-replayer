/*
 * Postgres pooler.
 */
int postgres_init();
int postgres_pexec(char *stmt, const char **params, size_t nparams);
int postgres_exec(char *stmt);
void postgres_free();
