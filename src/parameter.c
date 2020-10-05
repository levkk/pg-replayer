
#include "parameter.h"
#include "helpers.h"

#include <string.h>

/*
 * Initialize the parameter.
 */
struct Parameter *parameter_init(int32_t len, char *value) {
	struct Parameter *param = malloc(sizeof(struct Parameter));

	param->len = len;
	param->value = malloc(len + 1);
	memcpy(param->value, value, len);
	param->value[len] = '\0';

	return param;
}

/*
 * Debug
 */
void parameter_debug(struct Parameter *param) {
	assert(param != NULL);

	printf("DEBUG: Parameter(len=%d, value='%s')\n", param->len, param->value);
}

/*
 * Deallocate the structure.
 */
void parameter_free(struct Parameter *param) {
	free_safe(param->value);
	free_safe(param);
}
