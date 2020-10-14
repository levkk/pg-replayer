/* 
 * Tests
 */

#include "replayer.h"
#include "helpers.h"
#include "statement.h"
#include "parameter.h"
#include "postgres.h"
#include "list.h"

#include <assert.h>

void test_list() {
	int i;
	struct List *head = list_init();

	for(i = 0; i < 25; i++) {
		int *it = malloc(sizeof(int));
		*it = i;
		list_add(head, it);
	}

	struct List *it = head;
	while (it != NULL) {
		printf("%d\n", *(int*)it->value);
		it = list_next(it);
	}

	assert(list_len(head) == 25);
	assert(!list_empty(head));

	it = head;
	while (it != NULL) {
		void *val = list_remove(head, it);
		free(val);
		it = list_next(it);
	}

	assert(list_len(head) == 0);
	assert(list_empty(head));

}

int main() {
	test_list();
}
