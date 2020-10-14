#include <stdlib.h>

/*
 * A simple linked list.
 */
struct List {
  struct List *next;
  struct List *prev;
  struct List *tail;
  struct List *head;
  void *value;
};


struct List *list_init();
void list_free(struct List *node);
struct List *list_head(struct List *node);
struct List *list_tail(struct List *node);
struct List *list_add(struct List *node, void *value);
struct List *list_find(struct List *node, int (*cmp)(void*));
struct List *list_next(struct List *node);
void *list_remove(struct List *head, struct List *node);
size_t list_len(struct List *head);
int list_empty(struct List *head);
