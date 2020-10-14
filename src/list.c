/*
 * A simple linked list.
 */
#include "list.h"
#include <stdlib.h>
#include <assert.h>

/*
 * List init
 */
struct List *list_init() {
  struct List *head = malloc(sizeof(struct List));
  head->next = NULL;
  head->value = NULL;
  return head;
}

/*
 * List dealloc
 */
void list_free(struct List *head) {
  struct List *it = head;
  while (it != NULL) {
    struct List *next = it->next;
    free(it);
    it = next;
  }
}

/*
 * List len
 */
size_t list_len(struct List *head) {
  int count = 0;

  struct List *it = head;
  while (it != NULL && it->value != NULL) {
    count++;
    it = it->next;
  }

  return count;
}

/*
 *
 */
int list_empty(struct List *head) {
  return head->value == NULL ? 1 : 0;
}

/*
 * Find the tail of the list.
 */
struct List *list_tail(struct List *node) {
  struct List *it = node;
  while (it->next != NULL)
    it = it->next;
  return it;
}

/*
 * Find the head of the list.
 */
struct List *list_head(struct List *node) {
  assert(node != NULL);
  struct List *it = node;
  while (it->prev != NULL)
    it = it->prev;
  return it;
}

/*
 * Add a value to the list.
 */
struct List *list_add(struct List *head, void *value) {
  assert(head != NULL);

  /* Empty list */
  if (head->value == NULL) {
    head->value = value;
    return head;
  }

  /* Find the tail */
  struct List *it = head;
  while (it->next != NULL) {
    it = it->next;
  }

  struct List *node = malloc(sizeof(struct List));
  node->value = value;
  it->next = node;
  node->prev = it;

  return node;
}

/*
 * List next
 */
struct List *list_next(struct List *node) {
  assert(node != NULL);

  /* Empty list */
  if (node->value == NULL) {
    return NULL;
  }

  return node->next;
}

/*
 * Find a value in the list.
 */
struct List *list_find(struct List *node, int (*cmp)(void*)) {
  struct List *it = list_head(node);

  do {
    if (!cmp(it->value))
      return it;
    it = it->next;
  } while(it->next != NULL);

  return NULL;
}

/*
 * Remove the node from the list.
 */
void *list_remove(struct List *head, struct List *node) {
  assert(head != NULL);
  assert(node != NULL);
  
  struct List *it = head;

  while (it != NULL) {
    if (it == node) {
      it->prev = it->next;
      it->next = it->prev;
      void *value = node->value;
      node->value = NULL;

      if (node != head)
        free(node);
      return value;
    }
    it = list_next(it);
  }

  return NULL;
}
