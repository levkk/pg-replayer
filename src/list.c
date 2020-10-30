/*
 * A simple linked list.
 */
#include "list.h"
#include "helpers.h"
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

  if (node == NULL) {
    log_info("[List] No memory");
    exit(1);
  }

  node->next = NULL;
  node->value = value;
  it->next = node;

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
 * Remove the node from the list.
 */
void *list_remove(struct List *head, struct List *node) {
  assert(head != NULL);
  assert(node != NULL);
  
  struct List *it = head, *prev = head;

  while (it != NULL) {
    if (it == node) {
      void *value = node->value;
      node->value = NULL;

      if (node != head) {
        prev->next = node->next;
        free(node);
      }

      return value;
    }

    prev = it;
    it = it->next;
  }

  return NULL;
}
