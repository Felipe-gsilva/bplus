#include "queue.h"
#include "b-tree-buf.h"

queue *alloc_queue(void) {
  queue *root = malloc(sizeof(queue));
  if (!root) {
    puts("!!Error: Memory allocation failed");
    return NULL;
  }
  root->next = NULL;
  root->page = NULL;
  root->counter = 0;
  if (DEBUG)
    puts("@Allocated queue");
  return root;
}

void clear_queue(queue *q) {
  if (!q) {
    puts("!!Error: NULL queue pointer\n");
    return;
  }
  queue *current = q->next;
  queue *next_node;
  while (current) {
    next_node = current->next;
    if (current->page) {
      clear_page(current->page);
      current->page = NULL;
    }
    free(current);
    current = next_node;
  }
  q->next = NULL;
  q->counter = 0;

  if (DEBUG) {
    puts("@Queue cleared");
  }
}
void print_queue(queue *q) {
  if (!q) {
    fprintf(stderr, "!!Error: NULL queue pointer\n");
    return;
  }

  if (!q->next) {
    fprintf(stderr, "!!Error: Empty queue\n");
    return;
  }

  printf("Queue contents:\n");
  queue *current = q->next;
  int node_count = 0;

  while (current) {
    if (!current->page) {
      fprintf(stderr, "!!Error: NULL page in queue node %d\n", node_count);
      current = current->next;
      continue;
    }

    printf("Node %d (RRN: %d) Keys: ", node_count, current->page->rrn);

    print_page(current->page);
    printf("\n");

    current = current->next;
    node_count++;
  }

  if (DEBUG) {
    printf("Total nodes in queue: %d\n", node_count);
  }
}

void push_page(b_tree_buf *b, page *p) {
  if (!b->q || !p) {
    puts("!!Error: NULL queue pointer or page");
    return;
  }

  page *temp_page = queue_search(b->q, p->rrn);
  if (temp_page != NULL) {
    if (DEBUG)
      puts("@Page already found in queue");
    temp_page->keys_num = p->keys_num;
    temp_page->child_num = p->child_num;
    temp_page->leaf = p->leaf;
    memcpy(temp_page->keys, p->keys, sizeof(key) * p->keys_num);
    memcpy(temp_page->children, p->children, sizeof(u16) * p->child_num);
    return;
  }

  if (b->q->counter > P) {
    page *popped_page = pop_page(b);
    if (DEBUG && popped_page) {
      printf("@Popped page with RRN %hu from queue\n", popped_page->rrn);
    }
  }

  queue *new_node = malloc(sizeof(queue)); // Use malloc instead of alloc_queue
  if (!new_node) {
    puts("!!Error: Memory allocation failed");
    return;
  }

  new_node->page = p;
  new_node->next = NULL;

  queue *temp = b->q;
  while (temp->next != NULL) {
    temp = temp->next;
  }
  temp->next = new_node;
  b->q->counter++;
  if (DEBUG)
    puts("@Pushed page onto queue");
}

page *pop_page(b_tree_buf *b) {
  if (!b->q || b->q->next == NULL) {
    puts("!!Error: NULL or Empty queue pointer");
    return NULL;
  }

  queue *head = b->q->next;
  page *page = head->page;

  b->q->next = head->next;
  b->q->counter--;

  if (DEBUG)
    puts("@Popped from queue");

  head->page = NULL;
  free(head);
  return page;
}
page *queue_search(queue *q, u16 rrn) {
  if (!q) {
    if (DEBUG)
      puts("!!Error: NULL or Empty queue pointer");
    return NULL;
  }
  queue *temp = q->next;
  while (temp != NULL) {
    if (temp->page && temp->page->rrn == rrn) {
      if (DEBUG)
        printf("@Page with RRN %hu found in queue\n", rrn);
      return temp->page;
    }
    temp = temp->next;
  }

  if (DEBUG)
    printf("@Page with RRN %hu not found in queue\n", rrn);
  return NULL;
}
