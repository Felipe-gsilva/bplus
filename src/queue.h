#ifndef _QUEUE
#define _QUEUE

#include "defines.h"

// queue max
#define P 5 

queue *alloc_queue(void);

void clear_queue(queue *queue);

void print_queue(queue *queue);

void push_page(b_tree_buf *b, page *page);

page *pop_page(b_tree_buf *b);

page *queue_search(queue *queue, u16 rrn);

#endif
