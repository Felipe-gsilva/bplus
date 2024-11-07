#include "test.h"

#include "../src/b-tree-buf.h"
#include "../src/free-rrn-list.h"
#include "../src/io-buf.h"
#include "../src/queue.h"

void test_queue_search(void) {
  b_tree_buf *b = alloc_tree_buf();
  if (!b) {
    puts("!!Error: Could not allocate b_tree_buf");
    return;
  }

  for (u16 i = 0; i < 5; i++) {
    page *p = alloc_page();
    if (!p) {
      puts("!!Error: Could not allocate page");
      continue;
    }
    p->rrn = i;
    push_page(b, p);
  }

  for (u16 i = 0; i < 5; i++) {
    page *p = queue_search(b->q, i);
    if (p) {
      printf("Page with RRN %hu found in queue\n", i);
    } else {
      printf("Page with RRN %hu not found in queue\n", i);
    }
  }

  clear_tree_buf(b);
}

void test_tree(b_tree_buf *b, io_buf *data, int n) {
  if (!b || !data) {
    puts("!!Invalid parameters");
    return;
  }
  int errors = 0;
  u16 pos;
  if (!b->i) {
    load_list(b->i, b->io->br->free_rrn_address);
    if (b->i && DEBUG) {
      puts("@Loaded rrn list");
    }
  }
  data_record d;
  page *p = alloc_page();
  for (int i = 0; i < n; i++) {
    d = *load_data_record(data, i);
    puts("--------------");
    printf("TEST NUM %d\n", i);
    printf("tested ids %s\n", d.placa);

    p = b_search(b, d.placa, &pos);
    if (!p) {
      errors++;
    }
    if (p)
      print_page(p);
  }
  printf("ASSERTS: %d \tERRRORS: %d\t", n - errors, errors);
  if (DEBUG) {
    puts("@Built tree");
  }
}
