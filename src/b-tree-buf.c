#include "b-tree-buf.h"
#include "free-rrn-list.h"
#include "io-buf.h"
#include "queue.h"

b_tree_buf *alloc_tree_buf(void) {
  b_tree_buf *b = malloc(sizeof(b_tree_buf));
  if (!b) {
    puts("!!Could not allocate b_tree_buf_BUFFER");
    return NULL;
  }

  b->root = NULL;
  b->io = alloc_io_buf();
  if (!b->io) {
    free(b);
    puts("!!Could not allocate io_buf");
    return NULL;
  }

  b->q = alloc_queue();
  if (!b->q) {
    free(b->io);
    free(b);
    puts("!!Could not allocate queue");
    return NULL;
  }

  b->i = alloc_ilist();
  if (!b->i) {
    free(b->q);
    free(b->io);
    free(b);
    puts("!!Could not allocate ilist");
    return NULL;
  }

  if (DEBUG)
    puts("@Allocated b_tree_buf_BUFFER");
  return b;
}

void clear_tree_buf(b_tree_buf *b) {
  if (b) {
    clear_ilist(b->i);
    clear_queue(b->q);
    clear_io_buf(b->io);
    if (b->root) {
      page *q_page = queue_search(b->q, b->root->rrn);
      if (!q_page) {
        clear_page(b->root);
        b->root = NULL;
      }
    }
    free(b);
    b = NULL;
  }
  if (DEBUG)
    puts("@B_TREE_BUFFER cleared");
}

void populate_index_header(index_header_record *bh, const char *file_name) {
  if (bh == NULL) {
    puts("!!Header pointer is NULL, cannot populate");
    return;
  }

  bh->page_size = sizeof(page);
  bh->root_rrn = 0;
  strcpy(bh->free_rrn_address, file_name);
  bh->free_rrn_address[strlen(file_name) + 1] = '\0';
  bh->header_size = (sizeof(u16) * 3) + strlen(file_name) + 1;
}

void build_tree(b_tree_buf *b, io_buf *data, int n) {
  if (!b || !data) {
    puts("!!Invalid parameters");
    return;
  }
  if (!b->i) {
    load_list(b->i, b->io->br->free_rrn_address);
    if (b->i && DEBUG) {
      puts("@Loaded rrn list");
    }
  }
  data_record *d;
  for (int i = 0; i < n; i++) {
    d = load_data_record(data, i);
    if (!d) {
      printf("!!Failed to load record %d\n", i);
      continue;
    }
    if (DEBUG)
      print_data_record(d);

    btree_status status = b_insert(b, data, d, i);
    if ((status != BTREE_SUCCESS) && (status != BTREE_INSERTED_IN_PAGE)) {
      printf("!!Failed to insert record %d, error: %d\n", i - 1, status);
      exit(0);
    }
  }
  if (d)
    free(d);

  if (DEBUG) {
    puts("@Built tree");
  }
}

page *load_page(b_tree_buf *b, u16 rrn) {
  if (!b->io) {
    puts("!!Error while loading page");
    return NULL;
  }

  page *page;
  page = queue_search(b->q, rrn);
  if (page != NULL) {
    if (DEBUG) {

      puts("@Page found on queue");
      print_page(page);
    }
    return page;
  }

  if (!page)
    page = alloc_page();

  int byte_offset = (b->io->br->header_size) + ((b->io->br->page_size) * rrn);

  if (fseek(b->io->fp, byte_offset, SEEK_SET)) {
    puts("!!Error: could not fseek");
    return NULL;
  }

  if (fread(page, b->io->br->page_size, 1, b->io->fp) != 1) {
    if (DEBUG)
      puts("!!Error: could not read page");
    return NULL;
  }

  if (page)
    push_page(b, page);

  if (DEBUG)
    printf("Loaded page RRN: %hu, keys_num: %u\n", page->rrn, page->keys_num);

  return page;
}

int write_root_rrn(b_tree_buf *b, u16 rrn) {
  if (!b) {
    puts("!!Error: NULL b_tree_buf");
    return BTREE_ERROR_IO;
  }

  b->io->br->root_rrn = rrn;

  fseek(b->io->fp, 0, SEEK_SET);
  size_t flag = fwrite(&rrn, sizeof(u16), 1, b->io->fp);
  if (flag != 1) {
    puts("!!Error: Could not update root rrn");
    exit(-1);
  }

  fflush(b->io->fp);

  return BTREE_SUCCESS;
}

void b_update(b_tree_buf *b, io_buf *data, free_rrn_list *ld,
              const char *placa) {}

page *b_search(b_tree_buf *b, const char *s, u16 *return_pos) {
  u16 pos;
  page *return_page = NULL;
  key key;
  strcpy(key.id, s);
  if (!b->root)
    b->root = load_page(b, b->io->br->root_rrn);
  page *temp = b->root;
  int flag = search_key(b, temp, key, &pos, &return_page);
  if (flag == BTREE_FOUND_KEY) {
    *return_pos = pos;
    return return_page;
  }
  puts("--> Could not find key");
  return NULL;
}

int search_in_page(page *p, key key, int *return_pos) {
  if (!p) {
    puts("!!Error: no page");
    return BTREE_ERROR_INVALID_PAGE;
  }

  for (int i = 0; i < p->keys_num; i++) {
    if (p->keys[i].id[0] == '\0') {
      *return_pos = i;
      return BTREE_NOT_FOUND_KEY;
    }

    if (DEBUG)
      printf("page key id: %s\t key id: %s\n", p->keys[i].id, key.id);
    if (strcmp(p->keys[i].id, key.id) == 0) {
      puts("@Curr key was found");
      *return_pos = i;
      return BTREE_FOUND_KEY;
    }

    if (strcmp(p->keys[i].id, key.id) > 0) {
      *return_pos = i;
      if (DEBUG)
        puts("@Curr key is greater than the new one");
      return BTREE_NOT_FOUND_KEY;
    }
  }

  *return_pos = p->keys_num;
  return BTREE_NOT_FOUND_KEY;
}

u16 search_key(b_tree_buf *b, page *p, key key, u16 *found_pos,
               page **return_page) {
  if (!b || !p) {
    puts("!!Error: NULL parameters");
    return BTREE_ERROR_INVALID_PAGE;
  }

  page *current = p;
  while (current != NULL) {
    int pos;
    int flag = search_in_page(current, key, &pos);

    if (flag == BTREE_FOUND_KEY) {
      *found_pos = pos;
      *return_page = current;
      return BTREE_FOUND_KEY;
    }

    if (current->leaf || current->children[pos] == (u16)-1) {
      if (current != p)
        free(current);
      *return_page = NULL;
      return BTREE_NOT_FOUND_KEY;
    }

    page *next = load_page(b, current->children[pos]);
    if (!next) {
      if (current != p)
        free(current);
      *return_page = NULL;
      return BTREE_ERROR_IO;
    }

    if (current != p)
      free(current);
    current = next;
  }

  *return_page = NULL;
  return BTREE_NOT_FOUND_KEY;
}

void populate_key(key *k, data_record *d, u16 rrn) {
  if (!k || !d) {
    puts("!!Error: NULL key and data record");
    return;
  }
  memcpy(k->id, d->placa, strlen(d->placa));
  k->id[strlen(d->placa)] = '\0';
  k->data_register_rrn = rrn;
}

btree_status insert_in_page(page *p, key k, page *r_child, int pos) {
  if (!p) {
    if (DEBUG)
      puts("!!Error: page NULL");
    return BTREE_ERROR_INVALID_PAGE;
  }
  if (pos < 0 || pos > p->keys_num) {
    if (DEBUG)
      puts("!!Error: invalid position");
    return BTREE_ERROR_INVALID_PAGE;
  }
  if (p->keys_num >= ORDER - 1) {
    if (DEBUG)
      puts("!!Error: page overflow");
    return BTREE_ERROR_PAGE_FULL;
  }

  if (DEBUG) {
    printf("Current state - keys: %hu, children: %hu, inserting at pos: %d\n",
           p->keys_num, p->child_num, pos);
  }

  for (int i = p->keys_num - 1; i >= pos; i--) {
    p->keys[i + 1] = p->keys[i];
  }
  p->keys[pos] = k;
  p->keys_num++;

  if (!p->leaf) {
    for (int i = p->child_num - 1; i >= pos + 1; i--) {
      p->children[i + 1] = p->children[i];
    }
    p->children[pos + 1] = r_child ? r_child->rrn : (u16)-1;
    p->child_num++;
  }

  if (DEBUG) {
    printf("After insertion - keys: %hu, children: %hu\n", p->keys_num,
           p->child_num);
    for (int i = 0; i < p->child_num; i++) {
      printf("i:%d= %hu\t", i, p->children[i]);
    }
    puts("");
  }

  return BTREE_INSERTED_IN_PAGE;
}

btree_status b_insert(b_tree_buf *b, io_buf *data, data_record *d, u16 rrn) {
  if (!b || !data || !d || rrn == (u16)-1)
    return BTREE_ERROR_INVALID_PAGE;

  key k;
  populate_key(&k, d, rrn);

  if (!b->root) {
    b->root = load_page(b, b->io->br->root_rrn);
    if (!b->root) {
      b->root = alloc_page();
      if (!b->root) {
        return BTREE_ERROR_MEMORY;
      }
      b->root->rrn = get_free_rrn(b->i);
    }
  }

  key *return_key = malloc(sizeof(key));
  if (!return_key) {
    return BTREE_ERROR_MEMORY;
  }

  page *return_page = NULL;
  bool promoted = false;
  btree_status flag =
      insert_key(b, b->root, k, return_key, &return_page, &promoted);

  if (flag == BTREE_PROMOTION) {
    if (DEBUG)
      puts("@Creating new root due to promotion");

    page *new_root = alloc_page();
    if (!new_root) {
      free(return_key);
      return BTREE_ERROR_MEMORY;
    }

    new_root->leaf = false;
    new_root->keys_num = 1;
    new_root->child_num = 2;
    new_root->keys[0] = *return_key;
    new_root->children[0] = b->root->rrn;
    new_root->children[1] = return_page->rrn;

    u16 new_rrn = get_free_rrn(b->i);
    if (new_rrn == (u16)-1) {
      free(return_key);
      return BTREE_ERROR_IO;
    }
    b->root = new_root;
    b->root->rrn = new_rrn;

    if (write_root_rrn(b, b->root->rrn) != BTREE_SUCCESS ||
        write_index_record(b, b->root) != BTREE_SUCCESS) {
      free(return_key);
      return BTREE_ERROR_IO;
    }

    flag = BTREE_SUCCESS;
  }
  if (flag != BTREE_SUCCESS && flag != BTREE_ERROR_DUPLICATE &&
      flag != BTREE_INSERTED_IN_PAGE) {
    if (DEBUG)
      printf("@Error during insertion: %d\n", flag);
  } else {
    if (DEBUG)
      puts("@Normal insertion completed");
  }

  return flag;
}

btree_status b_split(b_tree_buf *b, page *p, page **r_child, key *promo_key,
                     key *incoming_key, bool *promoted) {
  key temp_keys[ORDER];
  u16 temp_children[ORDER + 1];
  memset(&temp_children, (u16)-1, sizeof(temp_children));
  memset(&temp_keys, (u16)-1, sizeof(temp_keys));

  int i = 0;
  while (i < p->keys_num && strcmp(p->keys[i].id, incoming_key->id) < 0) {
    temp_keys[i] = p->keys[i];
    i++;
  }
  temp_keys[i] = *incoming_key;
  for (int j = i; j < p->keys_num; j++) {
    temp_keys[j + 1] = p->keys[j];
  }

  if (!p->leaf) {
    memcpy(temp_children, p->children, p->child_num * sizeof(u16));
    memmove(&temp_children[i + 1], &temp_children[i],
            (p->child_num - i) * sizeof(u16));
    temp_children[i + 1] = (promoted) ? (*r_child)->rrn : (u16)-1;
    p->child_num++;
  }

  page *new_page = alloc_page();
  if (!new_page)
    return BTREE_ERROR_MEMORY;

  new_page->rrn = get_free_rrn(b->i);
  if (new_page->rrn == (u16)-1) {
    free(new_page);
    return BTREE_ERROR_IO;
  }

  int split = ORDER / 2;
  *promo_key = temp_keys[split];

  p->keys_num = split;
  for (i = 0; i < split; i++) {
    p->keys[i] = temp_keys[i];
  }

  new_page->leaf = p->leaf;
  new_page->keys_num = 0;
  for (i = split + 1; i < ORDER; i++) {
    new_page->keys[new_page->keys_num++] = temp_keys[i];
  }

  if (!p->leaf) {
    p->child_num = split + 1;
    new_page->child_num = 0;
    for (i = 0; i <= split; i++) {
      p->children[i] = temp_children[i];
    }
    for (i = split + 1; i <= ORDER; i++) {
      new_page->children[new_page->child_num++] = temp_children[i];
    }
  }

  for (i = split; i < ORDER - 1; i++) {
    memset(&p->keys[i], 0, sizeof(key));
  }

  btree_status status;
  if ((status = write_index_record(b, p)) != BTREE_SUCCESS ||
      (status = write_index_record(b, new_page)) != BTREE_SUCCESS) {
    free(new_page);
    return status;
  }

  *r_child = new_page;
  *promoted = true;
  return BTREE_PROMOTION;
}

btree_status insert_key(b_tree_buf *b, page *p, key k, key *promo_key,
                        page **r_child, bool *promoted) {
  if (!b || !promo_key || !p)
    return BTREE_ERROR_INVALID_PAGE;

  int pos;
  btree_status status = search_in_page(p, k, &pos);
  if (status == BTREE_FOUND_KEY)
    return BTREE_ERROR_DUPLICATE;

  if (!p->leaf) {
    page *child = load_page(b, p->children[pos]);
    if (!child)
      return BTREE_ERROR_IO;

    key temp_key;
    page *temp_child = *r_child;
    status = insert_key(b, child, k, &temp_key, &temp_child, promoted);

    if (status == BTREE_PROMOTION) {
      k = temp_key;
      *r_child = temp_child;
      if (p->keys_num < ORDER - 1) {
        *promoted = false;
        int flag = insert_in_page(p, k, temp_child, pos);
        if (flag == BTREE_INSERTED_IN_PAGE) {
          if (write_index_record(b, p) != BTREE_SUCCESS)
            return BTREE_ERROR_IO;
        }
        return flag;
      }
      return b_split(b, p, r_child, promo_key, &k, promoted);
    }
    return status;
  }

  if (p->keys_num < ORDER - 1) {
    *promoted = false;
    if (insert_in_page(p, k, NULL, pos) == BTREE_INSERTED_IN_PAGE) {
      if (write_index_record(b, p) != BTREE_SUCCESS) {
        printf("Error writing page %hu to disk.\n", p->rrn);
        return BTREE_ERROR_IO;
      }
    } else {
      return BTREE_ERROR_IO;
    }
    return BTREE_SUCCESS;
  }
  *promoted = false;
  return b_split(b, p, r_child, promo_key, &k, promoted);
}

void b_remove(b_tree_buf *b, io_buf *data, const char *placa) {
  if (!b || !data || !placa) {
    puts("!!Error: NULL parameters");
    return;
  }

  key k;
  strcpy(k.id, placa);

  if (!b->root) {
    b->root = load_page(b, b->io->br->root_rrn);
    if (!b->root) {
      puts("!!Error: Root page not found");
      return;
    }
  }

  bool merged = false;
  btree_status status = remove_key(b, b->root, k, &merged);

  if (status == BTREE_SUCCESS && b->root->keys_num == 0) {
    if (!b->root->leaf) {
      page *new_root = load_page(b, b->root->children[0]);
      if (new_root) {
        b->root = new_root;
        write_root_rrn(b, b->root->rrn);
      }
    } else {
      insert_list(b->i, b->root->rrn);
      clear_page(b->root);
      b->root = NULL;
    }
  }

  if (status != BTREE_SUCCESS) {
    printf("!!Error: Failed to remove key %s\n", placa);
  }
}

btree_status remove_key(b_tree_buf *b, page *p, key k, bool *merged) {
  int pos;
  btree_status status = search_in_page(p, k, &pos);

  if (status == BTREE_FOUND_KEY) {
    if (p->leaf) {
      for (int i = pos; i < p->keys_num - 1; i++) {
        p->keys[i] = p->keys[i + 1];
      }
      p->keys_num--;
      insert_list(b->i, p->rrn);
      *merged = (p->keys_num < (ORDER - 1) / 2);
      return BTREE_SUCCESS;
    } else {
      page *child = load_page(b, p->children[pos]);
      page *sibling = load_page(b, p->children[pos + 1]);
      if (child->keys_num > (ORDER - 1) / 2) {
        key pred_key = child->keys[child->keys_num - 1];
        remove_key(b, child, pred_key, merged);
        p->keys[pos] = pred_key;
      } else if (sibling->keys_num > (ORDER - 1) / 2) {
        key succ_key = sibling->keys[0];
        remove_key(b, sibling, succ_key, merged);
        p->keys[pos] = succ_key;
      } else {
        merge(b, p, pos);
        remove_key(b, child, k, merged);
      }
      return BTREE_SUCCESS;
    }
  } else {
    if (p->leaf) {
      return BTREE_NOT_FOUND_KEY;
    } else {
      page *child = load_page(b, p->children[pos]);
      bool child_merged = false;
      status = remove_key(b, child, k, &child_merged);
      if (child_merged) {
        if (pos > 0) {
          redistribute(b, p, pos - 1);
        } else {
          redistribute(b, p, pos);
        }
      }
      return status;
    }
  }
}

btree_status redistribute(b_tree_buf *b, page *p, int pos) {
  page *left = load_page(b, p->children[pos]);
  page *right = load_page(b, p->children[pos + 1]);

  if (left->keys_num > (ORDER - 1) / 2) {
    for (int i = right->keys_num; i > 0; i--) {
      right->keys[i] = right->keys[i - 1];
    }
    right->keys[0] = p->keys[pos];
    p->keys[pos] = left->keys[left->keys_num - 1];
    left->keys_num--;
    right->keys_num++;
  } else if (right->keys_num > (ORDER - 1) / 2) {
    left->keys[left->keys_num] = p->keys[pos];
    p->keys[pos] = right->keys[0];
    for (int i = 0; i < right->keys_num - 1; i++) {
      right->keys[i] = right->keys[i + 1];
    }
    right->keys_num--;
    left->keys_num++;
  } else {
    return merge(b, p, pos);
  }

  write_index_record(b, left);
  write_index_record(b, right);
  write_index_record(b, p);

  return BTREE_SUCCESS;
}

btree_status merge(b_tree_buf *b, page *p, int pos) {
  page *left = load_page(b, p->children[pos]);
  page *right = load_page(b, p->children[pos + 1]);

  left->keys[left->keys_num] = p->keys[pos];
  left->keys_num++;

  for (int i = 0; i < right->keys_num; i++) {
    left->keys[left->keys_num + i] = right->keys[i];
  }
  left->keys_num += right->keys_num;

  for (int i = pos; i < p->keys_num - 1; i++) {
    p->keys[i] = p->keys[i + 1];
    p->children[i + 1] = p->children[i + 2];
  }
  p->keys_num--;

  insert_list(b->i, right->rrn);
  clear_page(right);

  write_index_record(b, left);
  write_index_record(b, p);

  return BTREE_SUCCESS;
}

void print_page(page *page) {
  puts("\n----------PAGE----------");
  printf("page rrn: %hu\n", page->rrn);
  for (int i = 0; i < page->keys_num; i++) {
    printf("key id: %s | data_rrn: %hu;\t", page->keys[i].id,
           page->keys[i].data_register_rrn);
  }
  printf("\n");
  printf("children rrn: ");
  for (int i = 0; i < page->child_num; i++) {
    printf("i:%d= %hu\t", i, page->children[i]);
  }
  printf("\nchild_number %hu\n", page->child_num);
  printf("keys number %hu\n", page->keys_num);
  if (page->leaf)
    puts("This page is a leaf");
  puts("------------------------\n");
}

int write_index_header(io_buf *io) {
  if (!io || !io->fp) {
    puts("!!NULL file");
    return BTREE_ERROR_IO;
  }
  if (io->br == NULL) {
    io->br = malloc(sizeof(index_header_record));
    if (io->br == NULL) {
      puts("!!Memory allocation error");
      return BTREE_ERROR_MEMORY;
    }
  }
  if (io->br->page_size == 0) {
    puts("!!Error: page size == 0");
    return BTREE_ERROR_INVALID_PAGE;
  }

  size_t free_rrn_len = strlen(io->br->free_rrn_address) + 1;
  io->br->header_size = sizeof(u16) * 3 + free_rrn_len;

  fseek(io->fp, 0, SEEK_SET);

  if (fwrite(&io->br->root_rrn, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error while writing root_rrn");
    return BTREE_ERROR_IO;
  }

  if (fwrite(&io->br->page_size, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error while writing page_size");
    return BTREE_ERROR_IO;
  }

  if (fwrite(&io->br->header_size, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error while writing size");
    return BTREE_ERROR_IO;
  }

  if (fwrite(io->br->free_rrn_address, free_rrn_len, 1, io->fp) != 1) {
    puts("!!Error while writing free_rrn_address");
    return BTREE_ERROR_IO;
  }

  if (DEBUG) {
    printf("@Successfully written on index: root_rrn: %hu, page_size: %hu, "
           "size: %hu, "
           "free_rrn_address: %s\n",
           io->br->root_rrn, io->br->page_size, io->br->header_size,
           io->br->free_rrn_address);
  }

  fflush(io->fp);
  return BTREE_SUCCESS;
}

void load_index_header(io_buf *io) {
  if (!io || !io->fp) {
    puts("!!Invalid IO buffer or file pointer");
    return;
  }

  if (!io->br) {
    io->br = malloc(sizeof(index_header_record));
    if (!io->br) {
      puts("!!Memory allocation error");
      return;
    }
    memset(io->br, 0, sizeof(index_header_record));
  }

  fseek(io->fp, 0, SEEK_SET);

  if (fread(&io->br->root_rrn, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error reading root_rrn");
    return;
  }

  if (fread(&io->br->page_size, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error reading page_size");
    return;
  }

  if (fread(&io->br->header_size, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error reading size");
    return;
  }

  printf("root_rrn: %hu, page_size: %hu, size: %hu\n", io->br->root_rrn,
         io->br->page_size, io->br->header_size);

  size_t rrn_len = io->br->header_size - (3 * sizeof(u16));

  io->br->free_rrn_address = malloc(rrn_len + 1);
  if (!io->br->free_rrn_address) {
    puts("!!Memory allocation error for free_rrn_address");
    return;
  }

  if (fread(io->br->free_rrn_address, rrn_len, 1, io->fp) != 1) {
    puts("!!Error reading free_rrn_address");
    free(io->br->free_rrn_address);
    io->br->free_rrn_address = NULL;
    return;
  }
  io->br->free_rrn_address[rrn_len] = '\0';

  if (DEBUG) {
    puts("@Index header Record Loaded");
    printf("-->index_header: root_rrn: %hu page_size: %hu size: %hu "
           "free_rrn_list: %s\n",
           io->br->root_rrn, io->br->page_size, io->br->header_size,
           io->br->free_rrn_address);
  }
}

int write_index_record(b_tree_buf *b, page *p) {
  if (!b->io || !p) {
    puts("!!Error: invalid parameters");
    return BTREE_ERROR_IO;
  }

  if (!b->io->br || !b->io->fp) {
    puts("!!Error: invalid buffer or file pointer");
    return BTREE_ERROR_MEMORY;
  }

  int byte_offset =
      (b->io->br->header_size) + ((b->io->br->page_size) * p->rrn);
  if (DEBUG) {
    puts("////////");
    printf("@Writting following page: \n");
    print_page(p);
    printf("byte_offset %d\n", byte_offset);
  }

  if (fseek(b->io->fp, byte_offset, SEEK_SET) != 0) {
    puts("!!Error: seek operatb->ion failed");
    return BTREE_ERROR_IO;
  }

  if (fwrite(p, b->io->br->page_size, 1, b->io->fp) != 1) {
    puts("!!Error: write operatb->ion failed");
    return BTREE_ERROR_IO;
  }

  fflush(b->io->fp);

  if (DEBUG)
    printf("@Successfully wrote page %hu at offset %d\n", p->rrn, byte_offset);

  page *q_page = queue_search(b->q, p->rrn);
  if (q_page != NULL) {
    memcpy(q_page, p, sizeof(page));
  } else {
    push_page(b, p);
  }

  return BTREE_SUCCESS;
}

void create_index_file(io_buf *io, const char *file_name) {
  if (!io || !file_name) {
    puts("!!Invalid io buffer or file name");
    return;
  }

  strcpy(io->address, file_name);

  if (io->hr == NULL) {
    io->hr = malloc(sizeof(data_header_record));
    if (io->hr == NULL) {
      puts("!!Memory allocation failed for data_header_record");
      return;
    }
  }

  if (io->br == NULL) {
    free(io->hr);
    io->hr = NULL;
    io->br = malloc(sizeof(index_header_record));
    if (io->br == NULL) {
      puts("!!Memory allocation failed for index_header_record");
      return;
    }
    io->br->free_rrn_address = malloc(sizeof(char) * MAX_ADDRESS);
    if (io->br->free_rrn_address == NULL) {
      puts("!!Memory allocation failed for free_rrn_address");
      return;
    }
  }

  if (io->fp != NULL) {
    puts("!!File already opened");
    return;
  }

  printf("Loading file: %s\n", io->address);
  io->fp = fopen(io->address, "r+b");
  if (!io->fp) {
    if (DEBUG)
      printf("!!Error opening file: %s. Creating it...\n", io->address);
    io->fp = fopen(io->address, "wb");
    if (io->fp) {
      fclose(io->fp);
    }
    io->fp = fopen(io->address, "r+b");
    if (!io->fp) {
      puts("!!Failed to create and open file");
      return;
    }
  }

  char list_name[MAX_ADDRESS];
  strcpy(list_name, file_name);
  char *dot = strrchr(list_name, '.');
  if (dot) {
    strcpy(dot, ".hlp");
  }

  load_index_header(io);

  if (strcmp(io->br->free_rrn_address, list_name) != 0) {
    strcpy(io->br->free_rrn_address, list_name);
    populate_index_header(io->br, list_name);
    write_index_header(io);
  }

  if (DEBUG) {
    puts("@Index file created successfully");
  }
}

page *alloc_page(void) {
  page *p = malloc(sizeof(page));
  if (p) {
    memset(p, 0, sizeof(page));
    p->leaf = true;
    memset(p->children, (u16)-1, sizeof(u16) * (ORDER));
    memset(p->keys, (u16)-1, sizeof(key) * (ORDER - 1));
  }
  return p;
}

void clear_page(page *page) {
  if (page) {
    free(page);
    if (DEBUG)
      puts("@Successfully freed page");
    return;
  }
  puts("Error while freeing page");
}
