#include "b-tree-buf.h"
#include "free-rrn-list.h"
#include "io-buf.h"
#include "queue.h"

page **g_allocated;
u16 g_n = 0;

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
  if (!b || !b->io) {
    puts("!!Error: invalid parameters");
    return NULL;
  }

  page *page = queue_search(b->q, rrn);
  if (page) {
    if (DEBUG)
      puts("@Page found in queue");
    return page;
  }

  page = alloc_page();
  if (!page)
    return NULL;

  page->rrn = rrn;

  size_t byte_offset =
      (size_t)(b->io->br->header_size) + ((size_t)(b->io->br->page_size) * rrn);

  if (fseek(b->io->fp, byte_offset, SEEK_SET) != 0) {
    free(page);
    return NULL;
  }

  size_t bytes_read = fread(page, 1, b->io->br->page_size, b->io->fp);
  if (bytes_read != b->io->br->page_size) {
    free(page);
    return NULL;
  }

  push_page(b, page);

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
  if (!b || !b->root || !s)
    return NULL;

  key k;
  strncpy(k.id, s, TAMANHO_PLACA - 1);
  k.id[TAMANHO_PLACA - 1] = '\0';

  page *found_page = NULL;
  *return_pos = search_key(b, b->root, k, return_pos, &found_page);

  if (found_page->leaf)
    return found_page;

  *return_pos = (u16)-1;
  return NULL;
}

void b_range_search(b_tree_buf *b, io_buf *data, key_range *range) {
  if (!b || !range || !b->root) {
    puts("!!Invalid parameters for range search");
    return;
  }

  page *curr = b->root;
  while (!curr->leaf) {
    int i;
    for (i = 0; i < curr->keys_num; i++) {
      if (strcmp(range->start_id, curr->keys[i].id) < 0) {
        break;
      }
    }
    curr = load_page(b, curr->children[i]);
    if (!curr) {
      puts("!!Error loading page during range search");
      return;
    }
  }

  bool found_any = false;
  while (curr) {
    if (curr->keys_num > 0 && strcmp(curr->keys[0].id, range->end_id) > 0) {
      break;
    }

    for (int i = 0; i < curr->keys_num; i++) {
      if (strcmp(curr->keys[i].id, range->end_id) > 0) {
        break;
      }

      if (strcmp(curr->keys[i].id, range->start_id) >= 0) {
        found_any = true;
        data_record *record =
            load_data_record(data, curr->keys[i].data_register_rrn);
        if (record) {
          print_data_record(record);
          free(record);
        }
      }
    }

    if (curr->next_leaf == (u16)-1) {
      break;
    }

    page *next = load_page(b, curr->next_leaf);
    curr = next;
  }

  if (!found_any) {
    puts("Nenhum registro encontrado no intervalo especificado.");
  }
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

u16 search_key(b_tree_buf *b, page *p, key k, u16 *found_pos,
               page **return_page) {
  if (!p)
    return (u16)-1;

  int pos;
  int result = search_in_page(p, k, &pos);

  if (DEBUG) {
    printf("page key id: %s     key id: %s\n",
           p->keys_num > 0 ? p->keys[0].id : "", k.id);
  }

  if (result == BTREE_FOUND_KEY) {
    *found_pos = pos;
    *return_page = p;
    return pos;
  }

  if (p->leaf) {
    *return_page = p;
    *found_pos = pos;
    return (u16)-1;
  }

  page *next = load_page(b, p->children[pos]);
  if (!next)
    return (u16)-1;

  u16 ret = search_key(b, next, k, found_pos, return_page);

  if (next != b->root && !queue_search(b->q, next->rrn)) {
    free(next);
  }

  return ret;
}

void populate_key(key *k, data_record *d, u16 rrn) {
  if (!k || !d)
    return;

  strncpy(k->id, d->placa, TAMANHO_PLACA);
  k->data_register_rrn = rrn;

  if (DEBUG) {
    printf("@Populated key with ID: %s and data RRN: %hu\n", k->id,
           k->data_register_rrn);
  }
}

btree_status insert_in_page(page *p, key k, page *r_child, int pos) {
  if (!p)
    return BTREE_ERROR_INVALID_PAGE;

  if (DEBUG) {
    printf("Current state - keys: %d, children: %d, inserting at pos: %d\n",
           p->keys_num, p->child_num, pos);
  }

  for (int i = p->keys_num - 1; i >= pos; i--) {
    p->keys[i + 1] = p->keys[i];
  }

  p->keys[pos] = k;
  p->keys_num++;

  if (!p->leaf && r_child) {
    for (int i = p->child_num - 1; i >= pos + 1; i--) {
      p->children[i + 1] = p->children[i];
    }
    p->children[pos + 1] = r_child->rrn;
    p->child_num++;
  }

  if (DEBUG) {
    printf("After insertion - keys: %d, children: %d\n", p->keys_num,
           p->child_num);
    printf("Inserted key with data RRN: %hu\n", k.data_register_rrn);
  }

  return BTREE_INSERTED_IN_PAGE;
}

btree_status b_insert(b_tree_buf *b, io_buf *data, data_record *d, u16 rrn) {
  if (!b || !data || !d)
    return BTREE_ERROR_INVALID_PAGE;

  key new_key;
  populate_key(&new_key, d, rrn);

  if (!b->root) {
    b->root = alloc_page();
    if (!b->root)
      return BTREE_ERROR_MEMORY;

    b->root->rrn = get_free_rrn(b->i);
    if (b->root->rrn < 0) {
      free(b->root);
      b->root = NULL;
      return BTREE_ERROR_IO;
    }

    b->root->keys[0] = new_key;
    b->root->keys_num = 1;
    b->root->leaf = true;

    return write_index_record(b, b->root);
  }

  key promo_key;
  page *r_child = NULL;
  bool promoted = false;

  btree_status status =
      insert_key(b, b->root, new_key, &promo_key, &r_child, &promoted);

  if (status < 0) {
    return status;
  }

  if (promoted) {
    page *new_root = alloc_page();
    if (!new_root)
      return BTREE_ERROR_MEMORY;

    new_root->rrn = get_free_rrn(b->i);
    if (new_root->rrn < 0) {
      free(new_root);
      return BTREE_ERROR_IO;
    }

    new_root->leaf = false;
    new_root->keys[0] = promo_key;
    new_root->keys_num = 1;
    new_root->children[0] = b->root->rrn;
    new_root->children[1] = r_child->rrn;
    new_root->child_num = 2;

    b->root = new_root;

    btree_status write_status = write_root_rrn(b, b->root->rrn);
    if (write_status < 0)
      return write_status;

    write_status = write_index_record(b, b->root);
    if (write_status < 0)
      return write_status;
  }

  return BTREE_SUCCESS;
}

btree_status b_split(b_tree_buf *b, page *p, page **r_child, key *promo_key,
                     key *incoming_key, bool *promoted) {
  if (!b || !p || !r_child || !promo_key || !incoming_key)
    return BTREE_ERROR_INVALID_PAGE;

  key temp_keys[ORDER];
  u16 temp_children[ORDER + 1];

  memset(temp_keys, 0, sizeof(temp_keys));
  memset(temp_children, 0xFF, sizeof(temp_children));

  for (int i = 0; i < p->keys_num; i++) {
    temp_keys[i] = p->keys[i];
  }

  if (!p->leaf) {
    for (int i = 0; i <= p->child_num; i++) {
      temp_children[i] = p->children[i];
    }
  }

  int pos = p->keys_num - 1;
  while (pos >= 0 && strcmp(temp_keys[pos].id, incoming_key->id) > 0) {
    temp_keys[pos + 1] = temp_keys[pos];
    if (!p->leaf) {
      temp_children[pos + 2] = temp_children[pos + 1];
    }
    pos--;
  }

  temp_keys[pos + 1] = *incoming_key;
  if (!p->leaf && *r_child) {
    temp_children[pos + 2] = (*r_child)->rrn;
  }

  page *new_page = alloc_page();
  if (!new_page)
    return BTREE_ERROR_MEMORY;

  new_page->rrn = get_free_rrn(b->i);
  if (new_page->rrn == (u16)-1) {
    free(new_page);
    return BTREE_ERROR_IO;
  }

  int split = (ORDER - 1) / 2;

  if (p->leaf) {
    p->keys_num = split + 1;
    new_page->keys_num = ORDER - (split + 1);
    new_page->leaf = true;

    for (int i = 0; i < p->keys_num; i++) {
      p->keys[i] = temp_keys[i];
    }

    for (int i = 0; i < new_page->keys_num; i++) {
      new_page->keys[i] = temp_keys[i + split + 1];
    }

    new_page->next_leaf = p->next_leaf;
    p->next_leaf = new_page->rrn;

    *promo_key = new_page->keys[0];
  } else {
    p->keys_num = split;
    new_page->keys_num = ORDER - split - 1;
    new_page->leaf = false;

    for (int i = 0; i < p->keys_num; i++) {
      p->keys[i] = temp_keys[i];
    }

    *promo_key = temp_keys[split];

    for (int i = 0; i < new_page->keys_num; i++) {
      new_page->keys[i] = temp_keys[i + split + 1];
    }

    for (int i = 0; i <= p->keys_num; i++) {
      p->children[i] = temp_children[i];
    }

    for (int i = 0; i <= new_page->keys_num; i++) {
      new_page->children[i] = temp_children[i + split + 1];
    }

    p->child_num = p->keys_num + 1;
    new_page->child_num = new_page->keys_num + 1;
  }

  btree_status status;
  if ((status = write_index_record(b, p)) != BTREE_SUCCESS) {
    free(new_page);
    return status;
  }

  if ((status = write_index_record(b, new_page)) != BTREE_SUCCESS) {
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
    page *temp_child = NULL;
    status = insert_key(b, child, k, &temp_key, &temp_child, promoted);

    if (child != b->root && !queue_search(b->q, child->rrn)) {
      clear_page(child);
    }

    if (status == BTREE_PROMOTION) {
      k = temp_key;
      *r_child = temp_child;
      if (p->keys_num < ORDER - 1) {
        *promoted = false;
        status = insert_in_page(p, k, temp_child, pos);
        if (status == BTREE_INSERTED_IN_PAGE) {
          return write_index_record(b, p);
        }
        return status;
      }
      return b_split(b, p, r_child, promo_key, &k, promoted);
    }
    return status;
  }

  if (p->keys_num < ORDER - 1) {
    *promoted = false;
    status = insert_in_page(p, k, NULL, pos);
    if (status == BTREE_INSERTED_IN_PAGE) {
      return write_index_record(b, p);
    }
    return status;
  }

  return b_split(b, p, r_child, promo_key, &k, promoted);
}

btree_status b_remove(b_tree_buf *b, io_buf *data, char *key_id) {
  if (!b || !b->root || !data || !key_id)
    return BTREE_ERROR_INVALID_PAGE;

  if (DEBUG)
    printf("@Removing key: %s\n", key_id);

  u16 pos;
  page *p = b_search(b, key_id, &pos);
  if (!p || strcmp(p->keys[pos].id, key_id) != 0) {
    if (DEBUG)
      puts("@Key not found");
    return BTREE_NOT_FOUND_KEY;
  }

  if (p->leaf) {
    if (DEBUG)
      printf("@Removing key from leaf page RRN: %hu at position: %hu\n", p->rrn,
             pos);
    u16 data_rrn = p->keys[pos].data_register_rrn;

    for (int i = pos; i < p->keys_num - 1; i++)
      p->keys[i] = p->keys[i + 1];
    p->keys_num--;

    if (data_rrn != (u16)-1) {
      if (fseek(data->fp,
                data->hr->header_size + (data_rrn * sizeof(data_record)),
                SEEK_SET) == 0) {
        data_record empty_record;
        memset(&empty_record, '*', sizeof(data_record));
        fwrite(&empty_record, sizeof(data_record), 1, data->fp);
        fflush(data->fp);
        insert_list(b->i, data_rrn);
      }
    }

    if (p == b->root && p->keys_num == 0) {
      clear_page(b->root);
      b->root = NULL;
      return BTREE_SUCCESS;
    }

    btree_status status = write_index_record(b, p);
    if (status < 0)
      return status;

    if (p != b->root && p->keys_num < (ORDER - 1) / 2) {
      if (DEBUG)
        puts("@Leaf underflow detected");

      page *left = get_sibling(b, p, true);
      if (left && left->keys_num > (ORDER - 1) / 2) {
        return redistribute(b, left, p, true);
      }

      page *right = get_sibling(b, p, false);
      if (right && right->keys_num > (ORDER - 1) / 2) {
        return redistribute(b, right, p, false);
      }

      if (left) {
        return merge(b, left, p);
      } else if (right) {
        return merge(b, p, right);
      }
    }

    return BTREE_SUCCESS;
  }

  if (DEBUG)
    puts("@Key found in internal node - not removing");
  return BTREE_SUCCESS;
}

btree_status redistribute(b_tree_buf *b, page *donor, page *receiver,
                          bool from_left) {
  if (!b || !donor || !receiver)
    return BTREE_ERROR_INVALID_PAGE;

  if (from_left) {
    for (int i = receiver->keys_num; i > 0; i--)
      receiver->keys[i] = receiver->keys[i - 1];
    receiver->keys[0] = donor->keys[donor->keys_num - 1];
    donor->keys_num--;
    receiver->keys_num++;
  } else {
    receiver->keys[receiver->keys_num] = donor->keys[0];
    receiver->keys_num++;

    for (int i = 0; i < donor->keys_num - 1; i++) {
      donor->keys[i] = donor->keys[i + 1];
    }
    donor->keys_num--;
  }

  btree_status status = write_index_record(b, donor);
  if (status < 0)
    return status;

  return write_index_record(b, receiver);
}

btree_status merge(b_tree_buf *b, page *left, page *right) {
  if (!b || !left || !right)
    return BTREE_ERROR_INVALID_PAGE;

  for (int i = 0; i < right->keys_num; i++) {
    left->keys[left->keys_num + i] = right->keys[i];
  }
  left->keys_num += right->keys_num;

  left->next_leaf = right->next_leaf;

  btree_status status = write_index_record(b, left);
  if (status < 0)
    return status;

  insert_list(b->i, right->rrn);

  return BTREE_SUCCESS;
}

page *get_sibling(b_tree_buf *b, page *p, bool left) {
  if (!b || !p || !b->root)
    return NULL;

  page *parent = find_parent(b, b->root, p);
  if (!parent)
    return NULL;

  int pos;
  for (pos = 0; pos <= parent->child_num; pos++) {
    if (parent->children[pos] == p->rrn)
      break;
  }

  if (left && pos > 0) {
    return load_page(b, parent->children[pos - 1]);
  } else if (!left && pos < parent->child_num - 1) {
    return load_page(b, parent->children[pos + 1]);
  }

  return NULL;
}

void print_page(page *p) {
  if (!p) {
    puts("!!Página nula");
    return;
  }

  printf("RRN: %hu | Folha: %d | Chaves: %d | Filhos: %d\n", p->rrn, p->leaf,
         p->keys_num, p->child_num);

  printf("Chaves: ");
  for (int i = 0; i < p->keys_num; i++) {
    printf("[%s]", p->keys[i].id);
  }
  printf("\n");

  if (!p->leaf) {
    printf("RRNs filhos: ");
    for (int i = 0; i < p->child_num; i++) {
      printf("%hu ", p->children[i]);
    }
    printf("\n");
  }

  if (p->leaf) {
    printf("Próxima folha: %hu\n", p->next_leaf);
  }
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

btree_status write_index_record(b_tree_buf *b, page *p) {
  if (!b || !b->io || !p)
    return BTREE_ERROR_IO;

  if (DEBUG) {
    puts("////////");
    puts("@Writting following page: ");
    print_page(p);
  }

  int byte_offset = b->io->br->header_size + (b->io->br->page_size * p->rrn);

  if (fseek(b->io->fp, byte_offset, SEEK_SET)) {
    puts("!!Error: could not fseek");
    return BTREE_ERROR_IO;
  }

  size_t written = fwrite(p, b->io->br->page_size, 1, b->io->fp);
  if (written != 1) {
    puts("!!Error: could not write page");
    return BTREE_ERROR_IO;
  }

  fflush(b->io->fp);

  if (DEBUG) {
    printf("@Successfully wrote page %hu at offset %d\n", p->rrn, byte_offset);
  }

  if (!queue_search(b->q, p->rrn)) {
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
  page *p = NULL;
  if (posix_memalign((void **)&p, sizeof(void *), sizeof(page)) != 0) {
    puts("!!Erro: falha na alocação da página");
    return NULL;
  }

  memset(p, 0, sizeof(page));
  p->leaf = true;
  p->next_leaf = (u16)-1;

  for (int i = 0; i < ORDER; i++) {
    p->children[i] = (u16)-1;
  }

  return p;
}

void clear_all_pages(void) {
  if (!g_allocated)
    return;

  if (DEBUG)
    puts("clearing pages:");

  for (int i = 0; i < g_n; i++) {
    if (g_allocated[i]) {
      if (DEBUG)
        printf("%hu\t", g_allocated[i]->rrn);
      free(g_allocated[i]);
      g_allocated[i] = NULL;
    }
  }

  free(g_allocated);
  g_allocated = NULL;
  g_n = 0;

  if (DEBUG)
    puts("");
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

void track_page(page *p) {
  if (!p)
    return;

  if (!g_allocated) {
    g_allocated = malloc(sizeof(page *));
    if (!g_allocated)
      return;
    g_n = 0;
  } else {
    page **tmp = realloc(g_allocated, sizeof(page *) * (g_n + 1));
    if (!tmp)
      return;
    g_allocated = tmp;
  }

  g_allocated[g_n++] = p;
}

page *find_parent(b_tree_buf *b, page *current, page *target) {
  if (!b || !current || !target)
    return NULL;

  if (target == b->root)
    return NULL;

  for (int i = 0; i < current->child_num; i++) {
    if (current->children[i] == target->rrn) {
      return current;
    }
  }

  if (!current->leaf) {
    for (int i = 0; i < current->child_num; i++) {
      page *child = load_page(b, current->children[i]);
      if (!child)
        continue;

      page *result = find_parent(b, child, target);

      if (child != b->root && !queue_search(b->q, child->rrn)) {
        clear_page(child);
      }

      if (result)
        return result;
    }
  }

  return NULL;
}
