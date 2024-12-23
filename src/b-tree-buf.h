#ifndef _B_TREE_BUF_H
#define _B_TREE_BUF_H

#include "defines.h"

b_tree_buf *alloc_tree_buf(void);

void build_tree(b_tree_buf *b, io_buf *data, int n);

btree_status handle_underflow(b_tree_buf *b, page *p);

page *get_sibling(b_tree_buf *b, page *p, bool left);

page *find_parent(b_tree_buf *b, page *current, page *target);

btree_status b_insert(b_tree_buf *b, io_buf *data, data_record *d, u16 rrn);

btree_status insert_key(b_tree_buf *b, page *p, key k, key *promo_key,
                        page **r_child, bool *promoted);

btree_status b_split(b_tree_buf *b, page *p, page **r_child, key *promo_key,
                     key *incoming_key, bool *promoted);

btree_status insert_in_page(page *p, key k, page *r_child, int pos);

void create_index_file(io_buf *io, const char *file_name);

void clear_tree_buf(b_tree_buf *b);

int write_root_rrn(b_tree_buf *b, u16 rrn);

page *b_search(b_tree_buf *b, const char *s, u16 *return_pos);

void b_range_search(b_tree_buf *b, io_buf *data, key_range *range);

u16 search_key(b_tree_buf *b, page *p, key key, u16 *found_pos,
               page **return_page);

int search_in_page(page *page, key key, int *return_pos);

btree_status b_remove(b_tree_buf *b, io_buf *data, char *key_id);

btree_status remove_key(b_tree_buf *b, page *p, key k, bool *merged);

btree_status redistribute(b_tree_buf *b, page *donor, page *receiver, bool from_left);

btree_status merge(b_tree_buf *b, page *left, page *right);

void print_page(page *page);

page *load_page(b_tree_buf *b, u16 rrn);

void populate_index_header(index_header_record *bh, const char *file_name);

void load_index_header(io_buf *io);

int write_index_header(io_buf *io);

int write_index_record(b_tree_buf *b, page *p);

page *alloc_page(void);

page *new_page(u16 rrn);

void clear_page(page *page);

void clear_all_pages(void);

void track_page(page *p);

#endif
