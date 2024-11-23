#ifndef _FREE_RRN_LIST
#define _FREE_RRN_LIST
#include "defines.h"

free_rrn_list *alloc_ilist(void);

void clear_ilist(free_rrn_list *i);

void load_list(free_rrn_list *i, char* s);

u16 *load_rrn_list(free_rrn_list *i);

u16 get_free_rrn(free_rrn_list *i);

u16 get_last_free_rrn(free_rrn_list *i);

u16 *load_rrns(free_rrn_list *i);

void insert_list(free_rrn_list *i, int rrn); 

#endif
