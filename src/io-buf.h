#ifndef _IO_BUF_H
#define _IO_BUF_H

#include "defines.h"

io_buf *alloc_io_buf(void);

void print_data_record(data_record *hr);

void load_file(io_buf *io, char *file_name, const char *type);

void create_data_file(io_buf *io, char *file_name);

void load_data_header(io_buf *io);

data_record *load_data_record(io_buf *io, u16 rrn);

void populate_header(data_header_record *hp, const char *file_name);

void prepend_data_header(io_buf *io);

void write_data_header(io_buf *io);

void write_data_record(io_buf *io, data_record *d, u16 rrn);

void clear_io_buf(io_buf *io_buf);

void d_insert(io_buf *io, data_record *d, free_rrn_list *ld, u16 rrn);

#endif
