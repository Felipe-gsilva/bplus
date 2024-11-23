#include "io-buf.h"
#include "b-tree-buf.h"
#include "free-rrn-list.h"

io_buf *alloc_io_buf(void) {
  io_buf *io = malloc(sizeof(io_buf));
  if (!io) {
    puts("!!Could not allocate IO_BUFFER");
    return NULL;
  }

  io->hr = malloc(sizeof(data_header_record));
  io->br = malloc(sizeof(index_header_record));
  if (!io->hr || !io->br) {
    puts("!!Could not allocate header records");
    free(io);
    return NULL;
  }

  io->hr->free_rrn_address = malloc(sizeof(char) * MAX_ADDRESS);
  io->br->free_rrn_address = malloc(sizeof(char) * MAX_ADDRESS);
  if (!io->hr->free_rrn_address || !io->br->free_rrn_address) {
    puts("!!Could not allocate free_rrn_address");
    free(io->hr);
    free(io->br);
    free(io);
    return NULL;
  }

  if (DEBUG) {
    puts("@Allocated IO_BUFFER");
  }
  return io;
}

void d_insert(io_buf *io, data_record *d, free_rrn_list *ld, u16 rrn) {
  if (!io || !d || !ld) {
    puts("!!Error: NULL parameters on d_insert");
  }
  if (rrn == (u16)-1)
    rrn = get_free_rrn(ld);

  write_data_record(io, d, rrn);
}

void load_data_header(io_buf *io) {
  if (!io || !io->fp) {
    puts("!!Invalid IO buffer or file pointer");
    return;
  }

  data_header_record temp_hr = {0};

  fseek(io->fp, 0, SEEK_SET);
  if (fread(&temp_hr.header_size, sizeof(u16), 1, io->fp) != 1 ||
      fread(&temp_hr.record_size, sizeof(u16), 1, io->fp) != 1) {
    puts("!!Error while reading header record (fixed part)");
    return;
  }

  if (DEBUG)
    printf("Header_size: %hu \t Record size: %hu\n", temp_hr.header_size,
           temp_hr.record_size);

  if (!io->hr) {
    io->hr = malloc(sizeof(data_header_record));
    if (!io->hr) {
      puts("!!Memory allocation error");
      return;
    }
  }

  if (temp_hr.header_size > MAX_ADDRESS || temp_hr.header_size < 4 ||
      temp_hr.record_size != sizeof(data_record)) {
    return;
  }

  io->hr->record_size = temp_hr.record_size;
  io->hr->header_size = temp_hr.header_size;

  size_t rrn_len = io->hr->header_size - (2 * sizeof(u16));

  io->hr->free_rrn_address = malloc(rrn_len + 1);
  if (!io->hr->free_rrn_address) {
    puts("!!Memory allocation error for free_rrn_address");
    return;
  }

  fseek(io->fp, sizeof(u16) * 2, SEEK_SET);
  if (fread(io->hr->free_rrn_address, rrn_len, 1, io->fp) != 1) {
    puts("!!Error while reading free_rrn_address");
    free(io->hr->free_rrn_address);
    io->hr->free_rrn_address = NULL;
    return;
  }

  io->hr->free_rrn_address[rrn_len] = '\0';

  if (DEBUG) {
    printf("--> data_header: record_size: %hu size: %hu free_rrn_address: %s\n",
           io->hr->record_size, io->hr->header_size, io->hr->free_rrn_address);
  }
}

void print_data_record(data_record *hr) {
  puts("\n--------DATA RECORD--------");
  printf("Placa: %s\n", hr->placa);
  printf("Modelo: %s\n", hr->modelo);
  printf("Marca: %s\n", hr->marca);
  printf("Ano: %d\n", hr->ano);
  printf("Categoria: %s\n", hr->categoria);
  printf("Quilometragem: %d\n", hr->quilometragem);
  printf("Status: %s\n", hr->status);
  puts("---------------------------\n");
}

data_record *load_data_record(io_buf *io, u16 rrn) {
  if (!io || !io->fp) {
    puts("!!Invalid IO buffer or file pointer");
    return NULL;
  }

  data_record *hr = malloc(sizeof(data_record));

  if (!hr) {
    puts("!!Memory allocation failed for data record");
    return NULL;
  }

  int byte_offset = io->hr->header_size + (io->hr->record_size * rrn);
  if (DEBUG)
    printf("Header size: %d, Record size: %d, RRN: %d, byte_offset: %d\n",
           io->hr->header_size, io->hr->record_size, rrn, byte_offset);

  if (fseek(io->fp, byte_offset, SEEK_SET) != 0) {
    puts("!!Error seeking to byte offset");
    free(hr);
    return NULL;
  }

  size_t t = fread(hr, sizeof(data_record), 1, io->fp);
  if (t != 1) {
    puts("!!Error while reading data record");
    free(hr);
    return NULL;
  }
  return hr;
}

void prepend_data_header(io_buf *io) {
  if (!io || !io->fp || !io->hr || !io->hr->free_rrn_address) {
    puts("!!Invalid input in prepend_data_header");
    return;
  }

  size_t free_rrn_len = strlen(io->hr->free_rrn_address) + 1,
         header_size = sizeof(u16) * 2 + free_rrn_len;

  if (io->hr->header_size != header_size) {
    io->hr->header_size = header_size;
  }

  fseek(io->fp, 0, SEEK_END);
  long original_file_size = ftell(io->fp);

  if (original_file_size <= 0) {
    puts("!!File is empty, no content to prepend");
    original_file_size = 0;
  }

  char *buffer = NULL;
  if (original_file_size > 0) {
    buffer = malloc(original_file_size);
    if (buffer == NULL) {
      puts("!!Memory allocation failed for file content");
      return;
    }

    fseek(io->fp, 0, SEEK_SET);
    if (fread(buffer, 1, original_file_size, io->fp) != original_file_size) {
      puts("!!Error reading original file content");
      free(buffer);
      return;
    }
  }

  fclose(io->fp);
  io->fp = fopen(io->address, "wb");
  if (io->fp == NULL) {
    puts("!!Error reopening file in write mode");
    if (buffer)
      free(buffer);
    return;
  }

  printf("free rrn address: %s\n", io->hr->free_rrn_address);
  fseek(io->fp, 0, SEEK_SET);
  if (fwrite(&io->hr->header_size, sizeof(u16), 1, io->fp) != 1 ||
      fwrite(&io->hr->record_size, sizeof(u16), 1, io->fp) != 1 ||
      fwrite(io->hr->free_rrn_address, free_rrn_len, 1, io->fp) != 1) {
    puts("!!Error writing header to file");
    if (buffer)
      free(buffer);
    return;
  }

  if (buffer && original_file_size > 0) {
    if (fwrite(buffer, 1, original_file_size, io->fp) != original_file_size) {
      puts("!!Error writing original content after header");
    }
    free(buffer);
  }

  if (DEBUG)
    printf("@Successfully written header: %hu %hu %s\n", io->hr->record_size,
           io->hr->header_size, io->hr->free_rrn_address);

  fflush(io->fp);
}

void write_data_record(io_buf *io, data_record *d, u16 rrn) {
  if (!io || !io->fp || !d) {
    puts("!!Invalid input in write_data_record");
    return;
  }

  int byte_offset = io->hr->header_size + (io->hr->record_size * rrn);
  fseek(io->fp, byte_offset, SEEK_SET);
  size_t t = fwrite(d, sizeof(data_record), 1, io->fp);
  if (t != 1) {
    puts("!!Error while writing data record");
  }
}

void populate_header(data_header_record *hp, const char *file_name) {
  if (hp == NULL) {
    puts("!!Header pointer is NULL, cannot populate");
    return;
  }

  hp->record_size = sizeof(data_record);
  strcpy(hp->free_rrn_address, file_name);
  hp->free_rrn_address[strlen(file_name) + 1] = '\0';
  hp->header_size = strlen(file_name) + 1 + sizeof(u16) * 2;
}

void load_file(io_buf *io, char *file_name, const char *type) {
  if (!file_name) {
    puts("!!ERROR: no file");
    return;
  }

  if (io->fp != NULL) {
    if (DEBUG)
      puts("--> buffer already filled\n--> closing logical link\n");
    if (fclose(io->fp) != 0) {
      puts("!!ERROR: failed to close file");
      return;
    }
    io->fp = NULL;
  }

  memcpy(io->address, file_name, strlen(file_name));
  io->address[strlen(file_name) + 1] = '\0';

  printf("@Loading file: %s\n", file_name);

  io->fp = fopen(io->address, "r+b");
  if (io->fp == NULL) {
    puts("!!Error opening file");
    return;
  }

  if (strcmp(type, "index") == 0) {
    if (io->br == NULL) {
      io->br = malloc(sizeof(index_header_record));
      if (io->br == NULL) {
        puts("!!Memory allocation failed for index_header_record");
        return;
      }
    }
    load_index_header(io);
    if (!io->br || !io->hr)
      exit(-1);
  }

  if (strcmp(type, "data") == 0) {
    if (io->hr == NULL) {
      io->hr = malloc(sizeof(data_header_record));
      if (io->hr == NULL) {
        puts("!!Memory allocation failed for data_header_record");
        fclose(io->fp);
        return;
      }
    }
    load_data_header(io);
  }
  if (strcmp(type, "data") == 0 &&
      (io->hr->record_size == 0 || io->hr->header_size == 0)) {
    puts("!!Error: one or more inputs in data_header_record are 0");
    fclose(io->fp);
    return;
  }

  if (strcmp(type, "index") == 0 &&
      (io->br->page_size == 0 || io->br->header_size == 0)) {
    puts("!!Error: one or more inputs in index_header_record are 0");
    fclose(io->fp);
    return;
  }

  puts("@File loaded");
}

void create_data_file(io_buf *io, char *file_name) {
  if (!io || !file_name) {
    puts("!!Invalid IO buffer or file name");
    return;
  }

  strcpy(io->address, file_name);

  if (io->fp != NULL) {
    puts("!!File already open");
    return;
  }

  int prepend = 0;
  char list_name[MAX_ADDRESS];
  strcpy(list_name, file_name);
  char *dot = strrchr(list_name, '.');
  if (dot)
    strcpy(dot, ".hlp");

  if (DEBUG)
    printf("@Loading File: %s\n", io->address);
  io->fp = fopen(io->address, "r+b");
  if (!io->fp) {
    if (DEBUG)
      printf("!!Error opening file: %s. Creating it...\n", io->address);
    io->fp = fopen(io->address, "wb");
    if (!io->fp) {
      puts("!!Error creating file");
      return;
    }
    fclose(io->fp);
    io->fp = fopen(io->address, "r+b");
    prepend = 1;
    populate_header(io->hr, list_name);
  } else {
    if (io->hr == NULL) {
      io->hr = malloc(sizeof(data_header_record));
      if (io->hr == NULL) {
        puts("!!Memory allocation failed for data_header_record");
        fclose(io->fp);
        return;
      }
    }

    load_data_header(io);

    if (io->hr->record_size != sizeof(data_record)) {
      puts("needs to prepend");
      prepend = 1;
      populate_header(io->hr, list_name);
    }
  }

  if (prepend == 1) {
    if (DEBUG)
      puts("@Prepending data header");
    populate_header(io->hr, list_name);
    prepend_data_header(io);
  } else {
    if (DEBUG)
      puts("@Header already exists, no need to prepend");
  }

  if (DEBUG) {
    puts("@Data file created successfully");
  }
}

void clear_io_buf(io_buf *io) {
  if (!io)
    return;

  if (io->fp) {
    fclose(io->fp);
    io->fp = NULL;
  }

  if (io->hr) {
    free(io->hr->free_rrn_address);
    free(io->hr);
    io->hr = NULL;
  }

  if (io->br) {
    free(io->br->free_rrn_address);
    free(io->br);
    io->br = NULL;
  }

  free(io);
  if (DEBUG) {
    puts("@IO_BUFFER cleared");
  }
}
