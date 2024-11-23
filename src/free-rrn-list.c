#include "free-rrn-list.h"
#include "io-buf.h"

void sort_list(u16 A[], int n) {
  if (n < 1)
    return;
  int h = 1;
  while (h < n / 3) {
    h = 3 * h + 1;
  }

  while (h >= 1) {
    for (int i = h; i < n; i++) {
      int aux = A[i];
      int j = i;
      while (j >= h && A[j - h] > aux) {
        A[j] = A[j - h];
        j -= h;
      }
      A[j] = aux;
    }
    h = h / 3;
  }
}

bool rrn_exists(u16 A[], int n, int rrn) {
  if (!A || n <= 0)
    return false;

  int left = 0;
  int right = n - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    if (A[mid] == rrn)
      return true;
    if (A[mid] < rrn)
      left = mid + 1;
    else
      right = mid - 1;
  }
  return false;
}

void write_rrn_list_to_file(free_rrn_list *i) {
  if (!i || !i->io->fp)
    return;

  fseek(i->io->fp, 0, SEEK_SET);
  if (i->n > 0) {
    if (fwrite(&i->n, sizeof(u16), 1, i->io->fp) != 1) {
      puts("!!Error: Failed to write RRN count");
      return;
    }

    size_t written = fwrite(i->free_rrn, sizeof(u16), i->n, i->io->fp);
    if (written != i->n) {
      printf("!!Error: Expected to write %d elements, but wrote %zu\n", i->n,
             written);
      return;
    }
    return;
  }

  i->n = 0;
  if (fwrite(&i->n, sizeof(u16), 1, i->io->fp) != 1) {
    puts("!!Error: Failed to write empty RRN count");
  }

  fflush(i->io->fp);
}

free_rrn_list *alloc_ilist(void) {
  free_rrn_list *i = malloc(sizeof(free_rrn_list));
  if (!i)
    exit(-1);
  i->io = alloc_io_buf();
  i->n = 0;
  i->free_rrn = NULL;
  return i;
}

void clear_ilist(free_rrn_list *i) {
  if (!i)
    return;

  if (i->io && i->io->fp) {
    fclose(i->io->fp);
    i->io->fp = NULL;
  }

  free(i->free_rrn);
  i->free_rrn = NULL;

  clear_io_buf(i->io);
  free(i);
}

void load_list(free_rrn_list *i, char *s) {
  if (!i || !s) {
    puts("!!Error: Invalid parameters");
    return;
  }

  if (i->free_rrn) {
    free(i->free_rrn);
    i->free_rrn = NULL;
  }

  if (!i->io) {
    i->io = alloc_io_buf();
    if (!i->io) {
      puts("!!Error: Failed to allocate IO buffer");
      return;
    }
  }

  strcpy(i->io->address, s);
  i->io->address[strlen(s)] = '\0';

  i->io->fp = fopen(i->io->address, "r+b");
  if (!i->io->fp) {
    printf("Creating new RRN list file: %s\n", s);
    i->io->fp = fopen(i->io->address, "wb");
    if (!i->io->fp) {
      printf("!!Error: Cannot create file %s\n", s);
      return;
    }
    i->n = 1;
    i->free_rrn = malloc(sizeof(u16));
    if (!i->free_rrn) {
      puts("!!Error: Failed to allocate RRN list");
      fclose(i->io->fp);
      return;
    }
    i->free_rrn[0] = 0;
    fwrite(&i->n, sizeof(u16), 1, i->io->fp);
    fwrite(i->free_rrn, sizeof(u16), i->n, i->io->fp);
    fclose(i->io->fp);
    i->io->fp = fopen(i->io->address, "r+b");
  }

  fseek(i->io->fp, 0, SEEK_SET);
  size_t read = fread(&i->n, sizeof(u16), 1, i->io->fp);
  if (read != 1) {
    i->n = 1;
    i->free_rrn = malloc(sizeof(u16));
    if (!i->free_rrn) {
      puts("!!Error: Failed to allocate RRN list");
      return;
    }
    i->free_rrn[0] = 0;
    fseek(i->io->fp, 0, SEEK_SET);
    fwrite(&i->n, sizeof(u16), 1, i->io->fp);
    fwrite(i->free_rrn, sizeof(u16), i->n, i->io->fp);
  } else if (i->n > 0) {
    i->free_rrn = load_rrn_list(i);
    if (!i->free_rrn) {
      i->n = 1;
      i->free_rrn = malloc(sizeof(u16));
      if (!i->free_rrn) {
        puts("!!Error: Failed to allocate RRN list");
        return;
      }
      i->free_rrn[0] = 0;
      fseek(i->io->fp, 0, SEEK_SET);
      fwrite(&i->n, sizeof(u16), 1, i->io->fp);
      fwrite(i->free_rrn, sizeof(u16), i->n, i->io->fp);
    }
  }

  fflush(i->io->fp);
  if(DEBUG)
    printf("@Loaded RRN list with %d entries\n", i->n);
}

u16 *load_rrn_list(free_rrn_list *i) {
  if (!i->io->fp || i->n == 0)
    return NULL;

  u16 *list = malloc(sizeof(u16) * i->n);
  if (!list) {
    puts("!!Error: memory allocation failed");
    return NULL;
  }

  fseek(i->io->fp, sizeof(u16), SEEK_SET);
  size_t read = fread(list, sizeof(u16), i->n, i->io->fp);

  if (read != i->n) {
    free(list);
    printf("!!Error: Expected to read %d elements, but read %zu\n", i->n, read);
    return NULL;
  }

  if (DEBUG) {
    for (int j = 0; j < i->n; j++)
      printf("i->list[%d]: %hu\t", j, list[j]);
    puts("");
  }
  return list;
}

u16 get_free_rrn(free_rrn_list *i) {
  if (!i || !i->io->fp) {
    puts("!!Error: Invalid list or file pointer");
    exit(1);
  }

  free(i->free_rrn);
  i->free_rrn = load_rrn_list(i);

  if (!i->free_rrn || i->n == 0) {
    puts("!!Error: No free RRNs available; initializing with default");
    i->n = 1;
    i->free_rrn = malloc(sizeof(u16));
    if (!i->free_rrn) {
      puts("!!Error: Failed to allocate RRN list");
      exit(1);
    }
    i->free_rrn[0] = 0;
    write_rrn_list_to_file(i);
    return 0;
  }

  int rrn = i->free_rrn[0];
  i->n--;

  if (i->n > 0)
    memmove(i->free_rrn, i->free_rrn + 1, sizeof(u16) * i->n);

  if (i->n < 1) {
    u16 new_rrn = rrn + 1;
    while (rrn_exists(i->free_rrn, i->n, new_rrn))
      new_rrn++;

    u16 *new_list = realloc(i->free_rrn, (i->n + 1) * sizeof(u16));
    if (!new_list) {
      puts("!!Error: Failed to reallocate RRN list");
    }
    i->free_rrn = new_list;
    i->free_rrn[i->n] = new_rrn;
    i->n++;
  }

  sort_list(i->free_rrn, i->n);
  write_rrn_list_to_file(i);
  return rrn;
}

u16 get_last_free_rrn(free_rrn_list *i) {
  if (!i || !i->io->fp) {
    puts("!!Error: invalid list or file pointer");
    return (u16)-1;
  }

  if (!i->free_rrn)
    i->free_rrn = load_rrn_list(i);

  if (!i->free_rrn || i->n == 0) {
    puts("!!Error: no free RRNs available");
    return (u16)-1;
  }
  return i->free_rrn[i->n - 1];
}

void insert_list(free_rrn_list *i, int rrn) {
  if (!i || !i->io->fp) {
    puts("!!Error: NULL rrn list or file pointer");
    return;
  }

  if(DEBUG)
    printf("Current count before insertion: %d\n", i->n);
  if (rrn_exists(i->free_rrn, i->n, rrn)) {
    if(DEBUG)
      printf("@RRN %d already exists in the list\n", rrn);
    return;
  }

  u16 *new_list = realloc(i->free_rrn, (i->n + 1) * sizeof(u16));
  if (!new_list) {
    puts("!!Error: Memory allocation failed");
    return;
  }

  i->free_rrn = new_list;
  i->free_rrn[i->n++] = rrn;
  if (DEBUG)
    printf("New count after insertion: %d\n", i->n);

  sort_list(i->free_rrn, i->n);
  write_rrn_list_to_file(i);

  if (DEBUG) {
    printf("List after insertion:\n");
    for (int j = 0; j < i->n; j++)
      printf("%hu ", i->free_rrn[j]);
    puts("");
    printf("@RRN %d added and list sorted. New list:\n", rrn);
    for (int j = 0; j < i->n; j++)
      printf("%d ", i->free_rrn[j]);
    puts("");
  }
}
