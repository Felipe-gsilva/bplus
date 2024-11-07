#ifndef _DEFINES_H
#define _DEFINES_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEBUG 0 // 1 for dev mode, 0 for prod mode
#define ORDER 8

// in bytes
#define MAX_ADDRESS 4096

// size data record
#define TAMANHO_PLACA 8
#define TAMANHO_MODELO 20
#define TAMANHO_MARCA 20
#define TAMANHO_CATEGORIA 15
#define TAMANHO_STATUS 16

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef enum {
  BTREE_INSERTED_IN_PAGE = 5,
  BTREE_NOT_FOUND_KEY = 3,
  BTREE_FOUND_KEY = 2,
  BTREE_PROMOTION = 1,
  BTREE_NO_PROMOTION = 0,
  BTREE_SUCCESS = 0,
  BTREE_ERROR_MEMORY = -1,
  BTREE_ERROR_IO = -2,
  BTREE_ERROR_DUPLICATE = -3,
  BTREE_ERROR_INVALID_PAGE = -4,
  BTREE_ERROR_PAGE_FULL = -5
} btree_status;

typedef enum {  // not integrated yet
  IO_SUCCESS = 0,
  IO_ERROR = -1
} io_status;

typedef struct index_header_record index_header_record;
typedef struct data_header_record data_header_record;
typedef struct b_tree_buf b_tree_buf;
typedef struct io_buf io_buf;
typedef struct index_record index_record;
typedef struct data_record data_record;
typedef struct queue queue;
typedef struct key key;
typedef struct page page;
typedef struct app app;
typedef struct free_rrn_list free_rrn_list;


#pragma pack(push, 1)
struct key {
  u16 data_register_rrn;
  char id[TAMANHO_PLACA];
};
#pragma pack(pop)

#pragma pack(push, 1)
struct page {
  u16 rrn;
  key keys[ORDER - 1];
  u16 children[ORDER];
  u8 child_num;
  u8 keys_num;
  u8 leaf;
};
#pragma pack(pop)


struct queue {
  queue *next;
  page *page;
  u16 counter;
};

struct data_record {
  char placa[TAMANHO_PLACA];
  char modelo[TAMANHO_MODELO];
  char marca[TAMANHO_MARCA];
  int ano;
  char categoria[TAMANHO_CATEGORIA];
  int quilometragem;
  char status[TAMANHO_STATUS];
};

struct data_header_record {
  u16 header_size;
  u16 record_size;
  char *free_rrn_address;
};

struct index_header_record {
  u16 root_rrn;
  u16 page_size;
  u16 header_size;
  char *free_rrn_address;
};

struct io_buf {
  char address[MAX_ADDRESS];
  FILE *fp;
  data_header_record *hr;
  index_header_record *br;
};

struct b_tree_buf {
  page *root;
  io_buf *io;
  queue *q;
  free_rrn_list *i;
};

struct free_rrn_list {
  io_buf *io;
  u16 *free_rrn;
  u16 n;
};

struct app {
  io_buf *idx;
  io_buf *data;
  b_tree_buf *b;
  free_rrn_list *ld;
};

#endif
