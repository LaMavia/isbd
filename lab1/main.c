#include "crc64.h"
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "md5.h"

#define min(a, b)                                                              \
  ({                                                                           \
    __typeof__(a) _a = (a);                                                    \
    __typeof__(b) _b = (b);                                                    \
    _a < _b ? _a : _b;                                                         \
  })

#define max(a, b)                                                              \
  ({                                                                           \
    __typeof__(a) _a = (a);                                                    \
    __typeof__(b) _b = (b);                                                    \
    _a > _b ? _a : _b;                                                         \
  })

typedef struct {
  size_t i;
  int fd;
} seq_read_ctx;

typedef struct {
  ssize_t l;
  ssize_t r;
  size_t s;
  int fd;
} rand_read_ctx;

typedef struct {
  size_t i;
  size_t size;
  int fd;
  uint8_t *mem;
} seq_mmap_ctx;

typedef struct {
  ssize_t l;
  ssize_t r;
  size_t s;
  int fd;
  uint8_t *mem;
  size_t size;
} rand_mmap_ctx;

int parse_args(int argc, char *argv[], size_t *bs, int *fd, bool *verbose) {
  int ibs;
  if (argc < 3) {
    printf("Use: ./main block-size file-name [-v]\n");
    return 1;
  }

  ibs = atoi(argv[1]);
  if (ibs <= 0) {
    fprintf(stderr, "Error: buffer size must be positive but got %d\n", ibs);
    return 1;
  }

  *bs = (size_t)ibs;
  *fd = open(argv[2], O_RDONLY);
  if (*fd == -1) {
    fprintf(stderr, "Error: failed to open file '%s': %s\n", argv[1],
            strerror(errno));
    return 1;
  }

  *verbose = false;
  if (argc >= 4) {
    if (strcmp(argv[3], "-v") == 0) {
      *verbose = true;
    }
  }

  return 0;
}

void seq_read_ctx_init(seq_read_ctx *ctx, int fd) {
  lseek(fd, 0, SEEK_SET);

  ctx->fd = fd;
  ctx->i = 0;
}

size_t seq_read_iter(void *ctx_, uint8_t *buffer, size_t buffer_size,
                     bool *done) {
  seq_read_ctx *ctx = ctx_;
  size_t read_len = read(ctx->fd, buffer, buffer_size);

  *done = read_len < buffer_size;

  return read_len;
}

void rand_read_ctx_init(rand_read_ctx *ctx, int fd, size_t block_size) {
  struct stat st;
  lseek(fd, 0, SEEK_SET);
  fstat(fd, &st);

  size_t r_offset =
      st.st_size % block_size == 0 ? block_size : (st.st_size % block_size);

  ctx->l = 0;
  ctx->r = st.st_size - r_offset;
  ctx->fd = fd;
  ctx->s = 0;
}

size_t rand_read_iter(void *ctx_, uint8_t *buffer, size_t buffer_size,
                      bool *done) {
  rand_read_ctx *ctx = ctx_;
  size_t read_len;
  size_t pos = (1 - ctx->s) * ctx->l + ctx->s * ctx->r;
  *done = ctx->r - ctx->l < buffer_size;

  lseek(ctx->fd, pos, SEEK_SET);
  read_len = read(ctx->fd, buffer, buffer_size);

  ctx->l += (1 - ctx->s) * buffer_size;
  ctx->r -= ctx->s * buffer_size;
  ctx->s = 1 - ctx->s;

  return read_len;
}

void seq_mmap_ctx_init(seq_mmap_ctx *ctx, int fd) {
  struct stat st;
  lseek(fd, 0, SEEK_SET);
  fstat(fd, &st);

  ctx->i = 0;
  ctx->fd = fd;
  ctx->size = st.st_size;
  ctx->mem = (uint8_t *)mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
}

void seq_mmap_ctx_free(seq_mmap_ctx *ctx) { munmap(ctx->mem, ctx->size); }

size_t seq_mmap_iter(void *ctx_, uint8_t *buffer, size_t buffer_size,
                     bool *done) {
  seq_mmap_ctx *ctx = ctx_;
  size_t read_len = min(buffer_size, ctx->size - ctx->i);

  memcpy(buffer, ctx->mem + ctx->i, read_len);
  ctx->i += buffer_size;

  *done = ctx->i >= ctx->size;
  return read_len;
}

void rand_mmap_ctx_init(rand_mmap_ctx *ctx, int fd, size_t block_size) {
  struct stat st;
  lseek(fd, 0, SEEK_SET);
  fstat(fd, &st);

  size_t r_offset =
      st.st_size % block_size == 0 ? block_size : (st.st_size % block_size);

  ctx->l = 0;
  ctx->r = st.st_size - r_offset;
  ctx->fd = fd;
  ctx->s = 0;
  ctx->size = st.st_size;
  ctx->mem = (uint8_t *)mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
}

void rand_mmap_ctx_free(rand_mmap_ctx *ctx) { munmap(ctx->mem, ctx->size); }

size_t rand_mmap_iter(void *ctx_, uint8_t *buffer, size_t buffer_size,
                      bool *done) {
  rand_mmap_ctx *ctx = ctx_;
  size_t pos = (1 - ctx->s) * ctx->l + ctx->s * ctx->r;
  // printf("size=%zu, pos=%zu, l=%zu, r=%zu\n", ctx->size, pos, ctx->l,
  // ctx->r);
  size_t read_len = min(buffer_size, ctx->size - pos);
  *done = ctx->r - ctx->l <= 0;

  memcpy(buffer, ctx->mem + pos, read_len);

  ctx->l += (1 - ctx->s) * buffer_size;
  ctx->r -= ctx->s * buffer_size;
  ctx->s = 1 - ctx->s;

  return read_len;
}

int main(int argc, char *argv[]) {
  int parse_arg_code;
  size_t bs;
  int fd;
  bool verbose;

  if ((parse_arg_code = parse_args(argc, argv, &bs, &fd, &verbose)) != 0) {
    return parse_arg_code;
  }

  seq_read_ctx c1;
  seq_read_ctx_init(&c1, fd);
  crc64_be_blocks(seq_read_iter, &c1, bs, "seq read()", 0, verbose);

  rand_read_ctx c2;
  rand_read_ctx_init(&c2, fd, bs);
  crc64_be_blocks(rand_read_iter, &c2, bs, "rand read()", 0, verbose);

  seq_mmap_ctx c3;
  seq_mmap_ctx_init(&c3, fd);
  crc64_be_blocks(seq_mmap_iter, &c3, bs, "seq mmap()", 0, verbose);
  seq_mmap_ctx_free(&c3);

  rand_mmap_ctx c4;
  rand_mmap_ctx_init(&c4, fd, bs);
  crc64_be_blocks(rand_mmap_iter, &c4, bs, "rand mmap()", 0, verbose);
  rand_mmap_ctx_free(&c4);

  close(fd);
}
