#ifndef MD5_H
#define MD5_H

#define _POSIX_C_SOURCE 199309L

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
  uint64_t size;      // Size of input in bytes
  uint32_t buffer[4]; // Current accumulation of hash
  uint8_t input[64];  // Input to be used in the next step
  uint8_t digest[16]; // Result of algorithm
} MD5Context;

void md5Init(MD5Context *ctx);
void md5Update(MD5Context *ctx, uint8_t *input, size_t input_len);
void md5Finalize(MD5Context *ctx);
void md5Step(uint32_t *buffer, uint32_t *input);

void md5Blocks(bool (*next)(void *, uint8_t *, size_t), void *iter_ctx,
               size_t buffer_size, char *name);
// void md5String(char *input, uint8_t *result);
// void md5File(FILE *file, uint8_t *result);

#endif
