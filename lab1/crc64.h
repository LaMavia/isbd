/* SPDX-License-Identifier: GPL-2.0 */
#ifndef _LINUX_CRC64_H
#define _LINUX_CRC64_H

#define _POSIX_C_SOURCE 199309L

#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/**
 * crc64_be - Calculate bitwise big-endian ECMA-182 CRC64
 * @crc: seed value for computation. 0 or (uint64_t)~0 for a new CRC
 * calculation, or the previous crc64 value if computing incrementally.
 * @p: pointer to buffer over which CRC64 is run
 * @len: length of buffer @p
 */
uint64_t crc64_be(uint64_t crc, const void *p, size_t len);

void crc64_be_blocks(bool (*next)(void *, uint8_t *, size_t), void *iter_ctx,
                     size_t buffer_size, char *name, uint64_t seed);

void print_buffer(const uint8_t *buffer, size_t read_len);

#endif /* _LINUX_CRC64_H */
