#ifndef STATIC_TRIE_H
#define STATIC_TRIE_H

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "common_dv1.hpp"
#include "../../ds_common/src/bv.hpp"
#include "../../ds_common/src/vint.hpp"
#include "../../ds_common/src/gen.hpp"

// Function qualifiers
#ifndef __fq1
#define __fq1
#endif

#ifndef __fq2
#define __fq2
#endif

// Function qualifiers
#ifndef __gq1
#define __gq1
#endif

#ifndef __gq2
#define __gq2
#endif

namespace madras_dv1 {

class iter_ctx {
  public:
    int32_t cur_idx;
    uint16_t key_len;
    uint8_t *key;
    uint32_t *node_path;
    uint32_t *child_count;
    uint16_t *last_tail_len;
    bool to_skip_first_leaf;
    bool is_allocated = false;
    __fq1 __fq2 iter_ctx() {
      is_allocated = false;
    }
    __fq1 __fq2 ~iter_ctx() {
      close();
    }
    __fq1 __fq2 void close() {
      if (is_allocated) {
        delete [] key;
        delete [] node_path;
        delete [] child_count;
        delete [] last_tail_len;
      }
      is_allocated = false;
    }
    void init(uint16_t max_key_len, uint16_t max_level) {
      max_level++;
      max_key_len++;
      if (!is_allocated) {
        key = new uint8_t[max_key_len];
        node_path = new uint32_t[max_level];
        child_count = new uint32_t[max_level];
        last_tail_len = new uint16_t[max_level];
      }
      memset(node_path, 0, max_level * sizeof(uint32_t));
      memset(child_count, 0, max_level * sizeof(uint32_t));
      memset(last_tail_len, 0, max_level * sizeof(uint16_t));
      node_path[0] = 1;
      child_count[0] = 1;
      cur_idx = key_len = 0;
      to_skip_first_leaf = false;
      is_allocated = true;
    }
};

#define DCT_INSERT_AFTER -2
#define DCT_INSERT_BEFORE -3
#define DCT_INSERT_LEAF -4
#define DCT_INSERT_EMPTY -5
#define DCT_INSERT_THREAD -6
#define DCT_INSERT_CONVERT -7
#define DCT_INSERT_CHILD_LEAF -8

struct min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
};

class cmn {
  public:
    __fq1 __fq2 static uint32_t read_uint16(const uint8_t *ptr) {
      return *((uint16_t *) ptr); // faster endian dependent
      // uint32_t ret = *ptr++;
      // ret |= (*ptr << 8);
      // return ret;
    }
    __fq1 __fq2 static uint32_t read_uint24(const uint8_t *ptr) {
      //       return *((uint32_t *) ptr) & 0x00FFFFFF; // faster endian dependent
      uint32_t ret = *ptr++;
      ret |= (*ptr++ << 8);
      ret |= (*ptr << 16);
      return ret;
    }
    __fq1 __fq2 static uint32_t read_uint32(uint8_t *ptr) {
      return *((uint32_t *) ptr);
    }
    __fq1 __fq2 static uint64_t read_uint64(uint8_t *t) {
      return *((uint64_t *) t);
    }
    __fq1 __fq2 static int memcmp(const void *ptr1, const void *ptr2, size_t num) {
        const unsigned char *a = (const unsigned char *)ptr1;
        const unsigned char *b = (const unsigned char *)ptr2;
        for (size_t i = 0; i < num; ++i) {
            if (a[i] != b[i]) {
                return (a[i] < b[i]) ? -1 : 1;
            }
        }
        return 0;
    }
};

__gq1 __gq2 static const uint8_t bit_count[256] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
__gq1 __gq2 static const uint8_t select_lookup_tbl[8][256] = {{
  8, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  7, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  8, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  7, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
  6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1
}, {
  8, 8, 8, 2, 8, 3, 3, 2, 8, 4, 4, 2, 4, 3, 3, 2, 8, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  8, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  8, 7, 7, 2, 7, 3, 3, 2, 7, 4, 4, 2, 4, 3, 3, 2, 7, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  7, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  8, 8, 8, 2, 8, 3, 3, 2, 8, 4, 4, 2, 4, 3, 3, 2, 8, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  8, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  8, 7, 7, 2, 7, 3, 3, 2, 7, 4, 4, 2, 4, 3, 3, 2, 7, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
  7, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2
}, {
  8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 4, 8, 4, 4, 3, 8, 8, 8, 5, 8, 5, 5, 3, 8, 5, 5, 4, 5, 4, 4, 3, 
  8, 8, 8, 6, 8, 6, 6, 3, 8, 6, 6, 4, 6, 4, 4, 3, 8, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3, 
  8, 8, 8, 7, 8, 7, 7, 3, 8, 7, 7, 4, 7, 4, 4, 3, 8, 7, 7, 5, 7, 5, 5, 3, 7, 5, 5, 4, 5, 4, 4, 3, 
  8, 7, 7, 6, 7, 6, 6, 3, 7, 6, 6, 4, 6, 4, 4, 3, 7, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3, 
  8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 4, 8, 4, 4, 3, 8, 8, 8, 5, 8, 5, 5, 3, 8, 5, 5, 4, 5, 4, 4, 3, 
  8, 8, 8, 6, 8, 6, 6, 3, 8, 6, 6, 4, 6, 4, 4, 3, 8, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3, 
  8, 8, 8, 7, 8, 7, 7, 3, 8, 7, 7, 4, 7, 4, 4, 3, 8, 7, 7, 5, 7, 5, 5, 3, 7, 5, 5, 4, 5, 4, 4, 3, 
  8, 7, 7, 6, 7, 6, 6, 3, 7, 6, 6, 4, 6, 4, 4, 3, 7, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3
}, {
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 
  8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5, 5, 4, 
  8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 
  8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 
  8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5, 5, 4, 
  8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 
  8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4
}, {
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 
  8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 
  8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5
}, {
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6
}, {
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7
}, {
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8
}};

class leapfrog {
  public:
    __fq1 __fq2 leapfrog() {};
    __fq1 __fq2 virtual ~leapfrog() {};
    __fq1 __fq2 virtual void find_pos(uint32_t& node_id, const uint8_t *trie_loc, uint8_t key_byte) = 0;
};

class leapfrog_asc : public leapfrog {
  private:
    uint8_t *min_pos_loc;
    min_pos_stats min_stats;
  public:
    __fq1 __fq2 void find_pos(uint32_t& node_id, const uint8_t *trie_loc, uint8_t key_byte) {
      uint8_t min_offset = min_pos_loc[((trie_loc[node_id] - min_stats.min_len) * 256) + key_byte];
      node_id += min_offset;
    }
    __fq1 __fq2 leapfrog_asc() {
    }
    __fq1 __fq2 void init(min_pos_stats _stats, uint8_t *_min_pos_loc) {
      min_stats = _stats;
      min_pos_loc = _min_pos_loc;
    }
};

class bvlt_rank {
  protected:
    uint64_t *bm_loc;
    uint8_t *lt_rank_loc;
    uint8_t multiplier;
    uint8_t lt_width;
  public:
    __fq1 __fq2 bool operator[](size_t pos) {
      return ((bm_loc[multiplier * (pos / 64)] >> (pos % 64)) & 1) != 0;
    }
    __fq1 __fq2 bool is_set1(size_t pos) {
      return ((bm_loc[pos / 64] >> (pos % 64)) & 1) != 0;
    }
    __fq1 __fq2 uint32_t rank1(uint32_t bv_pos) {
      uint8_t *rank_ptr = lt_rank_loc + bv_pos / nodes_per_bv_block * lt_width;
      uint32_t rank = cmn::read_uint32(rank_ptr);
      #if nodes_per_bv_block == 512
      int pos = (bv_pos / nodes_per_bv_block_n) % width_of_bv_block_n;
      if (pos > 0) {
        rank_ptr += 4;
        //while (pos--)
        //  rank += *rank_ptr++;
        rank += (rank_ptr[pos] + (((uint32_t)(*rank_ptr) << pos) & 0x100));
      }
      #else
      int pos = (bv_pos / nodes_per_bv_block_n) % (width_of_bv_block_n + 1);
      if (pos > 0)
        rank += rank_ptr[4 + pos - 1];
      #endif
      uint64_t mask = (bm_init_mask << (bv_pos % nodes_per_bv_block_n)) - 1;
      uint64_t bm = bm_loc[(bv_pos / nodes_per_bv_block_n) * multiplier];
      // return rank + __popcountdi2(bm & (mask - 1));
      return rank + static_cast<uint32_t>(__builtin_popcountll(bm & mask));
    }
    __fq1 __fq2 uint8_t *get_rank_loc() {
      return lt_rank_loc;
    }
    __fq1 __fq2 bvlt_rank() {
    }
    __fq1 __fq2 void init(uint8_t *_lt_rank_loc, uint64_t *_bm_loc, uint8_t _multiplier, uint8_t _lt_unit_count) {
      lt_rank_loc = _lt_rank_loc;
      bm_loc = _bm_loc;
      multiplier = _multiplier;
      lt_width = _lt_unit_count * width_of_bv_block;
    }
};

struct input_ctx {
  const uint8_t *key;
  uint32_t key_len;
  uint32_t key_pos;
  uint32_t node_id;
  int32_t cmp;
};

class inner_trie_fwd {
  public:
    bvlt_rank tail_lt;
    __fq1 __fq2 inner_trie_fwd() {
    }
    __fq1 __fq2 virtual ~inner_trie_fwd() {
    }
    __fq1 __fq2 virtual bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) = 0;
    __fq1 __fq2 virtual bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) = 0;
    __fq1 __fq2 virtual inner_trie_fwd *new_instance(uint8_t *mem) = 0;
};

class tail_ptr_map {
  public:
    __fq1 __fq2 tail_ptr_map() {
    }
    __fq1 __fq2 virtual ~tail_ptr_map() {
    }
    __fq1 __fq2 virtual bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) = 0;
    __fq1 __fq2 virtual void get_tail_str(uint32_t node_id, gen::byte_str& tail_str) = 0;
    __fq1 __fq2 static uint32_t read_len(uint8_t *t) {
      while (*t & 0x10 && *t < 32)
        t++;
      t--;
      uint32_t ret;
      read_len_bw(t, ret);
      return ret;
    }
    __fq1 __fq2 static uint8_t *read_len_bw(uint8_t *t, uint32_t& out_len) {
      out_len = 0;
      while (*t & 0x10 && *t < 32) {
        out_len <<= 4;
        out_len += (*t-- & 0x0F);
      }
      return t;
    }
};

class tail_ptr_flat_map : public tail_ptr_map {
  public:
    gen::int_bv_reader int_ptr_bv;
    bvlt_rank *tail_lt;
    uint8_t *trie_loc;
    inner_trie_fwd *inner_trie;
    uint8_t *data;
    __fq1 __fq2 tail_ptr_flat_map() {
      inner_trie = nullptr;
    }
    __fq1 __fq2 virtual ~tail_ptr_flat_map() {
      if (inner_trie != nullptr)
        delete inner_trie;
    }
    __fq1 __fq2 void init(inner_trie_fwd *_dict_obj, bvlt_rank *_tail_lt, uint8_t *_trie_loc, uint8_t *tail_loc) {
      tail_lt = _tail_lt;
      trie_loc = _trie_loc;
      uint8_t encoding_type = tail_loc[2];
      uint8_t *data_loc = tail_loc + cmn::read_uint32(tail_loc + 12);
      uint8_t *ptrs_loc = tail_loc + cmn::read_uint32(tail_loc + 24);

      uint8_t ptr_lt_ptr_width = tail_loc[0];
      inner_trie = nullptr;
      if (encoding_type == 't') {
        inner_trie = _dict_obj->new_instance(data_loc);
        int_ptr_bv.init(ptrs_loc, ptr_lt_ptr_width);
      } else {
        uint8_t group_count = *data_loc;
        if (group_count == 1)
          int_ptr_bv.init(ptrs_loc, data_loc[1]);
        uint8_t *data_idx_start = data_loc + 8 + 512;
        data = tail_loc + cmn::read_uint32(data_idx_start);
      }
    }
    __fq1 __fq2 uint32_t get_tail_ptr(uint32_t node_id, uint32_t& ptr_bit_count) {
      if (ptr_bit_count == UINT32_MAX)
        ptr_bit_count = tail_lt->rank1(node_id);
      return (int_ptr_bv[ptr_bit_count++] << 8) | trie_loc[node_id];
    }
    __fq1 __fq2 bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) {
      uint32_t tail_ptr = get_tail_ptr(node_id, ptr_bit_count);
      if (inner_trie != nullptr)
        return inner_trie->compare_trie_tail(tail_ptr, in_ctx);
      uint8_t *tail = data + tail_ptr;
      if (*tail >= 32) {
        do {
          if (in_ctx.key_pos >= in_ctx.key_len || *tail != in_ctx.key[in_ctx.key_pos])
            return false;
          tail++;
          in_ctx.key_pos++;
        } while (*tail >= 32);
        if (*tail == 0)
          return true;
      } else {
        uint32_t bin_len = read_len(tail);
        while (*tail < 32)
          tail++;
        if (in_ctx.key_pos + bin_len > in_ctx.key_len)
          return false;
        if (cmn::memcmp(tail, in_ctx.key + in_ctx.key_pos, bin_len) == 0) {
          in_ctx.key_pos += bin_len;
          return true;
        }
        if (*tail == in_ctx.key[in_ctx.key_pos])
          in_ctx.key_pos++;
        return false;
      }
      return false;
    }
    __fq1 __fq2 void get_tail_str(uint32_t node_id, gen::byte_str& tail_str) {
      uint32_t tail_ptr = UINT32_MAX;
      tail_ptr = get_tail_ptr(node_id, tail_ptr); // avoid a stack entry
      if (inner_trie != nullptr) {
        inner_trie->copy_trie_tail(tail_ptr, tail_str);
        return;
      }
      uint8_t *t = data + tail_ptr;
      uint8_t byt = *t;
      while (byt > 31) {
        t++;
        tail_str.append(byt);
        byt = *t;
      }
    }
};

class GCFC_rev_cache {
  private:
    nid_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    __fq1 __fq2 bool try_find(uint32_t& node_id, input_ctx& in_ctx) {
      if (node_id >= max_node_id)
        return false;
      nid_cache *cche = cche0 + (node_id & cache_mask);
      if (node_id == cmn::read_uint24(&cche->child_node_id1)) {
        if (cche->tail0_len == 0)
          node_id = cmn::read_uint24(&cche->parent_node_id1);
        else {
          if (in_ctx.key_pos + cche->tail0_len > in_ctx.key_len)
            return false;
          if (cmn::memcmp(in_ctx.key + in_ctx.key_pos, &cche->parent_node_id1, cche->tail0_len) != 0)
            return false;
          in_ctx.key_pos += cche->tail0_len;
          node_id = 0;
        }
        return true;
      }
      return false;
    }
    __fq1 __fq2 bool try_find(uint32_t& node_id, gen::byte_str& tail_str) {
      if (node_id >= max_node_id)
        return false;
      nid_cache *cche = cche0 + (node_id & cache_mask);
      if (node_id == cmn::read_uint24(&cche->child_node_id1)) {
        if (cche->tail0_len == 0)
          node_id = cmn::read_uint24(&cche->parent_node_id1);
        else {
          tail_str.append(&cche->parent_node_id1, cche->tail0_len);
          node_id = 0;
        }
        return true;
      }
      return false;
    }
    __fq1 __fq2 GCFC_rev_cache() {
    }
    __fq1 __fq2 void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (nid_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class bvlt_select : public bvlt_rank {
  protected:
    uint8_t *lt_sel_loc1;
    uint32_t bv_bit_count;
  public:
    __fq1 __fq2 uint32_t bin_srch_lkup_tbl(uint32_t first, uint32_t last, uint32_t given_count) {
      while (first + 1 < last) {
        const uint32_t middle = (first + last) >> 1;
        if (given_count < cmn::read_uint32(lt_rank_loc + middle * lt_width))
          last = middle;
        else
          first = middle;
      }
      return first;
    }
    __fq1 __fq2 uint32_t select1(uint32_t target_count) {
      if (target_count == 0)
        return 0;
      uint8_t *select_loc = lt_sel_loc1 + target_count / sel_divisor * 3;
      uint32_t block = cmn::read_uint24(select_loc);
      // uint32_t end_block = cmn::read_uint24(select_loc + 3);
      // if (block + 4 < end_block)
      //   block = bin_srch_lkup_tbl(block, end_block, target_count);
      uint8_t *block_loc = lt_rank_loc + block * lt_width;
      while (cmn::read_uint32(block_loc) < target_count) {
        block++;
        block_loc += lt_width;
      }
      block--;
      uint32_t bv_pos = block * nodes_per_bv_block;
      block_loc -= lt_width;
      uint32_t remaining = target_count - cmn::read_uint32(block_loc);
      if (remaining == 0)
        return bv_pos;
      block_loc += 4;
      #if nodes_per_bv_block == 256
      size_t pos_n = 0;
      while (block_loc[pos_n] < remaining && pos_n < width_of_bv_block_n)
        pos_n++;
      if (pos_n > 0) {
        remaining -= block_loc[pos_n - 1];
        bv_pos += (nodes_per_bv_block_n * pos_n);
      }
      #else
      // size_t pos_n = 0;
      // while (block_loc[pos_n] < remaining && pos_n < width_of_bv_block_n) {
      //   bv_pos += nodes_per_bv_block_n;
      //   remaining -= block_loc[pos_n];
      //   pos_n++;
      // }
      size_t pos_n = 1;
      while (get_count(block_loc, pos_n) < remaining && pos_n < width_of_bv_block_n)
        pos_n++;
      if (pos_n-- > 1) {
        remaining -= get_count(block_loc, pos_n);
        bv_pos += (nodes_per_bv_block_n * pos_n);
      }
      #endif
      if (remaining == 0)
        return bv_pos;
      uint64_t bm = bm_loc[(bv_pos / 64) * multiplier];
      return bv_pos + bm_select1(remaining, bm);
    }
    __fq1 __fq2 inline uint32_t get_count(uint8_t *block_loc, size_t pos_n) {
      return (block_loc[pos_n] + (((uint32_t)(*block_loc) << pos_n) & 0x100));
    }
    __fq1 __fq2 inline uint32_t bm_select1(uint32_t remaining, uint64_t bm) {

      // uint64_t isolated_bit = _pdep_u64(1ULL << (remaining - 1), bm);
      // size_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      // if (bit_loc == 65) {
      //   printf("WARNING: UNEXPECTED bit_loc=65, bit_loc: %u\n", remaining);
      //   return 64;
      // }

      size_t bit_loc = 0;
      while (bit_loc < 64) {
        uint8_t next_count = bit_count[(bm >> bit_loc) & 0xFF];
        if (next_count >= remaining)
          break;
        bit_loc += 8;
        remaining -= next_count;
      }
      if (remaining > 0)
        bit_loc += select_lookup_tbl[remaining - 1][(bm >> bit_loc) & 0xFF];

      // size_t bit_loc = 0;
      // do {
      //   bit_loc = __builtin_ffsll(bm);
      //   remaining--;
      //   bm &= ~(1ULL << (bit_loc - 1));
      // } while (remaining > 0);

      // size_t bit_loc = 0;
      // uint64_t bm_mask = bm_init_mask << bit_loc;
      // while (remaining > 0) {
      //   if (bm & bm_mask)
      //     remaining--;
      //   bit_loc++;
      //   bm_mask <<= 1;
      // }
      return bit_loc;
    }
    __fq1 __fq2 uint8_t *get_select_loc1() {
      return lt_sel_loc1;
    }
    __fq1 __fq2 bvlt_select() {
    }
    __fq1 __fq2 void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc1, uint32_t _bv_bit_count, uint64_t *_bm_loc, uint8_t _multiplier, uint8_t _lt_width) {
      bvlt_rank::init(_lt_rank_loc, _bm_loc, _multiplier, _lt_width);
      lt_sel_loc1 = _lt_sel_loc1;
      bv_bit_count = _bv_bit_count;
    }

};

struct trie_flags {
  uint64_t bm_ptr;
  uint64_t bm_term;
  uint64_t bm_child;
  uint64_t bm_leaf;
};

class inner_trie : public inner_trie_fwd {
  protected:
    uint32_t node_count;
    uint8_t trie_level;
    uint8_t lt_not_given;
    uint8_t *trie_loc;
    tail_ptr_map *tail_map;
    bvlt_select child_lt;
    bvlt_select term_lt;
    GCFC_rev_cache rev_cache;

  public:
    __fq1 __fq2 bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) {
      do {
        if (tail_lt.is_set1(node_id)) {
          uint32_t ptr_bit_count = UINT32_MAX;
          if (!tail_map->compare_tail(node_id, in_ctx, ptr_bit_count))
            return false;
        } else {
          if (in_ctx.key_pos >= in_ctx.key_len || trie_loc[node_id] != in_ctx.key[in_ctx.key_pos])
            return false;
          in_ctx.key_pos++;
        }
        if (!rev_cache.try_find(node_id, in_ctx))
          node_id = child_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    __fq1 __fq2 bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) {
      do {
        if (tail_lt.is_set1(node_id)) {
          tail_map->get_tail_str(node_id, tail_str);
        } else
          tail_str.append(trie_loc[node_id]);
        if (!rev_cache.try_find(node_id, tail_str))
          node_id = child_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    __fq1 __fq2 inner_trie(uint8_t _trie_level = 0) {
      trie_level = _trie_level;
    }
    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      inner_trie *it = new inner_trie(trie_level + 1);
      it->load_inner_trie(mem);
      return it;
    }
    __fq1 __fq2 void load_inner_trie(uint8_t *trie_bytes) {

      tail_map = nullptr;
      trie_loc = nullptr;

printf("%x\n", trie_bytes[0]);

      node_count = cmn::read_uint32(trie_bytes + 16);
printf("%u\n", node_count);
      uint32_t node_set_count = cmn::read_uint32(trie_bytes + 24);
      uint32_t key_count = cmn::read_uint32(trie_bytes + 28);
      if (key_count > 0) {
        uint32_t rev_cache_count = cmn::read_uint32(trie_bytes + 48);
        uint32_t rev_cache_max_node_id = cmn::read_uint32(trie_bytes + 56);
        uint8_t *rev_cache_loc = trie_bytes + cmn::read_uint32(trie_bytes + 68);
        rev_cache.init(rev_cache_loc, rev_cache_count, rev_cache_max_node_id);

        uint8_t *term_select_lkup_loc = trie_bytes + cmn::read_uint32(trie_bytes + 76);
        uint8_t *term_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 80);
        uint8_t *child_select_lkup_loc = trie_bytes + cmn::read_uint32(trie_bytes + 84);
        uint8_t *child_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 88);

        uint8_t *tail_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 100);
        uint8_t *trie_tail_ptrs_data_loc = trie_bytes + cmn::read_uint32(trie_bytes + 104);

        uint32_t tail_size = cmn::read_uint32(trie_tail_ptrs_data_loc);
        //uint32_t trie_flags_size = cmn::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 8;
        trie_loc = tails_loc + tail_size;

        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 116));
        uint64_t *tf_ptr_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 120));

        uint8_t encoding_type = tails_loc[2];
        uint8_t *tail_data_loc = tails_loc + cmn::read_uint32(tails_loc + 12);
        if (encoding_type == 't' || *tail_data_loc == 1) {
          tail_ptr_flat_map *tail_flat_map = new tail_ptr_flat_map();  // Release where?
          tail_flat_map->init(this, &tail_lt, trie_loc, tails_loc);
          tail_map = tail_flat_map;
        }

        lt_not_given = 0;
        if (term_select_lkup_loc == trie_bytes) term_select_lkup_loc = nullptr;
        if (term_lt_loc == trie_bytes) term_lt_loc = nullptr;
        if (child_select_lkup_loc == trie_bytes) child_select_lkup_loc = nullptr;
        if (child_lt_loc == trie_bytes) child_lt_loc = nullptr;
        if (tail_lt_loc == trie_bytes) tail_lt_loc = nullptr; // TODO: to build if dessicated?
        uint8_t bvlt_block_count = tail_lt_loc == nullptr ? 2 : 3;

        if (trie_level == 0) {
          term_lt.init(term_lt_loc, term_select_lkup_loc, node_count, tf_loc + 1, 4, bvlt_block_count);
          child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, tf_loc + 2, 4, bvlt_block_count);
        } else {
          child_lt.init(child_lt_loc, child_select_lkup_loc, node_count * 2, tf_loc, 1, 1);
        }
        tail_lt.init(tail_lt_loc, trie_level == 0 ? tf_loc : tf_ptr_loc, trie_level == 0 ? 4 : 1, trie_level == 0 ? 3 : 1);
      }

    }

    __fq1 __fq2 virtual ~inner_trie() {
      if (tail_map != nullptr) {
        delete tail_map;
      }
      if (lt_not_given & BV_LT_TYPE_CHILD) {
        delete [] child_lt.get_rank_loc();
        delete [] child_lt.get_select_loc1();
      }
      if (lt_not_given & BV_LT_TYPE_TERM) {
        delete [] term_lt.get_rank_loc();
        delete [] term_lt.get_select_loc1();
      }
    }

};

struct ctx_vars_next {
  public:
    gen::byte_str tail;
};

class GCFC_fwd_cache {
  private:
    fwd_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    __fq1 __fq2 int try_find(input_ctx& in_ctx) {
      if (in_ctx.node_id >= max_node_id)
        return -1;
      uint8_t key_byte = in_ctx.key[in_ctx.key_pos];
      do {
        uint32_t parent_node_id = in_ctx.node_id;
        uint32_t cache_idx = (parent_node_id ^ (parent_node_id << MDX_CACHE_SHIFT) ^ key_byte) & cache_mask;
        fwd_cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = cmn::read_uint24(&cche->parent_node_id1);
        if (parent_node_id == cache_node_id && cche->node_byte == key_byte) {
          in_ctx.key_pos++;
          if (in_ctx.key_pos < in_ctx.key_len) {
            in_ctx.node_id = cmn::read_uint24(&cche->child_node_id1);
            key_byte = in_ctx.key[in_ctx.key_pos];
            continue;
          }
          in_ctx.node_id += cche->node_offset;
          return 0;
        }
        return -1;
      } while (1);
      return -1;
    }
    __fq1 __fq2 GCFC_fwd_cache() {
    }
    __fq1 __fq2 void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (fwd_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class static_trie : public inner_trie {
  protected:
    bvlt_select *leaf_lt;
    GCFC_fwd_cache fwd_cache;
    trie_flags *trie_flags_loc;
    leapfrog *leaper;
    uint16_t max_tail_len;
    uint32_t key_count;
    uint8_t *trie_bytes;

  private:
    size_t max_key_len;
    bldr_options *opts;
    uint16_t max_level;
  public:
    __fq1 __fq2 bool lookup(input_ctx& in_ctx) {
      in_ctx.key_pos = 0;
      in_ctx.node_id = 1;
      trie_flags *tf;
      uint64_t bm_mask;
printf("Node id0: %u\n", in_ctx.node_id);
      do {
        int ret = fwd_cache.try_find(in_ctx);
printf("Node id1: %u\n", in_ctx.node_id);
        bm_mask = bm_init_mask << (in_ctx.node_id % nodes_per_bv_block_n);
        tf = trie_flags_loc + in_ctx.node_id / nodes_per_bv_block_n;
        if (ret == 0)
          return bm_mask & tf->bm_leaf;
        if (leaper != nullptr) {
          if ((bm_mask & tf->bm_leaf) == 0 && (bm_mask & tf->bm_child) == 0) {
            leaper->find_pos(in_ctx.node_id, trie_loc, in_ctx.key[in_ctx.key_pos]);
            bm_mask = bm_init_mask << (in_ctx.node_id % nodes_per_bv_block_n);
            tf = trie_flags_loc + in_ctx.node_id / nodes_per_bv_block_n;
          }
        }
        uint32_t ptr_bit_count = UINT32_MAX;
        do {
          if ((bm_mask & tf->bm_ptr) == 0) {
            if (in_ctx.key[in_ctx.key_pos] == trie_loc[in_ctx.node_id]) {
              in_ctx.key_pos++;
              break;
            }
            #ifdef MDX_IN_ORDER
              if (in_ctx.key[in_ctx.key_pos] < trie_loc[in_ctx.node_id])
                return false;
            #endif
          } else {
            uint32_t prev_key_pos = in_ctx.key_pos;
            if (tail_map->compare_tail(in_ctx.node_id, in_ctx, ptr_bit_count))
              break;
            if (prev_key_pos != in_ctx.key_pos)
              return false;
          }
printf("Node id2: %u\n", in_ctx.node_id);
          if (bm_mask & tf->bm_term)
            return false;
          in_ctx.node_id++;
          bm_mask <<= 1;
          if (bm_mask == 0) {
            bm_mask = bm_init_mask;
            tf++;
          }
        } while (1);
        if (in_ctx.key_pos == in_ctx.key_len) {
printf("Node id3: %u\n", in_ctx.node_id);
          return 0 != (bm_mask & tf->bm_leaf);
        }
        if ((bm_mask & tf->bm_child) == 0)
          return false;
        in_ctx.node_id = term_lt.select1(child_lt.rank1(in_ctx.node_id) + 1);
printf("Node id4: %u\n", in_ctx.node_id);
      } while (1);
      return false;
    }

    __fq1 __fq2 bool reverse_lookup(uint32_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      leaf_id++;
      uint32_t node_id = leaf_lt->select1(leaf_id) - 1;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, to_reverse);
    }

    __fq1 __fq2 bool reverse_lookup_from_node_id(uint32_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      gen::byte_str tail(ret_key, max_key_len);
      do {
        if (tail_lt[node_id]) {
          size_t prev_len = tail.length();
          tail_map->get_tail_str(node_id, tail);
          reverse_byte_str(tail.data() + prev_len, tail.length() - prev_len);
        } else {
          tail.append(trie_loc[node_id]);
        }
        if (!rev_cache.try_find(node_id, tail))
          node_id = child_lt.select1(term_lt.rank1(node_id)) - 1;
      } while (node_id != 0);
      if (to_reverse)
        reverse_byte_str(tail.data(), tail.length());
      *in_size_out_key_len = tail.length();
      return true;
    }

    __fq1 __fq2 void reverse_byte_str(uint8_t *str, size_t len) {
      size_t i = len / 2;
      while (i--) {
        uint8_t b = str[i];
        size_t im = len - i - 1;
        str[i] = str[im];
        str[im] = b;
      }
    }

    __fq1 __fq2 uint32_t leaf_rank1(uint32_t node_id) {
      return leaf_lt->rank1(node_id);
    }

    __fq1 __fq2 void push_to_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t node_id, uint32_t child_count) {
      ctx.cur_idx++;
      cv.tail.clear();
      update_ctx(ctx, cv, node_id, child_count);
    }

    __fq1 __fq2 void insert_arr(uint32_t *arr, int arr_len, int pos, uint32_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void insert_arr(uint16_t *arr, int arr_len, int pos, uint16_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void insert_into_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t node_id, uint32_t child_count) {
      insert_arr(ctx.child_count, ctx.cur_idx, 0, child_count);
      insert_arr(ctx.node_path, ctx.cur_idx, 0, node_id);
      insert_arr(ctx.last_tail_len, ctx.cur_idx, 0, (uint16_t) cv.tail.length());
      ctx.cur_idx++;
    }

    __fq1 __fq2 void update_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t node_id, uint32_t child_count) {
      ctx.child_count[ctx.cur_idx] = child_count;
      ctx.node_path[ctx.cur_idx] = node_id;
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = cv.tail.length();
      memcpy(ctx.key + ctx.key_len, cv.tail.data(), cv.tail.length());
      ctx.key_len += cv.tail.length();
    }

    __fq1 __fq2 void clear_last_tail(iter_ctx& ctx) {
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
    }

    __fq1 __fq2 uint32_t pop_from_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t& child_count) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      return read_from_ctx(ctx, cv, child_count);
    }

    __fq1 __fq2 uint32_t read_from_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t& child_count) {
      child_count = ctx.child_count[ctx.cur_idx];
      uint32_t node_id = ctx.node_path[ctx.cur_idx];
      return node_id;
    }

    __fq1 __fq2 int next(iter_ctx& ctx, uint8_t *key_buf) {
      ctx_vars_next cv;
      uint8_t *tail = new uint8_t[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      uint32_t child_count;
      uint32_t node_id = read_from_ctx(ctx, cv, child_count);
      while (node_id < node_count) {
        if (leaf_lt != nullptr && !(*leaf_lt)[node_id] && !child_lt[node_id]) {
          node_id++;
          continue;
        }
        if (leaf_lt == nullptr || (*leaf_lt)[node_id]) {
          if (ctx.to_skip_first_leaf) {
            if (!child_lt[node_id]) {
              while (term_lt[node_id]) {
                if (ctx.cur_idx == 0) {
                  delete [] tail;
                  return -2;
                }
                node_id = pop_from_ctx(ctx, cv, child_count);
              }
              node_id++;
              update_ctx(ctx, cv, node_id, child_count);
              ctx.to_skip_first_leaf = false;
              continue;
            }
          } else {
            cv.tail.clear();
            if (tail_lt[node_id])
              tail_map->get_tail_str(node_id, cv.tail);
            else
              cv.tail.append(trie_loc[node_id]);
            update_ctx(ctx, cv, node_id, child_count);
            memcpy(key_buf, ctx.key, ctx.key_len);
            ctx.to_skip_first_leaf = true;
            delete [] tail;
            return ctx.key_len;
          }
        }
        ctx.to_skip_first_leaf = false;
        if (child_lt[node_id]) {
          child_count++;
          cv.tail.clear();
          if (tail_lt[node_id])
            tail_map->get_tail_str(node_id, cv.tail);
          else
            cv.tail.append(trie_loc[node_id]);
          update_ctx(ctx, cv, node_id, child_count);
          node_id = term_lt.select1(child_count);
          child_count = child_lt.rank1(node_id);
          push_to_ctx(ctx, cv, node_id, child_count);
        }
      }
      delete [] tail;
      return -2;
    }

    __fq1 __fq2 uint32_t get_max_level() {
      return max_level;
    }

    __fq1 __fq2 uint32_t get_key_count() {
      return key_count;
    }

    __fq1 __fq2 uint32_t get_node_count() {
      return node_count;
    }

    __fq1 __fq2 uint32_t get_max_key_len() {
      return max_key_len;
    }

    __fq1 __fq2 uint32_t get_max_tail_len() {
      return max_tail_len;
    }

    // __fq1 __fq2 bool find_first(const uint8_t *prefix, int prefix_len, iter_ctx& ctx, bool for_next = false) {
    //   input_ctx in_ctx;
    //   in_ctx.key = prefix;
    //   in_ctx.key_len = prefix_len;
    //   bool is_found = lookup(in_ctx);
    //   if (!is_found && in_ctx.cmp == 0)
    //     in_ctx.cmp = 10000;
    //     uint8_t key_buf[max_key_len];
    //     int key_len;
    //     // TODO: set last_key_len
    //   ctx.cur_idx = 0;
    //   reverse_lookup_from_node_id(lkup_node_id, &key_len, key_buf, nullptr, nullptr, cmp, &ctx);
    //   ctx.cur_idx--;
    //     // for (int i = 0; i < ctx.key_len; i++)
    //     //   printf("%c", ctx.key[i]);
    //     // printf("\nlsat tail len: %d\n", ctx.last_tail_len[ctx.cur_idx]);
    //   if (for_next) {
    //     ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
    //     ctx.last_tail_len[ctx.cur_idx] = 0;
    //     ctx.to_skip_first_leaf = false;
    //   }
    //   return true;
    // }

    __fq1 __fq2 bvlt_select *get_leaf_lt() {
      return leaf_lt;
    }

    __fq1 __fq2 uint8_t *get_trie_loc() {
      return trie_loc;
    }

    __fq1 __fq2 uint8_t *get_trie_bytes() {
      return trie_bytes;
    }

    __fq1 __fq2 void load_static_trie(uint8_t *_trie_bytes = nullptr) {

printf("%x\n", _trie_bytes[0]);

      if (_trie_bytes != nullptr)
        trie_bytes = _trie_bytes;

      load_inner_trie(trie_bytes);
      opts = (bldr_options *) (trie_bytes + cmn::read_uint32(trie_bytes + 20));
      key_count = cmn::read_uint32(trie_bytes + 28);
      if (key_count > 0) {
        max_tail_len = cmn::read_uint16(trie_bytes + 40) + 1;

        uint32_t node_set_count = cmn::read_uint32(trie_bytes + 24);
        uint8_t *leaf_select_lkup_loc = trie_bytes + cmn::read_uint32(trie_bytes + 92);
        uint8_t *leaf_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 96);
        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 116));
        trie_flags_loc = (trie_flags *) tf_loc;

        if (leaf_select_lkup_loc == trie_bytes) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == trie_bytes) leaf_lt_loc = nullptr;
        if (opts->trie_leaf_count > 0) {
          leaf_lt = new bvlt_select();
          leaf_lt->init(leaf_lt_loc, leaf_select_lkup_loc, node_count, tf_loc + 3, 4, 1);
        }
      }

      if (key_count > 0) {
        max_key_len = cmn::read_uint32(trie_bytes + 32);
        max_level = cmn::read_uint16(trie_bytes + 42);
        uint32_t fwd_cache_count = cmn::read_uint32(trie_bytes + 44);
        uint32_t fwd_cache_max_node_id = cmn::read_uint32(trie_bytes + 52);
        uint8_t *fwd_cache_loc = trie_bytes + cmn::read_uint32(trie_bytes + 64);
        fwd_cache.init(fwd_cache_loc, fwd_cache_count, fwd_cache_max_node_id);

        min_pos_stats min_stats;
        memcpy(&min_stats, trie_bytes + 60, 4);
        uint8_t *min_pos_loc = trie_bytes + cmn::read_uint32(trie_bytes + 72);
        if (min_pos_loc == trie_bytes)
          min_pos_loc = nullptr;
        if (min_pos_loc != nullptr) {
          if (!opts->sort_nodes_on_freq) {
            leaper = new leapfrog_asc();
            ((leapfrog_asc *) leaper)->init(min_stats, min_pos_loc);
          }
        }

      }

    }

    __fq1 __fq2 static_trie() {
      init_vars();
      trie_level = 0;
    }

    __fq1 __fq2 void init_vars() {

      trie_bytes = nullptr;

      max_key_len = max_level = 0;
      leaper = nullptr;

      max_tail_len = 0;
      tail_map = nullptr;
      trie_loc = nullptr;
      leaf_lt = nullptr;

    }

    __fq1 __fq2 virtual ~static_trie() {
      if (leaper != nullptr)
        delete leaper;
      if (lt_not_given & BV_LT_TYPE_LEAF) {
        delete [] leaf_lt->get_rank_loc();
        delete [] leaf_lt->get_select_loc1();
      }
      if (leaf_lt != nullptr)
        delete leaf_lt;
    }

    __fq1 __fq2 void load(const char* filename) {

      init_vars();
      struct stat file_stat;
      memset(&file_stat, 0, sizeof(file_stat));
      stat(filename, &file_stat);
      trie_bytes = new uint8_t[file_stat.st_size];

      FILE *fp = fopen(filename, "rb");
      long bytes_read = fread(trie_bytes, 1, file_stat.st_size, fp);
      if (bytes_read != file_stat.st_size) {
        printf("Read error: [%s], %ld, %lu\n", filename, (long) file_stat.st_size, bytes_read);
        return;
      }
      fclose(fp);

      load_static_trie(trie_bytes);

    }

};

class cleanup_interface {
  public:
    __fq1 __fq2 virtual ~cleanup_interface() {
    }
    __fq1 __fq2 virtual void release() = 0;
};

class cleanup : public cleanup_interface {
  private:
    uint8_t *bytes;
  public:
    __fq1 __fq2 cleanup() {
    }
    __fq1 __fq2 virtual ~cleanup() {
    }
    __fq1 __fq2 void release() {
      delete [] bytes;
    }
    __fq1 __fq2 void init(uint8_t *_bytes) {
      bytes = _bytes;
    }
};

}
#endif

