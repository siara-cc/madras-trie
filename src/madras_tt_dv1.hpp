#ifndef STATIC_TRIE_TT_H
#define STATIC_TRIE_TT_H

#include <math.h>
#include <time.h>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstdlib>
#include <stdarg.h>
#include <sys/stat.h> 
#include <sys/types.h>
#include <immintrin.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif
#include <stdint.h>

#include "common_dv1.hpp"
#include "../../ds_common/src/gen.hpp"

namespace madras_tt_dv1 {

class iter_ctx {
  private:
    iter_ctx(iter_ctx const&);
    iter_ctx& operator=(iter_ctx const&);
  public:
    int32_t cur_idx;
    uint16_t key_len;
    uint8_t *key;
    uint32_t *node_path;
    uint32_t *child_count;
    uint16_t *last_tail_len;
    bool to_skip_first_leaf;
    bool is_allocated = false;
    iter_ctx() {
      is_allocated = false;
    }
    ~iter_ctx() {
      close();
    }
    void close() {
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

#define DCT_BIN '*'
#define DCT_TEXT 't'
#define DCT_WORDS 'w'
#define DCT_FLOAT 'f'
#define DCT_DOUBLE 'd'
#define DCT_S64_INT '0'
#define DCT_S64_DEC1 '1'
#define DCT_S64_DEC2 '2'
#define DCT_S64_DEC3 '3'
#define DCT_S64_DEC4 '4'
#define DCT_S64_DEC5 '5'
#define DCT_S64_DEC6 '6'
#define DCT_S64_DEC7 '7'
#define DCT_S64_DEC8 '8'
#define DCT_S64_DEC9 '9'
#define DCT_U64_INT 'i'
#define DCT_U64_DEC1 'j'
#define DCT_U64_DEC2 'k'
#define DCT_U64_DEC3 'l'
#define DCT_U64_DEC4 'm'
#define DCT_U64_DEC5 'n'
#define DCT_U64_DEC6 'o'
#define DCT_U64_DEC7 'p'
#define DCT_U64_DEC8 'q'
#define DCT_U64_DEC9 'r'
#define DCT_U15_DEC1 'X'
#define DCT_U15_DEC2 'Z'

struct min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
};

struct input_ctx {
  const uint8_t *key;
  uint32_t key_len;
  uint32_t key_pos;
  uint32_t node_id;
  int32_t cmp;
};

struct trie_flags_reader {
  uint64_t *flags_loc;
  bool operator[](size_t pos) {
    return (flags_loc[pos / 64] & ((uint64_t)1 << (pos % 64))) != 0;
  }
  void init(uint64_t *_flags_loc) {
    flags_loc = _flags_loc;
  }
};

const uint8_t bit_count[256] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
const uint8_t select_lookup_tbl[8][256] = {{
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

struct ctx_vars_next {
  public:
    gen::byte_str tail;
};

class statssski {
  private:
    statssski(statssski const&);
    statssski& operator=(statssski const&);
    uint8_t *min_pos_loc;
    min_pos_stats min_stats;
  public:
    void find_min_pos(uint32_t& node_id, uint8_t node_byte, uint8_t key_byte) {
        uint8_t min_offset = min_pos_loc[((node_byte - min_stats.min_len) * 256) + key_byte];
        node_id += min_offset;
    }
    statssski() {
    }
    void init(min_pos_stats _stats, uint8_t *_min_pos_loc) {
      min_stats = _stats;
      min_pos_loc = _min_pos_loc;
    }
};

class bv_lookup_tbl {
  private:
    bv_lookup_tbl(bv_lookup_tbl const&);
    bv_lookup_tbl& operator=(bv_lookup_tbl const&);
    uint8_t *lt_sel_loc1;
    uint8_t *lt_sel_loc0;
    uint8_t *lt_rank_loc;
    uint64_t *bm_loc;
    uint32_t bv_bit_count;
  public:
    bool operator[](size_t pos) {
      return (bm_loc[pos / 64] & ((uint64_t)1 << (pos % 64))) != 0;
    }
    uint32_t rank1(uint32_t bv_pos) {
      uint32_t rank = block_rank1(bv_pos);
      uint64_t bm = bm_loc[bv_pos / nodes_per_bv_block_n];
      uint64_t mask = bm_init_mask << (bv_pos % nodes_per_bv_block_n);
      // return rank + __popcountdi2(bm & (mask - 1));
      return rank + static_cast<uint32_t>(__builtin_popcountll(bm & (mask - 1)));
    }
    uint32_t block_rank1(uint32_t bv_pos) {
      uint8_t *rank_ptr = lt_rank_loc + bv_pos / nodes_per_bv_block * width_of_bv_block;
      uint32_t rank = gen::read_uint32(rank_ptr);
      int pos = (bv_pos / nodes_per_bv_block_n) % (width_of_bv_block_n + 1);
      if (pos > 0) {
        rank_ptr += 4;
        // pos--;
        // rank += rank_ptr[pos];
        while (pos--)
          rank += *rank_ptr++;
      }
      return rank;
    }
    uint32_t bin_srch_lkup_tbl(uint32_t first, uint32_t last, uint32_t given_count) {
      while (first + 1 < last) {
        const uint32_t middle = (first + last) >> 1;
        if (given_count < gen::read_uint32(lt_rank_loc + middle * width_of_bv_block))
          last = middle;
        else
          first = middle;
      }
      return first;
    }
    uint32_t select1(uint32_t target_count) {
      if (target_count == 0)
        return 0;
      uint32_t bv_pos = 0;
      uint32_t block_count = block_select(target_count, bv_pos, false);
      if (block_count == target_count)
        return bv_pos;
      return bv_pos + bm_select1(target_count - block_count - 1, bm_loc[bv_pos / nodes_per_bv_block_n]);
    }
    uint32_t bm_select1(uint32_t remaining, uint64_t bm) {

      uint64_t isolated_bit = _pdep_u64(1ULL << remaining, bm);
      size_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      if (bit_loc == 65) {
        printf("WARNING: UNEXPECTED bit_loc=65, bit_loc: %u\n", remaining);
        return 64;
      }

      // size_t bit_loc = 0;
      // while (bit_loc < 64) {
      //   uint8_t next_count = bit_count[(bm >> bit_loc) & 0xFF];
      //   if (block_count + next_count >= target_count)
      //     break;
      //   bit_loc += 8;
      //   block_count += next_count;
      // }
      // if (block_count < target_count)
      //   bit_loc += select_lookup_tbl[target_count - block_count - 1][(bm >> bit_loc) & 0xFF];

      // uint64_t bm_mask = bm_init_mask << bit_loc;
      // while (block_count < target_count) {
      //   if (bm & bm_mask)
      //     block_count++;
      //   bit_loc++;
      //   bm_mask <<= 1;
      // }
      return bit_loc;
    }
    uint32_t block_select(uint32_t target_count, uint32_t& bv_pos, bool sel0) {
      uint8_t *select_loc = (sel0 ? lt_sel_loc0 : lt_sel_loc1) + target_count / sel_divisor * 3;
      uint32_t block = gen::read_uint24(select_loc);
      // uint32_t end_block = gen::read_uint24(select_loc + 3);
      // if (block + 10 < end_block)
      //   block = bin_srch_lkup_tbl(block, end_block, target_count);
      while ((sel0 ? block * nodes_per_bv_block - 
          gen::read_uint32(lt_rank_loc + block * width_of_bv_block) :
          gen::read_uint32(lt_rank_loc + block * width_of_bv_block)) < target_count)
        block++;
      block--;
      uint32_t cur_count = gen::read_uint32(lt_rank_loc + block * width_of_bv_block);
      if (sel0)
        cur_count = block * nodes_per_bv_block - cur_count;
      bv_pos = block * nodes_per_bv_block;
      if (cur_count == target_count)
        return cur_count;
      uint8_t *bv3 = lt_rank_loc + block * width_of_bv_block + 4;
      int pos3;
      for (pos3 = 0; pos3 < width_of_bv_block_n && bv_pos + nodes_per_bv_block_n < bv_bit_count; pos3++) {
        uint8_t count3 = bv3[pos3];
        if (sel0)
          count3 = nodes_per_bv_block_n - count3;
        if (cur_count + count3 < target_count) {
          bv_pos += nodes_per_bv_block_n;
          cur_count += count3;
        } else
          break;
      }
      // if (pos3) {
      //   pos3--;
      //   cur_count += bv3[pos3];
      // }
      return cur_count;
    }
    uint32_t select0(uint32_t target_count) {
      if (target_count == 0)
        return 0;
      uint32_t bv_pos = 0;
      uint32_t block_count = block_select(target_count, bv_pos, true);
      if (block_count == target_count)
        return bv_pos;
      return bv_pos + bm_select1(target_count - block_count - 1, ~(bm_loc[bv_pos / nodes_per_bv_block_n]));
    }
    uint8_t *get_select_loc1() {
      return lt_sel_loc1;
    }
    uint8_t *get_select_loc0() {
      return lt_sel_loc0;
    }
    uint8_t *get_rank_loc() {
      return lt_rank_loc;
    }
    bv_lookup_tbl() {
    }
    void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc1, uint8_t *_lt_sel_loc0, uint32_t _bv_bit_count, uint64_t *_bm_start_loc) {
      lt_rank_loc = _lt_rank_loc;
      lt_sel_loc1 = _lt_sel_loc1;
      lt_sel_loc0 = _lt_sel_loc0;
      bv_bit_count = _bv_bit_count;
      bm_loc = (uint64_t *) _bm_start_loc;
    }

};

class inner_trie_fwd {
  private:
    inner_trie_fwd(inner_trie_fwd const&);
    inner_trie_fwd& operator=(inner_trie_fwd const&);
  public:
    bv_lookup_tbl *tail_lt;
    inner_trie_fwd() {
    }
    virtual ~inner_trie_fwd() {
    }
    virtual bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) = 0;
    virtual bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) = 0;
    virtual inner_trie_fwd *new_instance(uint8_t *mem) = 0;
};

class tail_ptr_map {
  private:
    tail_ptr_map(tail_ptr_map const&);
    tail_ptr_map& operator=(tail_ptr_map const&);
  public:
    tail_ptr_map() {
    }
    virtual ~tail_ptr_map() {
    }
    virtual bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) = 0;
    virtual void get_tail_str(uint32_t node_id, uint8_t node_byte, gen::byte_str& tail_str, uint32_t& ptr_bit_count) = 0;
    virtual void get_tail_str(gen::byte_str& tail_str, uint8_t grp_no, uint32_t tail_ptr) = 0;
};

class tail_ptr_flat_map : public tail_ptr_map {
  private:
    tail_ptr_flat_map(tail_ptr_flat_map const&);
    tail_ptr_flat_map& operator=(tail_ptr_flat_map const&);
  public:
    gen::int_bv_reader *int_ptr_bv;
    inner_trie_fwd *dict_obj;
    inner_trie_fwd *col_trie;
    uint8_t *trie_loc;
    uint8_t *data;
    tail_ptr_flat_map() {
      col_trie = nullptr;
      int_ptr_bv = nullptr;
    }
    virtual ~tail_ptr_flat_map() {
      if (col_trie != nullptr)
        delete col_trie;
      if (int_ptr_bv != nullptr)
        delete int_ptr_bv;
    }
    void init(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint8_t *tail_loc) {
      dict_obj = _dict_obj;
      trie_loc = _trie_loc;
      uint8_t encoding_type = tail_loc[2];
      uint8_t *data_loc = tail_loc + gen::read_uint32(tail_loc + 12);
      uint8_t *ptrs_loc = tail_loc + gen::read_uint32(tail_loc + 24);

      uint8_t ptr_lt_ptr_width = tail_loc[0];
      col_trie = nullptr;
      if (encoding_type == 't') {
        col_trie = dict_obj->new_instance(data_loc);
        int_ptr_bv = new gen::int_bv_reader();
        int_ptr_bv->init(ptrs_loc, ptr_lt_ptr_width);
      } else {
        uint8_t group_count = *data_loc;
        if (group_count == 1) {
          int_ptr_bv = new gen::int_bv_reader();
          int_ptr_bv->init(ptrs_loc, data_loc[1]);
        }
        uint8_t *data_idx_start = data_loc + 2 + 512;
          data = tail_loc;
          data += gen::read_uint32(data_idx_start);
      }
    }
    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count) {
      if (ptr_bit_count == UINT32_MAX)
        ptr_bit_count = dict_obj->tail_lt->rank1(node_id);
      uint32_t flat_ptr = (*int_ptr_bv)[ptr_bit_count++];
      flat_ptr <<= 8;
      flat_ptr |= node_byte;
      return flat_ptr;
    }
    bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) {
      uint32_t tail_ptr = get_tail_ptr(trie_loc[node_id], node_id, ptr_bit_count);
      if (col_trie != nullptr)
        return col_trie->compare_trie_tail(tail_ptr, in_ctx);
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
      }
      return false;
    }
    void get_tail_str(uint32_t node_id, uint8_t node_byte, gen::byte_str& tail_str, uint32_t& ptr_bit_count) {
      uint32_t tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count);
      get_tail_str(tail_str, 0, tail_ptr);
    }
    void get_tail_str(gen::byte_str& tail_str, uint8_t grp_no, uint32_t tail_ptr) {
      if (col_trie != nullptr) {
        col_trie->copy_trie_tail(tail_ptr, tail_str);
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
    GCFC_rev_cache(GCFC_rev_cache const&);
    GCFC_rev_cache& operator=(GCFC_rev_cache const&);
    madras_dv1::nid_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    bool try_find(uint32_t& node_id) {
      if (node_id >= max_node_id)
        return false;
      madras_dv1::nid_cache *cche = cche0 + (node_id & cache_mask);
      if (node_id == gen::read_uint24(&cche->child_node_id1)) {
        node_id = gen::read_uint24(&cche->parent_node_id1);
        return true;
      }
      return false;
    }
    GCFC_rev_cache() {
    }
    void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (madras_dv1::nid_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class inner_trie : public inner_trie_fwd {
  private:
    inner_trie(inner_trie const&);
    inner_trie& operator=(inner_trie const&);
  protected:
    uint8_t *dict_buf;
    uint32_t node_count;
    uint32_t node_set_count;
    uint8_t flags_width;
    uint8_t trie_level;
    uint8_t lt_not_given;
    uint8_t *trie_loc;
    trie_flags_reader tf_ptr;
    tail_ptr_map *tail_map;
    bv_lookup_tbl term_lt;
    bv_lookup_tbl child_lt;
    GCFC_rev_cache rev_cache;

  public:
    bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) {
      do {
        if (tf_ptr[node_id]) {
          uint32_t ptr_bit_count = UINT32_MAX;
          if (!tail_map->compare_tail(node_id, in_ctx, ptr_bit_count))
            return false;
        } else {
          if (in_ctx.key_pos >= in_ctx.key_len || trie_loc[node_id] != in_ctx.key[in_ctx.key_pos])
            return false;
          in_ctx.key_pos++;
        }
        if (!rev_cache.try_find(node_id))
          node_id = term_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) {
      do {
        if (tf_ptr[node_id]) {
          uint32_t ptr_bit_count = UINT32_MAX;
          tail_map->get_tail_str(node_id, trie_loc[node_id], tail_str, ptr_bit_count);
        } else
          tail_str.append(trie_loc[node_id]);
        if (!rev_cache.try_find(node_id))
          node_id = term_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    inner_trie(uint8_t _trie_level = 0) {
      trie_level = _trie_level;
    }
    inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      inner_trie *it = new inner_trie(trie_level + 1);
      it->dict_buf = mem;
      it->load_inner_trie();
      return it;
    }
    void load_inner_trie() {

      tail_map = nullptr;
      trie_loc = nullptr;
      tail_lt = nullptr;

      node_count = gen::read_uint32(dict_buf + 14);
      madras_dv1::bldr_options *opts = (madras_dv1::bldr_options *) (dict_buf + gen::read_uint32(dict_buf + 18));
      node_set_count = gen::read_uint32(dict_buf + 22);
      flags_width = opts->trie_leaf_count > 0 ? 32 : 24;
      uint32_t key_count = gen::read_uint32(dict_buf + 26);
      if (key_count > 0) {
        uint32_t rev_cache_count = gen::read_uint32(dict_buf + 46);
        uint32_t rev_cache_max_node_id = gen::read_uint32(dict_buf + 54);
        uint8_t *rev_cache_loc = dict_buf + gen::read_uint32(dict_buf + 66);
        rev_cache.init(rev_cache_loc, rev_cache_count, rev_cache_max_node_id);

        uint8_t *term_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 74);
        uint8_t *term_lt_loc = dict_buf + gen::read_uint32(dict_buf + 78);
        uint8_t *child_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 82);
        uint8_t *child_lt_loc = dict_buf + gen::read_uint32(dict_buf + 86);

        uint8_t *tail_lt_loc = dict_buf + gen::read_uint32(dict_buf + 98);
        uint8_t *trie_tail_ptrs_data_loc = dict_buf + gen::read_uint32(dict_buf + 102);

        uint32_t tail_size = gen::read_uint32(trie_tail_ptrs_data_loc);
        //uint32_t trie_flags_size = gen::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 8;
        trie_loc = tails_loc + tail_size;

        uint64_t *tf_child_loc = (uint64_t *) (dict_buf + gen::read_uint32(dict_buf + 106));
        uint64_t *tf_term_loc = (uint64_t *) (dict_buf + gen::read_uint32(dict_buf + 110));
        uint64_t *tf_ptr_loc = (uint64_t *) (dict_buf + gen::read_uint32(dict_buf + 114));
        tf_ptr.init(tf_ptr_loc);

        uint8_t encoding_type = tails_loc[2];
        uint8_t *tail_data_loc = tails_loc + gen::read_uint32(tails_loc + 12);
        if (encoding_type == 't' || *tail_data_loc == 1) {
          tail_ptr_flat_map *tail_flat_map = new tail_ptr_flat_map();  // Release where?
          tail_flat_map->init(this, trie_loc, tails_loc);
          tail_map = tail_flat_map;
        }

        lt_not_given = 0;
        if (term_select_lkup_loc == dict_buf) term_select_lkup_loc = nullptr;
        if (term_lt_loc == dict_buf) term_lt_loc = nullptr;
        if (child_select_lkup_loc == dict_buf) child_select_lkup_loc = nullptr;
        if (child_lt_loc == dict_buf) child_lt_loc = nullptr;
        if (tail_lt_loc == dict_buf) tail_lt_loc = nullptr;
        //child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, tf_child_loc);
        term_lt.init(term_lt_loc, term_select_lkup_loc, child_select_lkup_loc, node_count * 2, tf_term_loc);
        if (tail_lt_loc != nullptr) {
          // TODO: to build if dessicated?
          tail_lt = new bv_lookup_tbl();
          tail_lt->init(tail_lt_loc, nullptr, nullptr, node_count, tf_ptr_loc);
        }
      }

    }

    virtual ~inner_trie() {
      if (tail_map != nullptr) {
        delete tail_map;
      }
      if (lt_not_given & BV_LT_TYPE_CHILD) {
        delete [] child_lt.get_rank_loc();
        delete [] child_lt.get_select_loc1();
        delete [] child_lt.get_select_loc0();
      }
      if (lt_not_given & BV_LT_TYPE_TERM) {
        delete [] term_lt.get_rank_loc();
        delete [] term_lt.get_select_loc0();
        delete [] term_lt.get_select_loc0();
      }
      if (tail_lt != nullptr)
        delete tail_lt;
    }

};

class GCFC_fwd_cache {
  private:
    GCFC_fwd_cache(GCFC_fwd_cache const&);
    GCFC_fwd_cache& operator=(GCFC_fwd_cache const&);
    madras_dv1::fwd_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    int try_find(input_ctx& in_ctx) {
      if (in_ctx.node_id >= max_node_id)
        return -1;
      uint8_t key_byte = in_ctx.key[in_ctx.key_pos];
      do {
        uint32_t cache_idx = (in_ctx.node_id ^ (in_ctx.node_id << MDX_CACHE_SHIFT) ^ key_byte) & cache_mask;
        madras_dv1::fwd_cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = gen::read_uint24(&cche->parent_node_id1);
        if (in_ctx.node_id == cache_node_id) {
          in_ctx.key_pos++;
          in_ctx.node_id = gen::read_uint24(&cche->child_node_id1);
          if (in_ctx.key_pos < in_ctx.key_len) {
            key_byte = in_ctx.key[in_ctx.key_pos];
            continue;
          }
          return 0;
        }
        return -1;
      } while (1);
      return -1;
    }
    GCFC_fwd_cache() {
    }
    void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (madras_dv1::fwd_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class static_trie : public inner_trie {
  protected:
    bv_lookup_tbl *leaf_lt;
    trie_flags_reader tf_leaf;
    GCFC_fwd_cache fwd_cache;
    statssski *sski;
    uint16_t max_tail_len;
    uint32_t key_count;

  private:
    static_trie(static_trie const&);
    static_trie& operator=(static_trie const&);
    size_t max_key_len;
    madras_dv1::bldr_options *opts;
    uint16_t max_level;
  public:
    bool lookup(input_ctx& in_ctx) {
      in_ctx.key_pos = 0;
      in_ctx.node_id = 0;
      do {
        int ret = fwd_cache.try_find(in_ctx);
        if (ret == 0)
          return tf_leaf[in_ctx.node_id];
        uint32_t louds_pos = term_lt.select0(in_ctx.node_id + 1);
        // TODO: check if no child
        in_ctx.node_id = louds_pos - in_ctx.node_id - 1;
        if (sski != nullptr) {
          if (!tf_leaf[in_ctx.node_id] && !child_lt[in_ctx.node_id])
            sski->find_min_pos(in_ctx.node_id, trie_loc[in_ctx.node_id], in_ctx.key[in_ctx.key_pos]);
        }
        uint32_t ptr_bit_count = UINT32_MAX;
        do {
          if (!tf_ptr[in_ctx.node_id]) {
            #ifndef MDX_IN_ORDER
              if (in_ctx.key[in_ctx.key_pos] == trie_loc[in_ctx.node_id]) {
            #else
              if (in_ctx.key[in_ctx.key_pos] <= trie_loc[in_ctx.node_id]) {
            #endif
                in_ctx.key_pos++;
                break;
              }
          } else {
            uint32_t prev_key_pos = in_ctx.key_pos;
            if (tail_map->compare_tail(in_ctx.node_id, in_ctx, ptr_bit_count))
              break;
            if (prev_key_pos != in_ctx.key_pos)
              return false;
          }
          in_ctx.node_id++;
          louds_pos++;
        } while (term_lt[louds_pos]);
        if (in_ctx.key_pos == in_ctx.key_len && tf_leaf[in_ctx.node_id])
          return true;
      } while (in_ctx.node_id < node_count);
      return false;
    }

    bool reverse_lookup(uint32_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      leaf_id++;
      uint32_t node_id = leaf_lt->select1(leaf_id) - 1;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, to_reverse);
    }

    bool reverse_lookup_from_node_id(uint32_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = false) {
      uint8_t tail_buf[max_tail_len + 1];
      gen::byte_str tail(tail_buf, max_tail_len);
      int key_len = 0;
      do {
        if (tf_ptr[node_id]) {
          uint32_t ptr_bit_count = UINT32_MAX;
          tail.clear();
          tail_map->get_tail_str(node_id, trie_loc[node_id], tail, ptr_bit_count);
          if (trie_level == 0) {
            int i = tail.length() - 1;
            while (i >= 0)
              ret_key[key_len++] = tail[i--];
          } else {
            memcpy(ret_key + key_len, tail.data(), tail.length());
            key_len += tail.length();
          }
        } else {
          ret_key[key_len++] = trie_loc[node_id];
        }
        if (!rev_cache.try_find(node_id))
          node_id = term_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      if (to_reverse) {
        int i = key_len / 2;
        while (i--) {
          uint8_t b = ret_key[i];
          int im = key_len - i - 1;
          ret_key[i] = ret_key[im];
          ret_key[im] = b;
        }
      }
      *in_size_out_key_len = key_len;
      return true;
    }

    uint32_t leaf_rank1(uint32_t node_id) {
      return leaf_lt->rank1(node_id);
    }

    uint32_t get_max_level() {
      return max_level;
    }

    uint32_t get_max_key_len() {
      return max_key_len;
    }

    uint32_t get_max_val_len() {
      return max_key_len;
    }

    uint32_t get_max_tail_len() {
      return max_tail_len;
    }

    // bool find_first(const uint8_t *prefix, int prefix_len, iter_ctx& ctx, bool for_next = false) {
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

    uint8_t *get_trie_loc() {
      return trie_loc;
    }

    void load_static_trie() {

      load_inner_trie();
      opts = (madras_dv1::bldr_options *) (dict_buf + gen::read_uint32(dict_buf + 18));
      flags_width = opts->trie_leaf_count > 0 ? 32 : 24;
      key_count = gen::read_uint32(dict_buf + 26);
      if (key_count > 0) {
        max_tail_len = gen::read_uint16(dict_buf + 38) + 1;

        uint8_t *leaf_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 90);
        uint8_t *leaf_lt_loc = dict_buf + gen::read_uint32(dict_buf + 94);
        uint64_t *tf_leaf_loc = (uint64_t *) (dict_buf + gen::read_uint32(dict_buf + 118));

        if (leaf_select_lkup_loc == dict_buf) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == dict_buf) leaf_lt_loc = nullptr;
        if (opts->trie_leaf_count > 0) {
          leaf_lt = new bv_lookup_tbl();
          leaf_lt->init(leaf_lt_loc, leaf_select_lkup_loc, nullptr, node_count, tf_leaf_loc);
          tf_leaf.init(tf_leaf_loc);
        }
      }

      if (key_count > 0) {
        max_key_len = gen::read_uint32(dict_buf + 30);
        max_level = gen::read_uint16(dict_buf + 40);
        uint32_t fwd_cache_count = gen::read_uint32(dict_buf + 42);
        uint32_t fwd_cache_max_node_id = gen::read_uint32(dict_buf + 50);
        uint8_t *fwd_cache_loc = dict_buf + gen::read_uint32(dict_buf + 62);
        fwd_cache.init(fwd_cache_loc, fwd_cache_count, fwd_cache_max_node_id);

        min_pos_stats min_stats;
        memcpy(&min_stats, dict_buf + 58, 4);
        uint8_t *min_pos_loc = dict_buf + gen::read_uint32(dict_buf + 70);
        if (min_pos_loc == dict_buf)
          min_pos_loc = nullptr;
        if (min_pos_loc != nullptr) {
          sski = new statssski();
          sski->init(min_stats, min_pos_loc);
        }

      }

    }

    static_trie() {
      init_vars();
      trie_level = 0;
    }

    void init_vars() {

      dict_buf = nullptr;

      max_key_len = max_level = 0;
      sski = nullptr;

      max_tail_len = 0;
      tail_map = nullptr;
      trie_loc = nullptr;
      tail_lt = nullptr;
      leaf_lt = nullptr;

    }

    virtual ~static_trie() {
      if (sski != nullptr)
        delete sski;
      if (lt_not_given & BV_LT_TYPE_LEAF) {
        delete [] leaf_lt->get_rank_loc();
        delete [] leaf_lt->get_select_loc1();
      }
      if (leaf_lt != nullptr)
        delete leaf_lt;
    }

    void load(const char* filename) {

      init_vars();
      struct stat file_stat;
      memset(&file_stat, 0, sizeof(file_stat));
      stat(filename, &file_stat);
      dict_buf = (uint8_t *) malloc(file_stat.st_size);

      FILE *fp = fopen(filename, "rb");
      long bytes_read = fread(dict_buf, 1, file_stat.st_size, fp);
      if (bytes_read != file_stat.st_size) {
        printf("Read error: [%s], %ld, %lu\n", filename, (long) file_stat.st_size, bytes_read);
        throw errno;
      }
      fclose(fp);

      // int len_will_need = (dict_size >> 1);
      // #ifndef _WIN32
      //       mlock(dict_buf, len_will_need);
      // #endif
      //madvise(dict_buf, len_will_need, MADV_WILLNEED);
      //madvise(dict_buf + len_will_need, dict_size - len_will_need, MADV_RANDOM);

      load_static_trie();

    }

};

}
#endif
