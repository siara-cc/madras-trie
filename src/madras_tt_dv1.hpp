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

typedef struct {
  uint64_t bm_child;
  uint64_t bm_term;
  uint64_t bm_ptr;
  uint64_t bm_leaf;
} trie_flags;

struct input_ctx {
  const uint8_t *key;
  uint32_t key_len;
  uint32_t key_pos;
  uint32_t node_id;
  int32_t cmp;
};

struct ctx_vars {
  const uint8_t *tf0;
  const trie_flags *tf;
  uint64_t bm_mask;
  uint32_t ptr_bit_count;
  uint8_t tf_multiplier;
  void update_tf(uint32_t node_id) {
    tf = (trie_flags *) (tf0 + node_id / nodes_per_bv_block_n * tf_multiplier);
    bm_mask = bm_init_mask << (node_id % nodes_per_bv_block_n);
  }
  void next_block() {
    if (bm_mask == 0) {
      bm_mask = bm_init_mask;
      tf = (trie_flags *) (((const uint8_t *) tf) + tf_multiplier);
    }
  }
  bool is_leaf_set() {
    return (bm_mask & tf->bm_leaf);
  }
  bool is_child_set() {
    return (bm_mask & tf->bm_child);
  }
  bool is_term_set() {
    return (bm_mask & tf->bm_term);
  }
  bool is_ptr_set() {
    return (bm_mask & tf->bm_ptr);
  }
  ctx_vars() {
  }
  ctx_vars(trie_flags *_tf, uint8_t _tf_multiplier) {
    tf0 = (const uint8_t *) _tf;
    tf_multiplier = _tf_multiplier;
  }
};

struct ctx_vars_next : public ctx_vars {
  public:
    ctx_vars_next(trie_flags *_tf, uint8_t _tf_multiplier) : ctx_vars (_tf, _tf_multiplier) {
    }
    gen::byte_str tail;
};

class statssski {
  private:
    uint8_t *min_pos_loc;
    min_pos_stats min_stats;
  public:
    void find_min_pos(uint32_t& node_id, uint8_t node_byte, uint8_t key_byte, ctx_vars& cv) {
      if ((cv.bm_mask & (cv.tf->bm_child | cv.tf->bm_leaf)) == 0) {
        uint8_t min_offset = min_pos_loc[((node_byte - min_stats.min_len) * 256) + key_byte];
        if ((node_id % nodes_per_bv_block_n) + min_offset < nodes_per_bv_block_n) {
          node_id += min_offset;
          cv.bm_mask <<= min_offset;
        } else {
          node_id += min_offset;
          cv.update_tf(node_id);
        }
      }
    }
    void init(min_pos_stats _stats, uint8_t *_min_pos_loc) {
      min_stats = _stats;
      min_pos_loc = _min_pos_loc;
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

class bv_lookup_tbl {
  private:
    uint8_t *lt_sel_loc;
    uint8_t *lt_rank_loc;
    uint8_t *bm_start_loc;
    size_t bm_multiplier;
    size_t bm_pos;
    uint32_t node_count;
  public:
    uint32_t rank(uint32_t node_id) {
      uint32_t rank = block_rank(node_id);
      uint8_t *t = bm_start_loc + node_id / nodes_per_bv_block_n * bm_multiplier;
      uint64_t bm = gen::read_uint64(t + bm_pos);
      uint64_t mask = bm_init_mask << (node_id % nodes_per_bv_block_n);
      // return rank + __popcountdi2(bm & (mask - 1));
      return rank + static_cast<uint32_t>(__builtin_popcountll(bm & (mask - 1)));
    }
    uint32_t block_rank(uint32_t node_id) {
      uint8_t *rank_ptr = lt_rank_loc + node_id / nodes_per_bv_block * width_of_bv_block;
      uint32_t rank = gen::read_uint32(rank_ptr);
      int pos = (node_id / nodes_per_bv_block_n) % (width_of_bv_block_n + 1);
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
    uint32_t block_select(uint32_t target_count, uint32_t& node_id) {
      uint8_t *select_loc = lt_sel_loc + target_count / sel_divisor * 3;
      uint32_t block = gen::read_uint24(select_loc);
      // uint32_t end_block = gen::read_uint24(select_loc + 3);
      // if (block + 10 < end_block)
      //   block = bin_srch_lkup_tbl(block, end_block, target_count);
      while (gen::read_uint32(lt_rank_loc + block * width_of_bv_block) < target_count)
        block++;
      block--;
      uint32_t cur_count = gen::read_uint32(lt_rank_loc + block * width_of_bv_block);
      node_id = block * nodes_per_bv_block;
      if (cur_count == target_count)
        return cur_count;
      uint8_t *bv3 = lt_rank_loc + block * width_of_bv_block + 4;
      int pos3;
      for (pos3 = 0; pos3 < width_of_bv_block_n && node_id + nodes_per_bv_block_n < node_count; pos3++) {
        uint8_t count3 = bv3[pos3];
        if (cur_count + count3 < target_count) {
          node_id += nodes_per_bv_block_n;
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
    void select(uint32_t& node_id, uint32_t target_count) {
      if (target_count == 0) {
        node_id = 0;
        return;
      }
      uint32_t block_count = block_select(target_count, node_id);
      if (block_count == target_count)
        return;
      uint8_t *t = bm_start_loc + node_id / nodes_per_bv_block_n * bm_multiplier;
      uint64_t bm = gen::read_uint64(t + bm_pos);

      uint32_t remaining = target_count - block_count - 1;
      uint64_t isolated_bit = _pdep_u64(1ULL << remaining, bm);
      size_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      // size_t bit_loc = find_nth_set_bit(bm, i) + 1;
      if (bit_loc == 65) {
        printf("WARNING: UNEXPECTED bit_loc=65, node_id: %u, nc: %u, bc: %u, ttc: %u\n", node_id, node_count, block_count, target_count);
        return;
      }
      node_id += bit_loc;

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
      // node_id += bit_loc;

      // uint64_t bm_mask = bm_init_mask << bit_loc;
      // while (block_count < target_count) {
      //   if (bm & bm_mask)
      //     block_count++;
      //   bit_loc++;
      //   bm_mask <<= 1;
      // }
      // node_id += bit_loc;
    }
    uint8_t *get_select_loc() {
      return lt_sel_loc;
    }
    uint8_t *get_rank_loc() {
      return lt_rank_loc;
    }
    void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc, uint32_t _node_count, trie_flags *_bm_start_loc, size_t _bm_multiplier, size_t _bm_pos) {
      lt_rank_loc = _lt_rank_loc;
      lt_sel_loc = _lt_sel_loc;
      node_count = _node_count;
      bm_start_loc = (uint8_t *) _bm_start_loc;
      bm_multiplier = _bm_multiplier;
      bm_pos = _bm_pos;
    }

};

class inner_trie_fwd {
  public:
    uint32_t key_count;
    uint16_t max_tail_len;
    bv_lookup_tbl *tail_lt;
    virtual ~inner_trie_fwd() {
    }
    virtual bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) = 0;
    virtual uint32_t leaf_rank(uint32_t node_id) = 0;
    virtual bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) = 0;
    virtual bool reverse_lookup(uint32_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) = 0;
    virtual bool reverse_lookup_from_node_id(uint32_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = false) = 0;
    virtual inner_trie_fwd *new_instance(uint8_t *mem) = 0;
};

class tail_ptr_map {
  public:
    virtual ~tail_ptr_map() {
    }
    virtual bool compare_tail(uint32_t node_id, input_ctx& in_ctx, ctx_vars& cv) = 0;
    virtual void get_tail_str(uint32_t node_id, uint8_t node_byte, gen::byte_str& tail_str, ctx_vars& cv) = 0;
    virtual void get_tail_str(gen::byte_str& tail_str, uint8_t grp_no, uint32_t tail_ptr) = 0;
};

class tail_ptr_flat_map : public tail_ptr_map {
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
    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, ctx_vars& cv) {
      if (cv.ptr_bit_count == UINT32_MAX)
        cv.ptr_bit_count = dict_obj->tail_lt->rank(node_id);
      uint32_t flat_ptr = (*int_ptr_bv)[cv.ptr_bit_count++];
      flat_ptr <<= 8;
      flat_ptr |= node_byte;
      return flat_ptr;
    }
    bool compare_tail(uint32_t node_id, input_ctx& in_ctx, ctx_vars& cv) {
      uint32_t tail_ptr = get_tail_ptr(trie_loc[node_id], node_id, cv);
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
    void get_tail_str(uint32_t node_id, uint8_t node_byte, gen::byte_str& tail_str, ctx_vars& cv) {
      uint32_t tail_ptr = get_tail_ptr(node_byte, node_id, cv);
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

class GCFC_fwd_cache {
  private:
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
        if (in_ctx.node_id == cache_node_id && cche->node_byte == key_byte) {
          in_ctx.key_pos++;
          if (in_ctx.key_pos < in_ctx.key_len) {
            in_ctx.node_id = gen::read_uint24(&cche->child_node_id1);
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
    void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (madras_dv1::fwd_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class GCFC_rev_cache {
  private:
    madras_dv1::nid_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    int try_find(uint32_t& node_id) {
      if (node_id >= max_node_id)
        return -1;
      madras_dv1::nid_cache *cche = cche0 + (node_id & cache_mask);
      if (node_id == gen::read_uint24(&cche->child_node_id1)) {
        node_id = gen::read_uint24(&cche->parent_node_id1);
        return 0;
      }
      return -1;
    }
    void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (madras_dv1::nid_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class static_trie_map : public inner_trie_fwd {

  protected:
    uint32_t node_set_count;
    tail_ptr_map *tail_map;
    bv_lookup_tbl *leaf_lt;
    GCFC_fwd_cache fwd_cache;
    bv_lookup_tbl term_lt;
    bv_lookup_tbl child_lt;
    GCFC_rev_cache rev_cache;
    uint32_t node_count;
    trie_flags *trie_flags_loc;
    uint8_t flags_width;
    uint8_t trie_level;
    uint8_t lt_not_given;
    uint8_t *trie_loc;

  private:
    statssski *sski;
    size_t max_key_len;
    size_t max_val_len;
    madras_dv1::bldr_options *opts;
    uint8_t *dict_buf;
    uint32_t val_count;
    uint8_t *names_loc;
    char *names_start;
    const char *column_encoding;
    bool is_mmapped;
    bool to_release_dict_buf;
    uint16_t max_level;

    static_trie_map(static_trie_map const&);
    static_trie_map& operator=(static_trie_map const&);

  public:
    static_trie_map(uint8_t _trie_level = 0) {
      init_vars();
      trie_level = _trie_level;
    }

    bool reverse_lookup(uint32_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      leaf_id++;
      uint32_t node_id;
      leaf_lt->select(node_id, leaf_id);
      node_id--;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, to_reverse);
    }

    bool reverse_lookup_from_node_id(uint32_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = false) {
      ctx_vars_next cv(trie_flags_loc, flags_width);
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      int key_len = 0;
      do {
        cv.update_tf(node_id);
        if (cv.bm_mask & cv.tf->bm_ptr) {
          cv.ptr_bit_count = UINT32_MAX;
          cv.tail.clear();
          tail_map->get_tail_str(node_id, trie_loc[node_id], cv.tail, cv);
          if (trie_level == 0) {
            int i = cv.tail.length() - 1;
            while (i >= 0)
              ret_key[key_len++] = cv.tail[i--];
          } else {
            memcpy(ret_key + key_len, cv.tail.data(), cv.tail.length());
            key_len += cv.tail.length();
          }
        } else {
          ret_key[key_len++] = trie_loc[node_id];
        }
        if (rev_cache.try_find(node_id) == -1) {
          child_lt.select(node_id, term_lt.rank(node_id));
          node_id--;
        }
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

    inner_trie_fwd *new_instance(uint8_t *mem) {
      static_trie_map *it = new static_trie_map(trie_level + 1);
      it->load_from_mem(mem);
      return it;
    }

    uint32_t leaf_rank(uint32_t node_id) {
      return leaf_lt->rank(node_id);
    }

    bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) {
      ctx_vars cv(trie_flags_loc, flags_width);
      do {
        cv.update_tf(node_id);
        if (cv.bm_mask & cv.tf->bm_ptr) {
          cv.ptr_bit_count = UINT32_MAX;
          tail_map->get_tail_str(node_id, trie_loc[node_id], tail_str, cv);
        } else
          tail_str.append(trie_loc[node_id]);
        if (rev_cache.try_find(node_id) == -1) {
          child_lt.select(node_id, term_lt.rank(node_id));
          node_id--;
        }
      } while (node_id != 0);
      return true;
    }

    bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) {
      ctx_vars cv(trie_flags_loc, flags_width);
      do {
        cv.update_tf(node_id);
        if (cv.is_ptr_set()) {
          cv.ptr_bit_count = UINT32_MAX;
          if (!tail_map->compare_tail(node_id, in_ctx, cv))
            return false;
        } else {
          if (in_ctx.key_pos >= in_ctx.key_len || trie_loc[node_id] != in_ctx.key[in_ctx.key_pos])
            return false;
          in_ctx.key_pos++;
        }
        if (rev_cache.try_find(node_id) == -1) {
          child_lt.select(node_id, term_lt.rank(node_id));
          node_id--;
        }
      } while (node_id != 0);
      return true;
    }

    bool lookup(input_ctx& in_ctx) {
      in_ctx.key_pos = 0;
      in_ctx.node_id = 1;
      ctx_vars cv(trie_flags_loc, flags_width);
      do {
        int ret = fwd_cache.try_find(in_ctx);
        cv.update_tf(in_ctx.node_id);
        if (ret == 0)
          return cv.is_leaf_set();
        if (sski != nullptr)
          sski->find_min_pos(in_ctx.node_id, trie_loc[in_ctx.node_id], in_ctx.key[in_ctx.key_pos], cv);
        cv.ptr_bit_count = UINT32_MAX;
        do {
          if (!cv.is_ptr_set()) {
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
            if (tail_map->compare_tail(in_ctx.node_id, in_ctx, cv))
              break;
            if (prev_key_pos != in_ctx.key_pos)
              return false;
          }
          if (cv.is_term_set())
            return false;
          in_ctx.node_id++;
          cv.bm_mask <<= 1;
          cv.next_block();
        } while (1);
        if (in_ctx.key_pos == in_ctx.key_len && cv.is_leaf_set())
          return true;
        if (!cv.is_child_set())
          return false;
        term_lt.select(in_ctx.node_id, child_lt.rank(in_ctx.node_id) + 1);
      } while (in_ctx.node_id < node_count);
      return false;
    }

    bool get(input_ctx& in_ctx, size_t *in_size_out_value_len, void *val) {
      bool is_found = lookup(in_ctx);
      if (is_found) {
        return true;
      }
      return false;
    }

    void push_to_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t node_id, uint32_t child_count) {
      ctx.cur_idx++;
      cv.tail.clear();
      update_ctx(ctx, cv, node_id, child_count);
    }

    void insert_arr(uint32_t *arr, int arr_len, int pos, uint32_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    void insert_arr(uint16_t *arr, int arr_len, int pos, uint16_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    void insert_into_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t node_id, uint32_t child_count) {
      insert_arr(ctx.child_count, ctx.cur_idx, 0, child_count);
      insert_arr(ctx.node_path, ctx.cur_idx, 0, node_id);
      insert_arr(ctx.last_tail_len, ctx.cur_idx, 0, (uint16_t) cv.tail.length());
      ctx.cur_idx++;
    }

    void update_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t node_id, uint32_t child_count) {
      ctx.child_count[ctx.cur_idx] = child_count;
      ctx.node_path[ctx.cur_idx] = node_id;
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = cv.tail.length();
      memcpy(ctx.key + ctx.key_len, cv.tail.data(), cv.tail.length());
      ctx.key_len += cv.tail.length();
    }

    void clear_last_tail(iter_ctx& ctx) {
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
    }

    uint32_t pop_from_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t& child_count) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      return read_from_ctx(ctx, cv, child_count);
    }

    uint32_t read_from_ctx(iter_ctx& ctx, ctx_vars_next& cv, uint32_t& child_count) {
      child_count = ctx.child_count[ctx.cur_idx];
      uint32_t node_id = ctx.node_path[ctx.cur_idx];
      cv.update_tf(node_id);
      return node_id;
    }

    int next(iter_ctx& ctx, uint8_t *key_buf, uint8_t *val_buf = nullptr, size_t *val_buf_len = nullptr) {
      ctx_vars_next cv(trie_flags_loc, flags_width);
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      uint32_t child_count;
      uint32_t node_id = read_from_ctx(ctx, cv, child_count);
      while (node_id < node_count) {
        if ((cv.bm_mask & cv.tf->bm_child) == 0 && (cv.bm_mask & cv.tf->bm_leaf) == 0) {
          node_id++;
          cv.bm_mask <<= 1;
          cv.next_block();
          continue;
        }
        if (cv.bm_mask & cv.tf->bm_leaf) {
          if (ctx.to_skip_first_leaf) {
            if ((cv.bm_mask & cv.tf->bm_child) == 0) {
              while (cv.bm_mask & cv.tf->bm_term) {
                if (ctx.cur_idx == 0)
                  return -2;
                node_id = pop_from_ctx(ctx, cv, child_count);
              }
              node_id++;
              cv.bm_mask <<= 1;
              update_ctx(ctx, cv, node_id, child_count);
              ctx.to_skip_first_leaf = false;
              cv.next_block();
              continue;
            }
          } else {
            cv.tail.clear();
            cv.ptr_bit_count = UINT32_MAX;
            if (cv.bm_mask & cv.tf->bm_ptr)
              tail_map->get_tail_str(node_id, trie_loc[node_id], cv.tail, cv);
            else
              cv.tail.append(trie_loc[node_id]);
            update_ctx(ctx, cv, node_id, child_count);
            memcpy(key_buf, ctx.key, ctx.key_len);
            ctx.to_skip_first_leaf = true;
            return ctx.key_len;
          }
        }
        ctx.to_skip_first_leaf = false;
        if (cv.bm_mask & cv.tf->bm_child) {
          child_count++;
          cv.tail.clear();
          cv.ptr_bit_count = UINT32_MAX;
          if (cv.bm_mask & cv.tf->bm_ptr)
            tail_map->get_tail_str(node_id, trie_loc[node_id], cv.tail, cv);
          else
            cv.tail.append(trie_loc[node_id]);
          update_ctx(ctx, cv, node_id, child_count);
          term_lt.select(node_id, child_count);
          child_count = child_lt.rank(node_id);
          cv.update_tf(node_id);
          cv.ptr_bit_count = UINT32_MAX;
          push_to_ctx(ctx, cv, node_id, child_count);
        }
      }
      return -2;
    }

    uint32_t get_max_level() {
      return max_level;
    }

    uint32_t get_max_key_len() {
      return max_key_len;
    }

    uint32_t get_max_tail_len() {
      return max_tail_len;
    }

    uint32_t get_max_val_len() {
      return max_val_len;
    }

    const char *get_table_name() {
      return names_start + gen::read_uint16(names_loc + 2);
    }

    const char *get_column_name(int i) {
      return names_start + gen::read_uint16(names_loc + (i + 2) * 2);
    }

    const char *get_column_types() {
      return names_start;
    }

    char get_column_type(int i) {
      return names_start[i];
    }

    const char *get_column_encodings() {
      return column_encoding;
    }

    char get_column_encoding(int i) {
      return column_encoding[i];
    }

    uint16_t get_column_count() {
      return val_count + (key_count > 0 ? 1 : 0);
    }

    uint8_t *get_trie_loc() {
      return trie_loc;
    }

    int64_t get_val_int60(uint8_t *val) {
      return gen::read_svint60(val);
    }

    double get_val_int60_dbl(uint8_t *val, char type) {
      int64_t i64 = gen::read_svint60(val);
      double ret = static_cast<double>(i64);
      ret /= gen::pow10(type - DCT_S64_DEC1 + 1);
      return ret;
    }

    uint64_t get_val_int61(uint8_t *val) {
      return gen::read_svint61(val);
    }

    double get_val_int61_dbl(uint8_t *val, char type) {
      uint64_t i64 = gen::read_svint61(val);
      double ret = static_cast<double>(i64);
      ret /= gen::pow10(type - DCT_U64_DEC1 + 1);
      return ret;
    }

    uint64_t get_val_int15(uint8_t *val) {
      return gen::read_svint15(val);
    }

    double get_val_int15_dbl(uint8_t *val, char type) {
      uint64_t i64 = gen::read_svint15(val);
      double ret = static_cast<double>(i64);
      ret /= gen::pow10(type - DCT_U15_DEC1 + 1);
      return ret;
    }

    void map_from_memory(uint8_t *mem) {
      load_from_mem(mem);
    }

    void load_into_vars() {

      node_count = gen::read_uint32(dict_buf + 14);
      node_set_count = gen::read_uint32(dict_buf + 22);
      madras_dv1::bldr_options *opts = (madras_dv1::bldr_options *) (dict_buf + gen::read_uint32(dict_buf + 18));
      flags_width = opts->trie_leaf_count > 0 ? 32 : 24;
      key_count = gen::read_uint32(dict_buf + 26);
      if (key_count > 0) {
        max_tail_len = gen::read_uint16(dict_buf + 38) + 1;
        uint32_t rev_cache_count = gen::read_uint32(dict_buf + 46);
        uint32_t rev_cache_max_node_id = gen::read_uint32(dict_buf + 54);
        uint8_t *rev_cache_loc = dict_buf + gen::read_uint32(dict_buf + 66);
        rev_cache.init(rev_cache_loc, rev_cache_count, rev_cache_max_node_id);

        uint8_t *term_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 74);
        uint8_t *term_lt_loc = dict_buf + gen::read_uint32(dict_buf + 78);
        uint8_t *child_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 82);
        uint8_t *child_lt_loc = dict_buf + gen::read_uint32(dict_buf + 86);
        uint8_t *leaf_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 90);
        uint8_t *leaf_lt_loc = dict_buf + gen::read_uint32(dict_buf + 94);

        uint8_t *tail_lt_loc = dict_buf + gen::read_uint32(dict_buf + 98);
        uint8_t *trie_tail_ptrs_data_loc = dict_buf + gen::read_uint32(dict_buf + 102);

        uint32_t tail_size = gen::read_uint32(trie_tail_ptrs_data_loc);
        uint32_t trie_flags_size = gen::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 8;
        trie_flags_loc = (trie_flags *) (tails_loc + tail_size);
        trie_loc = ((uint8_t *) trie_flags_loc) + trie_flags_size;

        uint8_t encoding_type = tails_loc[2];
        uint8_t *tail_data_loc = tails_loc + gen::read_uint32(tails_loc + 12);
        tail_ptr_flat_map *tail_flat_map = new tail_ptr_flat_map();
        tail_flat_map->init(this, trie_loc, tails_loc);
        tail_map = tail_flat_map;

        lt_not_given = 0;
        if (leaf_select_lkup_loc == dict_buf) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == dict_buf) leaf_lt_loc = nullptr;
        if (term_select_lkup_loc == dict_buf) term_select_lkup_loc = nullptr;
        if (term_lt_loc == dict_buf) term_lt_loc = nullptr;
        if (child_select_lkup_loc == dict_buf) child_select_lkup_loc = nullptr;
        if (child_lt_loc == dict_buf) child_lt_loc = nullptr;
        if (tail_lt_loc == dict_buf) tail_lt_loc = nullptr;
        child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, trie_flags_loc, flags_width, 0);
        term_lt.init(term_lt_loc,   term_select_lkup_loc,  node_count, trie_flags_loc, flags_width, 8);
        if (tail_lt_loc != nullptr) {
          // TODO: to build if dessicated?
          tail_lt = new bv_lookup_tbl();
          tail_lt->init(tail_lt_loc, nullptr, node_count, trie_flags_loc, flags_width, 16);
        }
        if (opts->trie_leaf_count > 0) {
          leaf_lt = new bv_lookup_tbl();
          leaf_lt->init(leaf_lt_loc, leaf_select_lkup_loc, node_count, trie_flags_loc, flags_width, 24);
        }
      }

      val_count = gen::read_uint16(dict_buf + 4);
      names_loc = dict_buf + gen::read_uint32(dict_buf + 6);
      uint8_t *val_table_loc = dict_buf + gen::read_uint32(dict_buf + 10);
      opts = (madras_dv1::bldr_options *) (dict_buf + gen::read_uint32(dict_buf + 18));
      names_start = (char *) names_loc + (val_count + (key_count > 0 ? 3 : 2)) * sizeof(uint16_t);
      column_encoding = names_start + gen::read_uint16(names_loc);

      max_val_len = gen::read_uint32(dict_buf + 34);

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

    void init_vars() {

      dict_buf = nullptr;
      is_mmapped = false;
      to_release_dict_buf = true;

      max_key_len = max_level = 0;
      sski = nullptr;

      max_tail_len = 0;
      tail_map = nullptr;
      trie_loc = nullptr;
      trie_flags_loc = nullptr;
      tail_lt = nullptr;
      leaf_lt = nullptr;

    }

    virtual ~static_trie_map() {
      if (is_mmapped)
        map_unmap();
      if (dict_buf != nullptr) {
        if (to_release_dict_buf)
          free(dict_buf);
      }
      if (sski != nullptr)
        delete sski;
      if (tail_map != nullptr) {
        delete tail_map;
      }
      if (lt_not_given & BV_LT_TYPE_CHILD) {
        delete [] child_lt.get_rank_loc();
        delete [] child_lt.get_select_loc();
      }
      if (lt_not_given & BV_LT_TYPE_TERM) {
        delete [] term_lt.get_rank_loc();
        delete [] term_lt.get_select_loc();
      }
      if (lt_not_given & BV_LT_TYPE_LEAF) {
        delete [] leaf_lt->get_rank_loc();
        delete [] leaf_lt->get_select_loc();
      }
      if (tail_lt != nullptr)
        delete tail_lt;
      if (leaf_lt != nullptr)
        delete leaf_lt;
    }

    void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    uint8_t *map_file(const char *filename, off_t& sz) {
#ifdef _WIN32
      load(filename);
      return dict_buf;
#else
      struct stat buf;
      int fd = open(filename, O_RDONLY);
      if (fd < 0) {
        perror("open: ");
        return nullptr;
      }
      fstat(fd, &buf);
      sz = buf.st_size;
      uint8_t *map_buf = (uint8_t *) mmap((caddr_t) 0, sz, PROT_READ, MAP_PRIVATE, fd, 0);
      if (map_buf == MAP_FAILED) {
        perror("mmap: ");
        close(fd);
        return nullptr;
      }
      close(fd);
      return map_buf;
#endif
    }

    void map_file_to_mem(const char *filename) {
      off_t dict_size;
      dict_buf = map_file(filename, dict_size);
      //       int len_will_need = (dict_size >> 2);
      //       //madvise(dict_buf, len_will_need, MADV_WILLNEED);
      // #ifndef _WIN32
      //       mlock(dict_buf, len_will_need);
      // #endif
      load_into_vars();
      is_mmapped = true;
    }

    void map_unmap() {
      // #ifndef _WIN32
      //       munlock(dict_buf, dict_size >> 2);
      //       int err = munmap(dict_buf, dict_size);
      //       if(err != 0){
      //         printf("UnMapping dict_buf Failed\n");
      //         return;
      //       }
      // #endif
      dict_buf = nullptr;
      is_mmapped = false;
    }

    void load_from_mem(uint8_t *mem) {
      dict_buf = mem;
      to_release_dict_buf = false;
      load_into_vars();
    }

    void load(const char* filename) {

      init_vars();
      struct stat file_stat;
      memset(&file_stat, 0, sizeof(file_stat));
      stat(filename, &file_stat);
      dict_buf = (uint8_t *) malloc(file_stat.st_size);

      FILE *fp = fopen(filename, "rb");
      size_t bytes_read = fread(dict_buf, 1, file_stat.st_size, fp);
      if (bytes_read != file_stat.st_size) {
        printf("Read error: [%s], %lld, %lu\n", filename, file_stat.st_size, bytes_read);
        throw errno;
      }
      fclose(fp);

      // int len_will_need = (dict_size >> 1);
      // #ifndef _WIN32
      //       mlock(dict_buf, len_will_need);
      // #endif
      //madvise(dict_buf, len_will_need, MADV_WILLNEED);
      //madvise(dict_buf + len_will_need, dict_size - len_will_need, MADV_RANDOM);

      load_into_vars();

    }

};

}
#endif
