#ifndef STATIC_DICT_H
#define STATIC_DICT_H

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
    bool last_leaf_set = false;
    uint32_t last_leaf_node_id;
    uint16_t last_leaf_len_offset;
    iter_ctx() {
      is_allocated = false;
      last_leaf_set = false;
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
      cur_idx = key_len = 0;
      to_skip_first_leaf = false;
      is_allocated = true;
      last_leaf_set = false;
      last_leaf_node_id = -1;
      last_leaf_len_offset = 0;
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

#define bm_init_mask 0x0000000000000001UL

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

struct tail_vars {
  uint32_t ptr_bit_count;
  uint32_t tail_ptr;
  uint8_t grp_no;
  gen::byte_str str;
  tail_vars() {
    grp_no = 0;
  }
  tail_vars(uint8_t *_tail_buf, uint32_t _max_tail_len) {
    grp_no = 0;
    str.set_buf_max_len(_tail_buf, _max_tail_len);
  }
};

struct ctx_vars {
  const uint8_t *tf0;
  int tf_multiplier;
  const trie_flags *tf;
  uint64_t bm_mask;
  tail_vars tail;
  ctx_vars() : tf_multiplier (32) {
  }
  ctx_vars(trie_flags *_tf, int _tf_multiplier) : tf_multiplier (_tf_multiplier) {
    init(_tf, _tf_multiplier);
  }
  void init(trie_flags *_tf, int _tf_multiplier) {
    tf0 = (const uint8_t *) _tf;
    tf_multiplier = _tf_multiplier;
    tail.ptr_bit_count = UINT32_MAX;
  }
  void next_block(uint32_t node_id) {
    if ((node_id % nodes_per_bv_block_n) == 0) {
      bm_mask = bm_init_mask;
      tf = (trie_flags *) (((const uint8_t *) tf) + tf_multiplier);
    }
  }
  void update_tf(uint32_t node_id) {
    tf = (trie_flags *) (tf0 + node_id / nodes_per_bv_block_n * tf_multiplier);
    bm_mask = bm_init_mask << (node_id % nodes_per_bv_block_n);
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
};

class GCFC_fwd_cache {
  private:
    madras_dv1::fwd_cache *cche0;
    uint8_t *loc;
    uint32_t count;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    GCFC_fwd_cache() {
    }
    void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (madras_dv1::fwd_cache *) _loc;
      count = _count;
      max_node_id = _max_node_id;
      cache_mask = count - 1;
    }
    int try_find(input_ctx& in_ctx) {
      if (count == 0 || in_ctx.node_id > max_node_id)
        return -1;
      uint8_t key_byte = in_ctx.key[in_ctx.key_pos];
      int times = MDX_CACHE_TIMES_FWD;
      uint32_t cache_idx = in_ctx.node_id;
      do {
        cache_idx = (in_ctx.node_id ^ (cache_idx << MDX_CACHE_SHIFT) ^ key_byte) & cache_mask;
        madras_dv1::fwd_cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = gen::read_uint24(&cche->parent_node_id1);
        if (in_ctx.node_id == cache_node_id && cche->node_byte == key_byte) {
          in_ctx.key_pos++;
          if (in_ctx.key_pos < in_ctx.key_len) {
            in_ctx.node_id = gen::read_uint24(&cche->child_node_id1);
            key_byte = in_ctx.key[in_ctx.key_pos];
            cache_idx = in_ctx.node_id;
            times = MDX_CACHE_TIMES_FWD;
            continue;
          }
          in_ctx.node_id += cche->node_offset;
          return 0;
        }
      } while (--times);
      return -1;
    }
};

class GCFC_rev_cache {
  private:
    madras_dv1::nid_cache *cche0;
    uint8_t *loc;
    uint32_t count;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    GCFC_rev_cache() {
    }
    void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (madras_dv1::nid_cache *) _loc;
      count = _count;
      max_node_id = _max_node_id;
      cache_mask = count - 1;
    }
    int try_find(uint32_t& node_id) {
      if (count == 0 || node_id > max_node_id)
        return -1;
      uint32_t cache_idx = node_id;
      int times = MDX_CACHE_TIMES_REV;
      do {
        cache_idx = ((cache_idx << MDX_CACHE_SHIFT) ^ node_id) & cache_mask;
        madras_dv1::nid_cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = gen::read_uint24(&cche->child_node_id1);
        if (node_id == cache_node_id) {
          node_id = gen::read_uint24(&cche->parent_node_id1) + 1;
          return 0;
        }
      } while (--times);
      return -1;
    }
};

class statssski {
  private:
    uint8_t *trie_loc;
    min_pos_stats min_stats;
    uint8_t *min_pos_loc;
  public:
    statssski() {
    }
    void init(uint8_t *_trie_loc, min_pos_stats _stats, uint8_t *_min_pos_loc) {
      trie_loc = _trie_loc;
      min_stats = _stats;
      min_pos_loc = _min_pos_loc;
    }
    void find_min_pos(uint32_t& node_id, uint8_t key_byte, ctx_vars& tv) {
      if ((tv.bm_mask & (tv.tf->bm_child | tv.tf->bm_leaf)) == 0) {
        uint8_t min_offset = min_pos_loc[((trie_loc[node_id] - min_stats.min_len) * 256) + key_byte];
        if ((node_id % nodes_per_bv_block_n) + min_offset < nodes_per_bv_block_n) {
          node_id += min_offset;
          tv.bm_mask <<= min_offset;
        } else {
          node_id += min_offset;
          tv.update_tf(node_id);
        }
      }
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
    uint32_t node_count;
    uint32_t node_set_count;
    uint32_t key_count;
    uint8_t *bm_start_loc;
    int bm_multiplier;
    int bm_pos;
    int lt_type;
  public:
    bv_lookup_tbl() {
    }
    void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc, uint32_t _node_count, uint32_t _node_set_count, uint32_t _key_count, trie_flags *_bm_start_loc, int _bm_multiplier, int _bm_pos, int _lt_type) {
      lt_rank_loc = _lt_rank_loc;
      lt_sel_loc = _lt_sel_loc;
      node_count = _node_count;
      node_set_count = _node_set_count;
      key_count = _key_count;
      bm_start_loc = (uint8_t *) _bm_start_loc;
      bm_multiplier = _bm_multiplier;
      bm_pos = _bm_pos;
      lt_type = _lt_type;
    }
    ~bv_lookup_tbl() {
    }
    uint8_t *write_bv3(uint32_t node_id, uint32_t& count, uint32_t& count3, uint8_t *buf3, uint8_t& pos3, uint8_t *lt_pos) {
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        memcpy(lt_pos, buf3, 3); lt_pos += 3;
        gen::copy_uint32(count, lt_pos); lt_pos += 4;
        count3 = 0;
        memset(buf3, 0, 3);
        pos3 = 0;
      } else if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
        buf3[pos3] = count3;
        //count3 = 0;
        pos3++;
      }
      return lt_pos;
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
    uint32_t rank(uint32_t node_id) {
      uint32_t rank = block_rank(node_id);
      uint8_t *t = bm_start_loc + node_id / nodes_per_bv_block_n * bm_multiplier;
      uint64_t bm = gen::read_uint64(t + bm_pos);
      uint64_t mask = bm_init_mask << (node_id % nodes_per_bv_block_n);
      // return rank + __popcountdi2(bm & (mask - 1));
      return rank + __builtin_popcountll(bm & (mask - 1));
    }
    void select(uint32_t& node_id, uint32_t target_count) {
      if (target_count == 0) {
        node_id = 2;
        return;
      }
      uint32_t block_count = block_select(target_count, node_id);
      uint8_t *t = bm_start_loc + node_id / nodes_per_bv_block_n * bm_multiplier;
      uint64_t bm = gen::read_uint64(t + bm_pos);

      int remaining = target_count - block_count - 1;
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

};

class static_dict_fwd {
  public:
    int key_count;
    uint16_t max_tail_len;
    bv_lookup_tbl tail_lt;
    virtual ~static_dict_fwd() {
    }
    virtual static_dict_fwd *new_instance(uint8_t *mem) = 0;
    virtual void map_from_memory(uint8_t *mem) = 0;
    virtual uint32_t leaf_rank(uint32_t node_id) = 0;
    virtual bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, bool return_first_byte = false) = 0;
};

class ptr_data_map {
  protected:
    uint8_t **grp_data;
    gen::int_bv_reader int_ptr_bv;

    uint32_t node_count;

    uint32_t read_len(uint8_t *t, uint8_t& len_len) {
      len_len = 1;
      if (*t < 15)
        return *t;
      t++;
      uint32_t ret = 0;
      while (*t & 0x80) {
        ret <<= 7;
        ret |= (*t++ & 0x7F);
        len_len++;
      }
      len_len++;
      ret <<= 4;
      ret |= (*t & 0x0F);
      return ret + 15;
    }

  public:
    uint8_t group_count;
    char data_type;
    char encoding_type;
    static_dict_fwd *dict_obj;
    static_dict_fwd *col_trie;

    ptr_data_map() {
      grp_data = nullptr;
      col_trie = nullptr;
    }

    ~ptr_data_map() {
      if (grp_data != nullptr)
        delete [] grp_data;
      if (col_trie != nullptr)
        delete col_trie;
    }

    bool exists() {
      return dict_obj != nullptr;
    }

    void init(uint8_t *_dict_buf, static_dict_fwd *_dict_obj, uint8_t *_trie_loc, trie_flags *_tf, uint8_t *data_loc, uint32_t _node_count, bool is_tail) {
      dict_obj = _dict_obj;
      data_type = data_loc[1];
      encoding_type = data_loc[2];
      uint8_t *grp_data_loc = data_loc + gen::read_uint32(data_loc + 12);
      group_count = *grp_data_loc;
      uint8_t *ptrs_loc = data_loc + gen::read_uint32(data_loc + 24);
      int_ptr_bv.init(ptrs_loc, grp_data_loc[1]);
      if (encoding_type == 't') {
        col_trie = dict_obj->new_instance(grp_data_loc);
        int_ptr_bv.init(ptrs_loc, data_loc[0]);
      } else {
        uint8_t *grp_data_idx_start = grp_data_loc + 514;
        grp_data = new uint8_t*[1];
        grp_data[0] = data_loc + gen::read_uint32(grp_data_idx_start);
      }
    }

};

class tail_ptr_data_map : public ptr_data_map {
  public:
    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, ctx_vars& cv) {
      if (cv.tail.ptr_bit_count == UINT32_MAX)
        cv.tail.ptr_bit_count = dict_obj->tail_lt.rank(node_id);
      uint32_t flat_ptr = int_ptr_bv[cv.tail.ptr_bit_count++];
      flat_ptr <<= 8;
      flat_ptr |= node_byte;
      return flat_ptr;
    }
    void get_tail_str(uint32_t node_id, uint8_t node_byte, ctx_vars& cv, bool return_first_byte = false) {
      //ptr_bit_count = UINT32_MAX;
      cv.tail.tail_ptr = get_tail_ptr(node_byte, node_id, cv);
      get_tail_str(cv.tail, return_first_byte);
    }
    uint8_t get_first_byte(uint8_t node_byte, uint32_t node_id, ctx_vars& cv) {
      cv.tail.tail_ptr = get_tail_ptr(node_byte, node_id, cv);
      if (encoding_type == 't') {
        //uint8_t tail_byte_str[dict_obj->max_tail_len];
        int tail_str_len;
        col_trie->reverse_lookup_from_node_id(cv.tail.tail_ptr, &tail_str_len, cv.tail.str.data() + cv.tail.str.length(), true);
        return cv.tail.str[cv.tail.str.length()];
      }
      uint8_t *tail = grp_data[cv.tail.grp_no];
      tail += cv.tail.tail_ptr;
      uint8_t first_byte = *tail;
      if (first_byte >= 32)
        return first_byte;
      uint8_t len_len;
      read_len(tail, len_len);
      return tail[len_len];
    }
    uint8_t get_first_byte(uint32_t tail_ptr, uint8_t grp_no) {
      uint8_t *tail = grp_data[grp_no];
      return tail[tail_ptr];
    }
    void get_tail_str(tail_vars& tv, bool return_first_byte = false) {
      if (encoding_type == 't') {
        int tail_str_len;
        col_trie->reverse_lookup_from_node_id(tv.tail_ptr, &tail_str_len, tv.str.data(), return_first_byte);
        tv.str.set_length(tail_str_len);
        return;
      }
      uint8_t *tail = grp_data[tv.grp_no];
      tv.str.clear();
      uint8_t *t = tail + tv.tail_ptr;
      if (*t >= 32) {
        uint8_t byt = *t;
        while (byt > 31) {
          t++;
          tv.str.append(byt);
          byt = *t;
        }
        return;
      }
      uint8_t len_len;
      uint32_t bin_len = read_len(t, len_len);
      t += len_len;
      while (bin_len--)
        tv.str.append(*t++);
    }
};

class static_dict : public static_dict_fwd {

  private:
    bool is_mmapped;
    bool to_release_dict_buf;

    uint16_t max_level;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

  public:
    uint8_t *dict_buf;
    madras_dv1::bldr_options *opts;
    int max_key_len;
    uint8_t trie_level;
    uint32_t node_count;
    uint32_t node_set_count;
    uint8_t *trie_loc;
    trie_flags *trie_flags_loc;
    statssski *sski;
    tail_ptr_data_map tail_map;
    GCFC_fwd_cache fwd_cache;
    GCFC_rev_cache rev_cache;
    bv_lookup_tbl term_lt;
    bv_lookup_tbl child_lt;
    bv_lookup_tbl leaf_lt;
    // val_ptr_data_map *val_map;
    static_dict(uint8_t _trie_level = 0) {
      init_vars();
      trie_level = _trie_level;
    }

    void init_vars() {

      dict_buf = nullptr;
      // val_map = nullptr;
      is_mmapped = false;
      to_release_dict_buf = true;

      max_key_len = max_tail_len = max_level = 0;
      trie_level = 0;
      trie_loc = nullptr;
      trie_flags_loc = nullptr;
      sski = nullptr;

    }

    virtual ~static_dict() {
      if (is_mmapped)
        map_unmap();
      if (dict_buf != nullptr) {
        if (to_release_dict_buf)
          free(dict_buf);
      }
      // if (val_map != nullptr) {
      //   delete [] val_map;
      // }
      if (sski != nullptr)
        delete sski;
    }

    void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    void load_into_vars() {

      node_count = gen::read_uint32(dict_buf + 14);
      opts = (madras_dv1::bldr_options *) (dict_buf + gen::read_uint32(dict_buf + 18));
      node_set_count = gen::read_uint32(dict_buf + 22);
      key_count = gen::read_uint32(dict_buf + 26);

      if (key_count > 0) {
        max_key_len = gen::read_uint32(dict_buf + 30);
        max_tail_len = gen::read_uint16(dict_buf + 38) + 1;
        max_level = gen::read_uint16(dict_buf + 40);
        uint32_t fwd_cache_count = gen::read_uint32(dict_buf + 42);
        uint32_t rev_cache_count = gen::read_uint32(dict_buf + 46);
        uint32_t fwd_cache_max_node_id = gen::read_uint32(dict_buf + 50);
        uint32_t rev_cache_max_node_id = gen::read_uint32(dict_buf + 54);
        min_pos_stats min_stats;
        memcpy(&min_stats, dict_buf + 58, 4);
        uint8_t *fwd_cache_loc = dict_buf + gen::read_uint32(dict_buf + 62);
        uint8_t *rev_cache_loc = dict_buf + gen::read_uint32(dict_buf + 66);
        uint8_t *min_pos_loc = dict_buf + gen::read_uint32(dict_buf + 70);
        if (min_pos_loc == dict_buf)
          min_pos_loc = nullptr;
        fwd_cache.init(fwd_cache_loc, fwd_cache_count, fwd_cache_max_node_id);
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
        uint32_t trie_size = gen::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 8;
        trie_loc = tails_loc + tail_size;
        trie_flags_loc = (trie_flags *) (trie_loc + trie_size);
        tail_map.init(dict_buf, this, trie_loc, trie_flags_loc, tails_loc, node_count, true);

        if (min_pos_loc != nullptr) {
          sski = new statssski();
          sski->init(trie_loc, min_stats, min_pos_loc);
        }

        if (term_select_lkup_loc == dict_buf) term_select_lkup_loc = nullptr;
        if (term_lt_loc == dict_buf) term_lt_loc = nullptr;
        if (child_select_lkup_loc == dict_buf) child_select_lkup_loc = nullptr;
        if (child_lt_loc == dict_buf) child_lt_loc = nullptr;
        if (leaf_select_lkup_loc == dict_buf) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == dict_buf) leaf_lt_loc = nullptr;

        int flags_width = 32;
        if (opts->trie_leaf_count == 0)
          flags_width = 24;
        child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, node_set_count, key_count, trie_flags_loc, flags_width, 0, BV_LT_TYPE_CHILD);
        term_lt.init(term_lt_loc,   term_select_lkup_loc,  node_count, node_set_count, key_count, trie_flags_loc, flags_width, 8, BV_LT_TYPE_TERM);
        if (tail_lt_loc != dict_buf)
          tail_lt.init(tail_lt_loc, nullptr, node_count, node_set_count, key_count, trie_flags_loc, flags_width, 16, BV_LT_TYPE_TAIL);
        leaf_lt.init(leaf_lt_loc,   leaf_select_lkup_loc,  node_count, node_set_count, key_count, trie_flags_loc, flags_width, 24, BV_LT_TYPE_LEAF);
      }

      // if (val_count > 0) {
      //   val_map = new val_ptr_data_map[val_count];
      //   for (uint32_t i = 0; i < val_count; i++) {
      //     uint8_t *val_buf = dict_buf + gen::read_uint32(val_table_loc + i * sizeof(uint32_t));
      //     val_map[i].init(val_buf, this, trie_loc, trie_flags_loc, val_buf, node_count, false);
      //   }
      // }

    }

    uint8_t *map_file(const char *filename, size_t& sz) {
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
      size_t dict_size;
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

    bool lookup(input_ctx& in_ctx) {
      #ifndef MDX_NO_NULLS
      if (in_ctx.key == nullptr) {
        in_ctx.node_id = 0;
        return trie_loc[32] == 0xFF;
      }
      if (in_ctx.key_len == 0) {
        in_ctx.node_id = 1;
        return trie_loc[33] == 0xFF;
      }
      #endif
      in_ctx.key_pos = 0;
      in_ctx.node_id = 2;
      uint8_t tail_str_buf[max_tail_len];
      ctx_vars tv(trie_flags_loc, opts->trie_leaf_count == 0 ? 24 : 32);
      tv.tail.str.set_buf_max_len(tail_str_buf, max_tail_len);
      uint8_t trie_byte, key_byte;
      do {
        int ret = fwd_cache.try_find(in_ctx);
        tv.update_tf(in_ctx.node_id);
        if (ret == 0) {
          if (tv.is_leaf_set())
            return true;
        }
        key_byte = in_ctx.key[in_ctx.key_pos];
          #ifndef MDX_NO_DART
          if (sski != nullptr)
            sski->find_min_pos(in_ctx.node_id, key_byte, tv);
          #endif
          tv.tail.ptr_bit_count = UINT32_MAX;
          do {
            trie_byte = trie_loc[in_ctx.node_id];
            if (tv.is_ptr_set())
              trie_byte = tail_map.get_first_byte(trie_byte, in_ctx.node_id, tv);
            #ifndef MDX_IN_ORDER
              if (key_byte == trie_byte)
                break;
            #else
              if (key_byte <= trie_byte)
                break;
            #endif
            if (tv.is_term_set())
              return false;
            in_ctx.node_id++;
            tv.bm_mask <<= 1;
            if (tv.bm_mask == 0) {
              tv.next_block(in_ctx.node_id);
            }
          } while (1);
        // if (key_byte == trie_byte) {
        in_ctx.cmp = 0;
        int tail_len = 1;
        if (tv.is_ptr_set()) {
          tail_map.get_tail_str(tv.tail);
          tail_len = tv.tail.str.length();
          in_ctx.cmp = gen::compare(tv.tail.str.data(), tail_len, in_ctx.key + in_ctx.key_pos, in_ctx.key_len - in_ctx.key_pos);
        }
        in_ctx.key_pos += tail_len;
        if (in_ctx.key_pos < in_ctx.key_len && (in_ctx.cmp == 0 || abs(in_ctx.cmp) - 1 == tail_len)) {
          if (!tv.is_child_set())
            return false;
          term_lt.select(in_ctx.node_id, child_lt.rank(in_ctx.node_id) + 1);
          continue;
        }
        if (in_ctx.cmp == 0 && in_ctx.key_pos == in_ctx.key_len && (tv.tf->bm_leaf & tv.bm_mask))
          return true;
        // }
        return false;
      } while (in_ctx.node_id < node_count);
      return false;
    }

    bool get(input_ctx& in_ctx, int *in_size_out_value_len, void *val) {
      bool is_found = lookup(in_ctx);
      if (is_found && in_ctx.node_id >= 0) {
        // if (val_count > 0)
        //   val_map[0].get_val(in_ctx.node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
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

    uint32_t get_leaf_rank(uint32_t node_id) {
      return leaf_lt.rank(node_id);
    }

    bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, bool return_first_byte = false) {
      #ifndef MDX_NO_NULLS
      if (node_id == 0) {
        *in_size_out_key_len = -1; // unconditional?
        return true;
      }
      if (node_id == 1) {
        *in_size_out_key_len = 0; // unconditional?
        return true;
      }
      #endif
      ctx_vars cv(trie_flags_loc, opts->trie_leaf_count == 0 ? 24 : 32);
      uint8_t tail[max_tail_len + 1];
      cv.tail.str.set_buf_max_len(tail, max_tail_len);
      int key_len = 0;
      node_id++;
      *in_size_out_key_len = 1;
      do {
        node_id--;
        cv.update_tf(node_id);
        if (cv.bm_mask & cv.tf->bm_ptr) {
          cv.tail.ptr_bit_count = UINT32_MAX;
          tail_map.get_tail_str(node_id, trie_loc[node_id], cv, return_first_byte);
          int i = cv.tail.str.length() - 1;
          ret_key[key_len] = cv.tail.str[i];
          if (return_first_byte)
            return true;
          key_len++; i--;
          while (i >= 0)
            ret_key[key_len++] = cv.tail.str[i--];
        } else {
          ret_key[key_len++] = trie_loc[node_id];
          if (return_first_byte)
            return true;
        }
        if (rev_cache.try_find(node_id) == -1)
          child_lt.select(node_id, term_lt.rank(node_id));
      } while (node_id > 2);
      if (trie_level > 1) {
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

    void map_from_memory(uint8_t *mem) {
      load_from_mem(mem);
    }

    uint32_t leaf_rank(uint32_t node_id) {
      return leaf_lt.rank(node_id);
    }

    bool get_col_val(uint32_t node_id, int col_val_idx, int *in_size_out_value_len, void *val, uint32_t *p_ptr_bit_count = nullptr) {
      //tail_map.get_tail(node_id, in_size_out_value_len, val, p_ptr_bit_count);
      return true;
    }

    static_dict_fwd *new_instance(uint8_t *mem) {
      static_dict *dict = new static_dict(trie_level + 1);
      dict->load_from_mem(mem);
      return dict;
    }

};

}
#endif
