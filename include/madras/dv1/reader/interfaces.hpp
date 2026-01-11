#ifndef __DV1_READER_IFACES_HPP
#define __DV1_READER_IFACES_HPP

#include "cmn.hpp"
#include "rank_select.hpp"

namespace madras { namespace dv1 {

class val_ctx {
  public:
    mdx_val *val = nullptr;
    size_t *val_len;
    size_t alloc_len;
    uintxx_t node_id;
    uintxx_t ptr_bit_count;
    uintxx_t ptr;
    uintxx_t next_pbc;
    int64_t i64;
    int64_t i64_delta;
    int32_t *i32_vals;
    int64_t *i64_vals;
    uint8_t *byts;
    uint64_t bm_leaf, bm_mask;
    uint8_t rpt_left;
    uint8_t grp_no;
    uint8_t is_init = 0;
    uint8_t to_alloc = 0;
    uint8_t to_alloc_uxx = 0;
    uint8_t is64 = 0;
    uint8_t count;
    uint8_t dec_count;
    virtual ~val_ctx() {
      unallocate();
    }
    void unallocate() {
      if (to_alloc) {
        if (alloc_len > 0)
          delete [] val->txt_bin;
        delete val;
        delete val_len;
        val = nullptr;
        alloc_len = 0;
        to_alloc = 0;
      }
      if (to_alloc_uxx) {
        delete [] i64_vals;
        delete [] i32_vals;
        delete [] byts;
        to_alloc_uxx = 0;
      }
      is_init = 0;
    }
    void init_pbc_vars() {
      grp_no = 0;
      rpt_left = 0;
      next_pbc = 0;
      ptr_bit_count = UINTXX_MAX;
    }
    // todo: this is leading to bugs. Allocation should be automatic
    void init(size_t _max_len, uint8_t _to_alloc_val = 1, uint8_t _to_alloc_uxx = 0) {
      alloc_len = _max_len;
      is_init = 1;
      to_alloc = _to_alloc_val;
      to_alloc_uxx = _to_alloc_uxx;
      if (_to_alloc_val) {
        val = new mdx_val();
        if (alloc_len > 0)
          val->txt_bin = new uint8_t[_max_len];
        val_len = new size_t;
        *val_len = 0;
      }
      node_id = 0;
      init_pbc_vars();
      if (to_alloc_uxx) {
        i64_vals = new int64_t[nodes_per_bv_block_n];
        i32_vals = new int32_t[nodes_per_bv_block_n];
        byts = new uint8_t[nodes_per_bv_block_n];
      }
      is64 = 0;
      is_init = 1;
      node_id = UINTXX_MAX;
      bm_leaf = UINT64_MAX;
      bm_mask = bm_init_mask;
      count = dec_count = 0;
    }
    void set_ptrs(mdx_val *_val_ptr, size_t *_buf_len_ptr) {
      if (to_alloc) {
        if (alloc_len > 0)
          delete[] val->txt_bin;
        alloc_len = 0;
        delete val;
        delete val_len;
      }
      to_alloc = 0;
      val = _val_ptr;
      val_len = _buf_len_ptr;
    }
    bool is_initialized() {
      return is_init == 1;
    }
};

class iter_ctx {
  public:
    __fq1 __fq2 iter_ctx(iter_ctx const&) = delete;
    __fq1 __fq2 iter_ctx& operator=(iter_ctx const&) = delete;
    int32_t cur_idx;
    uint16_t key_len;
    uint8_t *key;
    uintxx_t *node_path;
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
        delete [] last_tail_len;
      }
      is_allocated = false;
    }
    void init(uint16_t max_key_len, uint16_t max_level) {
      max_level++;
      max_key_len++;
      if (!is_allocated) {
        key = new uint8_t[max_key_len];
        node_path = new uintxx_t[max_level];
        last_tail_len = new uint16_t[max_level];
      }
      memset(node_path, 0, max_level * sizeof(uintxx_t));
      memset(last_tail_len, 0, max_level * sizeof(uint16_t));
      node_path[0] = 1;
      cur_idx = key_len = 0;
      to_skip_first_leaf = false;
      is_allocated = true;
    }
};

struct input_ctx {
  const uint8_t *key;
  uintxx_t key_len;
  uintxx_t key_pos;
  uintxx_t node_id;
  int32_t cmp;
};

class inner_trie_fwd {
  private:
    // __fq1 __fq2 inner_trie_fwd(inner_trie_fwd const&);
    // __fq1 __fq2 inner_trie_fwd& operator=(inner_trie_fwd const&);
  public:
    bvlt_rank tail_lt;
    uint8_t trie_level;
    __fq1 __fq2 inner_trie_fwd() {
    }
    __fq1 __fq2 virtual ~inner_trie_fwd() {
    }
    __fq1 __fq2 virtual bool compare_trie_tail(uintxx_t node_id, input_ctx& in_ctx) = 0;
    __fq1 __fq2 virtual bool copy_trie_tail(uintxx_t node_id, gen::byte_str& tail_str) = 0;
    __fq1 __fq2 virtual inner_trie_fwd *new_instance(uint8_t *mem) = 0;
};

}}

#endif
