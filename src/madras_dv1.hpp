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

namespace madras_dv1 {

class dict_iter_ctx {
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
    dict_iter_ctx() {
      is_allocated = false;
      last_leaf_set = false;
    }
    ~dict_iter_ctx() {
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

class static_dict_fwd {
  public:
    int key_count;
    uint16_t max_tail_len;
    virtual ~static_dict_fwd() {
    }
    virtual static_dict_fwd *new_instance(uint8_t *mem) = 0;
    virtual void map_from_memory(uint8_t *mem) = 0;
    virtual uint32_t leaf_rank(uint32_t node_id) = 0;
    virtual bool reverse_lookup(uint32_t leaf_id, int *in_size_out_key_len, uint8_t *ret_key, int *in_size_out_value_len = nullptr, void *ret_val = nullptr, bool return_first_byte = false, bool to_reverse = true) = 0;
    virtual bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, int *in_size_out_value_len = nullptr, void *ret_val = nullptr, int cmp = 0, dict_iter_ctx *ctx = nullptr, bool return_first_byte = false, bool to_reverse = true) = 0;
    virtual bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, bool return_first_byte = false) = 0;
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

struct ctx_vars {
  uint8_t *t;
  uint32_t node_id, child_count;
  uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
  gen::byte_str tail;
  uint32_t tail_ptr;
  uint32_t ptr_bit_count;
  uint8_t grp_no;
  ctx_vars() {
    memset(this, '\0', sizeof(*this));
    ptr_bit_count = UINT32_MAX;
  }
  static uint8_t *read_flags(uint8_t *t, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr) {
    t = gen::read_uint64(t, bm_leaf);
    t = gen::read_uint64(t, bm_term);
    t = gen::read_uint64(t, bm_child);
    return gen::read_uint64(t, bm_ptr);
  }
  void read_flags() {
    t = gen::read_uint64(t, bm_leaf);
    t = gen::read_uint64(t, bm_term);
    t = gen::read_uint64(t, bm_child);
    t = gen::read_uint64(t, bm_ptr);
  }
  void read_flags_block_begin() {
    if ((node_id % nodes_per_bv_block_n) == 0) {
      bm_mask = bm_init_mask;
      read_flags();
    }
  }
  void init_cv_nid0(uint8_t *trie_loc) {
    if (trie_loc == nullptr)
      return;
    t = trie_loc;
    read_flags();
    bm_mask = bm_init_mask;
    ptr_bit_count = 0;
  }
  void init_cv_from_node_id(uint8_t *trie_loc) {
    if (trie_loc == nullptr)
      return;
    t = trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n;
    if (node_id % nodes_per_bv_block_n) {
      read_flags();
      t += node_id % nodes_per_bv_block_n;
    }
    bm_mask = bm_init_mask << (node_id % nodes_per_bv_block_n);
  }
};

class ptr_data_map {
  private:
    uint32_t ptr_lt_ptr_width;
    uint32_t ptr_lt_blk_width;
    uint8_t *ptr_lt_loc;
    uint32_t ptr_lt_mask;
    uint8_t *ptrs_loc;
    uint8_t start_bits;
    uint8_t idx_step_bits;
    int8_t grp_idx_limit;
    uint8_t group_count;
    uint8_t inner_trie_start_grp;
    uint8_t *grp_data_loc;
    uint32_t two_byte_data_count;
    uint32_t idx2_ptr_count;
    uint8_t *two_byte_data_loc;
    uint8_t *code_lt_bit_len;
    uint8_t *code_lt_code_len;
    uint8_t **grp_data;

    uint8_t *dict_buf;
    uint8_t *trie_loc;
    uint32_t node_count;
    bool was_ptr_lt_given;

    int idx_map_arr[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t *idx2_ptrs_map_loc;
    uint8_t idx2_ptr_size;
    uint32_t idx_ptr_mask;
    uint32_t read_ptr_from_idx(uint32_t grp_no, uint32_t ptr) {
      int idx_map_start = idx_map_arr[grp_no];
      //ptr = gen::read_uint24(idx2_ptrs_map_loc + idx_map_start + ptr * 3);
      ptr = gen::read_uintx(idx2_ptrs_map_loc + idx_map_start + ptr * idx2_ptr_size, idx_ptr_mask);
      return ptr;
    }

    uint32_t scan_ptr_bits_tail(uint32_t node_id, uint32_t ptr_bit_count) {
      uint32_t trie_offset = (node_id / nodes_per_bv_block_n) * bytes_per_bv_block_n;
      uint8_t *t = trie_loc + trie_offset;
      int offset = 0;
      if (nodes_per_bv_block_n != nodes_per_ptr_block_n)
        offset = (node_id % nodes_per_bv_block_n) / nodes_per_ptr_block_n * nodes_per_ptr_block_n;
      uint64_t bm_mask = bm_init_mask << offset;
      uint64_t bm_ptr;
      t = gen::read_uint64(t + 24, bm_ptr) + offset;
      uint8_t *t_upto = t + (node_id % nodes_per_ptr_block_n);
      while (t < t_upto) {
        if (bm_ptr & bm_mask)
          ptr_bit_count += code_lt_bit_len[*t];
        bm_mask <<= 1;
        t++;
      }
      return ptr_bit_count;
    }

    uint32_t scan_ptr_bits_val(uint32_t node_id, uint32_t ptr_bit_count) {
      int offset = 0;
      if (nodes_per_bv_block_n != nodes_per_ptr_block_n)
        offset = (node_id % nodes_per_bv_block_n) / nodes_per_ptr_block_n * nodes_per_ptr_block_n;
      uint64_t bm_mask = bm_init_mask << offset;
      uint64_t bm_leaf = UINT64_MAX;
      if (dict_obj->key_count > 0)
        gen::read_uint64(trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n, bm_leaf);
      uint32_t start_node_id = node_id / nodes_per_ptr_block_n * nodes_per_ptr_block_n;
      while (start_node_id < node_id) {
        if (bm_leaf & bm_mask) {
          uint8_t code = read_ptr_bits8(ptr_bit_count);
          ptr_bit_count += code_lt_bit_len[code];
        }
        bm_mask <<= 1;
        start_node_id++;
      }
      return ptr_bit_count;
    }

    uint32_t get_ptr_block_t(uint32_t node_id) {
      uint8_t *block_ptr = ptr_lt_loc + (node_id / nodes_per_ptr_block) * ptr_lt_blk_width;
      uint32_t ptr_bit_count = gen::read_uintx(block_ptr, ptr_lt_mask);
      int pos = (node_id / nodes_per_ptr_block_n) % (nodes_per_ptr_block / nodes_per_ptr_block_n);
      if (pos) {
        pos--;
        uint8_t *ptr3 = block_ptr + ptr_lt_ptr_width + pos * 2;
        ptr_bit_count += gen::read_uint16(ptr3);
      }
      return ptr_bit_count;
    }

    uint32_t get_ptr_byts_words(uint32_t node_id) {
      uint8_t *block_ptr = ptr_lt_loc + (node_id / nodes_per_ptr_block_n) * ptr_lt_ptr_width;
      return gen::read_uintx(block_ptr, ptr_lt_mask);
    }

    uint32_t get_ptr_bit_count_tail(uint32_t node_id) {
      uint32_t ptr_bit_count = get_ptr_block_t(node_id);
      return scan_ptr_bits_tail(node_id, ptr_bit_count);
    }

    uint32_t get_ptr_bit_count_val(uint32_t node_id) {
      uint32_t ptr_bit_count = get_ptr_block_t(node_id);
      return scan_ptr_bits_val(node_id, ptr_bit_count);
    }

    uint8_t read_ptr_bits8(uint32_t& ptr_bit_count) {
      uint8_t *ptr_loc = ptrs_loc + ptr_bit_count / 8;
      uint8_t bits_filled = (ptr_bit_count % 8);
      uint8_t ret = (*ptr_loc++ << bits_filled);
      ret |= (*ptr_loc >> (8 - bits_filled));
      return ret;
    }

    uint32_t read_extra_ptr(uint32_t& ptr_bit_count, int bits_left) {
      uint8_t *ptr_loc = ptrs_loc + ptr_bit_count / 8;
      int bits_occu = (ptr_bit_count % 8);
      ptr_bit_count += bits_left;
      bits_left += (bits_occu - 8);
      uint64_t ret = *ptr_loc++ & (0xFF >> bits_occu);
      while (bits_left > 0) {
        ret = (ret << 8) | *ptr_loc++;
        bits_left -= 8;
      }
      ret >>= (bits_left * -1); // crude but avoids branching
      return ret;
    }

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
    char data_type;
    char encoding_type;
    uint8_t flags;
    uint32_t max_len;
    static_dict_fwd *dict_obj;
    static_dict_fwd *col_trie;
    static_dict_fwd **word_tries;
    static_dict_fwd **inner_tries;
    gen::int_bv_reader col_trie_int_bv;
    std::vector<uint8_t> prev_val;

    ptr_data_map() {
      dict_buf = trie_loc = nullptr;
      grp_data = nullptr;
      col_trie = nullptr;
      word_tries = nullptr;
      inner_tries = nullptr;
      was_ptr_lt_given = true;
    }

    ~ptr_data_map() {
      if (grp_data != nullptr)
        delete [] grp_data;
      if (col_trie != nullptr)
        delete col_trie;
      if (word_tries != nullptr) {
        for (int i = 0; i < group_count; i++) {
          delete word_tries[i];
        }
        delete word_tries;
      }
      if (inner_tries != nullptr) {
        for (int i = inner_trie_start_grp; i < group_count; i++) {
          if (inner_tries[i] != nullptr)
            delete inner_tries[i];
        }
        delete [] inner_tries;
      }
      if (!was_ptr_lt_given)
        delete [] ptr_lt_loc;
    }

    bool exists() {
      return dict_buf != nullptr;
    }

    void init(uint8_t *_dict_buf, static_dict_fwd *_dict_obj, uint8_t *_trie_loc, uint8_t *data_loc, uint32_t _node_count, bool is_tail) {

      dict_buf = _dict_buf;
      dict_obj = _dict_obj;
      trie_loc = _trie_loc;
      node_count = _node_count;

      ptr_lt_ptr_width = *data_loc;
      if (ptr_lt_ptr_width == 4)
        ptr_lt_mask = 0xFFFFFFFF;
      else
        ptr_lt_mask = 0x00FFFFFF;
      ptr_lt_blk_width = (ptr_lt_ptr_width + (nodes_per_ptr_block / nodes_per_ptr_block_n - 1) * 2);
      data_type = data_loc[1];
      encoding_type = data_loc[2];
      flags = data_loc[3];
      max_len = gen::read_uint32(data_loc + 4);
      ptr_lt_loc = data_loc + gen::read_uint32(data_loc + 8);
      grp_data_loc = data_loc + gen::read_uint32(data_loc + 12);
      two_byte_data_count = gen::read_uint32(data_loc + 16);
      idx2_ptr_count = gen::read_uint32(data_loc + 20);
      idx2_ptr_size = idx2_ptr_count & 0x80000000 ? 3 : 2;
      idx_ptr_mask = idx2_ptr_size == 3 ? 0x00FFFFFF : 0x0000FFFF;
      start_bits = (idx2_ptr_count >> 20) & 0x0F;
      grp_idx_limit = (idx2_ptr_count >> 24) & 0x1F;
      idx_step_bits = (idx2_ptr_count >> 29) & 0x03;
      idx2_ptr_count &= 0x000FFFFF;
      ptrs_loc = data_loc + gen::read_uint32(data_loc + 24);
      two_byte_data_loc = data_loc + gen::read_uint32(data_loc + 28);
      idx2_ptrs_map_loc = data_loc + gen::read_uint32(data_loc + 32);

      was_ptr_lt_given = true;
      col_trie = nullptr;
      if (encoding_type == 't') {
        col_trie = dict_obj->new_instance(grp_data_loc);
        col_trie_int_bv.init(ptrs_loc, ptr_lt_ptr_width);
      } else {
        group_count = *grp_data_loc;
        inner_trie_start_grp = grp_data_loc[1];
        code_lt_bit_len = grp_data_loc + 2;
        code_lt_code_len = code_lt_bit_len + 256;
        uint8_t *grp_data_idx_start = code_lt_bit_len + (data_type == 'w' ? 0 : 512);
        grp_data = new uint8_t*[group_count]();
        inner_tries = new static_dict_fwd*[group_count]();
        if (data_type == 'w')
          word_tries = new static_dict_fwd*[group_count]();
        for (int i = 0; i < group_count; i++) {
          grp_data[i] = (data_type == 'w' ? grp_data_loc : data_loc);
          grp_data[i] += gen::read_uint32(grp_data_idx_start + i * 4);
          if (data_type == 'w') {
            word_tries[i] = dict_obj->new_instance(grp_data[i]);
          }
          if (*(grp_data[i]) != 0)
            inner_tries[i] = dict_obj->new_instance(grp_data[i]);
        }
        int _start_bits = start_bits;
        for (int i = 1; i <= grp_idx_limit; i++) {
          idx_map_arr[i] = idx_map_arr[i - 1] + (1 << _start_bits) * idx2_ptr_size;
          _start_bits += idx_step_bits;
        }
        if (ptr_lt_loc == data_loc) {
          create_ptr_lt(is_tail);
          was_ptr_lt_given = false;
        }
      }

    }

    void create_ptr_lt(bool is_tail) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      uint32_t bit_count4 = 0;
      uint32_t ptr_bit_count = 0;
      int pos4 = 0;
      int u16_arr_count = (nodes_per_ptr_block / nodes_per_ptr_block_n);
      u16_arr_count--;
      uint16_t bit_counts[u16_arr_count + 1];
      memset(bit_counts, 0, u16_arr_count * 2 + 2);
      uint32_t lt_size = gen::get_lkup_tbl_size2(node_count, nodes_per_ptr_block, ptr_lt_ptr_width + (nodes_per_ptr_block / nodes_per_ptr_block_n - 1) * 2);
      uint8_t *lt_pos = new uint8_t[lt_size];
      ptr_lt_loc = lt_pos;
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      bm_mask = bm_init_mask << node_id;
      uint8_t *t = trie_loc;
      if (ptr_lt_ptr_width == 4) {
        gen::copy_uint32(bit_count, lt_pos); lt_pos += 4;
      } else {
        gen::copy_uint24(bit_count, lt_pos); lt_pos += 3;
      }
      do {
        if ((node_id % nodes_per_bv_block_n) == 0) {
          bm_mask = bm_init_mask;
          if (dict_obj->key_count > 0)
            t = ctx_vars::read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
        }
        if (node_id && (node_id % nodes_per_ptr_block_n) == 0) {
          if (bit_count4 > 65535)
            printf("UNEXPECTED: PTR_LOOKUP_TBL bit_count3 > 65k\n");
          bit_counts[pos4] = bit_count4;
          pos4++;
        }
        if (node_id && (node_id % nodes_per_ptr_block) == 0) {
          for (int j = 0; j < u16_arr_count; j++) {
            gen::copy_uint16(bit_counts[j], lt_pos);
            lt_pos += 2;
          }
          bit_count += bit_counts[u16_arr_count];
          if (ptr_lt_ptr_width == 4) {
            gen::copy_uint32(bit_count, lt_pos); lt_pos += 4;
          } else {
            gen::copy_uint24(bit_count, lt_pos); lt_pos += 3;
          }
          bit_count4 = 0;
          pos4 = 0;
          memset(bit_counts, 0, 8);
        }
        if (is_tail) {
          if (bm_mask & bm_ptr) {
            bit_count4 += code_lt_bit_len[*t];
          }
        } else {
          if (bm_mask & bm_leaf || dict_obj->key_count == 0) {
            uint8_t code = read_ptr_bits8(ptr_bit_count);
            ptr_bit_count += code_lt_bit_len[code];
            bit_count4 += code_lt_bit_len[code];
          }
        }
        t++;
        node_id++;
        bm_mask <<= 1;
      } while (node_id < node_count);
      for (int j = 0; j < u16_arr_count; j++) {
        gen::copy_uint16(bit_counts[j], lt_pos);
        lt_pos += 2;
      }
      bit_count += bit_counts[u16_arr_count];
      if (ptr_lt_ptr_width == 4) {
        gen::copy_uint32(bit_count, lt_pos); lt_pos += 4;
      } else {
        gen::copy_uint24(bit_count, lt_pos); lt_pos += 3;
      }
      for (int j = 0; j < u16_arr_count; j++) {
        gen::copy_uint16(bit_counts[j], lt_pos);
        lt_pos += 2;
      }
    }

    bool next_val(ctx_vars& cv, int *in_size_out_value_len, uint8_t *ret_val) {
      if (cv.node_id >= node_count)
        return false;
      if (dict_obj->key_count == 0) {
        get_val(cv.node_id, in_size_out_value_len, ret_val, &cv.ptr_bit_count);
        return true;
      }
      cv.init_cv_from_node_id(trie_loc);
      do {
        cv.read_flags_block_begin();
        if (cv.bm_mask & cv.bm_leaf) {
          get_val(cv.node_id, in_size_out_value_len, ret_val, &cv.ptr_bit_count);
          return true;
        }
        cv.node_id++;
        cv.t++;
        cv.bm_mask <<= 1;
      } while (cv.node_id < node_count);
      return false;
    }

    uint8_t *get_words_loc(uint32_t node_id, uint32_t *p_ptr_byt_count = nullptr) {
      uint32_t ptr_byt_count = UINT32_MAX;
      if (p_ptr_byt_count == nullptr)
        p_ptr_byt_count = &ptr_byt_count;
      if (*p_ptr_byt_count == UINT32_MAX)
        *p_ptr_byt_count = get_ptr_byts_words(node_id);
      uint8_t *w = ptrs_loc + *p_ptr_byt_count;
      // printf("%u\n", *p_ptr_byt_count);
      int skip_count = node_id % nodes_per_bv_block_n;
      while (skip_count--) {
        int8_t vlen;
        uint32_t count = gen::read_fvint32(w, vlen);
        w += vlen;
        w += count;
        *p_ptr_byt_count += vlen;
        *p_ptr_byt_count += count;
      }
      return w;
    }

    uint8_t *get_val_loc(uint32_t node_id, uint32_t *p_ptr_bit_count = nullptr, uint8_t *p_grp_no = nullptr) {
      uint8_t grp_no = 0;
      if (p_grp_no == nullptr)
        p_grp_no = &grp_no;
      uint32_t ptr_bit_count = UINT32_MAX;
      if (p_ptr_bit_count == nullptr)
        p_ptr_bit_count = &ptr_bit_count;
      if (*p_ptr_bit_count == UINT32_MAX)
        *p_ptr_bit_count = get_ptr_bit_count_val(node_id);
      uint8_t code = read_ptr_bits8(*p_ptr_bit_count);
      uint8_t bit_len = code_lt_bit_len[code];
      *p_grp_no = code_lt_code_len[code] & 0x0F;
      uint8_t code_len = code_lt_code_len[code] >> 4;
      *p_ptr_bit_count += code_len;
      uint32_t ptr = read_extra_ptr(*p_ptr_bit_count, bit_len - code_len);
      if (*p_grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(*p_grp_no, ptr);
      if (ptr == 0)
        return nullptr;
      return grp_data[*p_grp_no] + ptr;
    }

    void get_delta_val(uint32_t node_id, int *in_size_out_value_len, void *ret_val) {
      uint8_t *val_loc;
      uint32_t ptr_bit_count = UINT32_MAX;
      uint32_t delta_node_id = node_id / nodes_per_bv_block_n;
      delta_node_id *= nodes_per_bv_block_n;
      uint8_t *t = trie_loc + (delta_node_id / nodes_per_ptr_block_n) * bytes_per_ptr_block_n;
      uint64_t bm_mask = bm_init_mask;
      uint64_t bm_leaf = UINT64_MAX;
      if (dict_obj->key_count > 0)
        gen::read_uint64(t, bm_leaf);
      int64_t col_val = 0;
      do {
        if (bm_mask & bm_leaf) {
          val_loc = get_val_loc(delta_node_id, &ptr_bit_count);
          if (val_loc != nullptr)
            col_val += gen::read_svint60(val_loc);
        }
        bm_mask <<= 1;
      } while (delta_node_id++ < node_id);
      *in_size_out_value_len = 8;
      if (data_type == DCT_S64_INT) {
        *((int64_t *) ret_val) = col_val;
        return;
      }
      double dbl = static_cast<double>(col_val);
      dbl /= gen::pow10(data_type - DCT_S64_DEC1 + 1);
      *((double *)ret_val) = dbl;
    }

    void get_col_trie_val(uint32_t node_id, int *in_size_out_value_len, void *ret_val) {
      uint32_t ptr_pos = node_id;
      if (dict_obj->key_count > 0)
        ptr_pos = dict_obj->leaf_rank(node_id);
      uint32_t col_trie_node_id = col_trie_int_bv[ptr_pos];
      col_trie->reverse_lookup_from_node_id(col_trie_node_id, in_size_out_value_len, (uint8_t *) ret_val, false);
    }

    void convert_back(uint8_t *val_loc, void *ret_val, int& ret_len) {
      ret_len = 8;
      switch (data_type) {
        case DCT_S64_INT: {
          int64_t i64 = gen::read_svint60(val_loc);
          memcpy(ret_val, &i64, sizeof(int64_t));
        } break;
        case DCT_S64_DEC1 ... DCT_S64_DEC9: {
          int64_t i64 = gen::read_svint60(val_loc);
          double dbl = static_cast<double>(i64);
          dbl /= gen::pow10(data_type - DCT_S64_DEC1 + 1);
          *((double *)ret_val) = dbl;
        } break;
        case DCT_U64_INT: {
          uint64_t u64 = gen::read_svint61(val_loc);
          *((uint64_t *) ret_val) = u64;
        } break;
        case DCT_U64_DEC1 ... DCT_U64_DEC9: {
          uint64_t u64 = gen::read_svint61(val_loc);
          double dbl = static_cast<double>(u64);
          dbl /= gen::pow10(data_type - DCT_U64_DEC1 + 1);
          *((double *)ret_val) = dbl;
        } break;
        case DCT_U15_DEC1 ... DCT_U15_DEC2: {
          uint64_t u64 = gen::read_svint15(val_loc);
          double dbl = static_cast<double>(u64);
          dbl /= gen::pow10(data_type - DCT_U15_DEC1 + 1);
          *((double *)ret_val) = dbl;
        } break;
      }
    }

    void get_val(uint32_t node_id, int *in_size_out_value_len, void *ret_val, uint32_t *p_ptr_bit_count = nullptr) {
      if (ret_val == nullptr || in_size_out_value_len == nullptr)
        return;
      uint8_t *val_loc;
      uint8_t grp_no = 0;
      switch (encoding_type) {
        case 'u':
          val_loc = get_val_loc(node_id, p_ptr_bit_count, &grp_no);
          if (val_loc == nullptr) {
            *in_size_out_value_len = -1;
            return;
          }
          if (val_loc - grp_data[grp_no] == 1) {
            *in_size_out_value_len = 0;
            return;
          }
          *in_size_out_value_len = 8;
          switch (data_type) {
            case DCT_TEXT: case DCT_BIN: {
              uint8_t val_str_buf[max_len];
              gen::byte_str val_str(val_str_buf, max_len);
              get_val_str(val_str, val_loc - grp_data[grp_no], grp_no, max_len);
              size_t val_len = val_str.length();
              *in_size_out_value_len = val_len;
              memcpy(ret_val, val_str.data(), val_len);
            } break;
            default:
              convert_back(val_loc, ret_val, *in_size_out_value_len);
          }
          break;
        case 'w': {
          uint8_t *line_loc = (p_ptr_bit_count == nullptr || *p_ptr_bit_count == UINT32_MAX ?
                            get_words_loc(node_id, p_ptr_bit_count) : ptrs_loc + *p_ptr_bit_count);
          uint8_t val_type = (*line_loc & 0xC0);
          if (val_type == 0x40 || val_type == 0x80) {
            (*p_ptr_bit_count)++;
            *in_size_out_value_len = (val_type == 0x40 ? -1 : 0);
            return;
          }
          if (val_type == 0x00) {
            (*p_ptr_bit_count)++;
            *in_size_out_value_len = prev_val.size();
            memcpy(ret_val, prev_val.data(), prev_val.size());
            return;
          }
          int8_t vlen;
          uint32_t line_byt_len = gen::read_ovint32(line_loc, vlen, 2);
          line_loc += vlen;
          *p_ptr_bit_count += vlen;
          int line_len = 0;
          uint8_t *out_buf = (uint8_t *) ret_val;
          while (line_byt_len > 0) {
            uint32_t trie_leaf_id = gen::read_vint32(line_loc, &vlen);
            line_loc += vlen;
            *p_ptr_bit_count += vlen;
            line_byt_len -= vlen;
            vlen--;
            int word_len;
            word_tries[vlen]->reverse_lookup(trie_leaf_id, &word_len, out_buf + line_len);
            line_len += word_len;
          }
          *in_size_out_value_len = line_len;
          prev_val.clear();
          gen::append_byte_vec(prev_val, out_buf, line_len);
        } break;
        case 't':
          get_col_trie_val(node_id, in_size_out_value_len, ret_val);
          if (*in_size_out_value_len == -1)
            return;
          if (data_type != DCT_TEXT && data_type != DCT_BIN)
            convert_back((uint8_t *) ret_val, ret_val, *in_size_out_value_len);
          break;
        case 'd':
          get_delta_val(node_id, in_size_out_value_len, ret_val);
          break;
      }
    }

    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint8_t& grp_no) {
      uint8_t bit_len = code_lt_bit_len[node_byte];
      grp_no = code_lt_code_len[node_byte] & 0x0F;
      uint8_t code_len = code_lt_code_len[node_byte] >> 4;
      uint8_t node_val_bits = 8 - code_len;
      node_byte <<= code_len; // Lose the code
      node_byte >>= code_len;
      uint32_t ptr = node_byte; // & ((1 << node_val_bits) - 1);
      if (bit_len > 0) {
        if (ptr_bit_count == UINT32_MAX)
          ptr_bit_count = get_ptr_bit_count_tail(node_id);
        ptr |= (read_extra_ptr(ptr_bit_count, bit_len) << node_val_bits);
      }
      if (grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      // printf("Grp: %u, ptr: %u\n", grp_no, ptr);
      return ptr;
    }

    void get_tail_str(gen::byte_str& ret, uint32_t node_id, uint8_t node_byte, uint32_t& tail_ptr, uint32_t& ptr_bit_count, uint8_t& grp_no) {
      ret.clear();
      //ptr_bit_count = UINT32_MAX;
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      get_tail_str(ret, tail_ptr, grp_no);
    }

    uint8_t get_first_byte(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint32_t& tail_ptr, uint8_t& grp_no) {
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_data[grp_no];
      if (*tail != 0) {
        uint8_t tail_byte_str[dict_obj->max_tail_len];
        int tail_byte_len;
        inner_tries[grp_no]->reverse_lookup_from_node_id(tail_ptr, &tail_byte_len, tail_byte_str, true);
        return *tail_byte_str; //[tail_byte_len - 1];
      }
      tail += tail_ptr;
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

    //const int max_tailset_len = 129;
    void get_tail_str(gen::byte_str& ret, uint32_t tail_ptr, uint8_t grp_no) {
      uint8_t *tail = grp_data[grp_no];
      if (*tail != 0) {
        int tail_str_len;
        inner_tries[grp_no]->reverse_lookup_from_node_id(tail_ptr, &tail_str_len, ret.data(), false);
        ret.set_length(tail_str_len);
        return;
      }
      ret.clear();
      uint8_t *t = tail + tail_ptr;
      if (*t < 32) {
        uint8_t len_len;
        uint32_t bin_len = read_len(t, len_len);
        t += len_len;
        while (bin_len--)
          ret.append(*t++);
        return;
      }
      uint8_t byt = *t;
      while (byt > 31) {
        t++;
        ret.append(byt);
        byt = *t;
      }
      if (byt == 0)
        return;
      uint8_t len_len = 0;
      uint32_t sfx_len = read_len(t, len_len);
      uint8_t *t_end = tail + tail_ptr;
      t = t_end;
      do {
        byt = *t--;
      } while (byt != 0 && t > tail);
      do {
        byt = *t--;
      } while (byt > 31 && t >= tail);
      t += 2;
      uint8_t prev_str_buf[ret.get_limit()];
      gen::byte_str prev_str(prev_str_buf, ret.get_limit());
      byt = *t++;
      while (byt != 0) {
        prev_str.append(byt);
        byt = *t++;
      }
      uint8_t last_str_buf[ret.get_limit()];
      gen::byte_str last_str(last_str_buf, ret.get_limit());
      while (t < t_end) {
        last_str.clear();
        byt = *t;
        while (byt > 31) {
          t++;
          last_str.append(byt);
          byt = *t;
        }
        uint32_t prev_sfx_len = read_len(t, len_len);
        last_str.append(prev_str.data() + prev_str.length()-prev_sfx_len, prev_sfx_len);
        t += len_len;
        prev_str.clear();
        prev_str.append(last_str.data(), last_str.length());
      }
      ret.append(prev_str.data() + prev_str.length()-sfx_len, sfx_len);
    }
    void get_val_str(gen::byte_str& ret, uint32_t val_ptr, uint8_t grp_no, int max_valset_len) {
      uint8_t *val = grp_data[grp_no];
      ret.clear();
      uint8_t *t = val + val_ptr;
      if (*t < 32) {
        uint8_t len_len;
        uint32_t bin_len = read_len(t, len_len);
        t += len_len;
        while (bin_len--)
          ret.append(*t++);
        return;
      }
      int len_left = 0;
      if (t[1] == 0 || t[1] > 31) {
        uint8_t *end_t = t;
        do {
          t--;
        } while (*t > 31 && t >= val);
        t++;
        do {
          ret.append(*t++);
        } while (t <= end_t);
        if (*t == 0)
          return;
        while (*t++ > 31)
          len_left++;
        t--;
        if (*t == 0)
          return;
        ret.clear();
      }
      uint8_t len_len = 0;
      read_len(t, len_len);
      uint8_t *t_end = t;
      uint8_t byt;
      do {
        byt = *t--;
      } while (byt > 31 && t > val);
      while (byt != 0 && t > val) {
        byt = *t--;
      }
      do {
        byt = *t--;
      } while (byt > 31 && t >= val);
      t += 2;
      uint8_t prev_str_buf[max_valset_len];
      gen::byte_str prev_str(prev_str_buf, max_valset_len);
      byt = *t++;
      while (byt != 0) {
        prev_str.append(byt);
        byt = *t++;
      }
      uint8_t last_str_buf[max_valset_len];
      gen::byte_str last_str(last_str_buf, max_valset_len);
      while (t <= t_end) {
        byt = *t++;
        while (byt > 31) {
          last_str.append(byt);
          byt = *t++;
        }
        uint32_t prev_pfx_len = read_len(t - 1, len_len);
        prev_str.truncate(prev_pfx_len);
        prev_str.append(last_str.data(), last_str.length());
        t += len_len;
        t--;
        last_str.clear();
      }
      ret.append(prev_str.data(), prev_str.length() - len_left);
    }
};

class bv_lookup_tbl {
  private:
    uint8_t *lt_sel_loc;
    uint8_t *lt_rank_loc;
    uint32_t node_count;
    uint32_t node_set_count;
    uint32_t key_count;
    uint8_t *trie_loc;
    int bm_pos;
    bool lt_given;
    int lt_type;
  public:
    bv_lookup_tbl() {
      lt_given = true;
    }
    void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc, uint32_t _node_count, uint32_t _node_set_count, uint32_t _key_count, uint8_t *_trie_loc, int _bm_pos, int _lt_type) {
      lt_rank_loc = _lt_rank_loc;
      lt_sel_loc = _lt_sel_loc;
      node_count = _node_count;
      node_set_count = _node_set_count;
      key_count = _key_count;
      trie_loc = _trie_loc;
      bm_pos = _bm_pos;
      lt_type = _lt_type;
      lt_given = true;
      if (lt_rank_loc == nullptr) {
        lt_given = false;
        create_rank_lt_from_trie();
        create_select_lt_from_trie();
      }
    }
    ~bv_lookup_tbl() {
      if (!lt_given) {
        delete [] lt_rank_loc;
        delete [] lt_sel_loc;
      }
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
    void create_rank_lt_from_trie() {
      uint32_t node_id = (lt_type == BV_LT_TYPE_LEAF ? 0 : 2);
      uint32_t count = 0;
      uint32_t count3 = 0;
      uint8_t buf3[3];
      uint8_t pos3 = 0;
      memset(buf3, 0, 3);
      uint32_t lt_size = gen::get_lkup_tbl_size2(node_count, nodes_per_bv_block, 7);
      uint8_t *lt_pos = new uint8_t[lt_size];
      lt_rank_loc = lt_pos;
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      bm_mask = bm_init_mask << node_id;
      ctx_vars::read_flags(trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n,
          bm_leaf, bm_term, bm_child, bm_ptr);
      gen::copy_uint32(0, lt_pos); lt_pos += 4;
      do {
        if ((node_id % nodes_per_bv_block_n) == 0) {
          bm_mask = bm_init_mask;
          ctx_vars::read_flags(trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n,
              bm_leaf, bm_term, bm_child, bm_ptr);
        }
        lt_pos = write_bv3(node_id, count, count3, buf3, pos3, lt_pos);
        uint32_t ct = 0;
        switch (lt_type) {
          case BV_LT_TYPE_TERM:
            ct = (bm_mask & bm_term ? 1 : 0);
            break;
          case BV_LT_TYPE_CHILD:
            ct = (bm_mask & bm_child ? 1 : 0);
            break;
          case BV_LT_TYPE_LEAF:
            ct = (bm_mask & bm_leaf ? 1 : 0);
            break;
        }
        count += ct;
        count3 += ct;
        node_id++;
        bm_mask <<= 1;
      } while (node_id < node_count);
      memcpy(lt_pos, buf3, 3); lt_pos += 3;
      // extra (guard)
      gen::copy_uint32(count, lt_pos); lt_pos += 4;
      memcpy(lt_pos, buf3, 3); lt_pos += 3;
    }
    void create_select_lt_from_trie() {
      uint32_t node_id = (lt_type == BV_LT_TYPE_LEAF ? 0 : 2);
      uint32_t sel_count = 0;
      uint32_t lt_size = 0;
      if (lt_type == BV_LT_TYPE_LEAF)
        lt_size = gen::get_lkup_tbl_size2(key_count, sel_divisor, 3);
      else if (lt_type == BV_LT_TYPE_CHILD) {
        lt_size = 0;
        if (node_set_count > 1)
          lt_size = gen::get_lkup_tbl_size2(node_set_count - 1, sel_divisor, 3);
      } else
        lt_size = gen::get_lkup_tbl_size2(node_set_count, sel_divisor, 3);

      uint8_t *lt_pos = new uint8_t[lt_size];
      lt_sel_loc = lt_pos;
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      bm_mask = bm_init_mask << node_id;
      ctx_vars::read_flags(trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n,
          bm_leaf, bm_term, bm_child, bm_ptr);
      gen::copy_uint24(0, lt_pos); lt_pos += 3;
      do {
        if ((node_id % nodes_per_bv_block_n) == 0) {
          bm_mask = bm_init_mask;
          ctx_vars::read_flags(trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n,
              bm_leaf, bm_term, bm_child, bm_ptr);
        }
        bool node_qualifies_for_select = false;
        switch (lt_type) {
          case BV_LT_TYPE_TERM:
            node_qualifies_for_select = (bm_mask & bm_term ? true : false);
            break;
          case BV_LT_TYPE_CHILD:
            node_qualifies_for_select = (bm_mask & bm_child ? true : false);
            break;
          case BV_LT_TYPE_LEAF:
            node_qualifies_for_select = (bm_mask & bm_leaf ? true : false);
            break;
        }
        if (node_qualifies_for_select) {
          if (sel_count && (sel_count % sel_divisor) == 0) {
            uint32_t val_to_write = node_id / nodes_per_bv_block;
            gen::copy_uint24(val_to_write, lt_pos); lt_pos += 3;
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", sel_count, val_to_write);
          }
          sel_count++;
        }
        node_id++;
        bm_mask <<= 1;
      } while (node_id < node_count);
      gen::copy_uint24(node_count / nodes_per_bv_block, lt_pos);
    }
    int bin_srch_lkup_tbl(uint32_t first, uint32_t last, uint32_t given_count) {
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
    uint32_t block_rank(uint32_t node_id) {
      uint8_t *rank_ptr = lt_rank_loc + node_id / nodes_per_bv_block * width_of_bv_block;
      uint32_t rank = gen::read_uint32(rank_ptr);
      int pos = (node_id / nodes_per_bv_block_n) % (width_of_bv_block_n + 1);
      if (pos > 0) {
        uint8_t *bv3 = rank_ptr + 4;
        while (pos--)
          rank += bv3[pos];
      }
      return rank;
    }
    uint32_t rank(uint32_t node_id) {
      uint32_t rank = block_rank(node_id);
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n;
      uint64_t bm;
      gen::read_uint64(t + bm_pos, bm);
      uint64_t mask = bm_init_mask << (node_id % nodes_per_bv_block_n);
      // return rank + __popcountdi2(bm & (mask - 1));
      return rank + __builtin_popcountll(bm & (mask - 1));
    }
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
    void select(uint32_t& node_id, uint32_t target_count) {
      if (target_count == 0) {
        node_id = 2;
        return;
      }
      uint32_t block_count = block_select(target_count, node_id);
      if (block_count == target_count)
        return;
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n;
      uint64_t bm;
      gen::read_uint64(t + bm_pos, bm);

      int remaining = target_count - block_count - 1;
      uint64_t isolated_bit = _pdep_u64(1ULL << remaining, bm);
      size_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      // size_t bit_loc = find_nth_set_bit(bm, i) + 1;
      if (bit_loc == 65) {
        printf("WARNING: UNEXPECTED bit_loc=65, node_id: %u, nc: %u, bc: %u, ttc: %u\n", node_id, node_count, block_count, target_count);
        return;
      }
      node_id += bit_loc;

      // The performance of this is not too different from using bmi2. Keeping this for now
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

class static_dict : public static_dict_fwd {

  private:
    uint8_t *val_buf;
    size_t dict_size;
    size_t val_size;
    bool is_mmapped;

    uint32_t node_set_count;
    uint32_t fwd_cache_count;
    uint32_t rev_cache_count;
    uint32_t fwd_cache_max_node_id;
    uint32_t rev_cache_max_node_id;
    uint16_t max_level;
    uint8_t *fwd_cache_loc;
    uint8_t *rev_cache_loc;
    uint8_t *term_lt_loc;
    uint8_t *child_lt_loc;
    uint8_t *leaf_lt_loc;
    uint8_t *term_select_lkup_loc;
    uint8_t *child_select_lkup_loc;
    uint8_t *leaf_select_lkup_loc;
    uint8_t *trie_loc;
    uint8_t *val_table_loc;

    bool to_release_dict_buf;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

  public:
    uint8_t *dict_buf;
    bldr_options *opts;
    uint32_t node_count;
    uint32_t val_count;
    uint8_t *names_loc;
    char *names_start;
    const char *column_encoding;
    uint32_t last_exit_loc;
    min_pos_stats min_stats;
    uint8_t *sec_cache_loc;
    ptr_data_map tail_map;
    ptr_data_map *val_map;
    bv_lookup_tbl term_lt;
    bv_lookup_tbl child_lt;
    bv_lookup_tbl leaf_lt;
    int max_key_len;
    int max_val_len;
    static_dict() {
      init_vars();
    }

    void init_vars() {

      dict_buf = nullptr;
      val_buf = nullptr;
      val_map = nullptr;
      is_mmapped = false;
      last_exit_loc = 0;
      to_release_dict_buf = true;

      max_key_len = max_tail_len = max_level = 0;
      fwd_cache_count = rev_cache_count = 0;
      fwd_cache_max_node_id = rev_cache_max_node_id = 0;
      fwd_cache_loc = rev_cache_loc = sec_cache_loc = nullptr;
      term_select_lkup_loc = term_lt_loc = 0;
      child_select_lkup_loc = child_lt_loc = leaf_select_lkup_loc = 0;
      leaf_lt_loc = trie_loc = 0;

    }

    virtual ~static_dict() {
      if (is_mmapped)
        map_unmap();
      if (dict_buf != nullptr) {
        if (to_release_dict_buf)
          free(dict_buf);
      }
      if (val_map != nullptr) {
        delete [] val_map;
      }
    }

    void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    void load_into_vars() {

      val_count = gen::read_uint16(dict_buf + 4);
      names_loc = dict_buf + gen::read_uint32(dict_buf + 6);
      val_table_loc = dict_buf + gen::read_uint32(dict_buf + 10);
      node_count = gen::read_uint32(dict_buf + 14);
      opts = (bldr_options *) dict_buf + gen::read_uint32(dict_buf + 18);
      node_set_count = gen::read_uint32(dict_buf + 22);
      key_count = gen::read_uint32(dict_buf + 26);
      names_start = (char *) names_loc + (val_count + (key_count > 0 ? 3 : 2)) * sizeof(uint16_t);
      column_encoding = names_start + gen::read_uint16(names_loc);

      max_val_len = gen::read_uint32(dict_buf + 34);

      if (key_count > 0) {
        max_key_len = gen::read_uint32(dict_buf + 30);
        max_tail_len = gen::read_uint16(dict_buf + 38) + 1;
        max_level = gen::read_uint16(dict_buf + 40);
        fwd_cache_count = gen::read_uint32(dict_buf + 42);
        rev_cache_count = gen::read_uint32(dict_buf + 46);
        fwd_cache_max_node_id = gen::read_uint32(dict_buf + 50);
        rev_cache_max_node_id = gen::read_uint32(dict_buf + 54);
        memcpy(&min_stats, dict_buf + 58, 4);
        fwd_cache_loc = dict_buf + gen::read_uint32(dict_buf + 62);
        rev_cache_loc = dict_buf + gen::read_uint32(dict_buf + 66);
        sec_cache_loc = dict_buf + gen::read_uint32(dict_buf + 70);
        if (sec_cache_loc == dict_buf)
          sec_cache_loc = nullptr;

        term_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 74);
        term_lt_loc = dict_buf + gen::read_uint32(dict_buf + 78);
        child_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 82);
        child_lt_loc = dict_buf + gen::read_uint32(dict_buf + 86);
        leaf_select_lkup_loc = dict_buf + gen::read_uint32(dict_buf + 90);
        leaf_lt_loc = dict_buf + gen::read_uint32(dict_buf + 94);
        uint8_t *trie_tail_ptrs_data_loc = dict_buf + gen::read_uint32(dict_buf + 98);

        uint32_t tail_size = gen::read_uint32(trie_tail_ptrs_data_loc);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 4;
        trie_loc = tails_loc + tail_size;
        tail_map.init(dict_buf, this, trie_loc, tails_loc, node_count, true);

        if (term_select_lkup_loc == dict_buf) term_select_lkup_loc = nullptr;
        if (term_lt_loc == dict_buf) term_lt_loc = nullptr;
        if (child_select_lkup_loc == dict_buf) child_select_lkup_loc = nullptr;
        if (child_lt_loc == dict_buf) child_lt_loc = nullptr;
        if (leaf_select_lkup_loc == dict_buf) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == dict_buf) leaf_lt_loc = nullptr;

        leaf_lt.init(leaf_lt_loc, leaf_select_lkup_loc, node_count, node_set_count, key_count, trie_loc, 0, BV_LT_TYPE_LEAF);
        term_lt.init(term_lt_loc, term_select_lkup_loc, node_count, node_set_count, key_count, trie_loc, 8, BV_LT_TYPE_TERM);
        child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, node_set_count, key_count, trie_loc, 16, BV_LT_TYPE_CHILD);
      }

      if (val_count > 0) {
        val_map = new ptr_data_map[val_count];
        for (uint32_t i = 0; i < val_count; i++) {
          val_buf = dict_buf + gen::read_uint32(val_table_loc + i * sizeof(uint32_t));
          val_map[i].init(val_buf, this, trie_loc, val_buf, node_count, false);
        }
      }

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
      dict_size = file_stat.st_size;
      dict_buf = (uint8_t *) malloc(dict_size);

      FILE *fp = fopen(filename, "rb");
      size_t bytes_read = fread(dict_buf, 1, dict_size, fp);
      if (bytes_read != dict_size) {
        printf("Read error: [%s], %lu, %lu\n", filename, dict_size, bytes_read);
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

    void find_child(uint32_t& node_id, uint32_t child_count) {
      term_lt.select(node_id, child_count);
    }

    int find_in_cache(const uint8_t *key, int key_len, int& key_pos, uint32_t& node_id) {
      if (fwd_cache_count == 0 || node_id > fwd_cache_max_node_id)
        return -1;
      uint8_t key_byte = key[key_pos];
      uint32_t cache_mask = fwd_cache_count - 1;
      fwd_cache *cche0 = (fwd_cache *) fwd_cache_loc;
      int times = MDX_CACHE_TIMES;
      uint32_t cache_idx = node_id;
      do {
        cache_idx = (node_id ^ (cache_idx << MDX_CACHE_SHIFT) ^ key_byte) & cache_mask;
        fwd_cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = gen::read_uint24(&cche->parent_node_id1);
        if (node_id == cache_node_id) {
          if (cche->node_byte == key_byte) {
            key_pos++;
            if (key_pos < key_len) {
              node_id = gen::read_uint24(&cche->child_node_id1);
              key_byte = key[key_pos];
              cache_idx = node_id;
              times = MDX_CACHE_TIMES;
              continue;
            }
            node_id += cche->node_offset;
            uint8_t *t = trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n;
            uint64_t bm_leaf;
            gen::read_uint64(t, bm_leaf);
            uint64_t bm_mask = (bm_init_mask << (node_id % 64));
            if (bm_leaf & bm_mask) {
              return 0;
            } else {
              return -1;
            }
          }
        }
      } while (--times);
      return -1;
    }

    uint8_t *get_t(uint32_t node_id, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr, uint64_t& bm_mask) {
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block_n * bytes_per_bv_block_n;
      t = ctx_vars::read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
      uint32_t node_id_rem = node_id % nodes_per_bv_block_n;
      t += node_id_rem;
      bm_mask = (bm_init_mask << node_id_rem);
      return t;
    }

    bool lookup(const uint8_t *key, int key_len, uint32_t& node_id) {
      #ifndef MDX_NO_NULLS
      if (key == nullptr) {
        node_id = 0;
        return trie_loc[32] == 0xFF;
      }
      if (key_len == 0) {
        node_id = 1;
        return trie_loc[33] == 0xFF;
      }
      #endif
      int key_pos = 0;
      node_id = 2;
      uint8_t tail_str_buf[max_tail_len];
      gen::byte_str tail_str(tail_str_buf, max_tail_len);
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      uint8_t trie_byte;
      uint8_t grp_no;
      uint32_t tail_ptr = 0;
      uint32_t ptr_bit_count;
      uint8_t *t = trie_loc;
      uint8_t key_byte = key[key_pos];
      do {
        int ret = find_in_cache(key, key_len, key_pos, node_id);
        if (ret == 0)
          return true;
        key_byte = key[key_pos];
        t = get_t(node_id, bm_leaf, bm_term, bm_child, bm_ptr, bm_mask);
        #ifndef MDX_NO_DART
        if (sec_cache_loc != nullptr && (bm_mask & bm_child) == 0 && (bm_mask & bm_leaf) == 0) {
          uint8_t min_offset = sec_cache_loc[((*t - min_stats.min_len) * 256) + key_byte];
          if ((node_id % nodes_per_bv_block_n) + min_offset < nodes_per_bv_block_n) {
            t += min_offset;
            node_id += min_offset;
            bm_mask <<= min_offset;
          } else {
            node_id += min_offset;
            t = get_t(node_id, bm_leaf, bm_term, bm_child, bm_ptr, bm_mask);
          }
        }
        #endif
        ptr_bit_count = UINT32_MAX;
        do {
          uint8_t node_byte = trie_byte = *t++;
          if (bm_mask & bm_ptr)
            trie_byte = tail_map.get_first_byte(node_byte, node_id, ptr_bit_count, tail_ptr, grp_no);
          #ifndef MDX_IN_ORDER
            if (key_byte == trie_byte)
              break;
          #else
            if (key_byte <= trie_byte)
              break;
          #endif
          if (bm_mask & bm_term)
            return false;
          bm_mask <<= 1;
          node_id++;
          if ((node_id % nodes_per_bv_block_n) == 0) {
            bm_mask = bm_init_mask;
            t = ctx_vars::read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
          }
        } while (1);
        if (key_byte == trie_byte) {
          int cmp = 0;
          int tail_len = 1;
          if (bm_mask & bm_ptr) {
            tail_map.get_tail_str(tail_str, tail_ptr, grp_no);
            tail_len = tail_str.length();
            cmp = gen::compare(tail_str.data(), tail_len, key + key_pos, key_len - key_pos);
          }
          key_pos += tail_len;
          if (key_pos < key_len && (cmp == 0 || abs(cmp) - 1 == tail_len)) {
            if ((bm_mask & bm_child) == 0)
              return false;
            term_lt.select(node_id, child_lt.rank(node_id) + 1);
            continue;
          }
          if (cmp == 0 && key_pos == key_len && (bm_leaf & bm_mask))
            return true;
        }
        return false;
      } while (node_id < node_count);
      return false;
    }

    bool get_col_val(uint32_t node_id, int col_val_idx, int *in_size_out_value_len, void *val, uint32_t *p_ptr_bit_count = nullptr) {
      val_map[col_val_idx].get_val(node_id, in_size_out_value_len, val, p_ptr_bit_count);
      return true;
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, void *val) {
      uint32_t node_id;
      return get(key, key_len, in_size_out_value_len, val, node_id);
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, void *val, uint32_t& node_id) {
      bool is_found = lookup(key, key_len, node_id);
      if (is_found && node_id >= 0) {
        if (val_count > 0)
          val_map[0].get_val(node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
    }

    void push_to_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      ctx.cur_idx++;
      cv.tail.clear();
      update_ctx(ctx, cv);
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

    void insert_into_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      insert_arr(ctx.child_count, ctx.cur_idx, 0, cv.child_count);
      insert_arr(ctx.node_path, ctx.cur_idx, 0, cv.node_id);
      insert_arr(ctx.last_tail_len, ctx.cur_idx, 0, (uint16_t) cv.tail.length());
      ctx.cur_idx++;
    }

    void update_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      ctx.child_count[ctx.cur_idx] = cv.child_count;
      ctx.node_path[ctx.cur_idx] = cv.node_id;
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = cv.tail.length();
      memcpy(ctx.key + ctx.key_len, cv.tail.data(), cv.tail.length());
      ctx.key_len += cv.tail.length();
    }

    void clear_last_tail(dict_iter_ctx& ctx) {
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
    }

    void pop_from_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      read_from_ctx(ctx, cv);
    }

    void read_from_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      cv.child_count = ctx.child_count[ctx.cur_idx];
      cv.node_id = ctx.node_path[ctx.cur_idx];
      cv.init_cv_from_node_id(trie_loc);
    }

    int next(dict_iter_ctx& ctx, uint8_t *key_buf, uint8_t *val_buf = nullptr, int *val_buf_len = nullptr) {
      ctx_vars cv;
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      read_from_ctx(ctx, cv);
      while (cv.node_id < 2) {
        if (cv.node_id == 0)
          cv.read_flags_block_begin();
        uint8_t trie_byte = *cv.t;
        cv.t++;
        cv.node_id++;
        cv.bm_mask <<= 1;
        if (cv.node_id == 1 && trie_byte == 0xFF) {
          if (val_count > 0)
            val_map[0].get_val(cv.node_id, val_buf_len, val_buf);
          update_ctx(ctx, cv);
          return -1; // null
        }
        if (cv.node_id == 2 && trie_byte == 0xFF) {
          if (val_count > 0)
            val_map[0].get_val(cv.node_id, val_buf_len, val_buf);
          update_ctx(ctx, cv);
          return 0; // empty
        }
      }
      while (cv.node_id < node_count) {
        cv.read_flags_block_begin();
        if ((cv.bm_mask & cv.bm_child) == 0 && (cv.bm_mask & cv.bm_leaf) == 0) {
          cv.t++;
          cv.bm_mask <<= 1;
          cv.node_id++;
          continue;
        }
        if (cv.bm_mask & cv.bm_leaf) {
          if (ctx.to_skip_first_leaf) {
            if ((cv.bm_mask & cv.bm_child) == 0) {
              while (cv.bm_mask & cv.bm_term) {
                if (ctx.cur_idx == 0)
                  return -2;
                pop_from_ctx(ctx, cv);
                cv.read_flags_block_begin();
              }
              cv.bm_mask <<= 1;
              cv.node_id++;
              cv.t++;
              update_ctx(ctx, cv);
              ctx.to_skip_first_leaf = false;
              continue;
            }
          } else {
            if (cv.bm_mask & cv.bm_ptr)
              tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no);
            else {
              cv.tail.clear();
              cv.tail.append(*cv.t);
            }
            update_ctx(ctx, cv);
            memcpy(key_buf, ctx.key, ctx.key_len);
            if (val_count > 0)
              val_map[0].get_val(cv.node_id, val_buf_len, val_buf);
            ctx.to_skip_first_leaf = true;
            return ctx.key_len;
          }
        }
        ctx.to_skip_first_leaf = false;
        if (cv.bm_mask & cv.bm_child) {
          cv.child_count++;
          if (cv.bm_mask & cv.bm_ptr)
            tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no);
          else {
            cv.tail.clear();
            cv.tail.append(*cv.t);
          }
          update_ctx(ctx, cv);
          find_child(cv.node_id, cv.child_count);
          cv.child_count = child_lt.rank(cv.node_id);
          cv.init_cv_from_node_id(trie_loc);
          cv.ptr_bit_count = UINT32_MAX;
          push_to_ctx(ctx, cv);
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

    uint32_t get_max_val_len(int col_val_idx) {
      return val_map[col_val_idx].max_len;
    }

    uint32_t get_leaf_rank(uint32_t node_id) {
      return leaf_lt.rank(node_id);
    }

    bool reverse_lookup(uint32_t leaf_id, int *in_size_out_key_len, uint8_t *ret_key, int *in_size_out_value_len = nullptr, void *ret_val = nullptr, bool return_first_byte = false, bool to_reverse = true) {
      leaf_id++;
      uint32_t node_id;
      leaf_lt.select(node_id, leaf_id);
      node_id--;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, in_size_out_value_len, ret_val, 0, nullptr, return_first_byte, to_reverse);
    }

    bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, int *in_size_out_value_len = nullptr, void *ret_val = nullptr, int cmp = 0, dict_iter_ctx *ctx = nullptr, bool return_first_byte = false, bool to_reverse = true) {
      bool ret = reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, return_first_byte);
      if (ret_val != nullptr && val_map[0].exists())
        val_map[0].get_val(node_id, in_size_out_value_len, ret_val);
      int key_len = *in_size_out_key_len;
      if (key_len > 1 && to_reverse) {
        int i = key_len / 2;
        while (i--) {
          uint8_t b = ret_key[i];
          ret_key[i] = ret_key[key_len - i - 1];
          ret_key[key_len - i - 1] = b;
        }
      }
      return ret;
    }

    bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, bool return_first_byte = false) {
      ctx_vars cv;
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      int key_len = 0;
      cv.node_id = node_id;
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
      cv.node_id++;
      uint32_t cache_mask = rev_cache_count - 1;
      do {
        cv.node_id--;
        cv.t = trie_loc + cv.node_id / nodes_per_bv_block_n * bytes_per_bv_block_n;
        cv.t = ctx_vars::read_flags(cv.t, cv.bm_leaf, cv.bm_term, cv.bm_child, cv.bm_ptr);
        cv.t += cv.node_id % nodes_per_bv_block_n;
        cv.bm_mask = bm_init_mask << (cv.node_id % nodes_per_bv_block_n);
        if (cv.bm_mask & cv.bm_ptr) {
          cv.ptr_bit_count = UINT32_MAX;
          tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no);
          for (int i = cv.tail.length() - 1; i >= 0; i--)
            ret_key[key_len++] = cv.tail[i];
        } else {
          ret_key[key_len++] = *cv.t;
        }
        if (return_first_byte) {
          *in_size_out_key_len = 1;
          return true;
        }
        bool do_select = (rev_cache_count == 0);
        if (!do_select) {
          nid_cache *cche0 = (nid_cache *) rev_cache_loc;
          uint32_t cache_idx = cv.node_id;
          int times = MDX_CACHE_TIMES;
          do {
            do_select = false;
            cache_idx = ((cache_idx << MDX_CACHE_SHIFT) ^ cv.node_id) & cache_mask;
            nid_cache *cche = cche0 + cache_idx;
            uint32_t cache_node_id = gen::read_uint24(&cche->child_node_id1);
            if (cv.node_id == cache_node_id) {
              cv.node_id = gen::read_uint24(&cche->parent_node_id1) + 1;
              break;
            }
            do_select = true;
          } while (--times);
        }
        if (do_select)
          child_lt.select(cv.node_id, term_lt.rank(cv.node_id));
      } while (cv.node_id > 2);
      *in_size_out_key_len = key_len;
      return true;
    }

    // bool find_first(const uint8_t *prefix, int prefix_len, dict_iter_ctx& ctx, bool for_next = false) {
    //   int cmp;
    //   uint32_t lkup_node_id;
    //   bool is_found = lookup(prefix, prefix_len, lkup_node_id, &cmp);
    //   if (!is_found && cmp == 0)
    //     cmp = 10000;
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

    const char *get_table_name() {
      return names_start + gen::read_uint16(names_loc + 2);
    }

    const char *get_column_name(int i) {
      return names_start + gen::read_uint16(names_loc + (i + 2) * 2);
    }

    const char *get_column_types() {
      return names_start;
    }

    const char get_column_type(int i) {
      return names_start[i];
    }

    const char *get_column_encodings() {
      return column_encoding;
    }

    const char get_column_encoding(int i) {
      return column_encoding[i];
    }

    const uint16_t get_column_count() {
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

    uint32_t leaf_rank(uint32_t node_id) {
      return leaf_lt.rank(node_id);
    }

    static_dict_fwd *new_instance(uint8_t *mem) {
      static_dict *dict = new static_dict();
      dict->load_from_mem(mem);
      return dict;
    }

    uint32_t get_last_exit_loc() {
      return last_exit_loc;
    }

};

}
#endif
