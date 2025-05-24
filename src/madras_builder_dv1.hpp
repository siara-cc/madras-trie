#ifndef builder_H
#define builder_H

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <cstring>
#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>
#include <time.h>
#include <functional> // for std::function

#include "common_dv1.hpp"
#include "../../leopard-trie/src/leopard.hpp"

#include "../../flavic48/src/flavic48.hpp"

#include "../../ds_common/src/bv.hpp"
#include "../../ds_common/src/gen.hpp"
#include "../../ds_common/src/vint.hpp"
#include "../../ds_common/src/huffman.hpp"

namespace madras_dv1 {

#define MDX_AFFIX_FULL 0x01
#define MDX_AFFIX_PARTIAL 0x02
#define MDX_AFFIXES 0x03
#define MDX_HAS_AFFIX 0x04
#define MDX_HAS_CHILD 0x10

typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uint8_t *> byte_ptr_vec;

struct tail_token {
  uint32_t token_pos;
  uint32_t token_len;
  uint32_t fwd_pos;
  uint32_t cmp_max;
};

struct bldr_min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
  bldr_min_pos_stats() {
    max_b = max_len = 0;
    min_b = min_len = 0xFF;
  }
};

struct trie_parts {
  uint32_t fwd_cache_count;
  uint32_t fwd_cache_size;
  uint32_t fwd_cache_max_node_id;
  uint32_t rev_cache_count;
  uint32_t rev_cache_size;
  uint32_t rev_cache_max_node_id;
  uint32_t sec_cache_count;
  uint32_t sec_cache_size;
  uint32_t louds_rank_lt_loc;
  uint32_t louds_sel1_lt_loc;
  uint32_t trie_flags_loc;
  uint32_t louds_rank_lt_sz;
  uint32_t louds_sel1_lt_sz;
  uint32_t tail_flags_loc;
  uint32_t term_rank_lt_sz;
  uint32_t child_rank_lt_sz;
  uint32_t leaf_rank_lt_sz;
  uint32_t tail_rank_lt_sz;
  uint32_t term_select_lt_sz;
  uint32_t child_select_lt_sz;
  uint32_t leaf_select_lt_sz;
  uint32_t opts_loc;
  uint32_t opts_size;
  uint32_t fwd_cache_loc;
  uint32_t rev_cache_loc;
  uint32_t sec_cache_loc;
  uint32_t term_select_lkup_loc;
  uint32_t term_rank_lt_loc;
  uint32_t child_rank_lt_loc;
  uint32_t trie_tail_ptrs_data_loc;
  uint32_t trie_tail_ptrs_data_sz;
  uint32_t leaf_select_lkup_loc;
  uint32_t leaf_rank_lt_loc;
  uint32_t tail_rank_lt_loc;
  uint32_t child_select_lkup_loc;
  uint32_t names_loc;
  uint32_t names_sz;
  uint32_t col_val_table_loc;
  uint32_t col_val_table_sz;
  uint32_t col_val_loc0;
  uint32_t null_val_loc;
  uint32_t empty_val_loc;
  uint32_t null_empty_sz;
  uint32_t total_idx_size;
  bldr_min_pos_stats min_stats;
};

void output_byte(uint8_t b, FILE *fp, std::vector<uint8_t> *out_vec) {
  if (fp == NULL)
    out_vec->push_back(b);
  else
    fputc(b, fp);
}

void output_u32(uint32_t u32, FILE *fp, std::vector<uint8_t> *out_vec) {
  if (fp == NULL)
    gen::append_uint32(u32, *out_vec);
  else
    gen::write_uint32(u32, fp);
}

void output_u64(uint32_t u64, FILE *fp, std::vector<uint8_t> *out_vec) {
  if (fp == NULL)
    gen::append_uint64(u64, *out_vec);
  else
    gen::write_uint64(u64, fp);
}

void output_u16(uint32_t u16, FILE *fp, std::vector<uint8_t> *out_vec) {
  if (fp == NULL)
    gen::append_uint16(u16, *out_vec);
  else
    gen::write_uint16(u16, fp);
}

void output_u24(uint32_t u24, FILE *fp, std::vector<uint8_t> *out_vec) {
  if (fp == NULL)
    gen::append_uint24(u24, *out_vec);
  else
    gen::write_uint24(u24, fp);
}

void output_bytes(const uint8_t *b, size_t len, FILE *fp, std::vector<uint8_t> *out_vec) {
  if (fp == NULL) {
    for (size_t i = 0; i < len; i++)
      out_vec->push_back(b[i]);
  } else
    fwrite(b, 1, len, fp);
}

void output_align8(size_t nopad_size, FILE *fp, std::vector<uint8_t> *out_vec) {
  if ((nopad_size % 8) == 0)
    return;
  size_t remaining = 8 - (nopad_size % 8);
  if (fp == NULL) {
    for (size_t i = 0; i < remaining; i++)
      out_vec->push_back(' ');
  } else {
    const char *padding = "       ";
    fwrite(padding, 1, remaining, fp);
  }
}

typedef int (*cmp_fn) (const uint8_t *v1, int len1, const uint8_t *v2, int len2);

struct uniq_info_base {
  uint32_t pos;
  uint32_t len;
  uint32_t arr_idx;
  uint32_t freq_count;
  union {
    uint32_t ptr;
    uint32_t repeat_freq;
  };
  uint8_t grp_no;
  uint8_t flags;
};

struct uniq_info : public uniq_info_base {
  uint32_t link_arr_idx;
  uint32_t cmp;
  uint32_t cmp_min;
  uint32_t cmp_max;
  uniq_info(uint32_t _pos, uint32_t _len, uint32_t _arr_idx, uint32_t _freq_count) {
    memset(this, '\0', sizeof(*this));
    pos = _pos; len = _len;
    arr_idx = _arr_idx;
    cmp_min = 0xFFFFFFFF;
    freq_count = _freq_count;
    link_arr_idx = 0xFFFFFFFF;
  }
};
typedef std::vector<uniq_info_base *> uniq_info_vec;

struct freq_grp {
  uint32_t grp_no;
  uint32_t grp_log2;
  uint32_t grp_limit;
  uint32_t count;
  uint32_t freq_count;
  uint32_t grp_size;
  uint8_t code;
  uint8_t code_len;
};

class builder_fwd {
  public:
    FILE *fp;
    byte_vec *out_vec;
    bldr_options *opts;
    uint16_t pk_col_count;
    uint16_t trie_level;
    builder_fwd(uint16_t _pk_col_count)
      : pk_col_count (_pk_col_count) {
    }
    virtual ~builder_fwd() {
    }
    virtual leopard::trie *get_memtrie() = 0;
    virtual builder_fwd *new_instance() = 0;
    virtual leopard::node_set_vars insert(const uint8_t *key, int key_len, uint32_t val_pos = UINT32_MAX) = 0;
    virtual uint32_t build() = 0;
    virtual void set_all_vals(gen::byte_blocks *_all_vals, bool to_delete_prev = true) = 0;
    virtual uint32_t write_trie(const char *filename = NULL) = 0;
    virtual uint32_t build_kv(bool to_build_trie = true) = 0;
    virtual void write_kv(bool to_close = true, const char *filename = NULL) = 0;
    virtual bldr_options *get_opts() = 0;
};

class ptr_groups {
  private:
    builder_fwd *bldr;
    std::vector<freq_grp> freq_grp_vec;
    std::vector<byte_vec> grp_data;
    byte_vec ptrs;
    gen::int_bit_vector flat_ptr_bv;
    gen::bit_vector<uint64_t> null_bv;
    byte_vec ptr_lookup_tbl;
    byte_vec idx2_ptrs_map;
    int last_byte_bits;
    char enc_type;
    char data_type;
    uint16_t pk_col_count;
    bool dessicate;
    uint8_t ptr_lkup_tbl_ptr_width;
    uint8_t len_grp_no;
    uint8_t rpt_grp_no;
    uint8_t rpt_seq_grp_no;
    uint8_t sug_col_width;
    uint8_t sug_col_height;
    uint8_t idx_limit;
    uint8_t start_bits;
    uint8_t step_bits_idx;
    uint8_t step_bits_rest;
    uint8_t idx_ptr_size;
    uint8_t reserved8_1;
    uint8_t reserved8_2;
    uint32_t next_idx;
    uint32_t ptr_lookup_tbl_sz;
    uint32_t ptr_lookup_tbl_loc;
    uint32_t grp_data_loc;
    uint32_t grp_data_size;
    uint32_t grp_ptrs_loc;
    uint32_t idx2_ptrs_map_loc;
    uint32_t null_bv_loc;
    uint32_t idx2_ptr_count;
    uint64_t tot_ptr_bit_count;
    uint32_t max_len;
    uint32_t reserved32_1;
  public:
    std::vector<builder_fwd *> inner_tries;
    size_t inner_trie_start_grp;
    uniq_info_base rpt_ui;
    ptr_groups() {
      reset();
    }
    ~ptr_groups() {
      reset_inner_tries();
    }
    void init(builder_fwd *_bldr_obj, int _step_bits_idx, int _step_bits_rest) {
      bldr = _bldr_obj;
      step_bits_idx = _step_bits_idx;
      step_bits_rest = _step_bits_rest;
    }
    void reset_inner_tries() {
      for (size_t i = 0; i < inner_tries.size(); i++)
        delete inner_tries[i];
      inner_tries.resize(0);
    }
    void reset() {
      freq_grp_vec.resize(0);
      grp_data.resize(0);
      inner_tries.resize(0);
      ptrs.clear();
      idx2_ptrs_map.resize(0);
      null_bv.reset();
      next_idx = 0;
      inner_trie_start_grp = 0;
      last_byte_bits = 64;
      gen::append_uint64(0, ptrs);
      max_len = 0;
      reserved32_1 = 0;
      len_grp_no = 0;
      rpt_grp_no = 0;
      rpt_seq_grp_no = 0;
      sug_col_width = 0;
      sug_col_height = 0;
      idx_limit = 0;
      // start_bits = 0; // initialised in set_idx_info
      // step_bits_idx = 0; // initialised in init()
      // step_bits_rest = 0;
      idx_ptr_size = 3;
      reserved8_1 = 0;
      reserved8_2 = 0;
    }
    uint32_t get_idx_limit() {
      return idx_limit;
    }
    uint32_t idx_map_arr[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    void set_idx_info(int _start_bits, uint8_t new_idx_limit, uint8_t _idx_ptr_size) {
      start_bits = _start_bits;
      idx_limit = new_idx_limit;
      idx_ptr_size = _idx_ptr_size;
      for (uint32_t i = 1; i <= idx_limit; i++) {
        idx_map_arr[i] = idx_map_arr[i - 1] + pow(2, _start_bits) * idx_ptr_size;
        _start_bits += step_bits_idx;
        //gen::gen_printf("idx_map_arr[%d] = %d\n", i, idx_map_arr[i]);
      }
    }
    void set_max_len(uint32_t _max_len) {
      max_len = _max_len;
    }
    void set_null(size_t idx) {
      null_bv.set(idx, true);
    }
    void set_col_sug(uint8_t _sug_width, uint8_t _sug_height) {
      sug_col_width = _sug_width;
      sug_col_height = _sug_height;
    }
    void set_grp_nos(uint8_t _len_grp_no, uint8_t _rpt_grp_no, uint8_t _rpt_seq_grp_no) {
      len_grp_no = _len_grp_no;
      rpt_grp_no = _rpt_grp_no;
      rpt_seq_grp_no = _rpt_seq_grp_no;
    }
    int get_idx_ptr_size() {
      return idx_ptr_size;
    }
    uint32_t *get_idx_map_arr() {
      return idx_map_arr;
    }
    uint32_t get_idx2_ptrs_count() {
      return idx2_ptrs_map.size() / get_idx_ptr_size();
    }
    void clear_freq_grps() {
      freq_grp_vec.clear();
    }
    void add_freq_grp(freq_grp freq_grp, bool just_add = false) {
      if (freq_grp.grp_no > idx_limit && !just_add)
        freq_grp.grp_size = 2;
      freq_grp_vec.push_back(freq_grp);
    }
    uint32_t check_next_grp(uint8_t grp_no, uint32_t cur_limit, uint32_t len) {
      if (grp_no >= bldr->get_opts()->max_groups)
        return cur_limit;
      if (grp_no <= idx_limit) {
        if (next_idx == cur_limit)
          return pow(2, log2(cur_limit) + step_bits_idx);
      } else {
        if ((freq_grp_vec[grp_no].grp_size + len) > cur_limit)
          return pow(2, log2(cur_limit) + step_bits_rest);
      }
      return cur_limit;
    }
    uint32_t next_grp(uint8_t& grp_no, uint32_t cur_limit, uint32_t len, uint32_t tot_freq_count, bool force_next_grp = false, bool just_add = false) {
      if (grp_no >= bldr->get_opts()->max_groups) // reeval curlimit?
        return cur_limit;
      bool next_grp = force_next_grp;
      if (!next_grp) {
        if (grp_no <= idx_limit) {
          if (next_idx == cur_limit) {
            next_grp = true;
            next_idx = 0;
          }
        } else {
          if ((freq_grp_vec[grp_no].grp_size + len) > cur_limit)
            next_grp = true;
        }
      }
      if (next_grp) {
        if (grp_no < idx_limit)
          cur_limit = pow(2, log2(cur_limit) + step_bits_idx);
        else
          cur_limit = pow(2, log2(cur_limit) + step_bits_rest);
        grp_no++;
        freq_grp fg = {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0, 0, 0};
        add_freq_grp(fg, just_add);
      }
      return cur_limit;
    }
    void update_current_grp(uint32_t grp_no, int32_t len, int32_t freq, uint32_t count = 0) {
      freq_grp_vec[grp_no].grp_size += len;
      freq_grp_vec[grp_no].freq_count += freq;
      if (count > 0)
        freq_grp_vec[grp_no].count += count;
      else
        freq_grp_vec[grp_no].count += (len < 0 ? -1 : 1);
      next_idx++;
    }
    uint32_t append_ptr2_idx_map(uint32_t grp_no, uint32_t _ptr) {
      if (idx2_ptrs_map.size() == idx_map_arr[grp_no - 1])
        next_idx = 0;
      if (idx_ptr_size == 2)
        gen::append_uint16(_ptr, idx2_ptrs_map);
      else
        gen::append_uint24(_ptr, idx2_ptrs_map);
      return next_idx++;
    }
    byte_vec& get_data(size_t grp_no) {
      grp_no--;
      while (grp_no >= grp_data.size()) {
        byte_vec data;
        data.push_back(0);
        data.push_back(15);
        grp_data.push_back(data);
      }
      return grp_data[grp_no];
    }
    uint32_t get_set_len_len(uint32_t len, byte_vec *vec = NULL) {
      uint32_t len_len = 0;
      uint8_t first_byte = 0x08;
      do {
        len_len++;
        if (vec != NULL)
          vec->push_back((len & 0x07) | 0x10 | first_byte);
        first_byte = 0;
          len >>= 3;
      } while (len > 0);
      return len_len;
    }
    uint32_t append_text(uint32_t grp_no, const uint8_t *val, uint32_t len, bool append0 = false) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (append0)
        grp_data_vec.push_back(15);
      return ptr;
    }
    uint32_t append_bin_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len, char data_type = MST_BIN) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      if (data_type == MST_TEXT || data_type == MST_BIN)
        gen::append_vint32(grp_data_vec, len);
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      return ptr;
    }
    uint32_t append_bin15_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len, char data_type = MST_BIN) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      if (data_type == MST_TEXT || data_type == MST_BIN) {
        get_set_len_len(len, &grp_data_vec);
        ptr = grp_data_vec.size() - 1;
      }
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      // grp_data_vec.push_back(0);
      // uint64_t u64;
      // flavic48::simple_decode(grp_data_vec.data() + ptr, 1, &u64);
      // printf("Grp: %d, u64: %llu, len: %u, %u\n", grp_no, u64, len, ptr);
      return ptr;
    }
    int append_ptr_bits(uint32_t given_ptr, int bits_to_append) {
      if (freq_grp_vec.size() == 2) {
        flat_ptr_bv.append(given_ptr);
        return last_byte_bits;
      }
      uint64_t ptr = given_ptr;
      uint64_t *last_ptr = (uint64_t *) (ptrs.data() + ptrs.size() - 8);
      while (bits_to_append > 0) {
        if (bits_to_append < last_byte_bits) {
          last_byte_bits -= bits_to_append;
          *last_ptr |= (ptr << last_byte_bits);
          bits_to_append = 0;
        } else {
          bits_to_append -= last_byte_bits;
          *last_ptr |= (ptr >> bits_to_append);
          last_byte_bits = 64;
          gen::append_uint64(0, ptrs);
          last_ptr = (uint64_t *) (ptrs.data() + ptrs.size() - 8);
        }
      }
      return last_byte_bits;
    }
    uint32_t get_hdr_size() {
      return 520 + grp_data.size() * 4 + inner_tries.size() * 4;
    }
    uint32_t get_data_size() {
      uint32_t data_size = 0;
      for (size_t i = 0; i < grp_data.size(); i++)
        data_size += gen::size_align8(grp_data[i].size());
      for (size_t i = 0; i < inner_tries.size(); i++)
        data_size += freq_grp_vec[i + inner_trie_start_grp].grp_size;
      return data_size;
    }
    uint32_t get_ptrs_size() {
      return ptrs.size();
    }
    uint32_t get_total_size() {
      if (enc_type == MSE_TRIE || enc_type == MSE_TRIE_2WAY) {
        return gen::size_align8(grp_data_size) + get_ptrs_size() +
                  null_bv.size_bytes() +
                  gen::size_align8(ptr_lookup_tbl_sz) + 7 * 4 + 20;
      }
      return gen::size_align8(grp_data_size) + get_ptrs_size() +
        null_bv.size_bytes() + gen::size_align8(idx2_ptrs_map.size()) +
        gen::size_align8(ptr_lookup_tbl_sz) + 7 * 4 + 20; // aligned to 8
    }
    void set_ptr_lkup_tbl_ptr_width(uint8_t width) {
      ptr_lkup_tbl_ptr_width = width;
    }
    typedef uniq_info_base *(*get_info_fn) (leopard::node *cur_node, uniq_info_vec& info_vec);
    static uniq_info_base *get_tails_info_fn(leopard::node *cur_node, uniq_info_vec& info_vec) {
      return (uniq_info *) info_vec[cur_node->get_tail()];
    }
    static uniq_info_base *get_vals_info_fn(leopard::node *cur_node, uniq_info_vec& info_vec) {
      return (uniq_info *) info_vec[cur_node->get_col_val()];
    }
    void build(uint32_t node_count, byte_ptr_vec& all_node_sets, get_info_fn get_info_func,
          uniq_info_vec& info_vec, bool is_tail, uint16_t _pk_col_count, bool dessicat,
          char encoding_type = 'u', char _data_type = '*', int col_trie_size = 0) {
      pk_col_count = _pk_col_count;
      dessicate = dessicat;
      enc_type = encoding_type;
      data_type = _data_type;
      if (encoding_type != MSE_TRIE && encoding_type != MSE_TRIE_2WAY && col_trie_size == 0 && (freq_grp_vec.size() > 2 || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB)) {
        ptr_lkup_tbl_ptr_width = 4;
        if (!dessicate)
          build_ptr_lookup_tbl(all_node_sets, get_info_func, is_tail, info_vec, encoding_type);
        if (encoding_type != MSE_STORE && encoding_type != MSE_VINTGB) {
          if (!is_tail && encoding_type != MSE_WORDS && encoding_type != MSE_WORDS_2WAY)
            build_val_ptrs(all_node_sets, get_info_func, info_vec);
        }
      }
      if (freq_grp_vec.size() <= 2 && !is_tail && col_trie_size == 0 && encoding_type != MSE_WORDS && encoding_type != MSE_WORDS_2WAY && encoding_type != MSE_STORE && encoding_type != MSE_VINTGB)
        build_val_ptrs(all_node_sets, get_info_func, info_vec); // flat
      ptr_lookup_tbl_loc = 7 * 4 + 20;
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || col_trie_size > 0 || (freq_grp_vec.size() <= 2 && encoding_type != MSE_STORE && encoding_type != MSE_VINTGB))
        ptr_lookup_tbl_sz = 0;
      else {
        if (dessicate)
          ptr_lookup_tbl_sz = 0;
        else { // TODO: PTR LT gets created unnecessarily for last level of tail tries
          ptr_lookup_tbl_sz = gen::get_lkup_tbl_size2(node_count, nodes_per_ptr_block,
            ptr_lkup_tbl_ptr_width + (nodes_per_ptr_block / nodes_per_ptr_block_n - 1)
            * (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB ? 3 : 2));
        }
      }
      grp_ptrs_loc = ptr_lookup_tbl_loc + gen::size_align8(ptr_lookup_tbl_sz);
      null_bv_loc = grp_ptrs_loc + ptrs.size();
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || col_trie_size > 0) {
        idx2_ptr_count = 0;
        grp_data_loc = null_bv_loc + null_bv.size_bytes();
        grp_data_size = col_trie_size;
        idx2_ptrs_map_loc = grp_data_loc + col_trie_size;
      } else {
        idx2_ptr_count = get_idx2_ptrs_count();
        idx2_ptrs_map_loc = null_bv_loc + null_bv.size_bytes();
        grp_data_loc = idx2_ptrs_map_loc + gen::size_align8(idx2_ptrs_map.size());
        grp_data_size = get_hdr_size() + get_data_size();
      }
      if (dessicate)
        ptr_lookup_tbl_loc = 0;
      printf("Ptr sz: %lu, Data sz: %u, lt size: %lu, lt sz: %u\n", ptrs.size(), grp_data_size, ptr_lookup_tbl.size(), ptr_lookup_tbl_sz);
    }
    #define CODE_LT_BIT_LEN 0
    #define CODE_LT_CODE_LEN 1
    void write_code_lookup_tbl(bool is_tail, FILE* fp, byte_vec *out_vec) {
      write_code_lt(is_tail, CODE_LT_BIT_LEN, fp, out_vec);
      write_code_lt(is_tail, CODE_LT_CODE_LEN, fp, out_vec);
    }
    void write_code_lt(bool is_tail, int which, FILE* fp, byte_vec *out_vec) {
      for (int i = 0; i < 256; i++) {
        uint8_t code_i = i;
        bool code_found = false;
        for (size_t j = 1; j < freq_grp_vec.size(); j++) {
          uint8_t code_len = freq_grp_vec[j].code_len;
          uint8_t code = freq_grp_vec[j].code;
          if ((code_i >> (8 - code_len)) == code) {
            if (which == CODE_LT_BIT_LEN) {
              int bit_len = freq_grp_vec[j].grp_log2;
              output_byte(bit_len, fp, out_vec);
            }
            if (which == CODE_LT_CODE_LEN) {
              output_byte((j - 1) | (code_len << 4), fp, out_vec);
            }
            code_found = true;
            break;
          }
        }
        if (!code_found) {
          //printf("Code not found: %d", i);
          //output_byte(0, fp, out_vec);
          output_byte(0, fp, out_vec);
        }
      }
    }
    void write_grp_data(uint32_t offset, bool is_tail, FILE* fp, byte_vec *out_vec) {
      int grp_count = grp_data.size() + inner_tries.size();
      output_byte(grp_count, fp, out_vec);
      if (inner_trie_start_grp > 0) {
        output_byte(inner_trie_start_grp - 1, fp, out_vec);
        //output_byte(freq_grp_vec[grp_count].grp_log2, fp, out_vec);
      } else if (freq_grp_vec.size() == 2)
        output_byte(freq_grp_vec[1].grp_log2, fp, out_vec);
      else
        output_byte(0, fp, out_vec);
      output_bytes((const uint8_t *) "      ", 6, fp, out_vec); // padding
      write_code_lookup_tbl(is_tail, fp, out_vec);
      uint32_t total_data_size = 0;
      for (size_t i = 0; i < grp_data.size(); i++) {
        output_u32(offset + grp_count * 4 + total_data_size, fp, out_vec);
        total_data_size += gen::size_align8(grp_data[i].size());
      }
      for (size_t i = 0; i < inner_tries.size(); i++) {
        output_u32(offset + grp_count * 4 + total_data_size, fp, out_vec);
        total_data_size += freq_grp_vec[i + inner_trie_start_grp].grp_size;
      }
      for (size_t i = 0; i < grp_data.size(); i++) {
        output_bytes(grp_data[i].data(), grp_data[i].size(), fp, out_vec);
        output_align8(grp_data[i].size(), fp, out_vec);
      }
      for (size_t i = 0; i < inner_tries.size(); i++) {
        inner_tries[i]->fp = fp;
        // inner_tries[i]->write_trie(NULL);
        inner_tries[i]->write_kv(false, nullptr);
      }
    }
    void write_ptr_lookup_tbl(FILE *fp, byte_vec *out_vec) {
      output_bytes(ptr_lookup_tbl.data(), ptr_lookup_tbl.size(), fp, out_vec);
      output_align8(ptr_lookup_tbl_sz, fp, out_vec);
    }
    void write_ptrs(FILE *fp, byte_vec *out_vec) {
      output_bytes(ptrs.data(), ptrs.size(), fp, out_vec);
    }
    void write_null_bv(FILE *fp, byte_vec *out_vec) {
      output_bytes((const uint8_t *) null_bv.raw_data()->data(), null_bv.size_bytes(), fp, out_vec);
    }
    void build_val_ptrs(byte_ptr_vec& all_node_sets, get_info_fn get_info_func, uniq_info_vec& info_vec) {
      ptrs.clear();
      last_byte_bits = 64;
      gen::append_uint64(0, ptrs);
      leopard::node_iterator ni(all_node_sets, pk_col_count == 0 ? 1 : 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if ((cur_node.get_flags() & NFLAG_LEAF) != 0) {
          uint32_t col_val = cur_node.get_col_val();
          if (col_val != UINT32_MAX) {
            if (col_val > UINT32_MAX - 65) {
              freq_grp& fg = freq_grp_vec[rpt_grp_no];
              append_ptr_bits(fg.code, fg.code_len);
              append_ptr_bits(UINT32_MAX - col_val, fg.grp_log2 - fg.code_len);
            } else {
              uniq_info_base *vi = get_info_func(&cur_node, info_vec);
              freq_grp& fg = freq_grp_vec[vi->grp_no];
              if (freq_grp_vec.size() > 2) {
                append_ptr_bits(fg.code, fg.code_len);
              }
              append_ptr_bits(vi->ptr, fg.grp_log2 - fg.code_len);
            }
          }
        }
        cur_node = ni.next();
      }
      append_ptr_bits(0x00, 8); // read beyond protection
    }
    byte_vec *get_ptr_lkup_tbl() {
      return &ptr_lookup_tbl;
    }
    void append_plt_count(uint32_t bit_count) {
      if (ptr_lkup_tbl_ptr_width == 4)
        gen::append_uint32(bit_count, ptr_lookup_tbl);
      else
        gen::append_uint24(bit_count, ptr_lookup_tbl);
    }
    void append_plt_count16(uint32_t *bit_counts, size_t u16_arr_count, char encoding_type) {
      for (size_t j = 0; j < u16_arr_count; j++) {
        if (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB)
          gen::append_uint24(bit_counts[j], ptr_lookup_tbl);
        else
          gen::append_uint16(bit_counts[j], ptr_lookup_tbl);
      }
    }
    void build_ptr_lookup_tbl(byte_ptr_vec& all_node_sets, get_info_fn get_info_func, bool is_tail,
          uniq_info_vec& info_vec, char encoding_type) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      uint32_t bit_count4 = 0;
      size_t pos4 = 0;
      size_t u16_arr_count = (nodes_per_ptr_block / nodes_per_ptr_block_n);
      u16_arr_count--;
      uint32_t bit_counts[u16_arr_count + 1];
      memset(bit_counts, '\0', u16_arr_count * 4 + 4);
      ptr_lookup_tbl.clear();
      append_plt_count(bit_count);
      leopard::node_iterator ni(all_node_sets, pk_col_count == 0 ? 1 : 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        if (node_id && (node_id % nodes_per_ptr_block_n) == 0) {
          if (bit_count4 > (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB ? 16777215 : 65535))
            std::cout << "UNEXPECTED: PTR_LOOKUP_TBL bit_count4 overflow: " << bit_count4 << std::endl;
          // printf("nid: %u, bit count: %u\n", node_id, bit_count4);
          if (encoding_type == MSE_VINTGB && (bit_count4 == 0 || (pos4 > 0 && (bit_count4 - bit_counts[pos4 - 1] == 0))))
            bit_count4 += (data_type == MST_INT ? 2 : 3);
          bit_counts[pos4] = bit_count4;
          pos4++;
        }
        if (node_id && (node_id % nodes_per_ptr_block) == 0) {
          append_plt_count16(bit_counts, u16_arr_count, encoding_type);
          bit_count += bit_counts[u16_arr_count];
          append_plt_count(bit_count);
          bit_count4 = 0;
          pos4 = 0;
          memset(bit_counts, '\0', u16_arr_count * 4 + 4);
        }
        if (cur_node_flags & (is_tail ? NFLAG_TAIL : NFLAG_LEAF)) {
          if (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB) {
            bit_count4 += cur_node.get_col_val();
            //printf("gcv: %u, bc4: %u\n", cur_node.get_col_val(), bit_count4);
          } else {
            uint32_t col_val = cur_node.get_col_val();
            if (col_val != UINT32_MAX) {
              uniq_info_base *vi;
              if (col_val > UINT32_MAX - 65)
                vi = &rpt_ui;
              else
                vi = get_info_func(&cur_node, info_vec);
              freq_grp& fg = freq_grp_vec[vi->grp_no];
              bit_count4 += fg.grp_log2;
            }
          }
        }
        node_id++;
        cur_node = ni.next();
      }
      bit_counts[pos4] = bit_count4;
      bit_count += bit_count4;
      append_plt_count16(bit_counts, u16_arr_count, encoding_type);
      bit_count += bit_counts[u16_arr_count];
      append_plt_count(bit_count);
      append_plt_count16(bit_counts, u16_arr_count, encoding_type);
    }
    void write_ptrs_data(uint8_t flags, bool is_tail, FILE *fp, byte_vec *out_vec) {
      //size_t ftell_start = ftell(fp);
      output_byte(ptr_lkup_tbl_ptr_width, fp, out_vec);
      output_byte(data_type, fp, out_vec);
      output_byte(enc_type, fp, out_vec);
      output_byte(flags, fp, out_vec);
      output_byte(len_grp_no - 1, fp, out_vec);
      output_byte(rpt_grp_no - 1, fp, out_vec);
      output_byte(rpt_seq_grp_no, fp, out_vec);
      output_byte(sug_col_width, fp, out_vec);
      output_byte(sug_col_height, fp, out_vec);
      output_byte(idx_limit, fp, out_vec);
      output_byte(start_bits, fp, out_vec);
      output_byte(step_bits_idx, fp, out_vec);
      output_byte(step_bits_rest, fp, out_vec);
      output_byte(idx_ptr_size, fp, out_vec);
      output_u32(null_bv.raw_data()->size(), fp, out_vec);
      output_byte(reserved8_1, fp, out_vec);
      output_byte(reserved8_2, fp, out_vec);

      output_u32(max_len, fp, out_vec);
      output_u32(ptr_lookup_tbl_loc, fp, out_vec);
      output_u32(grp_data_loc, fp, out_vec);
      output_u32(idx2_ptr_count, fp, out_vec);
      output_u32(idx2_ptrs_map_loc, fp, out_vec);
      output_u32(grp_ptrs_loc, fp, out_vec);
      output_u32(null_bv_loc, fp, out_vec);

      if (enc_type == MSE_TRIE || enc_type == MSE_TRIE_2WAY) {
        write_ptrs(fp, out_vec);
      } else {
        if (freq_grp_vec.size() > 2 || enc_type == MSE_STORE || enc_type == MSE_VINTGB)
          write_ptr_lookup_tbl(fp, out_vec);
        write_ptrs(fp, out_vec);
        write_null_bv(fp, out_vec);
        byte_vec *idx2_ptrs_map = get_idx2_ptrs_map();
        output_bytes(idx2_ptrs_map->data(), idx2_ptrs_map->size(), fp, out_vec);
        output_align8(idx2_ptrs_map->size(), fp, out_vec);
        write_grp_data(grp_data_loc + 520, is_tail, fp, out_vec); // group count, 512 lookup tbl, tail locs, tails
        output_align8(grp_data_size, fp, out_vec);
      }
      gen::gen_printf("Data size: %u, Ptrs size: %u, LkupTbl size: %u\nIdxMap size: %u, Total size: %u\n",
        get_data_size(), get_ptrs_size(), ptr_lookup_tbl_sz, get_idx2_ptrs_map()->size(), get_total_size());//, ftell(fp)-ftell_start);
    }
    void reset_freq_counts() {
      for (size_t i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        fg->grp_size = fg->freq_count = fg->count = 0;
      }
    }
    void set_freq_grps(std::vector<freq_grp> freq_grps) {
      freq_grp_vec = freq_grps;
    }
    std::vector<freq_grp>& get_freq_grps() {
      return freq_grp_vec;
    }
    freq_grp *get_freq_grp(int grp_no) {
      return &freq_grp_vec[grp_no];
    }
    size_t get_grp_count() {
      return freq_grp_vec.size();
    }
    byte_vec *get_idx2_ptrs_map() {
      return &idx2_ptrs_map;
    }
    byte_vec *get_ptrs() {
      return &ptrs;
    }
    void show_freq_codes() {
      gen::gen_printf("bits\tcd\tct_t\tfct_t\tlen_t\tcdln\tmxsz\tbyts\n");
      uint32_t sums[4];
      memset(sums, 0, sizeof(uint32_t) * 4);
      for (size_t i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        uint32_t byts = (fg->grp_log2 * fg->freq_count) >> 3;
        gen::gen_printf("%u\t%2x\t%u\t%u\t%u\t%u\t%u\t%u\n", fg->grp_log2, fg->code,
              fg->count, fg->freq_count, fg->grp_size, fg->code_len, fg->grp_limit, byts);
        sums[0] += fg->count; sums[1] += fg->freq_count; sums[2] += fg->grp_size; sums[3] += byts;
      }
      gen::gen_printf("Idx:%u,It:%u\t%u\t%u\t%u\t\t\t%u\n", idx_limit, inner_trie_start_grp, sums[0], sums[1], sums[2], sums[3]);
    }
    void build_freq_codes(bool is_val = false) {
      if (freq_grp_vec.size() == 2) {
        freq_grp *fg = &freq_grp_vec[1];
        fg->code = fg->code_len = 0;
        fg->grp_log2 = ceil(log2(fg->grp_size));
        if (!is_val) {
          if (fg->grp_log2 > 8)
            fg->grp_log2 -= 8;
          else
            fg->grp_log2 = 0;
        }
        flat_ptr_bv.init(&ptrs, fg->grp_log2, fg->freq_count);
        return;
      }
      std::vector<uint32_t> freqs;
      for (size_t i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      gen::huffman<uint32_t> _huffman(freqs);
      tot_ptr_bit_count = 0;
      for (size_t i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        uint32_t len;
        fg->code = (uint8_t) _huffman.get_code(i - 1, len);
        fg->code_len = len;
        if (i <= idx_limit) {
          fg->grp_log2 = ceil(log2(fg->grp_limit));
        } else if (inner_trie_start_grp > 0 && i >= inner_trie_start_grp) {
          if (fg->count < fg->grp_limit)
            fg->grp_log2 = ceil(log2(fg->count == 1 ? 2 : fg->count));
          else
            fg->grp_log2 = ceil(log2(fg->grp_limit));
        } else {
          fg->grp_log2 = ceil(log2(fg->grp_size));
        }
        if (is_val)
          fg->grp_log2 += fg->code_len;
        else {
          if (fg->grp_log2 > (8 - len))
            fg->grp_log2 -= (8 - len);
          else
            fg->grp_log2 = 0;
        }
        tot_ptr_bit_count += (fg->grp_log2 * fg->freq_count);
      }
      if (tot_ptr_bit_count > 4294967296LL)
        std::cout << "WARNING: ptr_bit_cout > 4gb" << std::endl;
    }
};

struct node_data {
  uint8_t *data;
  uint32_t len;
  uint32_t ns_id;
  uint8_t node_idx;
  uint8_t offset;
};
typedef std::vector<node_data> node_data_vec;

class tail_val_maps {
  private:
    builder_fwd *bldr;
    gen::byte_blocks& uniq_data;
    uniq_info_vec& ui_vec;
    //uniq_info_vec uniq_tails_fwd;
    ptr_groups ptr_grps;
    int start_nid, end_nid;
  public:
    tail_val_maps(builder_fwd *_bldr, gen::byte_blocks& _uniq_data, uniq_info_vec& _ui_vec)
        : bldr (_bldr), uniq_data (_uniq_data), ui_vec (_ui_vec) {
    }
    ~tail_val_maps() {
      for (size_t i = 0; i < ui_vec.size(); i++)
        delete ui_vec[i];
    }

    void init() {
      ptr_grps.init(bldr, bldr->get_opts()->step_bits_idx, bldr->get_opts()->step_bits_rest);
    }

    uint8_t *get_tail(gen::byte_blocks& all_tails, leopard::node n, uint32_t& len) {
      uint8_t *v = all_tails[n.get_tail()];
      size_t vlen;
      len = gen::read_vint32(v, &vlen);
      v += vlen;
      return v;
    }

    const double idx_cost_frac_cutoff = 0.1;
    uint32_t make_uniq_freq(uniq_info_vec& uniq_arr_vec, uniq_info_vec& uniq_freq_vec, uint32_t tot_freq_count, uint32_t& last_data_len, uint8_t& start_bits, uint8_t& grp_no, bool no_idx_map = false) {
      clock_t t = clock();
      uniq_freq_vec = uniq_arr_vec;
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.end(), [](const struct uniq_info_base *lhs, const struct uniq_info_base *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });

      uint32_t sum_freq = 0;
      if (start_bits < 7) {
        for (size_t i = 0; i < uniq_freq_vec.size(); i++) {
          uniq_info_base *vi = uniq_freq_vec[i];
          if (i >= pow(2, start_bits)) {
            double bit_width = log2(tot_freq_count / sum_freq);
            if (bit_width < 1.1) {
              gen::gen_printf("i: %d, freq: %u, Bit width: %.9f, start_bits: %d\n", i, sum_freq, bit_width, (int) start_bits);
              break;
            }
            start_bits++;
          }
          if (start_bits > 7) {
            start_bits = 7;
            break;
          }
          sum_freq += vi->freq_count;
        }
      }

      uint32_t freq_idx;
      uint32_t cumu_freq_idx = 0;
      grp_no = 0;
      if (!no_idx_map) {
        grp_no = 1;
        sum_freq = 0;
        freq_idx = 0;
        last_data_len = 2;
        uint32_t cutoff_bits = start_bits;
        uint32_t nxt_idx_limit = pow(2, cutoff_bits);
        for (size_t i = 0; i < uniq_freq_vec.size(); i++) {
          uniq_info_base *vi = uniq_freq_vec[i];
          last_data_len += vi->len;
          last_data_len++;
          sum_freq += vi->freq_count;
          if (last_data_len >= nxt_idx_limit) {
            double cost_frac = last_data_len + nxt_idx_limit * 3;
            double divisor = sum_freq * cutoff_bits;
            divisor /= 8;
            cost_frac /= (divisor == 0 ? 1 : divisor);
            if (cost_frac > idx_cost_frac_cutoff)
              break;
            grp_no++;
            sum_freq = 0;
            freq_idx = 0;
            last_data_len = 0;
            cutoff_bits += bldr->get_opts()->step_bits_idx;
            nxt_idx_limit = pow(2, cutoff_bits);
          }
          freq_idx++;
        }

        if (grp_no >= bldr->get_opts()->max_groups) {
          cutoff_bits = start_bits;
        }

        grp_no = 0;
        last_data_len = 0;
        if (cutoff_bits > start_bits) {
          grp_no = 1;
          freq_idx = 0;
          uint32_t next_bits = start_bits;
          nxt_idx_limit = pow(2, next_bits);
          for (cumu_freq_idx = 0; cumu_freq_idx < uniq_freq_vec.size(); cumu_freq_idx++) {
            uniq_info_base *vi = uniq_freq_vec[cumu_freq_idx];
            if (freq_idx == nxt_idx_limit) {
              next_bits += bldr->get_opts()->step_bits_idx;
              if (next_bits >= cutoff_bits) {
                break;
              }
              nxt_idx_limit = pow(2, next_bits);
              grp_no++;
              freq_idx = 0;
              last_data_len = 0;
            }
            vi->grp_no = grp_no;
            vi->ptr = freq_idx;
            last_data_len += vi->len;
            last_data_len++;
            freq_idx++;
          }
        }
        if (grp_no == 0 && start_bits == 1) {
          start_bits = ceil(log2(2 + uniq_freq_vec[0]->len + 1));
          if (start_bits == 0)
            start_bits++;
        }
      }

      // grp_no = 0;
      // uint32_t cumu_freq_idx = 0;
      //printf("%.1f\t%d\t%u\t%u\n", ceil(log2(freq_idx)), freq_idx, ftot, tail_len_tot);
      if (ptr_grps.inner_trie_start_grp == 0) {
        std::sort(uniq_freq_vec.begin(), uniq_freq_vec.begin() + cumu_freq_idx, [](const struct uniq_info_base *lhs, const struct uniq_info_base *rhs) -> bool {
          return (lhs->grp_no == rhs->grp_no) ? (lhs->arr_idx > rhs->arr_idx) : (lhs->grp_no < rhs->grp_no);
        });
        std::sort(uniq_freq_vec.begin() + cumu_freq_idx, uniq_freq_vec.end(), [](const struct uniq_info_base *lhs, const struct uniq_info_base *rhs) -> bool {
          uint32_t lhs_freq = lhs->freq_count / (lhs->len == 0 ? 1 : lhs->len);
          uint32_t rhs_freq = rhs->freq_count / (rhs->len == 0 ? 1 : rhs->len);
          lhs_freq = ceil(log10(lhs_freq));
          rhs_freq = ceil(log10(rhs_freq));
          return (lhs_freq == rhs_freq) ? (lhs->arr_idx > rhs->arr_idx) : (lhs_freq > rhs_freq);
        });
      }
      t = gen::print_time_taken(t, "Time taken for uniq_freq: ");
      return cumu_freq_idx;

    }

    void check_remaining_text(uniq_info_vec& uniq_freq_vec, gen::byte_blocks& uniq_data, bool is_tail) {

      uint32_t remain_tot = 0;
      uint32_t remain_cnt = 0;
      uint32_t cmp_min_tot = 0;
      uint32_t cmp_min_cnt = 0;
      uint32_t free_tot = 0;
      uint32_t free_cnt = 0;
      // gen::word_matcher wm(uniq_data);
      //fp = fopen("remain.txt", "w+");
      uint32_t freq_idx = 0;
      clock_t tt = clock();
      while (freq_idx < uniq_freq_vec.size()) {
        uniq_info *ti = (uniq_info *) uniq_freq_vec[freq_idx];
        freq_idx++;
        if (ti->flags & MDX_AFFIX_FULL)
         continue;
        if ((ti->flags & 0x07) == 0) {
          // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
          //printf("%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
          free_tot += ti->len;
          free_cnt++;
        }
        int remain_len = ti->len - ti->cmp_max;
        if (remain_len > 3) {
          // if (is_tail)
          //   wm.add_word_combis(ti->pos + 1, remain_len - 1, freq_idx);
          // else
          //   wm.add_word_combis(ti->pos + ti->cmp_max, remain_len, freq_idx);
          // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
          //fprintf(fp, "%.*s\n", remain_len - 1, uniq_tails.data() + ti->tail_pos + 1);
          remain_tot += remain_len;
          remain_tot--;
          remain_cnt++;
        }
        //fprintf(fp, "%u\t%u\t%u\t%u\n", ti->freq_count, ti->len, ti->arr_idx, ti->cmp_max);
        if (ti->cmp_min != 0xFFFFFFFF && ti->cmp_min > 4) {
          cmp_min_tot += (ti->cmp_min - 1);
          cmp_min_cnt++;
        }
      }
      tt = gen::print_time_taken(tt, "Time taken for remain: ");
      // wm.process_combis();
      // gen::print_time_taken(tt, "Time taken for process combis: ");

      //fclose(fp);
      gen::gen_printf("Free entries: %u, %u\n", free_tot, free_cnt);
      gen::gen_printf("Remaining: %u, %u\n", remain_tot, remain_cnt);
      gen::gen_printf("Cmp_min_tot: %u, %u\n", cmp_min_tot, cmp_min_cnt);

    }

    constexpr static uint32_t idx_ovrhds[] = {384, 3072, 24576, 196608, 1572864, 10782081};
    #define inner_trie_min_size 131072
    void build_tail_val_maps(bool is_tail, byte_ptr_vec& all_node_sets, uniq_info_vec& uniq_info_arr, gen::byte_blocks& uniq_data, uint32_t tot_freq_count, uint32_t _max_len, uint8_t max_repeats) {

      clock_t t = clock();

      uniq_info_vec uniq_info_arr_freq;
      uint8_t grp_no;
      uint32_t last_data_len;
      uint8_t start_bits = is_tail ? 7 : 1;
      uint32_t cumu_freq_idx = make_uniq_freq(uniq_info_arr, uniq_info_arr_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.reset();
      ptr_grps.set_idx_info(start_bits, grp_no, 3); //last_data_len > 65535 ? 3 : 2);
      ptr_grps.set_max_len(_max_len);

      uint32_t freq_idx = cumu_freq_idx;
      while (freq_idx < uniq_info_arr_freq.size()) {
        uniq_info_base *ti = uniq_info_arr_freq[freq_idx];
        last_data_len += ti->len;
        last_data_len++;
        freq_idx++;
      }

      freq_idx = 0;
      bool is_bin = false;
      if (uniq_info_arr[0]->flags & LPDU_BIN) {
        gen::gen_printf("Tail content not text.\n");
        // is_bin = true;
      }
      if (!is_bin) {
        uniq_info *prev_ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
        while (freq_idx < uniq_info_arr_freq.size()) {
          uniq_info *ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
          freq_idx++;
          if (ti->flags & LPDU_NULL || ti->flags & LPDU_EMPTY || ti->flags & LPDU_BIN) {
            if (freq_idx < uniq_info_arr_freq.size())
              prev_ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
            continue;
          }
          int cmp_ret = (is_tail ? 
            gen::compare_rev(uniq_data[prev_ti->pos], prev_ti->len, uniq_data[ti->pos], ti->len)
            : gen::compare(uniq_data[prev_ti->pos], prev_ti->len, uniq_data[ti->pos], ti->len));
          if (cmp_ret == 0)
            continue;
          uint32_t cmp = abs(cmp_ret);
          cmp--;
          bool partial_affix = bldr->get_opts()->partial_sfx_coding;
          if (cmp < bldr->get_opts()->sfx_min_tail_len)
            partial_affix = false;
          if (!bldr->get_opts()->idx_partial_sfx_coding && freq_idx < cumu_freq_idx)
            partial_affix = false;
          if (cmp == ti->len || partial_affix) {
            ti->flags |= (cmp == ti->len ? MDX_AFFIX_FULL : MDX_AFFIX_PARTIAL);
            ti->cmp = cmp;
            if (ti->cmp_max < cmp)
              ti->cmp_max = cmp;
            if (prev_ti->cmp_min > cmp) {
              prev_ti->cmp_min = cmp;
              if (cmp == ti->len && prev_ti->cmp >= cmp)
                prev_ti->cmp = cmp - 1;
            }
            if (cmp == ti->len) {
              if (prev_ti->cmp_max < cmp)
                prev_ti->cmp_max = cmp;
            }
            prev_ti->flags |= MDX_HAS_AFFIX;
            ti->link_arr_idx = prev_ti->arr_idx;
          }
          if (cmp != ti->len)
            prev_ti = ti;
        }
      }

      freq_idx = 0;
      grp_no = 1;
      uint32_t cur_limit = pow(2, start_bits);
      ptr_grps.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      ptr_grps.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      uint32_t savings_full = 0;
      uint32_t savings_count_full = 0;
      uint32_t savings_partial = 0;
      uint32_t savings_count_partial = 0;
      uint32_t sfx_set_len = 0;
      uint32_t sfx_set_max = bldr->get_opts()->sfx_set_max_dflt;
      uint32_t sfx_set_count = 0;
      uint32_t sfx_set_tot_cnt = 0;
      uint32_t sfx_set_tot_len = 0;
      while (freq_idx < uniq_info_arr_freq.size()) {
        uniq_info *ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
        last_data_len -= ti->len;
        last_data_len--;
        uint32_t it_nxt_limit = ptr_grps.check_next_grp(grp_no, cur_limit, ti->len);
        if (bldr->get_opts()->inner_tries && it_nxt_limit != cur_limit && 
              it_nxt_limit >= inner_trie_min_size && last_data_len >= inner_trie_min_size * 2) {
          break;
        }
        freq_idx++;
        if (is_bin) {
          uint32_t bin_len = ti->len;
          uint32_t len_len = ptr_grps.get_set_len_len(bin_len);
          bin_len += len_len;
          uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, bin_len, tot_freq_count);
          ti->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_data[ti->pos], ti->len);
          ti->grp_no = grp_no;
          ptr_grps.update_current_grp(grp_no, bin_len, ti->freq_count);
          cur_limit = new_limit;
          continue;
        } else if (ti->flags & MDX_AFFIX_FULL) {
          savings_full += ti->len;
          savings_full++;
          savings_count_full++;
          uniq_info *link_ti = (uniq_info *) uniq_info_arr[ti->link_arr_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, link_ti->len + 1, tot_freq_count);
            link_ti->grp_no = grp_no;
            ptr_grps.update_current_grp(link_ti->grp_no, link_ti->len + 1 - (is_tail ? 0 : ti->cmp), link_ti->freq_count);
            link_ti->ptr = ptr_grps.append_text(grp_no, uniq_data[link_ti->pos], link_ti->len - (is_tail ? 0 : ti->cmp), true);
            link_ti->flags &= ~MDX_AFFIX_PARTIAL;
          }
          //cur_limit = ptr_grps.next_grp(grp_no, cur_limit, 0);
          ptr_grps.update_current_grp(link_ti->grp_no, 0, ti->freq_count);
          if (is_tail)
            ti->ptr = link_ti->ptr + link_ti->len - ti->len;
          else
            ti->ptr = link_ti->ptr + ti->len - (link_ti->flags & MDX_AFFIX_PARTIAL ? link_ti->cmp : 0);
          ti->grp_no = link_ti->grp_no;
        } else {
          if (ti->flags & MDX_AFFIX_PARTIAL) {
            uint32_t cmp = ti->cmp;
            uint32_t remain_len = ti->len - cmp;
            uint32_t len_len = ptr_grps.get_set_len_len(cmp);
            remain_len += len_len;
            uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, remain_len, tot_freq_count);
            byte_vec& tail_val_data = ptr_grps.get_data(grp_no);
            if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
              ti->cmp_min = 0;
              if (sfx_set_len == 1 && is_tail)
                sfx_set_len += cmp;
              sfx_set_len += remain_len;
              sfx_set_count++;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              ptr_grps.update_current_grp(grp_no, remain_len, ti->freq_count);
              remain_len -= len_len;
              ti->ptr = ptr_grps.append_text(grp_no, uniq_data[ti->pos + (is_tail ? 0 : cmp)], remain_len);
              ptr_grps.get_set_len_len(cmp, &tail_val_data);
            } else {
              // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
              sfx_set_len = 1;
              sfx_set_tot_len += ti->len;
              sfx_set_count = 1;
              sfx_set_tot_cnt++;
              sfx_set_max = bldr->get_opts()->sfx_set_max_dflt;
              if (ti->len > sfx_set_max)
                sfx_set_max = ti->len * 2;
              ptr_grps.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
              ti->ptr = ptr_grps.append_text(grp_no, uniq_data[ti->pos], ti->len, true);
              ti->flags &= ~MDX_AFFIX_PARTIAL;
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_data[ti->tail_pos]);
            cur_limit = new_limit;
          } else {
            // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
            sfx_set_len = 1;
            sfx_set_tot_len += ti->len;
            sfx_set_count = 1;
            sfx_set_tot_cnt++;
            sfx_set_max = bldr->get_opts()->sfx_set_max_dflt;
            uint32_t len_len = 1;
            if (ti->flags & LPDU_BIN)
              len_len = ptr_grps.get_set_len_len(ti->len);
            if (ti->len > sfx_set_max)
              sfx_set_max = ti->len * 2;
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, ti->len + len_len, tot_freq_count); // todo: only if not null or empty
            ptr_grps.update_current_grp(grp_no, (ti->flags & LPDU_NULL || ti->flags & LPDU_EMPTY) ? 0 : ti->len + len_len, ti->freq_count);
            if (ti->flags & LPDU_NULL)
              ti->ptr = 0;
            else if (ti->flags & LPDU_EMPTY)
              ti->ptr = 1;
            else if (ti->flags & LPDU_BIN)
              ti->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_data[ti->pos], ti->len);
            else
              ti->ptr = ptr_grps.append_text(grp_no, uniq_data[ti->pos], ti->len, true);
          }
          ti->grp_no = grp_no;
        }
      }
      gen::gen_printf("Savings full: %u, %u\nSavings Partial: %u, %u / Sfx set: %u, %u\n", savings_full, savings_count_full, savings_partial, savings_count_partial, sfx_set_tot_len, sfx_set_tot_cnt);

      printf("rpt_ui.pos: %u\n", ptr_grps.rpt_ui.pos);
      if (ptr_grps.rpt_ui.pos == UINT32_MAX) { // repeats
        ptr_grps.rpt_ui.freq_count = ptr_grps.rpt_ui.repeat_freq;
        max_repeats++;
        ptr_grps.add_freq_grp((freq_grp) {++grp_no, 0, max_repeats, ptr_grps.rpt_ui.len, ptr_grps.rpt_ui.freq_count, max_repeats, 0, 0}, true);
        ptr_grps.set_grp_nos(0, grp_no, 0);
        ptr_grps.append_text(grp_no, (const uint8_t *) "", 1);
        ptr_grps.rpt_ui.grp_no = grp_no;
      }

      if (bldr->get_opts()->inner_tries && freq_idx < uniq_info_arr_freq.size()) {
        builder_fwd *inner_trie = bldr->new_instance();
        cur_limit = ptr_grps.next_grp(grp_no, cur_limit, uniq_info_arr_freq[freq_idx]->len, tot_freq_count, true);
        ptr_grps.inner_trie_start_grp = grp_no;
        uint32_t trie_entry_idx = 0;
        ptr_grps.inner_tries.push_back(inner_trie);
        while (freq_idx < uniq_info_arr_freq.size()) {
          uniq_info *ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
          uint8_t rev[ti->len];
          uint8_t *ti_data = uniq_data[ti->pos];
          for (uint32_t j = 0; j < ti->len; j++)
            rev[j] = ti_data[ti->len - j - 1];
          inner_trie->insert(rev, ti->len, freq_idx);
          ptr_grps.update_current_grp(grp_no, 1, ti->freq_count);
          ti->grp_no = grp_no;
          trie_entry_idx++;
          if (trie_entry_idx == cur_limit) {
            inner_trie = bldr->new_instance();
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, trie_entry_idx, tot_freq_count, true);
            trie_entry_idx = 0;
            ptr_grps.inner_tries.push_back(inner_trie);
          }
          freq_idx++;
        }
      }

      for (size_t it_idx = 0; it_idx < ptr_grps.inner_tries.size(); it_idx++) {
        builder_fwd *inner_trie = ptr_grps.inner_tries[it_idx];
        // todo: What was this for?
        // leopard::node_iterator ni_freq(inner_trie->get_memtrie()->all_node_sets, 0);
        // leopard::node cur_node = ni_freq.next();
        // while (cur_node != nullptr) {
        //   if (cur_node.get_flags() & NFLAG_CHILD) {
        //     uint32_t sum_freq = 0;
        //     leopard::node_set_handler nsh_children(inner_trie->get_memtrie()->all_node_sets, cur_node.get_child());
        //     for (size_t i = 0; i <= nsh_children.last_node_idx(); i++) {
        //       leopard::node child_node = nsh_children[i];
        //       if (child_node.get_flags() & NFLAG_LEAF) {
        //         uniq_info_base *ti = uniq_info_arr_freq[cur_node.get_col_val()];
        //         sum_freq += ti->freq_count;
        //       }
        //     }
        //     nsh_children.hdr()->freq = sum_freq;
        //   }
        //   cur_node = ni_freq.next();
        // }
        uint32_t trie_size = inner_trie->build();
        leopard::node_iterator ni(inner_trie->get_memtrie()->all_node_sets, 0);
        leopard::node n = ni.next();
        //int leaf_id = 0;
        uint32_t node_id = 0;
        while (n != nullptr) {
          uint32_t col_val_pos = n.get_col_val();
          if (n.get_flags() & NFLAG_LEAF) {
            uniq_info_base *ti = uniq_info_arr_freq[col_val_pos];
            ti->ptr = node_id; //leaf_id++;
          }
          n = ni.next();
          node_id++;
        }
        freq_grp *fg = ptr_grps.get_freq_grp(ptr_grps.inner_trie_start_grp + it_idx);
        fg->grp_size = trie_size;
        fg->grp_limit = node_id;
        fg->count = node_id;
        //printf("Inner Trie size:\t%u\n", trie_size);
      }

      for (freq_idx = 0; freq_idx < cumu_freq_idx; freq_idx++) {
        uniq_info_base *ti = uniq_info_arr_freq[freq_idx];
        ti->ptr = ptr_grps.append_ptr2_idx_map(ti->grp_no, ti->ptr);
      }

      // check_remaining_text(uniq_info_arr_freq, uniq_data, true);

      // int cmpr_blk_size = 65536;
      // size_t total_size = 0;
      // size_t tot_cmpr_size = 0;
      // uint8_t *cmpr_buf = (uint8_t *) malloc(cmpr_blk_size * 1.2);
      // for (int g = 1; g <= grp_no; g++) {
      //   byte_vec& gd = ptr_grps.get_data(g);
      //   if (gd.size() > cmpr_blk_size) {
      //     int cmpr_blk_count = gd.size() / cmpr_blk_size + 1;
      //     for (int b = 0; b < cmpr_blk_count; b++) {
      //       size_t input_size = (b == cmpr_blk_count - 1 ? gd.size() % cmpr_blk_size : cmpr_blk_size);
      //       size_t cmpr_size = gen::compress_block(CMPR_TYPE_ZSTD, gd.data() + (b * cmpr_blk_size), input_size, cmpr_buf);
      //       total_size += input_size;
      //       tot_cmpr_size += cmpr_size;
      //       printf("Grp_no: %d, grp_size: %lu, blk_count: %d, In size: %lu, cmpr size: %lu\n", g, gd.size(), cmpr_blk_count, input_size, cmpr_size);
      //     }
      //   } else {
      //     total_size += gd.size();
      //     tot_cmpr_size += gd.size();
      //   }
      // }
      // printf("Total Input size: %lu, Total cmpr size: %lu\n", total_size, tot_cmpr_size);

      ptr_grps.build_freq_codes(!is_tail);
      ptr_grps.show_freq_codes();

      gen::print_time_taken(t, "Time taken for build_tail_val_maps(): ");

    }

    void build_val_maps(uint32_t tot_freq_count, uint32_t _max_len, char data_type, uint8_t max_repeats) {
      clock_t t = clock();
      uint32_t last_data_len;
      uint8_t start_bits = 1;
      uint8_t grp_no;
      uniq_info_vec uniq_vals_freq;
      uint32_t cumu_freq_idx = make_uniq_freq(ui_vec, uniq_vals_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.reset();
      ptr_grps.set_idx_info(start_bits, grp_no, 3);
      ptr_grps.set_max_len(_max_len);
      uint32_t freq_idx = 0;
      uint32_t cur_limit = pow(2, start_bits);
      grp_no = 1;
      // gen::word_matcher wm(uniq_vals);
      ptr_grps.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      ptr_grps.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      while (freq_idx < uniq_vals_freq.size()) {
        uniq_info_base *vi = uniq_vals_freq[freq_idx];
        freq_idx++;
        uint8_t len_of_len = 0;
        if (data_type == MST_TEXT || data_type == MST_BIN)
          len_of_len = ptr_grps.get_set_len_len(vi->len);
        uint32_t len_plus_len = vi->len + len_of_len;
        cur_limit = ptr_grps.next_grp(grp_no, cur_limit, len_plus_len, tot_freq_count);
        vi->grp_no = grp_no;
        vi->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_data[vi->pos], vi->len, data_type);
        // if (data_type == MSE_TRIE)
        //   wm.add_all_combis(vi->pos, vi->len, vi->arr_idx);
        ptr_grps.update_current_grp(grp_no, len_plus_len, vi->freq_count);
      }
      // wm.process_combis();
      for (freq_idx = 0; freq_idx < cumu_freq_idx; freq_idx++) {
        uniq_info_base *vi = uniq_vals_freq[freq_idx];
        vi->ptr = ptr_grps.append_ptr2_idx_map(vi->grp_no, vi->ptr);
      }
      printf("rpt_ui.pos: %u\n", ptr_grps.rpt_ui.pos);
      if (ptr_grps.rpt_ui.pos == UINT32_MAX) { // repeats
        ptr_grps.rpt_ui.freq_count = ptr_grps.rpt_ui.repeat_freq;
        max_repeats++;
        ptr_grps.add_freq_grp((freq_grp) {++grp_no, 0, max_repeats, ptr_grps.rpt_ui.len, ptr_grps.rpt_ui.freq_count, max_repeats, 0, 0}, true);
        ptr_grps.set_grp_nos(0, grp_no, 0);
        ptr_grps.append_text(grp_no, (const uint8_t *) "", 1);
        ptr_grps.rpt_ui.grp_no = grp_no;
      }
      ptr_grps.build_freq_codes(true);
      ptr_grps.show_freq_codes();
      t = gen::print_time_taken(t, "Time taken for build_val_maps(): ");
    }

    uint32_t get_tail_ptr(uint32_t grp_no, uniq_info *ti) {
      uint32_t ptr = ti->ptr;
      if (grp_no <= ptr_grps.get_idx_limit()) {
        byte_vec& idx2_ptr_map = *(ptr_grps.get_idx2_ptrs_map());
        uint32_t pos = ptr_grps.idx_map_arr[grp_no - 1] + ptr * ptr_grps.get_idx_ptr_size();
        ptr = ptr_grps.get_idx_ptr_size() == 2 ? gen::read_uint16(idx2_ptr_map, pos) : gen::read_uint24(idx2_ptr_map, pos);
      }
      return ptr;
    }

    gen::byte_blocks *get_uniq_data() {
      return &uniq_data;
    }
    uniq_info_vec *get_ui_vec() {
      return &ui_vec;
    }
    ptr_groups *get_grp_ptrs() {
      return &ptr_grps;
    }

};

class sort_callbacks {
  public:
    virtual uint8_t *get_data_and_len(leopard::node& n, uint32_t& len, char type = '*') = 0;
    virtual void set_uniq_pos(uint32_t ns_id, uint8_t node_idx, size_t pos) = 0;
    virtual int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2, int trie_level) = 0;
    virtual void sort_data(node_data_vec& nodes_for_sort, int trie_level) = 0;
    virtual ~sort_callbacks() {
    }
};

class tail_sort_callbacks : public sort_callbacks {
  private:
    byte_ptr_vec& all_node_sets;
    gen::byte_blocks& all_tails;
    gen::byte_blocks& uniq_tails;
  public:
    tail_sort_callbacks(byte_ptr_vec& _all_node_sets, gen::byte_blocks& _all_tails, gen::byte_blocks& _uniq_tails)
      : all_node_sets (_all_node_sets), all_tails (_all_tails), uniq_tails (_uniq_tails) {
    }
    virtual ~tail_sort_callbacks() {
    }
    uint8_t *get_data_and_len(leopard::node& n, uint32_t& len, char type = '*') {
      if (n.get_flags() & NFLAG_TAIL) {
        size_t vlen;
        uint8_t *v = all_tails[n.get_tail()];
        len = gen::read_vint32(v, &vlen);
        v += vlen;
        return v;
      }
      return NULL;
    }
    void set_uniq_pos(uint32_t ns_id, uint8_t node_idx, size_t pos) {
      leopard::node_set_handler ns(all_node_sets, ns_id);
      leopard::node n = ns[node_idx];
      n.set_tail(pos);
    }
    int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2, int trie_level) {
      if (trie_level == 0)
        return gen::compare_rev(v1, len1, v2, len2);
      return gen::compare(v1, len1, v2, len2);
    }
    void sort_data(node_data_vec& nodes_for_sort, int trie_level) {
      clock_t t = clock();
      if (trie_level == 0) {
        std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [](const struct node_data& lhs, const struct node_data& rhs) -> bool {
          return gen::compare_rev(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
        });
      } else {
        std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [](const struct node_data& lhs, const struct node_data& rhs) -> bool {
          return gen::compare(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
        });
      }
      t = gen::print_time_taken(t, "Time taken for sort tails: ");
    }
};

class val_sort_callbacks : public sort_callbacks {
  private:
    byte_ptr_vec& all_node_sets;
    gen::byte_blocks& all_vals;
    gen::byte_blocks& uniq_vals;
  public:
    val_sort_callbacks(byte_ptr_vec& _all_node_sets, gen::byte_blocks& _all_vals, gen::byte_blocks& _uniq_vals)
      : all_node_sets (_all_node_sets), all_vals (_all_vals), uniq_vals (_uniq_vals) {
    }
    virtual ~val_sort_callbacks() {
    }
    uint8_t *get_data(gen::byte_blocks& vec, uint32_t pos, uint32_t& len, char type = '*') {
      if (pos >= vec.size())
        std::cout << "WARNING:: accessing beyond vec size !!!!!!!!!!!!!!!!!!!!!!!!!!!!!: " << pos << ", " << vec.size() << std::endl;
      uint8_t *v = vec[pos];
      switch (type) {
        case MST_TEXT:
        case MST_BIN: {
          size_t vlen;
          len = gen::read_vint32(v, &vlen);
          v += vlen;
        } break;
        case MST_INT:
        case MST_DECV ... MST_DEC9:
        case MST_DATE_US ... MST_DATETIME_ISOT_MS:
          len = *v & 0x07;
          len += 2;
          break;
      }
      return v;
    }
    uint8_t *get_data_and_len(leopard::node& n, uint32_t& len, char type = '*') {
      len = 0;
      if (n.get_flags() & NFLAG_LEAF) {
        uint32_t col_val = n.get_col_val();
        if (col_val == 1) {
          len = 1;
          return NULL;
        }
        return get_data(all_vals, n.get_col_val(), len, type);
      }
      return NULL;
    }
    void set_uniq_pos(uint32_t ns_id, uint8_t node_idx, size_t pos) {
      leopard::node_set_handler ns(all_node_sets, ns_id);
      leopard::node n = ns[node_idx];
      n.set_col_val(pos);
    }
    int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2, int trie_level) {
      if (v1 == NULL && v2 == NULL)
        return 0;
      if (v1 == NULL)
        return -1;
      if (v2 == NULL)
        return 1;
      return gen::compare(v1, len1, v2, len2);
    }
    void sort_data(node_data_vec& nodes_for_sort, int trie_level) {
      clock_t t = clock();
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [](const struct node_data& lhs, const struct node_data& rhs) -> bool {
        if (rhs.data == NULL)
          return false;
        if (lhs.data == NULL)
          return true;
        return gen::compare(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
      });
      t = gen::print_time_taken(t, "Time taken for sort vals: ");
    }
};

class uniq_maker {
  public:
    static uint32_t make_uniq(byte_ptr_vec& all_node_sets, gen::byte_blocks& all_data, gen::byte_blocks& uniq_data,
          uniq_info_vec& uniq_vec, sort_callbacks& sic, uint32_t& max_len, int trie_level = 0, size_t col_idx = 0, size_t column_count = 0, char type = MST_BIN) {
      node_data_vec nodes_for_sort;
      add_to_node_data_vec(nodes_for_sort, all_node_sets, sic, all_data, type);
      return sort_and_reduce(nodes_for_sort, all_data, uniq_data, uniq_vec, sic, max_len, trie_level, type);
    }
    static void add_to_node_data_vec(node_data_vec& nodes_for_sort, byte_ptr_vec& all_node_sets, sort_callbacks& sic,
          gen::byte_blocks& all_data, char type = MST_BIN) {
      for (uint32_t i = 1; i < all_node_sets.size(); i++) {
        leopard::node_set_handler cur_ns(all_node_sets, i);
        leopard::node n = cur_ns.first_node();
        for (size_t k = 0; k <= cur_ns.last_node_idx(); k++) {
          uint32_t len = 0;
          uint8_t *pos = sic.get_data_and_len(n, len, type);
          if (pos != NULL || len == 1) {
            // printf("%d, [%.*s]\n", len, len, pos);
            nodes_for_sort.push_back((struct node_data) { pos, len, i, (uint8_t) k});
          }
          n.next();
        }
      }
      gen::gen_printf("Nodes for sort size: %lu\n", nodes_for_sort.size());
    }
    static uint32_t sort_and_reduce(node_data_vec& nodes_for_sort, gen::byte_blocks& all_data,
          gen::byte_blocks& uniq_data, uniq_info_vec& uniq_vec, sort_callbacks& sic, uint32_t& max_len, int trie_level, char type = MST_BIN) {
      clock_t t = clock();
      if (nodes_for_sort.size() == 0)
        return 0;
      sic.sort_data(nodes_for_sort, trie_level);
      uint32_t freq_count = 0;
      uint32_t tot_freq = 0;
      node_data_vec::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->data;
      int prev_val_len = it->len;
      while (it != nodes_for_sort.end()) {
        int cmp = sic.compare(it->data, it->len, prev_val, prev_val_len, trie_level);
        if (cmp != 0) {
          uniq_info *ui_ptr = new uniq_info(0, prev_val_len, uniq_vec.size(), 0);
          ui_ptr->freq_count = freq_count;
          uniq_vec.push_back(ui_ptr);
          tot_freq += freq_count;
          for (int i = 0; i < prev_val_len; i++) {
            uint8_t b = prev_val[i];
            if (b >= 15 && b < 32)
              ui_ptr->flags |= LPDU_BIN;
              //uniq_vec[0]->flags |= LPDU_BIN;
          }
          if (trie_level == 0) // TODO: no need for vals
            uniq_data.push_back(prev_val, prev_val_len);
          else
            uniq_data.push_back_rev(prev_val, prev_val_len);
          ui_ptr->pos = uniq_data.size() - prev_val_len;
          if (max_len < prev_val_len)
            max_len = prev_val_len;
          freq_count = 0;
          prev_val = it->data;
          prev_val_len = it->len;
        }
        freq_count++; // += it->freq;
        sic.set_uniq_pos(it->ns_id, it->node_idx, uniq_vec.size());
        it++;
      }
      uniq_info *ui_ptr = new uniq_info(0, prev_val_len, uniq_vec.size(), 0);
      ui_ptr->freq_count = freq_count;
      uniq_vec.push_back(ui_ptr);
      for (int i = 0; i < prev_val_len; i++) {
        uint8_t b = prev_val[i];
        if (b >= 15 && b < 32)
          ui_ptr->flags |= LPDU_BIN;
          // uniq_vec[0]->flags |= LPDU_BIN;
      }
      if (trie_level == 0)
        uniq_data.push_back(prev_val, prev_val_len);
      else
        uniq_data.push_back_rev(prev_val, prev_val_len);
      ui_ptr->pos = uniq_data.size() - prev_val_len;
      if (max_len < prev_val_len)
        max_len = prev_val_len;
      t = gen::print_time_taken(t, "Time taken for make_uniq: ");
      return tot_freq;
    }
};

class fast_vint {
  private:
    char data_type;
    uint8_t hdr_size;
    uint8_t dec_count;
    std::vector<int64_t> i64_data;
    std::vector<int32_t> i32_data;
    std::vector<double> dbl_data;
    std::vector<uint8_t> dbl_exceptions;
    ptr_groups *ptr_grps;
    byte_vec *block_data;
    int64_t for_val;
    size_t count;
    bool is64bit;
    bool is_neg;
    bool is_dbl_exceptions;
  public:
    fast_vint(char _data_type) {
      data_type = _data_type;
      hdr_size = data_type == MST_DECV ? 3 : 2;
      dec_count = 0;
      for_val = INT64_MAX;
      count = 0;
      is64bit = false;
      is_neg = false;
      is_dbl_exceptions = false;
      block_data = nullptr;
    }
    ~fast_vint() {
    }
    void reset_block() {
      for_val = INT64_MAX;
      count = 0;
      dec_count = 0;
      is64bit = false;
      is_neg = false;
      is_dbl_exceptions = false;
      i64_data.clear();
      i32_data.clear();
      dbl_data.clear();
      dbl_exceptions.clear();
    }
    void set_block_data(byte_vec *bd) {
      block_data = bd;
    }
    void set_ptr_grps(ptr_groups *_ptr_grps) {
      ptr_grps = _ptr_grps;
    }
    void remove_neg_bit() {
      if (!is_neg) {
        for_val = INT64_MAX;
        for (size_t i = 0; i < i64_data.size(); i++) {
          i64_data[i] = flavic48::zigzag_decode(i64_data[i]);
          if (for_val > i64_data[i])
            for_val = i64_data[i];
        }
        for (size_t i = 0; i < i32_data.size(); i++)
          i32_data[i] = flavic48::zigzag_decode(i32_data[i]);
      }
    }
    void add(uint32_t node_id, uint8_t *data_pos, size_t data_len) {
      int64_t i64;
      if (data_type == MST_DECV) {
        double dbl = *((double *) data_pos);
        if (dbl == -0.0) {
          dbl = 0;
          ptr_grps->set_null(node_id);
        }
        uint8_t frac_width = flavic48::cvt_dbl2_i64(dbl, i64);
        if (frac_width == UINT8_MAX || std::abs(i64) > 18014398509481983LL) {
          dbl_exceptions.push_back(1);
          is_dbl_exceptions = true;
          is64bit = true;
        } else {
          if (dec_count < frac_width)
            dec_count = frac_width;
          dbl_exceptions.push_back(0);
          if (dbl < 0)
            is_neg = true;
        }
        dbl_data.push_back(dbl);
      } else {
        flavic48::simple_decode(data_pos, 1, &i64);
        dbl_exceptions.push_back(0);
        if (i64 == -1) { // null
          i64 = 0;
          ptr_grps->set_null(node_id);
        }
        i64_data.push_back(i64);
        if (i64 > INT32_MAX)
          is64bit = true;
        else
          i32_data.push_back(i64);
        if (i64 & 1)
          is_neg = true;
      }
      if (for_val > i64)
        for_val = i64;
      count++;
    }
    void print_bits(uint64_t u64) {
      uint64_t mask = (1ULL << 63);
      for (size_t i = 0; i < 64; i++) {
        printf("%d", u64 & mask ? 1 : 0);
        mask >>= 1;
      }
      printf("\n");
    }
    size_t build_block() {
      if (data_type == MST_DECV) {
        for_val = INT64_MAX;
        for (size_t i = 0; i < dbl_data.size(); i++) {
          int64_t i64;
          double dbl = dbl_data[i];
              // printf("dbl: %lu, %lf\n", i, dbl);
          i64 = static_cast<int64_t>(dbl * flavic48::tens[dec_count]);
          if (dbl_exceptions[i] == 0) {
            double dbl_back = static_cast<double>(i64);
            dbl_back /= flavic48::tens[dec_count];
            if (dbl != dbl_back) {
              dbl_exceptions[i] = 1;
              is_dbl_exceptions = true;
            }
          }
          if (dbl_exceptions[i] == 0) {
            i64 = flavic48::zigzag_encode(i64);
            if (i64 & 1)
              is_neg = true;
            i64_data.push_back(i64);
            if (i64 > INT32_MAX)
              is64bit = true;
            else
              i32_data.push_back(i64);
          } else {
            // printf("Exception: %lf\n", dbl);
            memcpy(&i64, &dbl, 8);
            i64 = flavic48::zigzag_encode(i64);
            i64_data.push_back(i64);
            is64bit = true;
            is_neg = true;
          }
          // if (is_dbl_exceptions)
          //   print_bits(static_cast<uint64_t>(i64));
          // printf("for_val: %lld, i64: %lld\n", for_val, i64_data[i]);
          if (for_val > i64)
            for_val = i64;
        }
      }
      remove_neg_bit();
      if (is_dbl_exceptions)
        for_val = 0;
      for (size_t i = 0; i < i64_data.size(); i++) {
        i64_data[i] -= for_val;
        // printf("data64: %lld\n", i64_data[i]);
      }
      for (size_t i = 0; i < i32_data.size(); i++) {
        i32_data[i] -= for_val;
        // printf("data32: %u\n", i32_data[i]);
      }
      // printf("for val: %lld\n", for_val);
      uint8_t hdr_b1 = (is64bit ? 0x80 : 0x00) | (is_neg ? 0x40 : 0x00);
      size_t last_size = block_data->size();
      block_data->resize(block_data->size() + i64_data.size() * 9 + hdr_size + 2);
      block_data->at(last_size + 0) = hdr_b1;
      block_data->at(last_size + 1) = count;
      if (hdr_size == 3)
        block_data->at(last_size + 2) = dec_count;
      // printf("Dec_count: %d\n", dec_count);
      size_t blk_size;
      if (is64bit)
        blk_size = flavic48::encode(i64_data.data(), count, block_data->data() + last_size + hdr_size, for_val, dbl_exceptions.data());
      else
        blk_size = flavic48::encode(i32_data.data(), count, block_data->data() + last_size + hdr_size, for_val);
      // printf("Total blk size: %lu, cur size: %lu\n", block_data->size(), blk_size);
      block_data->resize(last_size + blk_size + hdr_size);
      return blk_size + hdr_size;
    }
};

class builder : public builder_fwd {

  private:
    fwd_cache *f_cache;
    nid_cache *r_cache;
    uint32_t *f_cache_freq;
    uint32_t *r_cache_freq;
    //dfox uniq_basix_map;
    //basix uniq_basix_map;
    //art_tree at;

    //builder(builder const&);
    //builder& operator=(builder const&);

    void append64_t(byte_vec& byv, uint64_t b64) {
      // byv.push_back(b64 >> 56);
      // byv.push_back((b64 >> 48) & 0xFF);
      // byv.push_back((b64 >> 40) & 0xFF);
      // byv.push_back((b64 >> 32) & 0xFF);
      // byv.push_back((b64 >> 24) & 0xFF);
      // byv.push_back((b64 >> 16) & 0xFF);
      // byv.push_back((b64 >> 8) & 0xFF);
      // byv.push_back(b64 & 0xFF);
      byv.push_back(b64 & 0xFF);
      byv.push_back((b64 >> 8) & 0xFF);
      byv.push_back((b64 >> 16) & 0xFF);
      byv.push_back((b64 >> 24) & 0xFF);
      byv.push_back((b64 >> 32) & 0xFF);
      byv.push_back((b64 >> 40) & 0xFF);
      byv.push_back((b64 >> 48) & 0xFF);
      byv.push_back(b64 >> 56);
    }
    void append_flags(byte_vec& byv, uint64_t bm_leaf, uint64_t bm_child, uint64_t bm_term, uint64_t bm_ptr) {
      append64_t(byv, bm_child);
      append64_t(byv, bm_term);
      append64_t(byv, bm_ptr);
      if (get_opts()->trie_leaf_count > 0)
        append64_t(byv, bm_leaf);
    }
    void append_byte_vec(byte_vec& byv1, byte_vec& byv2) {
      for (size_t k = 0; k < byv2.size(); k++)
        byv1.push_back(byv2[k]);
    }

  public:
    leopard::trie memtrie;
    char *out_filename;
    byte_vec trie;
    gen::bit_vector<uint64_t> louds;
    byte_vec trie_flags;
    byte_vec trie_flags_tail;
    byte_vec trie_flags_leaf;
    uint32_t end_loc;
    gen::byte_blocks *all_vals;
    gen::byte_blocks uniq_tails;
    uniq_info_vec uniq_tails_rev;
    uniq_info_vec uniq_vals_fwd;
    gen::byte_blocks uniq_vals;
    gen::word_matcher wm;
    tail_val_maps tail_maps;
    tail_val_maps val_maps;
    int cur_col_idx;
    int cur_seq_idx;
    bool is_ns_sorted;
    bool is_processing_cols;
    std::vector<uint32_t> rec_pos_vec;
    size_t max_val_len;
    uint32_t max_level;
    uint32_t column_count;
    char *names;
    char *column_encodings;
    char *column_types;
    uint16_t *names_positions;
    uint16_t names_len;
    uint32_t prev_val_size;
    uint64_t *val_table;
    leopard::trie *col_trie;
    builder *col_trie_builder;
    builder *tail_trie_builder;
    char *sk_col_positions;
    uint8_t null_value[15];
    size_t null_value_len;
    uint8_t empty_value[15];
    size_t empty_value_len;
    trie_parts tp;
    builder(const char *out_file = NULL, const char *_names = "kv_tbl,key,value", const int _column_count = 2,
        const char *_column_types = "tt", const char *_column_encodings = "uu", int _trie_level = 0,
        uint16_t _pk_col_count = 1, const bldr_options *_opts = &dflt_opts,
        const char *_sk_col_positions = "",
        const uint8_t *_null_value = NULL_VALUE, size_t _null_value_len = NULL_VALUE_LEN,
        const uint8_t *_empty_value = EMPTY_VALUE, size_t _empty_value_len = EMPTY_VALUE_LEN)
        : memtrie(_null_value, _null_value_len, _empty_value, _empty_value_len),
          tail_maps (this, uniq_tails, uniq_tails_rev),
          val_maps (this, uniq_vals, uniq_vals_fwd),
          builder_fwd (_pk_col_count), wm (uniq_vals) {
      opts = new bldr_options[_opts->opts_count];
      memcpy(opts, _opts, sizeof(bldr_options) * _opts->opts_count);
      is_processing_cols = false;
      tail_maps.init();
      val_maps.init();
      if (_sk_col_positions == nullptr)
        _sk_col_positions = "";
      sk_col_positions = new char[strlen(_sk_col_positions) + 1];
      strcpy(sk_col_positions, _sk_col_positions);
      memcpy(null_value, _null_value, _null_value_len);
      null_value_len = _null_value_len;
      memcpy(empty_value, _empty_value, _empty_value_len);
      empty_value_len = _empty_value_len;
      trie_level = _trie_level;
      tp = {};
      col_trie = NULL;
      col_trie_builder = NULL;
      tail_trie_builder = NULL;
      column_count = _column_count;
      val_table = new uint64_t[_column_count];
      column_encodings = new char[_column_count];
      memset(column_encodings, 'u', _column_count);
      *column_encodings = MSE_TRIE; // first letter is for key
      memcpy(column_encodings, _column_encodings, gen::min(strlen(_column_encodings), _column_count));
      column_types = new char[_column_count];
      memset(column_types, '*', _column_count);
      memcpy(column_types, _column_types, gen::min(strlen(_column_types), _column_count));
      set_names(_names, _column_types, column_encodings);
      //art_tree_init(&at);
      memtrie.set_print_enabled(gen::is_gen_print_enabled);
      fp = NULL;
      out_filename = NULL;
      if (out_file != NULL)
        set_out_file(out_file);
      f_cache = nullptr;
      r_cache = nullptr;
      f_cache_freq = nullptr;
      r_cache_freq = nullptr;
      max_level = 0;
      cur_col_idx = 0;
      max_val_len = 8;
      rec_pos_vec.push_back(0);
      all_vals = new gen::byte_blocks();
      all_vals->push_back("\0", 2);
      cur_seq_idx = 0;
      is_ns_sorted = false;
    }

    virtual ~builder() {
      delete [] names;
      delete [] val_table;
      delete [] names_positions;
      delete [] column_encodings;
      delete [] column_types;
      delete [] sk_col_positions;
      if (out_filename != NULL)
        delete [] out_filename;
      if (col_trie_builder != NULL)
        delete col_trie_builder;
      if (tail_trie_builder != NULL)
        delete tail_trie_builder;
      if (f_cache != nullptr)
        delete [] f_cache;
      if (r_cache != nullptr)
        delete [] r_cache;
      if (f_cache_freq != nullptr)
        delete [] f_cache_freq;
      if (r_cache_freq != nullptr)
        delete [] r_cache_freq;
      if (all_vals != NULL)
        delete all_vals;
      //close_file(); // TODO: Close for nested tries?
    }

    leopard::trie *get_memtrie() {
      return &memtrie;
    }

    void close_file() {
      if (fp != NULL)
        fclose(fp);
      fp = NULL;
    }

    void set_names(const char *_names, const char *_column_types, const char *_column_encodings) {
      names_len = strlen(_names) + strlen(sk_col_positions) + column_count * 2 + 4;
      names = new char[names_len];
      memset(names, '*', column_count);
      memcpy(names, _column_types, gen::min(strlen(_column_types), column_count));
      names[column_count] = ',';
      memcpy(names + column_count + 1, _column_encodings, column_count);
      names[column_count * 2 + 1] = ',';
      memcpy(names + column_count * 2 + 2, _names, strlen(_names));
      names[column_count * 2 + 2 + strlen(_names)] = ',';
      memcpy(names + column_count * 2 + 2 + strlen(_names) + 1, sk_col_positions, strlen(sk_col_positions));
      // std::cout << names << std::endl;
      int pos_count = column_count + 2;
      names_positions = new uint16_t[pos_count];
      int idx = 0;
      int name_str_pos = column_count;
      while (name_str_pos < names_len) {
        if (names[name_str_pos] == ',') {
          names[name_str_pos] = '\0';
          if (idx < pos_count)
            names_positions[idx++] = name_str_pos + 1;
        }
        name_str_pos++;
      }
      name_str_pos--;
      names[column_count] = '\0';
      names[column_count * 2 + 1] = '\0';
      names[name_str_pos] = '\0';
    }

    void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    void set_out_file(const char *out_file) {
      if (out_file == NULL)
        return;
      int len = strlen(out_file);
      if (out_filename != NULL)
        delete [] out_filename;
      out_filename = new char[len + 1];
      strcpy(out_filename, out_file);
    }

    void set_all_vals(gen::byte_blocks *_all_vals, bool to_delete_prev = true) {
      if (all_vals != nullptr && to_delete_prev)
        delete all_vals;
      all_vals = _all_vals;
    }

    size_t size() {
      return memtrie.key_count;
    }

    // uint64_t avg_freq_all_ns;
    uint32_t set_ns_freq(uint32_t ns_id, int level) {
      if (ns_id == 0)
        return 1;
      leopard::node_set_handler ns(memtrie.all_node_sets, ns_id);
      leopard::node n = ns.first_node();
      leopard::node_set_header *ns_hdr = ns.hdr();
      uint32_t freq_count = 1;
      for (int i = 0; i <= ns_hdr->last_node_idx; i++) {
        uint32_t node_freq = set_ns_freq(n.get_child(), level + 1);
        freq_count += node_freq;
        n.next();
      }
      // avg_freq_all_ns += freq_count;
      ns_hdr->freq = freq_count; // > 0xFFFF ? 0xFFFF : freq_count;
      return freq_count;
    }

    void split_tails() {
      clock_t t = clock();
      typedef struct {
        uint8_t *part;
        uint32_t uniq_arr_idx;
        uint32_t ns_id;
        uint16_t part_len;
        uint8_t node_idx;
      } tail_part;
      typedef struct {
        uint8_t *uniq_part;
        uint32_t freq_count;
        uint16_t uniq_part_len;
      } tail_uniq_part;
      std::vector<tail_part> tail_parts;
      std::vector<tail_uniq_part> tail_uniq_parts;
      leopard::node_iterator ni(memtrie.all_node_sets, 1);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uint8_t *tail = (*memtrie.all_tails)[cur_node.get_tail()];
          size_t vlen;
          uint32_t tail_len = gen::read_vint32(tail, &vlen);
          tail += vlen;
          int last_word_len = 0;
          bool is_prev_non_word = false;
          std::vector<tail_part> tail_parts1;
          for (size_t i = 0; i < tail_len; i++) {
            uint8_t c = tail[i];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c > 127) {
              if (last_word_len >= 5 && is_prev_non_word) {
                if (tail_len - i < 5) {
                  last_word_len += (tail_len - i);
                  break;
                }
                if (i > last_word_len)
                  tail_parts1.push_back((tail_part) {tail + i - last_word_len, 0, (uint32_t) ni.get_cur_nsh_id(), (uint16_t) last_word_len, ni.get_cur_sib_id()});
                last_word_len = 0;
              }
              is_prev_non_word = false;
            } else {
              is_prev_non_word = true;
            }
            last_word_len++;
          }
          if (last_word_len < tail_len)
            tail_parts1.push_back((tail_part) {tail + tail_len - last_word_len, 0, (uint32_t) ni.get_cur_nsh_id(), (uint16_t) last_word_len, ni.get_cur_sib_id()});
          if (tail_parts1.size() > 0) {
            tail_parts.insert(tail_parts.end(), tail_parts1.begin(), tail_parts1.end());
            // printf("Tail: [%.*s]\n", (int) tail_len, tail);
            // for (size_t i = 0; i < tail_parts1.size(); i++) {
            //   tail_part *tp = &tail_parts1[i];
            //   printf("[%.*s]\n", tp->part_len, tp->part);
            // }
          }
        }
        cur_node = ni.next();
      }
      gen::gen_printf("Parts count: %lu\n", tail_parts.size());
      if (tail_parts.size() == 0)
        return;
      std::sort(tail_parts.begin(), tail_parts.end(), [](const tail_part& lhs, const tail_part& rhs) -> bool {
        return gen::compare(lhs.part, lhs.part_len, rhs.part, rhs.part_len) < 0;
      });
      tail_part *prev_tp = &tail_parts[0];
      uint32_t freq_count = 0;
      uint32_t uniq_arr_idx = 0;
      for (size_t i = 0; i < tail_parts.size(); i++) {
        tail_part *tp = &tail_parts[i];
        tp->uniq_arr_idx = uniq_arr_idx;
        int cmp = gen::compare(tp->part, tp->part_len, prev_tp->part, prev_tp->part_len);
        if (cmp == 0) {
          freq_count++;
        } else {
          tail_uniq_parts.push_back((tail_uniq_part) {prev_tp->part, freq_count, prev_tp->part_len});
          uniq_arr_idx++;
          freq_count = 1;
        }
        prev_tp = tp;
      }
      tail_uniq_parts.push_back((tail_uniq_part) {prev_tp->part, freq_count, prev_tp->part_len});
      std::sort(tail_parts.begin(), tail_parts.end(), [](const tail_part& lhs, const tail_part& rhs) -> bool {
        return lhs.part > rhs.part;
      });
      gen::gen_printf("Uniq Parts count: %lu\n", tail_uniq_parts.size());
      // std::sort(tail_uniq_parts.begin(), tail_uniq_parts.end(), [](const tail_uniq_part& lhs, const tail_uniq_part& rhs) -> bool {
      //   return lhs.freq_count > rhs.freq_count;
      // });
      // for (size_t i = 0; i < tail_uniq_parts.size(); i++) {
      //   tail_uniq_part *tup = &tail_uniq_parts[i];
      //   printf("%u\t[%.*s]\n", tup->freq_count, (int) tup->uniq_part_len, tup->uniq_part);
      // }
      leopard::node new_node;
      leopard::node_set_handler new_nsh(memtrie.all_node_sets, 0);
      leopard::node_set_handler nsh(memtrie.all_node_sets, 0);
      for (size_t i = 0; i < tail_parts.size(); i++) {
        tail_part *tp = &tail_parts[i];
        tail_uniq_part *utp = &tail_uniq_parts[tp->uniq_arr_idx];
        // if (utp->freq_count > 1) {
          nsh.set_pos(tp->ns_id);
          cur_node = nsh[tp->node_idx];
          uint8_t *tail = (*memtrie.all_tails)[cur_node.get_tail()];
          size_t vlen;
          uint32_t tail_len = gen::read_vint32(tail, &vlen);
          uint32_t orig_tail_new_len = tp->part - tail - vlen;
          uint32_t new_tail_len = tail_len - orig_tail_new_len;
          gen::copy_vint32(orig_tail_new_len, tail, vlen);
          //printf("Tail: [%.*s], pos: %u, len: %u, new len: %u, Part: [%.*s], len: %u\n", (int) tail_len, tail + vlen, cur_node.get_tail(), tail_len, orig_tail_new_len, (int) tp->part_len, tp->part, new_tail_len);
          size_t new_tail_pos = memtrie.all_tails->push_back_with_vlen(tp->part, new_tail_len);
          uint32_t new_ns_pos = leopard::node_set_handler::create_node_set(memtrie.all_node_sets, 1);
          new_nsh.set_pos(new_ns_pos);
          new_node = new_nsh.first_node();
          new_node.set_flags(cur_node.get_flags() | NFLAG_TERM);
          new_node.set_child(cur_node.get_child());
          new_node.set_col_val(cur_node.get_col_val());
          new_node.set_byte(*tp->part);
          //printf("New tail pos: %lu\n", new_tail_pos);
          new_node.set_tail(new_tail_pos);
          cur_node.set_flags((cur_node.get_flags() | NFLAG_CHILD) & ~NFLAG_LEAF);
          cur_node.set_child(new_ns_pos);
          cur_node.set_col_val(0);
          memtrie.node_set_count++;
          memtrie.node_count++;
        // }
      }
      gen::gen_printf("New NS count: %lu\n", memtrie.all_node_sets.size());
      gen::print_time_taken(t, "Time taken for tail split: ");
    }

    void sort_nodes_on_freq(leopard::node_set_handler& nsh) {
      leopard::node cur_node = nsh.first_node();
      leopard::node_s *ns = cur_node.get_node_struct();
      // uint32_t avg_freq = 0;
      // int n_count = nsh.last_node_idx() + 1;
      // node_set_handler nsh_child(all_node_sets, ns->child);
      // for (int i = 0; i < n_count; i++) {
      //   if (ns->child > 0) {
      //     avg_freq += nsh_child.hdr()->freq;
      //   } else
      //     avg_freq++;
      //   cur_node.next();
      //   ns = cur_node.get_node_struct();
      //   if (ns != nullptr)
      //     nsh_child.set_pos(ns->child);
      // }
      // avg_freq /= n_count;
      // ns = nsh.first_node().get_node_struct();
      // std::sort(ns, ns + nsh.last_node_idx(), [this,avg_freq,n_count](const node_s& lhs, const node_s& rhs) -> bool {
      std::sort(ns, ns + nsh.last_node_idx(), [this](const leopard::node_s& lhs, const leopard::node_s& rhs) -> bool {
        uint32_t lhs_freq = 1;
        uint32_t rhs_freq = 1;
        if (lhs.child > 0) {
          leopard::node_set_handler nsh_l(this->memtrie.all_node_sets, lhs.child);
          lhs_freq = nsh_l.hdr()->freq;
        }
        if (rhs.child > 0) {
          leopard::node_set_handler nsh_r(this->memtrie.all_node_sets, rhs.child);
          rhs_freq = nsh_r.hdr()->freq;
        }
        // if (lhs_freq >= avg_freq || rhs_freq >= avg_freq || n_count > 16)
        // if (n_count < 16)
          return lhs_freq > rhs_freq;
        // return lhs.b < rhs.b;
      });
      for (size_t i = 0; i <= nsh.last_node_idx(); i++) {
        leopard::node n = nsh[i];
        n.set_flags(n.get_flags() & ~NFLAG_TERM);
        if (i == nsh.last_node_idx())
          n.set_flags(n.get_flags() | NFLAG_TERM);
        // uint32_t node_freq = 1;
        // if (n.get_child() > 0) {
        //   node_set_handler nsh_c(this->all_node_sets, n.get_child());
        //   node_freq = nsh_c.hdr()->freq;
        // }
        // printf("%c(%d/%u) ", n.get_byte(), n.get_byte(), node_freq);
      }
      // printf("\n");
    }

    void swap_node_sets(uint32_t pos_from, uint32_t pos_to) {
      if (pos_from == pos_to) {
        memtrie.all_node_sets[pos_to][0] |= NODE_SET_SORTED;
        return;
      }
      while (memtrie.all_node_sets[pos_from][0] & NODE_SET_SORTED)
        pos_from = ((leopard::node_set_header *) memtrie.all_node_sets[pos_from])->swap_pos;
      uint8_t *ns = memtrie.all_node_sets[pos_to];
      memtrie.all_node_sets[pos_to] = memtrie.all_node_sets[pos_from];
      memtrie.all_node_sets[pos_from] = ns;
      leopard::node_set_header *nsh = (leopard::node_set_header *) memtrie.all_node_sets[pos_to];
      nsh->swap_pos = pos_from;
      memtrie.all_node_sets[pos_to][0] |= NODE_SET_SORTED;
    }

    void sort_node_sets() {
      clock_t t = clock();
      uint32_t nxt_set = 0;
      uint32_t nxt_node = 0;
      uint32_t node_set_id = 1;
      leopard::node_set_handler nsh(memtrie.all_node_sets, 0);
      if (get_opts()->sort_nodes_on_freq) {
        // avg_freq_all_ns = 0;
        set_ns_freq(1, 0);
        // printf("Avg freq: %llu\n", avg_freq_all_ns);
        // avg_freq_all_ns /= node_set_count;
        // printf("Avg freq: %llu\n", avg_freq_all_ns);
        sort_nodes_on_freq(nsh);
      }
      leopard::node n = nsh.first_node();
      while (nxt_set < memtrie.all_node_sets.size()) {
        if (n == NULL) {
          nxt_set++;
          if (nxt_set == memtrie.all_node_sets.size())
            break;
          nsh.set_pos(nxt_set);
          if (get_opts()->sort_nodes_on_freq)
            sort_nodes_on_freq(nsh);
          n = nsh.first_node();
          nxt_node = 0;
          continue;
        }
        uint32_t nxt_child = n.get_child();
        if (nxt_child == 0) {
          nxt_node++;
          n.next();
          continue;
        }
        n.set_child(node_set_id);
        swap_node_sets(nxt_child, node_set_id);
        node_set_id++;
        nxt_node++;
        n.next();
      }
      is_ns_sorted = true;
      gen::print_time_taken(t, "Time taken for sort_nodes(): ");
    }

    uint8_t append_tail_ptr(leopard::node *cur_node) {
      if ((cur_node->get_flags() & NFLAG_TAIL) == 0)
        return cur_node->get_byte();
      if (get_opts()->inner_tries && tail_trie_builder != nullptr) {
        uniq_info_base *ti = (*tail_maps.get_ui_vec())[cur_node->get_tail()];
        return ti->ptr & 0xFF;
      }
      uint8_t node_val;
      uint32_t ptr = 0;
      ptr_groups *ptr_grps = tail_maps.get_grp_ptrs();
        uniq_info_base *ti = (*tail_maps.get_ui_vec())[cur_node->get_tail()];
        uint8_t grp_no = ti->grp_no;
        // if (grp_no == 0 || ti->tail_ptr == 0)
        //   gen::gen_printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
        ptr = ti->ptr;
        freq_grp *fg = ptr_grps->get_freq_grp(grp_no);
        int node_val_bits = 8 - fg->code_len;
        node_val = (fg->code << node_val_bits) | (ptr & ((1 << node_val_bits) - 1));
        ptr >>= node_val_bits;
        ptr_grps->append_ptr_bits(ptr, fg->grp_log2);
      return node_val;
    }

    FILE *fpp;
    void dump_ptr(leopard::node *cur_node, uint32_t node_id) {
      if ((node_id % 64) == 0) {
        fputc('\n', fpp);
      }
      if ((cur_node->get_flags() & NFLAG_TAIL) == 0)
        return;
      uint32_t ptr = 0;
      ptr_groups *ptr_grps = tail_maps.get_grp_ptrs();
      uniq_info_base *ti = (*tail_maps.get_ui_vec())[cur_node->get_tail()];
      uint8_t grp_no = ti->grp_no;
      // if (grp_no == 0 || ti->tail_ptr == 0)
      //   gen::gen_printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
      ptr = ti->ptr;
      freq_grp *fg = ptr_grps->get_freq_grp(grp_no);
      int node_val_bits = 8 - fg->code_len;
      ptr >>= node_val_bits;
      if (fg->grp_log2 > 0)
        fprintf(fpp, "%u\t%u\t%u\n", grp_no, fg->grp_log2, ptr);
    }

    typedef struct {
      uint8_t *word_pos;
      uint32_t word_len;
      uint32_t word_ptr_pos;
      uint32_t node_id;
    } word_refs;

    void add_rev_node_id(byte_vec& rev_nids, uint32_t node_start, uint32_t node_end, uint32_t prev_node_start_id) {
      if (node_start == 0)
        return;
      if (node_end != UINT32_MAX) {
        node_end -= node_start;
        node_end--;
      }
      if (node_start == prev_node_start_id) {
        // printf("multiple words same node id: %u\n", node_start);
        return;
      }
      node_start -= prev_node_start_id;
      if (prev_node_start_id > 0)
        node_start--;
      gen::append_vint32(rev_nids, (node_start << 1) | (node_end != UINT32_MAX ? 1 : 0));
      if (node_end != UINT32_MAX)
         gen::append_vint32(rev_nids, node_end);
    }

    uint32_t build_words(std::vector<word_refs>& words_for_sort, std::vector<uint32_t>& word_ptrs,
                  uniq_info_vec& uniq_words_vec, gen::byte_blocks& uniq_words,
                  uint32_t max_word_count, uint32_t total_word_entries, uint32_t rpt_freq, uint32_t max_rpts,
                  gen::byte_blocks *rev_col_vals) {

      clock_t t = clock();

      std::sort(words_for_sort.begin(), words_for_sort.end(), [](const word_refs& lhs, const word_refs& rhs) -> bool {
        int cmp = gen::compare(lhs.word_pos, lhs.word_len, rhs.word_pos, rhs.word_len);
        return (cmp == 0) ? (lhs.node_id < rhs.node_id) : (cmp < 0);
      });

      byte_vec rev_nids;
      rev_col_vals->reset();
      rev_col_vals->push_back("\xFF\xFF", 2);
      uint32_t prev_node_id = 0;
      uint32_t prev_node_start_id = 0;
      uint32_t node_start = 0;
      uint32_t node_end = UINT32_MAX;

      uint32_t tot_freq = 0;
      uint32_t freq_count = 0;
      std::vector<word_refs>::iterator it = words_for_sort.begin();
      uint8_t *prev_val = it->word_pos;
      uint32_t prev_val_len = it->word_len;
      while (it != words_for_sort.end()) {
        int cmp = gen::compare(it->word_pos, it->word_len, prev_val, prev_val_len);
        // if (memcmp(it->word_pos, (const uint8_t *) "Etching", 7) == 0) {
        //   printf("[%.*s], nid: %u\n", (int) it->word_len, it->word_pos, it->node_id);
        // }
        if (cmp != 0) {
          uniq_info_base *ui_ptr = new uniq_info_base({0, prev_val_len, (uint32_t) uniq_words_vec.size()});
          ui_ptr->freq_count = freq_count;
          uniq_words_vec.push_back(ui_ptr);
          tot_freq += freq_count;
          ui_ptr->pos = uniq_words.push_back(prev_val, prev_val_len);
          freq_count = 0;
          prev_val = it->word_pos;
          prev_val_len = it->word_len;
              // if (memcmp(it->word_pos, (const uint8_t *) "Etching", 7) == 0)
              //   printf("ns: %u, ne: %u, pns: %u\n", node_start, node_end, prev_node_start_id);
              add_rev_node_id(rev_nids, node_start, node_end, prev_node_start_id);
              // printf("Rev nids size: %lu\n", rev_nids.size());
              ui_ptr->ptr = rev_col_vals->push_back_with_vlen(rev_nids.data(), rev_nids.size());
              // printf("Rev nids ptr\t%u\t%lu\n", ui_ptr->ptr, rev_nids.size());
              rev_nids.clear();
              prev_node_id = 0;
              prev_node_start_id = 0;
              node_end = UINT32_MAX;
              node_start = 0;
        }
        uint32_t cur_node_id = it->node_id >> get_opts()->sec_idx_nid_shift_bits;
        if (prev_node_id == 0) {
          node_start = cur_node_id;
        } else if (cur_node_id - prev_node_id < 2) {
          node_end = cur_node_id;
        } else {
          // if (memcmp(it->word_pos, (const uint8_t *) "Etching", 7) == 0)
          //   printf("ns: %u, ne: %u, pns: %u\n", node_start, node_end, prev_node_start_id);
          add_rev_node_id(rev_nids, node_start, node_end, prev_node_start_id);
          prev_node_start_id = node_start;
          if (node_end != UINT32_MAX)
            prev_node_start_id = node_end;
          node_start = cur_node_id;
          node_end = UINT32_MAX;
        }
        prev_node_id = cur_node_id;
        freq_count++; // += it->freq;
        word_ptrs[it->word_ptr_pos] = uniq_words_vec.size();
        it++;
      }
      add_rev_node_id(rev_nids, node_start, node_end, prev_node_start_id);
      printf("Size of rev nids: %lu\n", rev_nids.size());
      uniq_info_base *ui_ptr = new uniq_info_base({0, prev_val_len, (uint32_t) uniq_words_vec.size()});
      ui_ptr->freq_count = freq_count;
      uniq_words_vec.push_back(ui_ptr);
      tot_freq += freq_count;
      ui_ptr->pos = uniq_words.push_back(prev_val, prev_val_len);
      ui_ptr->ptr = rev_col_vals->push_back_with_vlen(rev_nids.data(), rev_nids.size());
      // printf("Rev nids size: %lu\n", rev_nids.size());

      printf("\nTotal words: %lu, uniq words: %lu, rpt_count: %u, max rpts: %u, max word count: %u, word_ptr_size: %lu\n",
        words_for_sort.size(), uniq_words_vec.size(), rpt_freq, max_rpts, max_word_count, word_ptrs.size());
      t = gen::print_time_taken(t, "Time taken for make_uniq_words: ");

      uint32_t last_data_len;
      uint8_t start_bits = 1;
      uint8_t grp_no, len_grp_no, rpt_grp_no;
      rpt_grp_no = 0;
      ptr_groups& ptr_grps = *val_maps.get_grp_ptrs();
      ptr_grps.reset();
      uniq_info_vec uniq_words_freq;
      uint32_t cumu_freq_idx = val_maps.make_uniq_freq(uniq_words_vec, uniq_words_freq, tot_freq, last_data_len, start_bits, grp_no, true);

      ptr_grps.set_idx_info(start_bits, grp_no, 3);
      ptr_grps.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0}, true);

      grp_no = 1;
      len_grp_no = grp_no;
      ptr_grps.next_grp(grp_no, pow(2, ceil(log2(max_word_count)) - 1), 0, tot_freq, true);
      ptr_grps.update_current_grp(len_grp_no, max_word_count, total_word_entries, max_word_count);
      ptr_grps.append_text(len_grp_no, (const uint8_t *) "L", 1);

      if (rpt_freq > 0 && max_rpts > 0) {
        rpt_grp_no = grp_no;
        ptr_grps.next_grp(grp_no, pow(2, ceil(log2(max_rpts)) - 1), 0, tot_freq, true);
        ptr_grps.update_current_grp(rpt_grp_no, max_rpts, rpt_freq, max_rpts);
        ptr_grps.append_text(rpt_grp_no, (const uint8_t *) "R", 1);
      }

      uint32_t freq_idx = 0;
      uint32_t max_word_len = 0;
      uint32_t cur_limit = pow(2, start_bits);

      char encoding_type = column_encodings[cur_col_idx];
      builder *cur_word_trie = create_word_trie_builder(encoding_type, rev_col_vals);
      ptr_grps.inner_tries.push_back(cur_word_trie);
      ptr_grps.inner_trie_start_grp = grp_no;
      ptr_grps.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0}, true);
      while (freq_idx < uniq_words_freq.size()) {
        uniq_info_base *vi = uniq_words_freq[freq_idx];
        freq_idx++;
        uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, 1, tot_freq, false, true);
        if (new_limit != cur_limit) {
          cur_word_trie = create_word_trie_builder(encoding_type, rev_col_vals);
          ptr_grps.inner_tries.push_back(cur_word_trie);
          cur_limit = new_limit;
        }
        if (max_word_len < vi->len)
          max_word_len = vi->len;
        vi->grp_no = grp_no;
        cur_word_trie->insert(uniq_words[vi->pos], vi->len, freq_idx - 1);
        ptr_grps.update_current_grp(grp_no, 1, vi->freq_count);
      }

      ptr_grps.set_grp_nos(len_grp_no, rpt_grp_no, 0);

      uint32_t sum_trie_sizes = 0;
      for (size_t it_idx = 0; it_idx < ptr_grps.inner_tries.size(); it_idx++) {
        builder_fwd *inner_trie = ptr_grps.inner_tries[it_idx];
        is_processing_cols = false;
        uint32_t trie_size = inner_trie->build();
        printf("Trie size: %u\n", trie_size);
        // ptr_groups *it_ptr_grps = inner_trie->get_grp_ptrs();
        // it_ptr_grps->build();
        leopard::node_iterator ni(inner_trie->get_memtrie()->all_node_sets, 0);
        leopard::node n = ni.next();
        int leaf_id = 0;
        uint32_t node_id = 0;
        // printf("Inner trie %lu\n", it_idx);
        while (n != nullptr) {
          uint32_t col_val_pos = n.get_col_val();
          if (n.get_flags() & NFLAG_LEAF) {
            uniq_info_base *ti = uniq_words_freq[col_val_pos];
            // printf("Ptr: %u\n", ti->ptr);
            if (encoding_type == MSE_WORDS_2WAY)
              n.set_col_val(ti->ptr);
            ti->ptr = leaf_id++;
          }
          n = ni.next();
          node_id++;
        }
        trie_size += inner_trie->build_kv(false);
        inner_trie->set_all_vals(nullptr, false);
        freq_grp *fg = ptr_grps.get_freq_grp(it_idx + ptr_grps.inner_trie_start_grp);
        fg->grp_size = trie_size;
        // fg->grp_limit = node_id;
        // fg->count = node_id;
        sum_trie_sizes += trie_size;
        //printf("Inner Trie size:\t%u\n", trie_size);
      }

      ptr_grps.build_freq_codes(true);
      ptr_grps.show_freq_codes();

      freq_grp *cur_fg;
      leopard::node_iterator ni(memtrie.all_node_sets, pk_col_count == 0 ? 1 : 0);
      leopard::node n = ni.next();
      while (n != nullptr && (n.get_flags() & NFLAG_LEAF) == 0) {
        n = ni.next();
        n.set_col_val(0);
      }
      uint32_t ptr_bit_count = 0;
      uint32_t word_count = 0;
      for (size_t i = 0; i < word_ptrs.size(); i++) {
        if (word_count == 0) {
          word_count = word_ptrs[i];
          if ((word_count & 0x40000000L) == 0) {
            if (ptr_bit_count > 0) {
              if (n.get_flags() & NFLAG_LEAF) {
                n.set_col_val(ptr_bit_count);
                // printf("ptr_bit_count: %u\n", ptr_bit_count);
              }
              do {
                n = ni.next();
                n.set_col_val(0);
              } while (n != nullptr && (n.get_flags() & NFLAG_LEAF) == 0);
              ptr_bit_count = 0;
            }
            cur_fg = ptr_grps.get_freq_grp(len_grp_no);
            ptr_grps.append_ptr_bits(cur_fg->code, cur_fg->code_len);
            ptr_grps.append_ptr_bits(word_count, cur_fg->grp_log2 - cur_fg->code_len);
            ptr_bit_count += cur_fg->grp_log2;
            continue;
          }
        }
        if (word_count & 0x40000000L) {
          uint32_t word_rpt_count = word_count & 0x3FFFFFFFL;
          cur_fg = ptr_grps.get_freq_grp(rpt_grp_no);
          ptr_grps.append_ptr_bits(cur_fg->code, cur_fg->code_len);
          ptr_grps.append_ptr_bits(word_rpt_count, cur_fg->grp_log2 - cur_fg->code_len);
          ptr_bit_count += cur_fg->grp_log2;
          while (word_rpt_count--) {
            if (n.get_flags() & NFLAG_LEAF) { // should be always true
              n.set_col_val(0);
              n = ni.next();
            }
            while ((n.get_flags() & NFLAG_LEAF) == 0) {
              n.set_col_val(0);
              n = ni.next();
            }
          }
          word_count = 0;
          continue;
        }
        uniq_info_base *vi = uniq_words_vec[word_ptrs[i]];
        cur_fg = ptr_grps.get_freq_grp(vi->grp_no);
        ptr_grps.append_ptr_bits(cur_fg->code, cur_fg->code_len);
        ptr_grps.append_ptr_bits(vi->ptr, cur_fg->grp_log2 - cur_fg->code_len);
        ptr_bit_count += cur_fg->grp_log2;
        // printf("%u\t%u\t%u\t%u\n", vi->grp_no, cur_fg->code_len, cur_fg->grp_log2, vi->ptr);
        word_count--;
      }
      if (ptr_bit_count > 0 && n != nullptr && (n.get_flags() & NFLAG_LEAF))
        n.set_col_val(ptr_bit_count);
      ptr_grps.append_ptr_bits(0x00, 8); // read beyond protection

      printf("Total size of tries: %u, Ptrs size: %u\n", sum_trie_sizes, ptr_grps.get_ptrs_size());
      t = gen::print_time_taken(t, "Time taken for build_words(): ");

      return 0;

    }

    #define MIN_WORD_SIZE 1
    void add_words(uint32_t node_id, uint8_t *words, uint32_t words_len, std::vector<word_refs>& words_for_sort,
            std::vector<uint32_t>& word_ptrs, uint8_t *prev_val, uint32_t prev_val_len,
            uint32_t& max_word_count, uint32_t& total_word_entries, uint32_t& rpt_freq, uint32_t& max_rpts) {
      // first 2 bits of word_count_pos:
      // 00 - count of words
      // 01 - repeat prev val
      // 19 - repeat prev seq
      // 11 - tbd
      // if ((node_id % nodes_per_bv_block_n) != 0 && words_len == prev_val_len &&
      //         std::memcmp(words, prev_val, prev_val_len) == 0) {
      //   rpt_freq++;
      //   size_t last_idx = word_ptrs.size() - 1;
      //   if (word_ptrs[last_idx] == 0) {
      //     word_ptrs.push_back(0x40000001L);
      //     last_idx++;
      //   } else {
      //     // if (word_ptrs[last_idx] & 0x40000000L) { // implied
      //       word_ptrs[last_idx]++;
      //     // }
      //   }
      //   if (max_rpts < (word_ptrs[last_idx] & 0x3FFFFFFFL))
      //     max_rpts = (word_ptrs[last_idx] & 0x3FFFFFFFL);
      //   return;
      // }
      total_word_entries++;
      uint32_t last_word_len = 0;
      bool is_prev_non_word = false;
      size_t vint_len = gen::get_vlen_of_uint32(words_len);
      size_t word_count_pos = word_ptrs.size();
      word_ptrs.push_back(0); // initially 0
      size_t word_count = 0;
      uint32_t ref_id = 0;
      for (size_t i = 0; i < words_len; i++) {
        uint8_t c = words[i];
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c > 127) {
          if (last_word_len >= MIN_WORD_SIZE && is_prev_non_word) {
            if (words_len - i < MIN_WORD_SIZE) {
              last_word_len += (words_len - i);
              break;
            }
            ref_id = word_ptrs.size();
            word_ptrs.push_back(0);
            words_for_sort.push_back((word_refs) {words + i - last_word_len, last_word_len, ref_id, node_id});
            word_count++;
            last_word_len = 0;
          }
          is_prev_non_word = false;
        } else {
          is_prev_non_word = true;
        }
        last_word_len++;
      }
      if (last_word_len > 0) {
        ref_id = word_ptrs.size();
        word_ptrs.push_back(0);
        words_for_sort.push_back((word_refs) {words + words_len - last_word_len, last_word_len, ref_id, node_id});
        word_count++;
      }
      if (max_word_count < word_count)
        max_word_count = word_count;
      word_ptrs[word_count_pos] = word_count;
    }

    bool lookup_memtrie(const uint8_t *key, size_t key_len, leopard::node_set_vars& nsv) {
      if (key == NULL) {
        key = null_value;
        key_len = null_value_len;
      }
      if (key_len == 0) {
        key = empty_value;
        key_len = empty_value_len;
      }
      nsv.level = 0;
      nsv.key_pos = 0;
      nsv.node_set_pos = 1;
      leopard::node_set_handler nsh(memtrie.all_node_sets, 1);
      uint8_t key_byte = key[nsv.key_pos];
      leopard::node n;
      n = nsh.first_node();
      nsv.cur_node_idx = 0;
      do {
        uint8_t trie_byte = n.get_byte();
        if (key_byte != trie_byte) {
          nsv.cur_node_idx++;
          trie_byte = n.next();
          if (n == NULL)
            return false;
          continue;
        }
        if (key_byte == trie_byte) {
          int tail_len = 1;
          uint8_t flags = n.get_flags();
          nsv.cmp = 0;
          if (flags & NFLAG_TAIL) {
            size_t vlen;
            uniq_info_base *ti = get_ti(&n);
            uint8_t *tail = uniq_tails[ti->pos];
            tail_len = ti->len;
            nsv.cmp = gen::compare(tail, tail_len, key + nsv.key_pos, key_len - nsv.key_pos);
          }
          if (nsv.cmp == 0 && nsv.key_pos + tail_len == key_len && (flags & NFLAG_LEAF)) {
            return true;
          }
          if (nsv.cmp == 0 || abs(nsv.cmp) - 1 == tail_len) {
            nsv.key_pos += tail_len;
            if (nsv.key_pos >= key_len) {
              return false;
            }
            if ((flags & NFLAG_CHILD) == 0) {
              return false;
            }
            nsv.node_set_pos = n.get_child();
            nsv.level++;
            nsh.set_pos(nsv.node_set_pos);
            key_byte = key[nsv.key_pos];
            n = nsh.first_node();
            nsv.cur_node_idx = 0;
            continue;
          }
          if (abs(nsv.cmp) - 1 == key_len - nsv.key_pos) {
            return false;
          }
          return false;
        }
        return false;
      } while (n != NULL);
      return false;
    }

    typedef struct {
      uint32_t link;
      uint32_t node_start;
      uint32_t node_end;
    } rev_trie_node_map;

    uint32_t build_col_trie(gen::byte_blocks *val_blocks, std::vector<rev_trie_node_map>& revmap_vec, gen::byte_blocks& col_trie_vals, char encoding_type) {
      uint32_t col_trie_size = col_trie_builder->build();
      printf("Col trie size: %u\n", col_trie_size);
      if (encoding_type == MSE_TRIE_2WAY) {
        col_trie_vals.push_back("\xFF\xFF", 2);
      }
      uint32_t max_node_id = 0;
      uint32_t node_id = 0;
      uint32_t leaf_id = 1;
      size_t node_map_count = 0;
      leopard::node n;
      leopard::node_set_vars nsv;
      leopard::node_set_handler cur_ns(memtrie.all_node_sets, 1);
      for (uint32_t i = 1; i < memtrie.all_node_sets.size(); i++) {
        cur_ns.set_pos(i);
        if (cur_ns.hdr()->flags & NODE_SET_LEAP) {
          node_id++;
        }
        n = cur_ns.first_node();
        for (size_t k = 0; k <= cur_ns.last_node_idx(); k++) {
          if ((n.get_flags() & NFLAG_LEAF) == 0) {
            n.next();
            node_id++;
            continue;
          }
          size_t len_len = 0;
          size_t data_len = 0; 
          uint8_t num_data[16];
          uint8_t *data_pos = (*val_blocks)[n.get_col_val()];
          switch (column_types[cur_col_idx]) {
            case MST_TEXT:
            case MST_BIN:
            case MST_SEC_2WAY: {
              data_len = gen::read_vint32(data_pos, &len_len);
              data_pos += len_len;
            } break;
            case MST_INT:
            case MST_DECV ... MST_DEC9:
            case MST_DATE_US ... MST_DATETIME_ISOT_MS: {
              data_len = *data_pos & 0x07;
              data_len += 2;
              if (encoding_type != MSE_DICT_DELTA) {
                int64_t i64;
                flavic48::simple_decode(data_pos, 1, &i64);
                i64 = flavic48::zigzag_decode(i64);
                // printf("%lld\n", i64);
                if (*data_pos == 0xF8 && data_pos[1] == 1) {
                  data_len = 1;
                  data_pos = num_data;
                  *num_data = 0;
                } else {
                  data_len = gen::get_svint60_len(i64);
                  data_pos = num_data;
                  gen::copy_svint60(i64, num_data, data_len);
                }
                // data_len = gen::read_svint60_len(data_pos);
              }
            } break;
          }
          if (!col_trie_builder->lookup_memtrie(data_pos, data_len, nsv))
            printf("Col trie value not found: %lu, [%.*s]!!\n", data_len, (int) data_len, data_pos);
          leopard::node_set_handler ct_nsh(col_trie->all_node_sets, nsv.node_set_pos);
          // printf("CT NSH: %u, node idx: %d, size: %lu\n", nsv.node_set_pos, nsv.cur_node_idx, col_trie->all_node_sets.size());
          leopard::node ct_node = ct_nsh[nsv.cur_node_idx];
          if (encoding_type == MSE_TRIE_2WAY && (ct_node.get_flags() & NFLAG_MARK) == 0) {
            uint32_t link = ct_node.get_col_val();
            rev_trie_node_map *ct_nm = &revmap_vec[link];
            rev_trie_node_map *prev_ct_nm;
            uint32_t prev_link = ct_nm->link;
            ct_nm->link = 0;
            while (prev_link != 0) {
              ct_nm = &revmap_vec[prev_link];
              uint32_t temp_link = ct_nm->link;
              ct_nm->link = link;
              link = prev_link;
              prev_link = temp_link;
            }
            node_map_count++;
            std::vector<uint8_t> rev_nids;
            // printf("\ncol_val: %u, Next entry 1: %u\n", ct_node.get_col_val(), ct_nm->link);
            uint32_t prev_node_id = 0;
            while (1) {
              uint32_t node_start = (ct_nm->node_start - prev_node_id) << 1;
              prev_node_id = ct_nm->node_start + 1;
              gen::append_vint32(rev_nids, node_start | (ct_nm->node_end != UINT32_MAX ? 1 : 0));
              if (ct_nm->node_end != UINT32_MAX) {
                gen::append_vint32(rev_nids, ct_nm->node_end - prev_node_id);
                prev_node_id = ct_nm->node_end + 1;
              }
              if (ct_nm->link == 0)
                break;
              // printf("Next entry: %u\n", ct_nm->link);
              ct_nm = &revmap_vec[ct_nm->link];
              node_map_count++;
            }
            ct_node.set_col_val(col_trie_vals.push_back_with_vlen(rev_nids.data(), rev_nids.size()));
            ct_node.set_flags(ct_node.get_flags() | NFLAG_MARK);
          }
          uint32_t col_trie_node_id = ct_nsh.hdr()->node_id + (ct_nsh.hdr()->flags & NODE_SET_LEAP ? 1 : 0) + nsv.cur_node_idx;
          if (max_node_id < col_trie_node_id)
            max_node_id = col_trie_node_id;
          n.set_col_val(col_trie_node_id);
          node_id++;
          n.next();
        }
      }
      printf("Node map count: %lu\n", node_map_count);
      if (encoding_type == MSE_TRIE_2WAY) {
        col_trie_builder->set_all_vals(&col_trie_vals);
      }
      byte_vec *ptr_grps = val_maps.get_grp_ptrs()->get_ptrs();
      int bit_len = ceil(log2(max_node_id + 1));
      val_maps.get_grp_ptrs()->set_ptr_lkup_tbl_ptr_width(bit_len);
      gen::gen_printf("Col trie bit_len: %d [log(%u)]\n", bit_len, max_node_id);
      gen::int_bit_vector int_bv(ptr_grps, bit_len, memtrie.node_count);
      int counter = 0;
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_LEAF) {
          int_bv.append(cur_node.get_col_val());
          counter++;
        }
        cur_node = ni.next();
      }
      gen::gen_printf("Col trie ptr count: %d\n", counter);
      return col_trie_size;
    }

    void add_to_rev_map(builder *rev_trie_bldr, leopard::node_set_vars& nsv, std::vector<rev_trie_node_map>& revmap_vec, uint32_t nid_shifted) {
      leopard::node_set_handler nsh_rev(rev_trie_bldr->memtrie.all_node_sets, nsv.node_set_pos);
      leopard::node n_rev = nsh_rev[nsv.cur_node_idx];
      uint32_t link = 0;
      bool to_append = true;
      // printf("nsv: %d, %u, %u, %u\n", nsv.find_state, nsv.node_set_pos, nsv.cur_node_idx, nsh_rev.last_node_idx());
      if (n_rev.get_col_val() > 0 && nsv.find_state == LPD_FIND_FOUND) { // || nsv.find_state == LPD_INSERT_LEAF)) {
        rev_trie_node_map *rev_map = &revmap_vec[n_rev.get_col_val()];
        // printf("Found: %u, %u, %u\n", rev_map->node_start, rev_map->node_end, nid_shifted);
        if (rev_map->node_start == nid_shifted)
          to_append = false;
        else if ((rev_map->node_start == (nid_shifted - 1) && rev_map->node_end == UINT32_MAX) || rev_map->node_end == (nid_shifted - 1)) {
          // printf("Range: %u to %u\n", rev_map->node_start, nid_shifted);
          rev_map->node_end = nid_shifted;
          to_append = false;
        } else {
          link = n_rev.get_col_val();
          n_rev.set_col_val(revmap_vec.size());
        }
      } else
        n_rev.set_col_val(revmap_vec.size());
      if (to_append) {
        rev_trie_node_map rev_map;
        rev_map.link = link;
        rev_map.node_start = nid_shifted;
        rev_map.node_end = UINT32_MAX;
        revmap_vec.push_back(rev_map);
      }
    }

    size_t process_repeats(bool to_mark, uint8_t& max_repeats) {
      size_t rpt_count = 0;
      size_t tot_rpt_count = 0;
      ptr_groups *ptr_grps = val_maps.get_grp_ptrs();
      ptr_grps->rpt_ui = {};
      if (to_mark) {
        ptr_grps->rpt_ui.pos = UINT32_MAX;
        ptr_grps->rpt_ui.len = 0;
        ptr_grps->rpt_ui.arr_idx = 0;
        ptr_grps->rpt_ui.freq_count = 0;
        ptr_grps->rpt_ui.repeat_freq = 0;
        ptr_grps->rpt_ui.grp_no = 0;
        ptr_grps->rpt_ui.flags = 0;
      }
      max_repeats = 0;
      uniq_info_base *prev_ui = nullptr;
      uint32_t node_id = 0;
      leopard::node_iterator ni(memtrie.all_node_sets, pk_col_count == 0 ? 1 : 0);
      leopard::node cur_node = ni.next();
      leopard::node prev_node = cur_node;
      while (cur_node != nullptr) {
        if ((cur_node.get_flags() & NODE_SET_LEAP) || ((cur_node.get_flags() & NFLAG_LEAF) == 0)) {
          if ((node_id % nodes_per_bv_block_n) == 0) {
            if (rpt_count > 0 && prev_ui != nullptr && to_mark) {
              prev_node.set_col_val(UINT32_MAX - rpt_count);
              prev_ui->freq_count -= rpt_count;
              ptr_grps->rpt_ui.len += rpt_count;
              ptr_grps->rpt_ui.repeat_freq++;
              if (max_repeats < rpt_count)
                max_repeats = rpt_count;
              rpt_count = 0;
            }
            prev_ui = nullptr;
          }
          cur_node = ni.next();
          node_id++;
          continue;
        }
        uniq_info_base *ui = get_vi(&cur_node);
        if ((node_id % nodes_per_bv_block_n) != 0) {
          if (ui == prev_ui) {
            if (to_mark) {
              cur_node.set_col_val(UINT32_MAX);
              prev_node = cur_node;
            }
            rpt_count++;
            tot_rpt_count++; // todo: sometimes exceeds leaf_count?
          } else {
            if (rpt_count > 0 && to_mark) {
              prev_node.set_col_val(UINT32_MAX - rpt_count);
              prev_ui->freq_count -= rpt_count;
              ptr_grps->rpt_ui.len += rpt_count;
              ptr_grps->rpt_ui.repeat_freq++;
              if (max_repeats < rpt_count)
                max_repeats = rpt_count;
              rpt_count = 0;
            }
          }
        } else {
          if (rpt_count > 0 && to_mark) {
            prev_node.set_col_val(UINT32_MAX - rpt_count);
            prev_ui->freq_count -= rpt_count;
            ptr_grps->rpt_ui.len += rpt_count;
            ptr_grps->rpt_ui.repeat_freq++;
            if (max_repeats < rpt_count)
              max_repeats = rpt_count;
            rpt_count = 0;
          }
        }
        prev_ui = ui;
        cur_node = ni.next();
        node_id++;
      }
      if (rpt_count > 0 && prev_ui != nullptr && to_mark) {
        prev_node.set_col_val(UINT32_MAX - rpt_count);
        prev_ui->freq_count -= rpt_count;
        ptr_grps->rpt_ui.len += rpt_count;
        ptr_grps->rpt_ui.repeat_freq++;
        if (max_repeats < rpt_count)
          max_repeats = rpt_count;
      }
      return tot_rpt_count;
    }

    // FILE *col_trie_fp;
    uint32_t build_col_val() {
      clock_t t = clock();
      char encoding_type = column_encodings[cur_col_idx];
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        init_col_trie_builder(encoding_type);
        // col_trie_fp = fopen("col_trie.txt", "wb+");
      }
      gen::gen_printf("\nCol: %s, ", names + names_positions[cur_col_idx + 2]);
      char data_type = column_types[cur_col_idx];
      gen::gen_printf("Type: %c, Enc: %c. ", data_type, encoding_type);
      std::vector<uint32_t> word_ptrs;
      std::vector<word_refs> words_for_sort;
      fast_vint fast_v(data_type);
      gen::byte_blocks *new_vals = nullptr;
      // if (encoding_type == MSE_DICT_DELTA || encoding_type == MSE_TRIE_2WAY)
        new_vals = new gen::byte_blocks();
      bool is_rec_pos_src_leaf_id = false;
      if (pk_col_count == 0 || rec_pos_vec[0] == UINT32_MAX)
        is_rec_pos_src_leaf_id = true;
      rec_pos_vec[0] = UINT32_MAX;
      // bool delta_next_block = true;
      node_data_vec nodes_for_sort;
      uint32_t max_len = 0;
      uint32_t rpt_freq = 0;
      uint32_t max_rpt_count = 0;
      uint32_t max_word_count = 0;
      uint32_t total_word_entries = 0;
      uint8_t num_data[16];
      uint8_t *data_pos = nullptr;
      size_t len_len = 0;
      uint32_t data_len = 0;
      std::vector<rev_trie_node_map> revmap_vec;
      revmap_vec.push_back(rev_trie_node_map());
      uint8_t *prev_val = nullptr;
      uint32_t prev_val_len = 0;
      int64_t prev_ival = 0;
      uint32_t prev_val_node_id = 0;
      uint32_t pos = 2;
      uint32_t node_id = 0;
      uint32_t leaf_id = 1;
      size_t data_size = 0;
      ptr_groups *ptr_grps = val_maps.get_grp_ptrs();
      fast_v.set_block_data(&ptr_grps->get_data(1));
      fast_v.set_ptr_grps(ptr_grps);
      leopard::node_iterator ni(memtrie.all_node_sets, pk_col_count == 0 ? 1 : 0);
      leopard::node n = ni.next();
      leopard::node prev_node = n;
      while (n != nullptr) {
        if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
          if (encoding_type == MSE_VINTGB) {
            size_t blk_size = fast_v.build_block();
            if (blk_size > (data_type == MST_DECV ? 3 : 2))
              prev_node.set_col_val(blk_size);
            fast_v.reset_block();
            data_size += blk_size;
          }
        }
        if ((n.get_flags() & NFLAG_LEAF) == 0) { // || ni.hdr()->flags & NODE_SET_LEAP
          n = ni.next();
          node_id++;
          continue;
        }
        if (memcmp((*all_vals)[0], "\xFF\xFF", 2) == 0) {
          size_t vlen;
          pos = n.get_col_val();
          data_len = gen::read_vint32((*all_vals)[pos], &vlen);
          // printf("Pos:\t%u\tData len:\t%u\n", pos, data_len);
          data_pos = (*all_vals)[pos + vlen];
        } else {
          size_t vlen;
          if (pk_col_count > 0 && !is_rec_pos_src_leaf_id)
            rec_pos_vec[leaf_id] = n.get_col_val();
          pos = rec_pos_vec[leaf_id];
          gen::read_vint32((*all_vals)[pos], &vlen);
          pos += vlen;
          for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
            data_pos = (*all_vals)[pos];
            len_len = 0;
            data_len = 0;
            uint32_t col_val = pos;
            n.set_col_val(col_val);
            // printf("Key: %u, %u, [%.*s]\n", col_val, data_len, (int) data_len, data_pos);
              char col_data_type = column_types[col_idx];
            switch (col_data_type) {
              case MST_TEXT:
              case MST_BIN:
              case MST_SEC_2WAY: {
                data_len = gen::read_vint32(data_pos, &len_len);
                data_pos += len_len;
                pos += len_len;
                pos += data_len;
              } break;
              case MST_DECV: {
                data_len = 8;
                pos += data_len;
              } break;
              case MST_INT:
              case MST_DEC0 ... MST_DEC9:
              case MST_DATE_US ... MST_DATETIME_ISOT_MS: {
                data_len = *data_pos & 0x07;
                data_len += 2;
                pos += data_len;
                if (encoding_type != MSE_DICT_DELTA && encoding_type != MSE_VINTGB) {
                  if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
                    if (*data_pos == 0xF8 && data_pos[1] == 1) {
                      data_len = 1;
                      *num_data = 0;
                    } else {
                      int64_t i64;
                      flavic48::simple_decode(data_pos, 1, &i64);
                      i64 = flavic48::zigzag_decode(i64);
                      data_len = gen::get_svint60_len(i64);
                      gen::copy_svint60(i64, num_data, data_len);
                    }
                    data_pos = num_data;
                  } else {
                    data_pos = (*new_vals)[new_vals->push_back(data_pos, data_len)];
                  }
                }
              } break;
            }
            if (cur_col_idx == col_idx)
              break;
          }
        }
        switch (encoding_type) {
          case MSE_WORDS:
          case MSE_WORDS_2WAY: {
            if (max_len < data_len)
              max_len = data_len;
            add_words(node_id, data_pos, data_len, words_for_sort, word_ptrs, prev_val, prev_val_len,
                        max_word_count, total_word_entries, rpt_freq, max_rpt_count);
            prev_val = data_pos;
            prev_val_len = data_len;
          } break;
          case MSE_TRIE:
          case MSE_TRIE_2WAY: {
            if (max_len < data_len)
              max_len = data_len;
            uint32_t nid_shifted = node_id >> get_opts()->sec_idx_nid_shift_bits;
            // printf("Data: [%.*s]\n", data_len, data_pos);
            leopard::node_set_vars nsv = col_trie_builder->insert(data_pos, data_len);
            if (encoding_type == MSE_TRIE_2WAY) {
              add_to_rev_map(col_trie_builder, nsv, revmap_vec, nid_shifted);
            }
            // fprintf(col_trie_fp, "%.*s\n", (int) data_len, data_pos);
          } break;
          case MSE_VINTGB: {
            n.set_col_val(0);
            fast_v.add(node_id, data_pos, data_len);
            if (max_len < data_len)
              max_len = data_len;
          } break;
          case MSE_STORE: {
            uint32_t ptr = ptr_grps->append_bin_to_grp_data(1, data_pos, data_len, data_type);
            byte_vec& data = ptr_grps->get_data(1);
            n.set_col_val(data.size() - ptr);
            if (max_len < data_len)
              max_len = data_len;
          } break;
          case MSE_DICT_DELTA: {
            int64_t i64;
            uint8_t frac_width = flavic48::simple_decode_single(data_pos, &i64);
            int64_t col_val = flavic48::zigzag_decode(i64);
            int64_t delta_val = col_val;
            if ((node_id / nodes_per_bv_block_n) == (prev_val_node_id / nodes_per_bv_block_n))
              delta_val -= prev_ival;
            prev_ival = col_val;
            prev_val_node_id = node_id;
            // printf("Node id: %u, delta value: %lld\n", node_id, delta_val);
            uint8_t v64[10];
            i64 = flavic48::zigzag_encode(delta_val);
            uint8_t *v_end = flavic48::simple_encode_single(i64, v64, 0);
            data_len = (v_end - v64);
            data_pos = (*new_vals)[new_vals->push_back(v64, data_len)];
            n.set_col_val(data_pos - (*new_vals)[0]);
            nodes_for_sort.push_back((struct node_data) {data_pos, data_len, ni.get_cur_nsh_id(), ni.get_cur_sib_id(), (uint8_t) len_len});
            // printf("Key: %u, %u, [%.*s]\n", col_val, data_len, (int) data_len, data_pos);
          } break;
          default:
            nodes_for_sort.push_back((struct node_data) {data_pos, data_len, ni.get_cur_nsh_id(), ni.get_cur_sib_id(), (uint8_t) len_len});
          // printf("RecNo: %lu, Pos: %u, data_len: %u, vlen: %lu\n", rec_no, pos, data_len, vlen);
        }
        leaf_id++;
        node_id++;
        prev_node = n;
        n = ni.next();
      }
      if (encoding_type == MSE_VINTGB) {
        size_t blk_size = fast_v.build_block();
        if (blk_size > (data_type == MST_DECV ? 3 : 2))
          prev_node.set_col_val(blk_size);
        data_size += blk_size;
      }
      uint32_t col_trie_size = 0;
      switch (encoding_type) {
        case MSE_WORDS:
        case MSE_WORDS_2WAY: {
          gen::byte_blocks uniq_words;
          uniq_info_vec uniq_words_vec;
          build_words(words_for_sort, word_ptrs, uniq_words_vec, uniq_words, max_word_count, total_word_entries, rpt_freq, max_rpt_count, new_vals);
          ptr_grps->set_max_len(max_len);
          ptr_grps->build(memtrie.node_count, memtrie.all_node_sets, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type);
        } break;
        case MSE_TRIE:
        case MSE_TRIE_2WAY: {
          // fclose(col_trie_fp);
          col_trie_size = build_col_trie(all_vals, revmap_vec, *new_vals, encoding_type);
          ptr_grps->set_max_len(col_trie->max_key_len);
          ptr_grps->build(memtrie.node_count, memtrie.all_node_sets, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type, col_trie_size);
        } break;
        case MSE_VINTGB:
        case MSE_STORE: {
          // ptr_grps->set_null(memtrie.node_count);
          ptr_grps->set_max_len(max_len);
          ptr_grps->build(memtrie.node_count, memtrie.all_node_sets, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type);
        } break;
        default: {
          val_sort_callbacks val_sort_cb(memtrie.all_node_sets, *all_vals, uniq_vals);
          uint32_t tot_freq_count = uniq_maker::sort_and_reduce(nodes_for_sort, *all_vals,
                      uniq_vals, uniq_vals_fwd, val_sort_cb, max_len, 0, data_type);

          uint8_t max_repeats;
          size_t rpt_count = process_repeats(false, max_repeats);
          printf("rpt enable perc: %d, actual perc: %u\n", get_opts()->rpt_enable_perc, (rpt_count * 100 / memtrie.node_count));
          if (get_opts()->rpt_enable_perc < (rpt_count * 100 / memtrie.node_count))
            process_repeats(true, max_repeats);
          printf("Max col len: %u, Rpt count: %lu, max: %d\n", max_len, rpt_count, max_repeats);

          if (data_type == MST_TEXT)
            val_maps.build_tail_val_maps(false, memtrie.all_node_sets, uniq_vals_fwd, uniq_vals, tot_freq_count, max_len, max_repeats);
          else
            val_maps.build_val_maps(tot_freq_count, max_len, data_type, max_repeats);
          
          ptr_grps->set_max_len(max_len);
          ptr_grps->build(memtrie.node_count, memtrie.all_node_sets, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type);
        }
      }

      uint32_t val_size = ptr_grps->get_total_size();
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        val_size += col_trie_builder->build_kv(false);
      }

      if (new_vals != nullptr) {
        delete new_vals;
        if (encoding_type == MSE_TRIE_2WAY) {
          col_trie_builder->all_vals = nullptr; // todo: standardize
        }
      }
      t = gen::print_time_taken(t, "Time taken for build_col_val: ");

      return val_size;

    }

    void write_col_val() {
      printf("trie level: %d, col idx: %d\n", trie_level, cur_col_idx);
      char data_type = column_types[cur_col_idx];
      char encoding_type = column_encodings[cur_col_idx];
      write_val_ptrs_data(data_type, encoding_type, 1, fp, out_vec); // TODO: fix flags
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        col_trie_builder->write_kv(false, nullptr);
      }
    }

    builder *new_col_trie_builder(bool two_way) {
      builder *new_ct_builder;
      //get_opts()->split_tails_method = 0;
      bldr_options ctb_opts[2];
      ctb_opts[0] = dflt_opts;
      ctb_opts[1] = dflt_opts;
      ctb_opts[0].max_groups = 1;
      ctb_opts[0].max_inner_tries = 2;
      ctb_opts[0].partial_sfx_coding = false;
      ctb_opts[0].sort_nodes_on_freq = false;
      if (two_way) {
        ctb_opts[0].opts_count = 2;
        ctb_opts[0].leap_frog = true;
        ctb_opts[1].inner_tries = 0;
        ctb_opts[1].max_inner_tries = 0;
        new_ct_builder = new builder(NULL, "col_trie,key,rev_nids", 2, "**", "us", 0, 1, ctb_opts);
      } else
        new_ct_builder = new builder(NULL, "col_trie,key", 1, "*", "u", 0, 1, ctb_opts);
      new_ct_builder->fp = fp;
      new_ct_builder->out_vec = out_vec;
      return new_ct_builder;
    }

    void init_col_trie_builder(char enc_type) {
      if (col_trie_builder != nullptr)
        delete col_trie_builder;
      col_trie_builder = new_col_trie_builder(enc_type == MSE_TRIE_2WAY);
      col_trie = &col_trie_builder->memtrie;
    }

    builder *create_word_trie_builder(char enc_type, gen::byte_blocks *rev_col_vals) {
      bldr_options wtb_opts[2];
      wtb_opts[0] = dflt_opts;
      wtb_opts[1] = dflt_opts;
      wtb_opts[0].max_groups = 1;
      wtb_opts[0].max_inner_tries = 2;
      wtb_opts[0].partial_sfx_coding = false;
      wtb_opts[0].sort_nodes_on_freq = false;
      wtb_opts[0].opts_count = 2;
      wtb_opts[0].leap_frog = true;
      wtb_opts[1].inner_tries = 0;
      wtb_opts[1].max_inner_tries = 0;
      builder *ret;
      if (enc_type == MSE_WORDS_2WAY) {
        ret = new builder(NULL, "word_trie,key,rev_nids", 2, "t*", "us", 0, 1, wtb_opts);
        ret->set_all_vals(rev_col_vals);
      } else
        ret = new builder(NULL, "word_trie,key", 1, "t", "u", 0, 1, wtb_opts);
      return ret;
    }

    void reset_for_next_col() {
      cur_seq_idx = 0;
      cur_col_idx++;
      // if (cur_col_idx < column_count && (column_encodings[cur_col_idx] == MSE_TRIE || column_encodings[cur_col_idx] == MSE_TRIE_2WAY))
      //   init_col_trie_builder();
      // if (all_vals != NULL)
      //   delete all_vals;
      // all_vals = new gen::byte_blocks();
      // all_vals->push_back("\0", 2);
      uniq_vals.reset();
      wm.reset();
      val_maps.get_grp_ptrs()->reset();
      for (size_t i = 0; i < uniq_vals_fwd.size(); i++)
        delete uniq_vals_fwd[i];
      uniq_vals_fwd.resize(0);
    }

    builder_fwd *new_instance() {
      bldr_options inner_trie_opts = inner_tries_dflt_opts;
      inner_trie_opts.trie_leaf_count = 0;
      inner_trie_opts.leaf_lt = false;
      inner_trie_opts.max_groups = get_opts()->max_groups;
      inner_trie_opts.max_inner_tries = get_opts()->max_inner_tries;
      if (get_opts()->max_inner_tries <= trie_level + 1) {
        inner_trie_opts.inner_tries = false;
      }
      builder *ret = new builder(NULL, "inner_trie,key", 1, "*", "u", trie_level + 1, 1, &inner_trie_opts);
      ret->fp = fp;
      ret->out_vec = out_vec;
      return ret;
    }

    uint32_t build_tail_trie(uint32_t tot_freq_count) {
      bldr_options tt_opts = inner_tries_dflt_opts;
      tt_opts.trie_leaf_count = 0;
      tt_opts.leaf_lt = false;
      tt_opts.inner_tries = true;
      tt_opts.max_groups = get_opts()->max_groups;
      tt_opts.max_inner_tries = get_opts()->max_inner_tries;
      tail_trie_builder = new builder(NULL, "tail_trie,key", 1, "t", "u", trie_level + 1, 1, &tt_opts);
      for (size_t i = 0; i < uniq_tails_rev.size(); i++) {
        uniq_info_base *ti = uniq_tails_rev[i];
        uint8_t rev[ti->len];
        uint8_t *ti_data = uniq_tails[ti->pos];
        for (uint32_t j = 0; j < ti->len; j++)
          rev[j] = ti_data[ti->len - j - 1];
        tail_trie_builder->insert(rev, ti->len, i);
      }
      leopard::node_iterator ni_freq(tail_trie_builder->memtrie.all_node_sets, 0);
      leopard::node cur_node = ni_freq.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_CHILD) {
          uint32_t sum_freq = 0;
          leopard::node_set_handler nsh_children(tail_trie_builder->memtrie.all_node_sets, cur_node.get_child());
          for (size_t i = 0; i <= nsh_children.last_node_idx(); i++) {
            leopard::node child_node = nsh_children[i];
            if (child_node.get_flags() & NFLAG_LEAF) {
              uniq_info_base *ti = uniq_tails_rev[cur_node.get_col_val()];
              sum_freq += ti->freq_count;
            }
          }
          nsh_children.hdr()->freq = sum_freq;
        }
        cur_node = ni_freq.next();
      }
      uint32_t trie_size = tail_trie_builder->build();
      int bit_len = ceil(log2(tail_trie_builder->memtrie.node_count + 1)) - 8;
      tail_maps.get_grp_ptrs()->set_ptr_lkup_tbl_ptr_width(bit_len);
      gen::gen_printf("Tail trie bit_len: %d [log(%u) - 8]\n", bit_len, tail_trie_builder->memtrie.node_count);
      uint32_t node_id = 0;
      leopard::node_iterator ni_tt(tail_trie_builder->memtrie.all_node_sets, 0);
      cur_node = ni_tt.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_LEAF) {
          uniq_info_base *ti = uniq_tails_rev[cur_node.get_col_val()];
          ti->ptr = node_id;
        }
        node_id++;
        cur_node = ni_tt.next();
      }
      byte_vec *tail_ptrs = tail_maps.get_grp_ptrs()->get_ptrs();
      gen::int_bit_vector int_bv(tail_ptrs, bit_len, uniq_tails_rev.size());
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uniq_info_base *ti = uniq_tails_rev[cur_node.get_tail()];
          int_bv.append(ti->ptr >> 8);
        }
        cur_node = ni.next();
      }
      return trie_size;
    }

    uint32_t build_trie() {
      clock_t t = clock();
      tail_sort_callbacks tail_sort_cb(memtrie.all_node_sets, *memtrie.all_tails, uniq_tails);
      uint32_t tot_freq_count = uniq_maker::make_uniq(memtrie.all_node_sets, *memtrie.all_tails,
          uniq_tails, uniq_tails_rev, tail_sort_cb, memtrie.max_tail_len, trie_level, 0, 0, MST_BIN);
      uint32_t tail_trie_size = 0;
      if (uniq_tails_rev.size() > 0) {
        if (get_opts()->inner_tries && get_opts()->max_groups == 1 && get_opts()->max_inner_tries >= trie_level + 1 && uniq_tails_rev.size() > 255) {
          tail_trie_size = build_tail_trie(tot_freq_count);
        } else {
          //get_opts()->inner_tries = false;
          tail_maps.build_tail_val_maps(true, memtrie.all_node_sets, uniq_tails_rev, uniq_tails, tot_freq_count, memtrie.max_tail_len, 0);
        }
      }
      uint32_t flag_counts[8];
      uint32_t char_counts[8];
      memset(flag_counts, '\0', sizeof(uint32_t) * 8);
      memset(char_counts, '\0', sizeof(uint32_t) * 8);
      uint32_t sfx_full_count = 0;
      uint32_t sfx_partial_count = 0;
      uint64_t bm_leaf = 0;
      uint64_t bm_term = 0;
      uint64_t bm_child = 0;
      uint64_t bm_ptr = 0;
      uint64_t bm_mask = bm_init_mask;
      byte_vec byte_vec64;
      //trie.reserve(node_count + (node_count >> 1));
      uint32_t ptr_count = 0;
      uint32_t node_count = 0;
      uint32_t louds_pos = 0;
      louds.set(louds_pos++, true);
      louds.set(louds_pos++, false);
      leopard::node_set_handler nsh_children(memtrie.all_node_sets, 1);
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t node_byte, cur_node_flags;
        if (cur_node.get_flags() & NODE_SET_LEAP) {
          if (get_opts()->sort_nodes_on_freq) {
            size_t no_tail_count = 0;
            leopard::node n = ni.get_cur_nsh()->first_node();
            for (size_t k = 0; k <= ni.get_cur_nsh()->last_node_idx(); k++) {
              if (n.get_flags() & NFLAG_TAIL)
                break;
              no_tail_count++;
              n.next();
            }
            node_byte = no_tail_count;
            //printf("%lu\n", no_tail_count);
          } else
            node_byte = ni.last_node_idx();
          cur_node_flags = 0;
        } else {
          node_byte = append_tail_ptr(&cur_node);
          cur_node_flags = cur_node.get_flags();
        }
        //dump_ptr(&cur_node, node_count);
        if (node_count && (node_count % 64) == 0) {
          // append_flags(trie_flags, bm_leaf, bm_child, bm_term, bm_ptr);
          if (trie_level == 0) {
            append64_t(trie_flags, bm_ptr);
            append64_t(trie_flags, bm_term);
            append64_t(trie_flags, bm_child);
            append64_t(trie_flags, bm_leaf);
          } else
            append64_t(trie_flags_tail, bm_ptr);
          append_byte_vec(trie, byte_vec64);
          bm_term = 0; bm_child = 0; bm_leaf = 0; bm_ptr = 0;
          bm_mask = 1UL;
          byte_vec64.clear();
        }
        if (cur_node_flags & NFLAG_LEAF)
          bm_leaf |= bm_mask;
        if (cur_node_flags & NFLAG_TERM)
          bm_term |= bm_mask;
        if (cur_node_flags & NFLAG_CHILD)
          bm_child |= bm_mask;
        if (cur_node_flags & NFLAG_TAIL)
          bm_ptr |= bm_mask;
        if (trie_level > 0) {
          if (cur_node_flags & NFLAG_CHILD) {
            nsh_children.set_pos(cur_node.get_child());
            for (size_t ci = 0; ci <= nsh_children.last_node_idx(); ci++)
              louds.set(louds_pos++, true);
          }
          louds.set(louds_pos++, false);
        }
        bm_mask <<= 1;
        byte_vec64.push_back(node_byte);
        node_count++;
        if (cur_node.get_flags() & NODE_SET_LEAP) {
          cur_node = ni.next();
          continue;
        }
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uniq_info_vec *uniq_tails_rev = tail_maps.get_ui_vec();
          uniq_info_base *ti = (*uniq_tails_rev)[cur_node.get_tail()];
          if (ti->flags & MDX_AFFIX_FULL)
            sfx_full_count++;
          if (ti->flags & MDX_AFFIX_PARTIAL)
            sfx_partial_count++;
          if (ti->len > 1)
            char_counts[(ti->len > 8) ? 7 : (ti->len - 2)]++;
          ptr_count++;
        }
        uint8_t flags = (cur_node.get_flags() & NFLAG_LEAF ? 1 : 0) +
          (cur_node.get_child() > 0 ? 2 : 0) + (cur_node.get_flags() & NFLAG_TAIL ? 4 : 0) +
          (cur_node.get_flags() & NFLAG_TERM ? 8 : 0);
        flag_counts[flags & 0x07]++;
        cur_node = ni.next();
      }
      // TODO: write on all cases?
      // append_flags(trie_flags, bm_leaf, bm_child, bm_term, bm_ptr);
      if (trie_level == 0) {
        append64_t(trie_flags, bm_ptr);
        append64_t(trie_flags, bm_term);
        append64_t(trie_flags, bm_child);
        append64_t(trie_flags, bm_leaf);
      } else
        append64_t(trie_flags_tail, bm_ptr);
      append_byte_vec(trie, byte_vec64);
      louds.set(louds_pos++, false);
      for (int i = 0; i < 8; i++) {
        gen::gen_printf("Flag %d: %d\tChar: %d: %d\n", i, flag_counts[i], i + 2, char_counts[i]);
      }
      gen::gen_printf("Tot ptr count: %u, Full sfx count: %u, Partial sfx count: %u\n", ptr_count, sfx_full_count, sfx_partial_count);
      tail_maps.get_grp_ptrs()->build(node_count, memtrie.all_node_sets, ptr_groups::get_tails_info_fn, 
              uniq_tails_rev, true, pk_col_count, get_opts()->dessicate, tail_trie_size == 0 ? 'u' : MSE_TRIE, MST_BIN, tail_trie_size);
      end_loc = trie_data_ptr_size();
      gen::print_time_taken(t, "Time taken for build_trie(): ");
      return end_loc;
    }
    uint32_t write_trie_tail_ptrs_data(FILE *fp, byte_vec *out_vec) {
      uint32_t tail_size = tail_maps.get_grp_ptrs()->get_total_size();
      gen::gen_printf("\nTrie: %u, Flags: %u, Tail size: %u\n", trie.size(), trie_flags.size(), tail_size);
      output_u32(tail_size, fp, out_vec);
      output_u32(trie_flags.size(), fp, out_vec);
      gen::gen_printf("Tail stats - ");
      tail_maps.get_grp_ptrs()->write_ptrs_data(1, true, fp, out_vec);
      if (get_opts()->inner_tries && tail_trie_builder != nullptr) {
        tail_trie_builder->fp = fp;
        tail_trie_builder->out_vec = out_vec;
        tail_trie_builder->write_trie(NULL);
      }
      //output_bytes(trie_flags.data(), trie_flags.size(), fp, out_vec);
      output_bytes(trie.data(), trie.size(), fp, out_vec);
      output_align8(trie.size(), fp, out_vec);
      return trie_data_ptr_size();
    }
    uint32_t write_val_ptrs_data(char data_type, char encoding_type, uint8_t flags, FILE *fp_val, byte_vec *out_vec) {
      uint32_t val_fp_offset = 0;
      if (get_uniq_val_count() > 0 || encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB) {
        gen::gen_printf("Stats - %c - ", encoding_type);
        val_maps.get_grp_ptrs()->write_ptrs_data(flags, false, fp, out_vec);
        val_fp_offset += val_maps.get_grp_ptrs()->get_total_size();
      }
      return val_fp_offset;
    }
    size_t trie_data_ptr_size() {
      size_t ret = 8 + gen::size_align8(trie.size()) + tail_maps.get_grp_ptrs()->get_total_size(); // + trie_flags.size();
      //if (get_uniq_val_count() > 0)
      //  ret += val_maps.get_grp_ptrs()->get_total_size();
      return ret;
    }
    size_t get_uniq_val_count() {
      return val_maps.get_ui_vec()->size();
    }

    leopard::node_set_vars insert(const uint8_t *key, int key_len, uint32_t val_pos = UINT32_MAX) {
      return memtrie.insert(key, key_len, val_pos);
    }

    void set_leaf_seq(uint32_t ns_id, uint32_t& seq_idx, std::function<void(uint32_t, uint32_t)> set_seq) {
      leopard::node_set_handler ns(memtrie.all_node_sets, ns_id);
      leopard::node n = ns.first_node();
      for (size_t i = 0; i <= ns.last_node_idx(); i++) {
        if (n.get_flags() & NFLAG_LEAF)
          set_seq(n.get_col_val(), seq_idx++);
        if (n.get_flags() & NFLAG_CHILD)
          set_leaf_seq(n.get_child(), seq_idx, set_seq);
        n.next();
      }
    }

    void set_level(uint32_t ns_id, uint32_t level) {
      if (max_level < level)
        max_level = level;
      leopard::node_set_handler ns(memtrie.all_node_sets, ns_id);
      leopard::node n = ns.first_node();
      for (size_t i = 0; i <= ns.last_node_idx(); i++) {
        if (n.get_flags() & NFLAG_CHILD)
          set_level(n.get_child(), level + 1);
        n.next();
      }
    }

    void set_node_id() {
      size_t num_leap = 0;
      size_t num_leap_no_tail = 0;
      uint32_t node_id = 0;
      leopard::node_set_handler nsh(memtrie.all_node_sets, 0);
      for (size_t i = 0; i < memtrie.all_node_sets.size(); i++) {
        nsh.set_pos(i);
        leopard::node_set_header *ns_hdr = nsh.hdr();
        ns_hdr->node_id = node_id;
        if (ns_hdr->last_node_idx > 4 && trie_level == 0 && get_opts()->leap_frog) {
          if (!get_opts()->sort_nodes_on_freq) {
            ns_hdr->flags |= NODE_SET_LEAP;
            node_id++;
            memtrie.node_count++;
          }
          num_leap++;
          size_t no_tail_count = 0;
          leopard::node n = nsh.first_node();
          for (size_t k = 0; k <= nsh.last_node_idx(); k++) {
            if (n.get_flags() & NFLAG_TAIL)
              break;
            no_tail_count++;
            n.next();
          }
          if (no_tail_count >= 6) {
            num_leap_no_tail++;
            if (get_opts()->sort_nodes_on_freq) {
              ns_hdr->flags |= NODE_SET_LEAP;
              node_id++;
              memtrie.node_count++;
            }
          }
        }
        node_id += ns_hdr->last_node_idx;
        node_id++;
      }
      // printf("Num leap: %lu, no tail: %lu\n", num_leap, num_leap_no_tail);
    }

    #define CACHE_FWD 1
    #define CACHE_REV 2
    uint32_t build_cache(int which, uint32_t& max_node_id) {
      clock_t t = clock();
      uint32_t cache_count = 64;
      while (cache_count < memtrie.key_count / 2048)
        cache_count <<= 1;
      //cache_count *= 2;
      if (which == CACHE_FWD) {
        for (int i = 0; i < get_opts()->fwd_cache_multiplier; i++)
          cache_count <<= 1;
        f_cache = new fwd_cache[cache_count + 1]();
        f_cache_freq = new uint32_t[cache_count]();
      }
      if (which == CACHE_REV) {
        for (int i = 0; i < get_opts()->rev_cache_multiplier; i++)
          cache_count <<= 1;
        r_cache = new nid_cache[cache_count + 1]();
        r_cache_freq = new uint32_t[cache_count]();
      }
      uint8_t tail_buf[memtrie.max_key_len];
      gen::byte_str tail_from0(tail_buf, memtrie.max_key_len);
      build_cache(which, 1, 0, cache_count - 1, tail_from0);
      max_node_id = 0;
      int sum_freq = 0;
      for (uint32_t i = 0; i < cache_count; i++) {
        if (which == CACHE_FWD) {
          fwd_cache *fc = &f_cache[i];
          uint32_t cche_node_id = gen::read_uint24(&fc->child_node_id1);
          if (max_node_id < cche_node_id)
            max_node_id = cche_node_id;
          sum_freq += f_cache_freq[i];
        }
        if (which == CACHE_REV) {
          nid_cache *rc = &r_cache[i];
          uint32_t cche_node_id = gen::read_uint24(&rc->child_node_id1);
          if (max_node_id < cche_node_id)
            max_node_id = cche_node_id;
          sum_freq += r_cache_freq[i];
        }
        // printf("NFreq:\t%u\tPNid:\t%u\tCNid:\t%u\tNb:\t%c\toff:\t%u\n", f_cache_freq[i], gen::read_uint24(&fc->parent_node_id1), gen::read_uint24(&fc->child_node_id1), fc->node_byte, fc->node_offset);
      }
      max_node_id++;
      gen::gen_printf("Sum of cache freq: %d, Max node id: %d\n", sum_freq, max_node_id);
      gen::print_time_taken(t, "Time taken for build_cache(): ");
      return cache_count;
    }

    uint32_t build_cache(int which, uint32_t ns_id, uint32_t parent_node_id, uint32_t cache_mask, gen::byte_str& tail_from0) {
      if (ns_id == 0)
        return 1;
      leopard::node_set_handler ns(memtrie.all_node_sets, ns_id);
      leopard::node n = ns.first_node();
      uint32_t cur_node_id = ns.hdr()->node_id + (ns.hdr()->flags & NODE_SET_LEAP ? 1 : 0);
      uint32_t freq_count = trie_level > 0 ? ns.hdr()->freq : 1;
      size_t parent_tail_len = tail_from0.length();
      for (int i = 0; i <= ns.hdr()->last_node_idx; i++) {
        if (n.get_flags() & NFLAG_TAIL) {
          uniq_info_base *ti = (*tail_maps.get_ui_vec())[n.get_tail()];
          uint8_t *ti_tail = (*tail_maps.get_uniq_data())[ti->pos];
          if (trie_level == 0)
            tail_from0.append(ti_tail, ti->len);
          else {
            for (uint8_t *t = ti_tail + ti->len - 1; t >= ti_tail; t--)
              tail_from0.append(*t);
          }
        } else
          tail_from0.append(n.get_byte());
        uint32_t node_freq = build_cache(which, n.get_child(), cur_node_id, cache_mask, tail_from0);
        tail_from0.set_length(parent_tail_len);
        freq_count += node_freq;
        if (n.get_child() > 0 && (n.get_flags() & NFLAG_TAIL) == 0) {
          uint8_t node_byte = n.get_byte();
          leopard::node_set_handler child_nsh(memtrie.all_node_sets, n.get_child());
          uint32_t child_node_id = child_nsh.hdr()->node_id;
          if (which == CACHE_FWD) {
            int node_offset = i + (ns.hdr()->flags & NODE_SET_LEAP ? 1 : 0);
            uint32_t cache_loc = (ns.hdr()->node_id ^ (ns.hdr()->node_id << MDX_CACHE_SHIFT) ^ node_byte) & cache_mask;
            fwd_cache *fc = f_cache + cache_loc;
            if (f_cache_freq[cache_loc] < node_freq && ns.hdr()->node_id < (1 << 24) && child_node_id < (1 << 24) && node_offset < 256) {
              f_cache_freq[cache_loc] = node_freq;
              gen::copy_uint24(ns.hdr()->node_id, &fc->parent_node_id1);
              gen::copy_uint24(child_node_id, &fc->child_node_id1);
              fc->node_offset = node_offset;
              fc->node_byte = node_byte;
            }
          }
        }
        if (which == CACHE_REV) {
          uint32_t cache_loc = cur_node_id & cache_mask;
          nid_cache *rc = r_cache + cache_loc;
          if (r_cache_freq[cache_loc] < node_freq && parent_node_id < (1 << 24) && cur_node_id < (1 << 24)) {
            r_cache_freq[cache_loc] = node_freq;
            gen::copy_uint24(cur_node_id, &rc->child_node_id1);
            rc->tail0_len = 0;
            gen::copy_uint24(parent_node_id, &rc->parent_node_id1);
            if (parent_tail_len < 9) {
              uint8_t *t = &rc->tail0_len + parent_tail_len;
              for (size_t k = 0; k < parent_tail_len; k++)
                *t-- = tail_from0[k];
              rc->tail0_len = parent_tail_len;
            }
          }
        }
        n.next();
        cur_node_id++;
      }
      return freq_count;
    }

    uint8_t min_pos[256][256];
    // uint32_t min_len_count[256];
    // uint32_t min_len_last_ns_id[256];
    bldr_min_pos_stats make_min_positions() {
      clock_t t = clock();
      bldr_min_pos_stats stats;
      memset(min_pos, 0xFF, 65536);
      // memset(min_len_count, 0, sizeof(uint32_t) * 256);
      // memset(min_len_last_ns_id, 0, sizeof(uint32_t) * 256);
      for (size_t i = 1; i < memtrie.all_node_sets.size(); i++) {
        leopard::node_set_handler cur_ns(memtrie.all_node_sets, i);
        uint8_t len = cur_ns.last_node_idx();
        if (stats.min_len > len)
          stats.min_len = len;
        if (stats.max_len < len)
          stats.max_len = len;
        // if (i < memtrie.node_set_count / 4) {
        //   min_len_count[len]++;
        //   min_len_last_ns_id[len] = i;
        // }
        leopard::node cur_node = cur_ns.first_node();
        for (size_t k = 0; k <= len; k++) {
          uint8_t b = cur_node.get_byte();
          if (min_pos[len][b] > k)
             min_pos[len][b] = k;
          if (stats.min_b > b)
            stats.min_b = b;
          if (stats.max_b < b)
            stats.max_b = b;
          cur_node.next();
        }
      }
      gen::print_time_taken(t, "Time taken for make_min_positions(): ");
      // for (int i = 0; i < 256; i++) {
      //   if (min_len_count[i] > 0)
      //     printf("%d\t%u\t%u\n", i, min_len_count[i], min_len_last_ns_id[i]);
      // }
      return stats;
    }

    uint32_t decide_min_stat_to_use(bldr_min_pos_stats& stats) {
      clock_t t = clock();
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        int good_b_count = 0;
        for (int j = stats.min_b; j <= stats.max_b; j++) {
          if (min_pos[i][j] > 0 && min_pos[i][j] != 0xFF)
            good_b_count++;
        }
        if (good_b_count > (i >> 1)) {
          stats.min_len = i;
          break;
        }
      }
      // for (int i = stats.min_len; i <= stats.max_len; i++) {
      //   printf("Len: %d:: ", i);
      //   for (int j = stats.min_b; j <= stats.max_b; j++) {
      //     int min = min_pos[i][j];
      //     if (min != 255)
      //       printf("%c(%d): %d, ", j, j, min);
      //   }
      //   printf("\n\n");
      // }
      gen::print_time_taken(t, "Time taken for decide_min_stat_to_use(): ");
      return 0; //todo
    }

    void write_sec_cache(bldr_min_pos_stats& stats, uint32_t sec_cache_size) {
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        for (int j = 0; j <= 255; j++) {
          uint8_t min_len = min_pos[i][j];
          if (min_len == 0xFF)
            min_len = 0;
          min_len++;
          output_byte(min_len, fp, out_vec);
        }
      }
    }

    uint32_t build() {

      clock_t t = clock();

      gen::gen_printf("Key count: %u\n", memtrie.key_count);

      tp = {};
      tp.opts_loc = MDX_HEADER_SIZE; // 136
      tp.opts_size = sizeof(bldr_options) * opts->opts_count;

      if (pk_col_count == 0)
        memtrie.node_count--;

      if (pk_col_count > 0) {
        if (get_opts()->split_tails_method > 0)
          split_tails();
        sort_node_sets();
        set_node_id();
        set_level(1, 1);
        tp.min_stats = make_min_positions();
        tp.trie_tail_ptrs_data_sz = build_trie();

        if (trie_level > 0) {
          get_opts()->fwd_cache = false;
          get_opts()->rev_cache = true;
        }
        if (get_opts()->fwd_cache) {
          tp.fwd_cache_count = build_cache(CACHE_FWD, tp.fwd_cache_max_node_id);
          tp.fwd_cache_size = tp.fwd_cache_count * 8; // 8 = parent_node_id (3) + child_node_id (3) + node_offset (1) + node_byte (1)
        } else
          tp.fwd_cache_max_node_id = 0;
        if (get_opts()->rev_cache) {
          tp.rev_cache_count = build_cache(CACHE_REV, tp.rev_cache_max_node_id);
          tp.rev_cache_size = tp.rev_cache_count * 12; // 6 = parent_node_id (3) + child_node_id (3)
        } else
          tp.rev_cache_max_node_id = 0;
        tp.sec_cache_count = decide_min_stat_to_use(tp.min_stats);
        tp.sec_cache_size = 0;
        if (get_opts()->leap_frog)
          tp.sec_cache_size = (tp.min_stats.max_len - tp.min_stats.min_len + 1) * 256; // already aligned

        if (trie_level == 0) {
          tp.term_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);
          tp.child_rank_lt_sz = tp.term_rank_lt_sz;
          tp.term_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count + 1, sel_divisor, 3);
          tp.child_select_lt_sz = 8;
          // if (memtrie.node_set_count > 1)
            tp.child_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count, sel_divisor, 3);
          tp.louds_rank_lt_sz = 0;
          tp.louds_sel1_lt_sz = 0;
        } else {
          tp.louds_rank_lt_sz = gen::get_lkup_tbl_size2(louds.get_highest() + 1, nodes_per_bv_block, width_of_bv_block);
          tp.louds_sel1_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count + 1, sel_divisor, 3);
          tp.term_rank_lt_sz = 0;
          tp.child_rank_lt_sz = 0;
          tp.term_select_lt_sz = 0;
          tp.child_select_lt_sz = 0;
        }

        tp.leaf_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);
        tp.leaf_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.key_count + 1, sel_divisor, 3);
        if (tail_maps.get_grp_ptrs()->get_grp_count() <= 2)
          tp.tail_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);

        // if (get_opts()->dessicate) {
        //   tp.term_rank_lt_sz = tp.child_rank_lt_sz = tp.leaf_rank_lt_sz = 0;
        //   tp.term_select_lt_sz = tp.child_select_lt_sz = tp.leaf_select_lt_sz = 0;
        //   tp.louds_rank_lt_sz = tp.louds_sel1_lt_sz = 0;
        // }
        if (!get_opts()->leaf_lt || get_opts()->trie_leaf_count == 0) {
          tp.leaf_select_lt_sz = 0;
          tp.leaf_rank_lt_sz = 0;
        }

        tp.fwd_cache_loc = tp.opts_loc + tp.opts_size;
        tp.rev_cache_loc = tp.fwd_cache_loc + tp.fwd_cache_size;
        tp.sec_cache_loc = tp.rev_cache_loc + gen::size_align8(tp.rev_cache_size);

        if (trie_level == 0) {
          tp.child_select_lkup_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.term_select_lkup_loc = tp.child_select_lkup_loc + gen::size_align8(tp.child_select_lt_sz);
          uint32_t total_rank_lt_size = tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.tail_rank_lt_sz;
          tp.term_rank_lt_loc = tp.term_select_lkup_loc + gen::size_align8(tp.term_select_lt_sz);
          tp.child_rank_lt_loc = tp.term_rank_lt_loc + width_of_bv_block;
          tp.trie_flags_loc = tp.term_rank_lt_loc + gen::size_align8(total_rank_lt_size);
          tp.tail_rank_lt_loc = tp.tail_rank_lt_sz == 0 ? 0 : tp.term_rank_lt_loc + width_of_bv_block * 2;
          tp.louds_rank_lt_loc = tp.term_rank_lt_loc; // dummy
          tp.louds_sel1_lt_loc = tp.term_select_lkup_loc; // dummy
          tp.trie_tail_ptrs_data_loc = tp.trie_flags_loc + trie_flags.size();
        } else {
          tp.louds_sel1_lt_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.louds_rank_lt_loc = tp.louds_sel1_lt_loc + gen::size_align8(tp.louds_sel1_lt_sz);
          tp.trie_flags_loc = tp.louds_rank_lt_loc + gen::size_align8(tp.louds_rank_lt_sz);
          tp.tail_rank_lt_loc = tp.trie_flags_loc + louds.size_bytes();
          tp.term_rank_lt_loc = tp.child_rank_lt_loc = gen::size_align8(tp.louds_rank_lt_loc); // All point to louds
          tp.term_select_lkup_loc = tp.child_select_lkup_loc = gen::size_align8(tp.louds_sel1_lt_loc); // All point to louds
          tp.trie_tail_ptrs_data_loc = tp.tail_rank_lt_loc + gen::size_align8(tp.tail_rank_lt_sz);
        }

        tp.leaf_rank_lt_loc = tp.trie_tail_ptrs_data_loc + tp.trie_tail_ptrs_data_sz;
        tp.tail_flags_loc = tp.leaf_rank_lt_loc + gen::size_align8(tp.leaf_rank_lt_sz);
        tp.leaf_select_lkup_loc = tp.tail_flags_loc + trie_flags_tail.size();

        if (!get_opts()->leap_frog)
          tp.sec_cache_loc = 0;
      } else {
        tp.leaf_select_lkup_loc = tp.opts_loc + tp.opts_size;
        tp.leaf_select_lt_sz = 0;
      }

      tp.names_loc = tp.leaf_select_lkup_loc + gen::size_align8(tp.leaf_select_lt_sz);
      tp.names_sz = (column_count + 2) * sizeof(uint16_t) + names_len;
      tp.col_val_table_loc = tp.names_loc + gen::size_align8(tp.names_sz);
      int val_count = column_count;
      tp.col_val_table_sz = val_count * sizeof(uint64_t);
      tp.null_val_loc = tp.col_val_table_loc + gen::size_align8(tp.col_val_table_sz);
      tp.empty_val_loc = tp.null_val_loc + 16;
      tp.col_val_loc0 = tp.empty_val_loc + 16;
      tp.null_empty_sz = 32;
      tp.total_idx_size = tp.opts_loc + tp.opts_size +
                (trie_level > 0 ? louds.size_bytes() : trie_flags.size()) +
                trie_flags_tail.size() +
                tp.fwd_cache_size + gen::size_align8(tp.rev_cache_size) + tp.sec_cache_size +
                (trie_level == 0 ? (gen::size_align8(tp.child_select_lt_sz) +
                     gen::size_align8(tp.term_select_lt_sz + tp.term_rank_lt_sz + tp.child_rank_lt_sz)) :
                  (gen::size_align8(tp.louds_sel1_lt_sz) + gen::size_align8(tp.louds_rank_lt_sz))) +
                gen::size_align8(tp.leaf_select_lt_sz) +
                gen::size_align8(tp.leaf_rank_lt_sz) + gen::size_align8(tp.tail_rank_lt_sz) +
                gen::size_align8(tp.names_sz) + gen::size_align8(tp.col_val_table_sz) + tp.null_empty_sz;
      if (pk_col_count > 0)
        tp.total_idx_size += trie_data_ptr_size();

      // if (get_opts()->dessicate) {
      //   tp.term_select_lkup_loc = tp.term_rank_lt_loc = tp.child_rank_lt_loc = 0;
      //   tp.leaf_select_lkup_loc = tp.leaf_rank_lt_loc = tp.child_select_lkup_loc = 0;
      // }

      gen::print_time_taken(t, "Time taken for build(): ");

      return tp.total_idx_size;

    }

    void open_file() {
      if (fp != NULL)
        fclose(fp);
      if (out_filename == nullptr)
        return;
      fp = fopen(out_filename, "wb+");
      fclose(fp);
      fp = fopen(out_filename, "rb+");
      if (fp == NULL)
        throw errno;
    }

    uint32_t write_trie(const char *filename = NULL) {

      if (tp.names_loc == 0)
        build();

      clock_t t = clock();
      if (filename != NULL) {
        set_out_file(filename);
        // if (fp != NULL) {
        //   close_file();
        //   fp = NULL;
        // }
      }

      if (fp == NULL)
        open_file();

      size_t actual_trie_size = fp == nullptr ? 0 : ftell(fp);

      if (fp == nullptr)
        out_vec->reserve(tp.total_idx_size);

      output_byte(0xA5, fp, out_vec); // magic byte
      output_byte(0x01, fp, out_vec); // version 1.0
      output_byte(pk_col_count, fp, out_vec);
      output_byte(trie_level, fp, out_vec);

      int val_count = column_count;
      output_u32(val_count, fp, out_vec);

      output_u32(tp.names_loc, fp, out_vec);
      output_u32(tp.col_val_table_loc, fp, out_vec);

      output_u32(memtrie.node_count, fp, out_vec);
      output_u32(tp.opts_size, fp, out_vec);
      output_u32(memtrie.node_set_count, fp, out_vec);
      output_u32(memtrie.key_count, fp, out_vec);
      output_u32(memtrie.max_key_len, fp, out_vec);
      output_u32(max_val_len, fp, out_vec);
      output_u16(memtrie.max_tail_len, fp, out_vec);
      output_u16(max_level, fp, out_vec);
      output_u32(tp.fwd_cache_count, fp, out_vec);
      output_u32(tp.rev_cache_count, fp, out_vec);
      output_u32(tp.fwd_cache_max_node_id, fp, out_vec);
      output_u32(tp.rev_cache_max_node_id, fp, out_vec);
      output_bytes((const uint8_t *) &tp.min_stats, 4, fp, out_vec);
      output_u32(tp.fwd_cache_loc, fp, out_vec);
      output_u32(tp.rev_cache_loc, fp, out_vec);
      output_u32(tp.sec_cache_loc, fp, out_vec);

      if (trie_level == 0) {
        output_u32(tp.term_select_lkup_loc, fp, out_vec);
        output_u32(tp.term_rank_lt_loc, fp, out_vec);
        output_u32(tp.child_select_lkup_loc, fp, out_vec);
        output_u32(tp.child_rank_lt_loc, fp, out_vec);
      } else {
        output_u32(tp.louds_sel1_lt_loc, fp, out_vec);
        output_u32(tp.louds_rank_lt_loc, fp, out_vec);
        output_u32(tp.louds_sel1_lt_loc, fp, out_vec);
        output_u32(tp.louds_rank_lt_loc, fp, out_vec);
      }
      output_u32(tp.leaf_select_lkup_loc, fp, out_vec);
      output_u32(tp.leaf_rank_lt_loc, fp, out_vec);
      output_u32(tp.tail_rank_lt_loc, fp, out_vec);
      output_u32(tp.trie_tail_ptrs_data_loc, fp, out_vec);
      output_u32(tp.louds_rank_lt_loc, fp, out_vec);
      output_u32(tp.louds_sel1_lt_loc, fp, out_vec);
      output_u32(tp.trie_flags_loc, fp, out_vec);
      output_u32(tp.tail_flags_loc, fp, out_vec);
      output_u32(tp.null_val_loc, fp, out_vec);
      output_u32(tp.empty_val_loc, fp, out_vec);
      output_u32(0, fp, out_vec); // padding

      output_bytes((const uint8_t *) opts, tp.opts_size, fp, out_vec);

      if (pk_col_count > 0) {
        write_fwd_cache();
        write_rev_cache();
        if (tp.sec_cache_size > 0)
          write_sec_cache(tp.min_stats, tp.sec_cache_size);
        // if (!get_opts()->dessicate) {
          if (trie_level > 0) {
            write_louds_select_lt(tp.louds_sel1_lt_sz);
            write_louds_rank_lt(tp.louds_rank_lt_sz);
          } else {
            write_bv_select_lt(BV_LT_TYPE_CHILD, tp.child_select_lt_sz);
            write_bv_select_lt(BV_LT_TYPE_TERM, tp.term_select_lt_sz);
            write_bv_rank_lt(BV_LT_TYPE_TERM | BV_LT_TYPE_CHILD | (tp.tail_rank_lt_sz == 0 ? 0 : BV_LT_TYPE_TAIL),
                tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.tail_rank_lt_sz);
          }
        // }
        if (trie_level > 0) {
          output_bytes((const uint8_t *) louds.raw_data()->data(), louds.raw_data()->size() * sizeof(uint64_t), fp, out_vec);
          if (tp.tail_rank_lt_sz > 0)
            write_bv_rank_lt(BV_LT_TYPE_TAIL, tp.tail_rank_lt_sz);
        } else
          output_bytes(trie_flags.data(), trie_flags.size(), fp, out_vec);

        write_trie_tail_ptrs_data(fp, out_vec);

        // if (!get_opts()->dessicate) {
          if (trie_level == 0 && tp.leaf_rank_lt_sz > 0)
            write_bv_rank_lt(BV_LT_TYPE_LEAF, tp.leaf_rank_lt_sz);
          output_bytes(trie_flags_tail.data(), trie_flags_tail.size(), fp, out_vec);
          if (get_opts()->leaf_lt && get_opts()->trie_leaf_count > 0)
            write_bv_select_lt(BV_LT_TYPE_LEAF, tp.leaf_select_lt_sz);
        // }
      }

      //val_table[0] = tp.col_val_loc0;
      write_names();
      write_col_val_table();
      write_null_empty();

      gen::gen_printf("\nNodes#: %u, Node set#: %u\nTrie bv: %u, Leaf bv: %u, Tail bv: %u\n"
        "Select lt - Term: %u, Child: %u, Leaf: %u\n"
        "Fwd cache: %u, Rev cache: %u, Sec cache: %u\nNode struct size: %u, Max tail len: %u\n",
            memtrie.node_count, memtrie.node_set_count, tp.term_rank_lt_sz + tp.child_rank_lt_sz,
            tp.leaf_rank_lt_sz, tp.tail_rank_lt_sz, tp.term_select_lt_sz, tp.child_select_lt_sz, tp.leaf_select_lt_sz,
            tp.fwd_cache_size, tp.rev_cache_size, tp.sec_cache_size, sizeof(leopard::node), memtrie.max_tail_len);

      // fp = fopen("nodes.txt", "wb+");
      // // dump_nodes(first_node, fp);
      // find_rpt_nodes(fp);
      // fclose(fp);

      gen::print_time_taken(t, "Time taken for write_trie(): ");
      gen::gen_printf("Idx size: %u\n", tp.total_idx_size);

      actual_trie_size = fp == nullptr ? out_vec->size() : (ftell(fp) - actual_trie_size);
      if (fp == nullptr && trie_level > 0)
        actual_trie_size = tp.total_idx_size;
      if (tp.total_idx_size != actual_trie_size)
        printf("WARNING: Trie size not matching: %lu, ^%ld, lvl: %d -----------------------------------------------------------------------------------------------------------\n", actual_trie_size, (long) actual_trie_size - tp.total_idx_size, trie_level);

      return tp.total_idx_size;

    }

    uint32_t build_kv(bool to_build_trie = true) {
      is_processing_cols = false;
      uint32_t kv_size = 0;
      if (to_build_trie)
        kv_size = build();
      is_processing_cols = true;
      prev_val_size = 0;
      uint64_t prev_val_loc = tp.col_val_loc0;
      for (cur_col_idx = 0; cur_col_idx < column_count; ) {
        char encoding_type = column_encodings[cur_col_idx];
        char data_type = column_types[cur_col_idx];
        if (pk_col_count > 0 && cur_col_idx < pk_col_count) {
          val_table[cur_col_idx] = 0;
          cur_col_idx++;
          continue;
        }
        uint32_t val_size = build_col_val();
        val_table[cur_col_idx] = prev_val_loc;
        prev_val_loc += val_size;
        prev_val_size = val_size;
        break;
      }
      if (!to_build_trie)
        prev_val_loc -= tp.col_val_loc0;
      kv_size += prev_val_loc;
      gen::gen_printf("%s size: %u\n", (to_build_trie ? "Total" : "Val"), kv_size);
      return kv_size;
    }

    void write_kv(bool to_close = true, const char *filename = NULL) {
      is_processing_cols = false;
      write_trie(filename);
      if (column_count > 1) {
        is_processing_cols = true;
        write_col_val();
      }
      //write_final_val_table(to_close, 0);
    }

    uint32_t build_and_write_all(bool to_close = true, const char *filename = NULL) {
      write_trie(filename);
      is_processing_cols = true;
      prev_val_size = 0;
      uint64_t prev_val_loc = tp.col_val_loc0;
      for (cur_col_idx = 0; cur_col_idx < column_count; ) {
        char encoding_type = column_encodings[cur_col_idx];
        char data_type = column_types[cur_col_idx];
        if (pk_col_count > 0 && cur_col_idx < pk_col_count) {
          val_table[cur_col_idx] = 0;
          cur_col_idx++;
          continue;
        }
        // if (all_vals->size() > 2 || encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || encoding_type == 'w') { // TODO: What if column contains only NULL and ""
        uint32_t val_size = build_col_val();
        write_col_val();
        val_table[cur_col_idx] = prev_val_loc;
          prev_val_loc += val_size;
          prev_val_size = val_size;
          reset_for_next_col();  // cur_col_idx++ happens here (?)
        // }
      }
      write_final_val_table();
      gen::gen_printf("Total size: %u\n", prev_val_loc);
      return prev_val_loc;
    }

    void write_names() {
      int name_count = column_count + 2;
      for (int i = 0; i < name_count; i++)
        output_u16(names_positions[i], fp, out_vec);
      output_bytes((const uint8_t *) names, names_len, fp, out_vec);
      output_align8(tp.names_sz, fp, out_vec);
    }

    void write_null_empty() {
      output_byte(null_value_len, fp, out_vec);
      output_bytes(null_value, 15, fp, out_vec);
      output_byte(empty_value_len, fp, out_vec);
      output_bytes(empty_value, 15, fp, out_vec);
    }

    void write_col_val_table() {
      for (size_t i = 0; i < column_count; i++)
        output_u64(val_table[i], fp, out_vec);
      output_align8(tp.col_val_table_sz, fp, out_vec);
    }

    void write_final_val_table(bool to_close = true) {
      if (fp == NULL) {
        for (size_t i = 0; i < column_count; i++)
          gen::copy_uint32(val_table[i], out_vec->data() + tp.col_val_table_loc + i * 8);
      } else {
        fseek(fp, tp.col_val_table_loc, SEEK_SET);
        write_col_val_table();
        fseek(fp, 0, SEEK_END);
      }
      int val_count = column_count;
      gen::gen_printf("Val count: %d, tbl:", val_count);
      for (int i = 0; i < val_count; i++)
        gen::gen_printf(" %u", val_table[i]);
      gen::gen_printf("\nCol sizes:");
      uint32_t total_size = val_table[0];
      for (int i = 1; i < val_count; i++) {
        gen::gen_printf(" %u", val_table[i] - val_table[i - 1]);
        total_size += val_table[i];
      }
      gen::gen_printf("\n");
      if (to_close)
        close_file();
    }

    // struct nodes_ptr_grp {
    //   uint32_t node_id;
    //   uint32_t ptr;
    // };

    void write_bv_n(uint32_t node_id, bool to_write, uint32_t& count, uint32_t& count_n, uint16_t *bit_counts_n, uint8_t& pos_n) {
      if (!to_write)
        return;
      size_t u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        output_u32(count, fp, out_vec);
        for (size_t i = nodes_per_bv_block == 256 ? 1: 0; i < u8_arr_count; i++) {
          output_byte(bit_counts_n[i], fp, out_vec);
        }
        // if (nodes_per_bv_block == 512) {
        //   for (size_t i = 1; i < pos_n; i++)
        //     count += bit_counts_n[i];
        // }
        count += count_n;
        count_n = 0;
        memset(bit_counts_n, 0xFF, u8_arr_count * sizeof(uint16_t));
        bit_counts_n[0] = 0x1E;
        pos_n = 1;
      } else if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
        bit_counts_n[pos_n] = count_n & 0xFF;
        uint8_t b0_mask = (0x100 >> pos_n);
        bit_counts_n[0] &= ~b0_mask;
        if (count_n > 255)
          bit_counts_n[0] |= ((count_n & 0x100) >> pos_n);
        // if (nodes_per_bv_block == 512)
        //   count_n = 0;
        pos_n++;
      }
    }

    void write_bv_rank_lt(uint8_t which, size_t rank_lt_sz) {
      uint32_t node_id = 0;
      size_t u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      uint32_t count_tail = 0;
      uint32_t count_term = 0;
      uint32_t count_child = 0;
      uint32_t count_leaf = 0;
      uint32_t count_tail_n = 0;
      uint32_t count_term_n = 0;
      uint32_t count_child_n = 0;
      uint32_t count_leaf_n = 0;
      uint16_t bit_counts_tail_n[u8_arr_count];
      uint16_t bit_counts_term_n[u8_arr_count];
      uint16_t bit_counts_child_n[u8_arr_count];
      uint16_t bit_counts_leaf_n[u8_arr_count];
      uint8_t pos_tail_n = 1;
      uint8_t pos_term_n = 1;
      uint8_t pos_child_n = 1;
      uint8_t pos_leaf_n = 1;
      memset(bit_counts_tail_n,  0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_term_n,  0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_child_n, 0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_leaf_n,  0xFF, u8_arr_count * sizeof(uint16_t));
      bit_counts_tail_n[0] = 0x1E;
      bit_counts_term_n[0] = 0x1E;
      bit_counts_child_n[0] = 0x1E;
      bit_counts_leaf_n[0] = 0x1E;
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n, pos_term_n);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n, pos_child_n);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n, pos_leaf_n);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n, pos_tail_n);
        count_tail_n += (cur_node_flags & NFLAG_TAIL ? 1 : 0);
        count_term_n += (cur_node_flags & NFLAG_TERM ? 1 : 0);
        count_child_n += (cur_node_flags & NFLAG_CHILD ? 1 : 0);
        count_leaf_n += (cur_node_flags & NFLAG_LEAF ? 1 : 0);
        node_id++;
        cur_node = ni.next();
      }
      node_id = nodes_per_bv_block; // just to make it write the last blocks
      for (size_t i = 0; i < 2; i++) {
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n, pos_term_n);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n, pos_child_n);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n, pos_leaf_n);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n, pos_tail_n);
      }
      output_align8(rank_lt_sz, fp, out_vec);
    }

    void write_louds_rank_lt(size_t rank_lt_sz) {
      uint32_t count = 0;
      uint32_t count_n = 0;
      int u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      uint16_t bit_counts_n[u8_arr_count];
      uint8_t pos_n = 1;
      memset(bit_counts_n, 0xFF, u8_arr_count * sizeof(uint16_t));
      bit_counts_n[0] = 0x1E;
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        write_bv_n(i, true, count, count_n, bit_counts_n, pos_n);
        count_n += (louds[i] ? 1 : 0);
      }
      bit_count = nodes_per_bv_block; // just to make it write last blocks
      write_bv_n(bit_count, true, count, count_n, bit_counts_n, pos_n);
      write_bv_n(bit_count, true, count, count_n, bit_counts_n, pos_n);
      output_align8(rank_lt_sz, fp, out_vec);
    }

    void write_fwd_cache() {
      output_bytes((const uint8_t *) f_cache, tp.fwd_cache_count * sizeof(fwd_cache), fp, out_vec);
    }

    void write_rev_cache() {
      output_bytes((const uint8_t *) r_cache, tp.rev_cache_count * sizeof(nid_cache), fp, out_vec);
      output_align8(tp.rev_cache_size, fp, out_vec);
    }

    bool node_qualifies_for_select(leopard::node *cur_node, uint8_t cur_node_flags, int which) {
      switch (which) {
        case BV_LT_TYPE_TERM:
          return (cur_node_flags & NFLAG_TERM) > 0;
        case BV_LT_TYPE_LEAF:
          return (cur_node_flags & NFLAG_LEAF) > 0;
        case BV_LT_TYPE_CHILD:
          return (cur_node_flags & NFLAG_CHILD) > 0;
      }
      return false;
    }

    void write_bv_select_lt(int which, size_t sel_lt_sz) {
      uint32_t node_id = 0;
      uint32_t one_count = 0;
      output_u24(0, fp, out_vec);
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        if (node_qualifies_for_select(&cur_node, cur_node_flags, which)) {
          one_count++;
          if (one_count && (one_count % sel_divisor) == 0) {
            uint32_t val_to_write = node_id / nodes_per_bv_block;
            output_u24(val_to_write, fp, out_vec);
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", one_count, val_to_write);
          }
        }
        node_id++;
        cur_node = ni.next();
      }
      output_u24(memtrie.node_count/nodes_per_bv_block, fp, out_vec);
      output_align8(sel_lt_sz, fp, out_vec);
    }

    void write_louds_select_lt(size_t sel_lt_sz) {
      uint32_t one_count = 0;
      output_u24(0, fp, out_vec);
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        if (louds[i]) {
          one_count++;
          if (one_count && (one_count % sel_divisor) == 0) {
            uint32_t val_to_write = i / nodes_per_bv_block;
            output_u24(val_to_write, fp, out_vec);
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", one_count, val_to_write);
          }
        }
      }
      output_u24(bit_count / nodes_per_bv_block, fp, out_vec);
      output_align8(sel_lt_sz, fp, out_vec);
    }

    uniq_info_base *get_ti(leopard::node *n) {
      tail_val_maps *tm = &tail_maps;
      uniq_info_vec *uniq_tails_rev = tm->get_ui_vec();
      return (*uniq_tails_rev)[n->get_tail()];
    }

    uint32_t get_tail_ptr(leopard::node *cur_node) {
      uniq_info_base *ti = get_ti(cur_node);
      return ti->ptr;
    }

    uniq_info_base *get_vi(leopard::node *n) {
      tail_val_maps *tm = &val_maps;
      uniq_info_vec *uniq_vals_fwd = tm->get_ui_vec();
      return (*uniq_vals_fwd)[n->get_col_val()];
    }

    void set_out_vec(byte_vec *ov) {
      out_vec = ov;
    }

    byte_vec *get_out_vec() {
      return out_vec;
    }

    bldr_options *get_opts() {
      if (opts->opts_count < 2)
        return opts;
      if (is_processing_cols)
        return opts + 1;
      return opts;
    }

    #define APPEND_REC_NOKEY 0
    #define APPEND_REC_KEY_MIDDLE 1
    #define APPEND_REC_KEY_LAST 2
    size_t append_rec_value(char type, char encoding_type, void *void_value, const uint8_t *byte_arr, size_t value_len, byte_vec& rec, int val_type) {
      int64_t *i64_ptr = (int64_t *) void_value;
      double *dbl_ptr = (double *) void_value;
      switch (type) {
        case MST_TEXT:
        case MST_BIN: {
          const uint8_t *value = byte_arr;
          if (*i64_ptr == 0) {
            value = null_value;
            value_len = null_value_len;
          }
          if (value_len == 0) {
            value = empty_value;
            value_len = empty_value_len;
          }
          if (val_type == APPEND_REC_NOKEY)
            gen::append_vint32(rec, value_len);
          for (size_t j = 0; j < value_len; j++)
            rec.push_back(value[j]);
          if (val_type == APPEND_REC_KEY_MIDDLE)
            rec.push_back(0);
        } break;
        case MST_INT:
        case MST_DECV ... MST_DEC9:
        case MST_DATE_US ... MST_DATETIME_ISOT_MS: {
          if (val_type == APPEND_REC_NOKEY) {
            if (type == MST_DECV) {
              uint8_t *v64 = (uint8_t *) dbl_ptr;
              value_len = 8;
              for (size_t vi = 0; vi < value_len; vi++)
                rec.push_back(v64[vi]);
            } else {
              if (*i64_ptr == INT64_MIN) { // null
                for (size_t i = 0; i < 9; i++)
                  rec.push_back(0xFF);
                value_len = 9;
              } else {
                int64_t i64 = *i64_ptr;
                if (type >= MST_DEC0 && type <= MST_DEC9) {
                  double dbl = *dbl_ptr;
                  i64 = static_cast<int64_t>(dbl * gen::pow10(type - MST_DEC0));
                }
                i64 = flavic48::zigzag_encode(i64);
                uint8_t v64[10];
                uint8_t *v_end = flavic48::simple_encode_single(i64, v64, 0);
                value_len = (v_end - v64);
                for (size_t vi = 0; vi < value_len; vi++)
                  rec.push_back(v64[vi]);
              }
            }
          } else {
            if (*i64_ptr == INT64_MIN) { // null
              rec.push_back(0);
              value_len = 1;
            } else {
              gen::append_svint60(rec, *i64_ptr);
              value_len = gen::get_svint60_len(*i64_ptr);
            }
          }
        } break;
      }
      if (max_val_len < value_len)
        max_val_len = value_len;
      return value_len;
    }

    size_t get_value_len(size_t i, char type, const uint64_t *values, const size_t value_lens[]) {
      size_t value_len = 0;
      if (value_lens != nullptr)
        value_len = value_lens[i];
      if (value_lens == nullptr) {
        if (type == MST_TEXT && values[i] != 0 && values[i] != UINT64_MAX)
          value_len = strlen((const char *) values[i]);
      }
      return value_len;
    }
    bool insert_record(const void *void_values, const size_t value_lens[] = NULL) {
      uint64_t *values = (uint64_t *) void_values;
      cur_seq_idx++;
      byte_vec rec;
      byte_vec key_rec;
      size_t i = 0;
      for (; i < column_count; i++) {
        uint8_t type = column_types[i];
        if (type == MST_SEC_2WAY)
          break;
        // printf("col: %lu - ", i);
        size_t value_len = get_value_len(i, type, values, value_lens);
        append_rec_value(type, column_encodings[i], (void *) &values[i], (const uint8_t *) values[i], value_len, rec, APPEND_REC_NOKEY);
        if (i < pk_col_count) {
          append_rec_value(type, column_encodings[i], (void *) &values[i], (const uint8_t *) values[i], value_len, key_rec,
             i < (pk_col_count - 1) ? APPEND_REC_KEY_MIDDLE : APPEND_REC_KEY_LAST);
        }
      }
      size_t sec_idx = 0;
      for (; i < column_count; i++, sec_idx++) {
        const char *sec_cols = names + names_positions[i + 2];
        size_t sec_cols_len = strlen(sec_cols);
        const char *cur_col = sec_cols;
        byte_vec sec_rec;
        for (size_t j = 0; j < sec_cols_len; j++) {
          if (sec_cols[j] == '+') {
            int sec_col_idx = atoi(cur_col) - 1;
            char sec_col_type = column_types[sec_col_idx];
            size_t value_len = get_value_len(sec_col_idx, sec_col_type, values, value_lens);
            append_rec_value(sec_col_type, column_encodings[sec_col_idx], (void *) &values[sec_col_idx], (const uint8_t *) values[sec_col_idx], value_len, sec_rec, APPEND_REC_KEY_MIDDLE);
            cur_col = sec_cols + j + 1;
          }
        }
        int sec_col_idx = atoi(cur_col) - 1;
        char sec_col_type = column_types[sec_col_idx];
        size_t value_len = get_value_len(sec_col_idx, sec_col_type, values, value_lens);
        append_rec_value(sec_col_type, column_encodings[sec_col_idx], (void *) &values[sec_col_idx], (const uint8_t *) values[sec_col_idx], value_len, sec_rec, APPEND_REC_KEY_LAST);
        append_rec_value('*', 'T', (void *) sec_rec.data(), (const uint8_t *) sec_rec.data(), sec_rec.size(), rec, APPEND_REC_NOKEY);
      }
      if (pk_col_count == 0) {
        uint32_t val_pos = all_vals->push_back_with_vlen(rec.data(), rec.size());
        rec_pos_vec.push_back(val_pos);
        leopard::node_set_handler::create_node_set(memtrie.all_node_sets, 1);
        leopard::node_set_handler nsh(memtrie.all_node_sets, cur_seq_idx);
        leopard::node n = nsh.first_node();
        n.set_flags(NFLAG_LEAF | NFLAG_TERM);
        nsh.hdr()->node_id = cur_seq_idx;
        memtrie.node_count++;
      } else {
        leopard::node n;
        leopard::node_set_vars nsv;
        bool exists = memtrie.lookup(key_rec.data(), key_rec.size(), nsv);
        bool to_append = true;
        uint32_t val_pos;
        if (exists) {
          leopard::node_set_handler nsh(memtrie.all_node_sets, nsv.node_set_pos);
          n = nsh[nsv.cur_node_idx];
          val_pos = n.get_col_val();
          size_t vlen;
          uint8_t *val_loc = (*all_vals)[val_pos];
          uint32_t old_len = gen::read_vint32(val_loc, &vlen);
          if (rec.size() <= old_len) {
            to_append = false;
            gen::copy_vint32(rec.size(), val_loc, vlen);
            memcpy(val_loc + vlen, rec.data(), rec.size());
            rec_pos_vec.push_back(val_pos);
          }
          return true;
        }
        if (to_append) {
          val_pos = all_vals->push_back_with_vlen(rec.data(), rec.size());
          rec_pos_vec.push_back(val_pos);
          memtrie.insert(key_rec.data(), key_rec.size(), val_pos);
          if (exists)
            n.set_col_val(val_pos);
        }
        // printf("Key: [%.*s]\n", (int) key_len, rec.data() + key_loc_pos);
      }
      return false;
    }

};

}

// 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx
// 16 - followed by pointer
// 0 2-15/18-31 bytes 16 ptrs bytes 1

// 0 0000 - terminator
// 0 0001 to 0014 - length of affix and terminator
// 0 0015 - If length more than 14

// 1 xxxx 01xxxxxx - dictionary reference 1024 bytes
// 1 xxxx 1xxxxxxx 01xxxxxx - dictionary reference 131kb
// 1 xxxx 1xxxxxxx 1xxxxxxx 01xxxxxx - dictionary reference 16mb
// 1 xxxx 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx - dictionary reference 2gb

// dictionary

// no need to refer data if ptr bits > data size
// RLE within grouped pointers
// ijklmnopqrs - triple bit overhead double
// pPqQrRsS - double bit overhead double
// xXyY - single bit overhead double
// zZ - 0 bit overhead double
// sub-byte width delta coding
// inverted delta coding ??
// 

#endif
