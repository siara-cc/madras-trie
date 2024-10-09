#ifndef builder_H
#define builder_H

#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <cstring>
#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>
#include <time.h>

#include "common_dv1.hpp"
#include "../../leopard-trie/src/leopard.hpp"

#include "../../ds_common/src/bv.hpp"
#include "../../ds_common/src/gen.hpp"
#include "../../ds_common/src/vint.hpp"
#include "../../ds_common/src/huffman.hpp"
#include "../../ds_common/src/match_words.hpp"
#include "../../ds_common/src/compress.hpp"

namespace madras_dv1 {

#define MDX_SUFFIX_FULL 0x01
#define MDX_SUFFIX_PARTIAL 0x02
#define MDX_SUFFIXES 0x03
#define MDX_HAS_SUFFIX 0x04
#define MDX_PREFIX_FULL 0x01
#define MDX_PREFIX_PARTIAL 0x02
#define MDX_PREFIXES 0x03
#define MDX_HAS_PREFIX 0x04
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
  uint32_t total_idx_size;
  bldr_min_pos_stats min_stats;
};

typedef int (*cmp_fn) (const uint8_t *v1, int len1, const uint8_t *v2, int len2);

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
    bldr_options opts;
    bool maintain_seq;
    bool no_primary_trie;
    int trie_level;
    builder_fwd(bldr_options _opts, bool _maintain_seq, bool _no_primary_trie)
      : opts (_opts), maintain_seq (_maintain_seq), no_primary_trie (_no_primary_trie) {
    }
    virtual ~builder_fwd() {
    }
    virtual leopard::trie *get_memtrie() = 0;
    virtual builder_fwd *new_instance() = 0;
    virtual bool insert(const uint8_t *key, int key_len, const void *val, int val_len, uint32_t val_pos = UINT32_MAX) = 0;
    virtual uint32_t build() = 0;
    virtual uint32_t write_trie(const char *filename = NULL) = 0;
};

class ptr_groups {
  private:
    builder_fwd *bldr;
    int step_bits_idx;
    int step_bits_rest;
    std::vector<freq_grp> freq_grp_vec;
    std::vector<byte_vec> grp_data;
    byte_vec ptrs;
    gen::int_bit_vector flat_ptr_bv;
    byte_vec ptr_lookup_tbl;
    byte_vec idx2_ptrs_map;
    int last_byte_bits;
    uint32_t idx_limit;
    int start_bits;
    char enc_type;
    bool no_primary_trie;
    bool dessicate;
    uint8_t idx_ptr_size;
    uint8_t ptr_lkup_tbl_ptr_width;
    uint32_t next_idx;
    uint32_t ptr_lookup_tbl_sz;
    uint32_t ptr_lookup_tbl_loc;
    uint32_t grp_data_loc;
    uint32_t grp_data_size;
    uint32_t grp_ptrs_loc;
    uint32_t idx2_ptrs_map_loc;
    uint32_t idx2_ptr_count;
    uint64_t tot_ptr_bit_count;
    uint32_t max_len;
  public:
    std::vector<builder_fwd *> inner_tries;
    size_t inner_trie_start_grp;
    ptr_groups() {
      reset();
    }
    ~ptr_groups() {
      for (size_t i = 0; i < inner_tries.size(); i++)
        delete inner_tries[i];
    }
    void init(builder_fwd *_bldr_obj, int _step_bits_idx, int _step_bits_rest) {
      bldr = _bldr_obj;
      step_bits_idx = _step_bits_idx;
      step_bits_rest = _step_bits_rest;
    }
    void reset() {
      freq_grp_vec.resize(0);
      grp_data.resize(0);
      ptrs.resize(0);
      idx2_ptrs_map.resize(0);
      next_idx = 0;
      idx_limit = 0;
      inner_trie_start_grp = 0;
      idx_ptr_size = 3;
      last_byte_bits = 64;
      max_len = 0;
      gen::append_uint64(0, ptrs);
    }
    uint32_t get_idx_limit() {
      return idx_limit;
    }
    uint32_t idx_map_arr[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    void set_idx_info(int _start_bits, uint32_t new_idx_limit, uint8_t _idx_ptr_size) {
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
    int get_idx_ptr_size() {
      return idx_ptr_size;
    }
    uint32_t *get_idx_map_arr() {
      return idx_map_arr;
    }
    uint32_t get_idx2_ptrs_count() {
      uint32_t idx2_ptr_count = idx2_ptrs_map.size() / get_idx_ptr_size() + (start_bits << 20) + (get_idx_limit() << 24) + (step_bits_idx << 29);
      if (get_idx_ptr_size() == 3)
        idx2_ptr_count |= 0x80000000;
      return idx2_ptr_count;
    }
    void clear_freq_grps() {
      freq_grp_vec.clear();
    }
    void add_freq_grp(freq_grp freq_grp) {
      freq_grp_vec.push_back(freq_grp);
    }
    uint32_t check_next_grp(uint8_t grp_no, uint32_t cur_limit, uint32_t len) {
      if (grp_no >= bldr->opts.max_groups)
        return cur_limit;
      if (grp_no <= idx_limit) {
        if (next_idx == cur_limit)
          return pow(2, log2(cur_limit) + step_bits_idx);
      } else {
        if ((freq_grp_vec[grp_no].grp_size + len) >= (cur_limit - 1))
          return pow(2, log2(cur_limit) + step_bits_rest);
      }
      return cur_limit;
    }
    uint32_t next_grp(uint8_t& grp_no, uint32_t cur_limit, uint32_t len, uint32_t tot_freq_count, bool force_next_grp = false) {
      if (grp_no >= bldr->opts.max_groups)
        return cur_limit;
      bool next_grp = force_next_grp;
      if (grp_no <= idx_limit) {
        if (next_idx == cur_limit) {
          next_grp = true;
          next_idx = 0;
        }
      } else {
        if ((freq_grp_vec[grp_no].grp_size + len) >= (cur_limit - 1))
          next_grp = true;
      }
      if (next_grp) {
        if (grp_no < idx_limit)
          cur_limit = pow(2, log2(cur_limit) + step_bits_idx);
        else
          cur_limit = pow(2, log2(cur_limit) + step_bits_rest);
        grp_no++;
        freq_grp_vec.push_back((freq_grp) {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0, 0, 0});
      }
      return cur_limit;
    }
    void update_current_grp(uint32_t grp_no, int32_t len, int32_t freq) {
      freq_grp_vec[grp_no].grp_size += len;
      freq_grp_vec[grp_no].freq_count += freq;
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
        data.push_back(1);
        grp_data.push_back(data);
      }
      return grp_data[grp_no];
    }
    uint32_t get_set_len_len(uint32_t len, byte_vec *vec = NULL) {
      uint32_t len_len = 0;
      do {
        len_len++;
        if (vec != NULL)
          vec->push_back((len & 0x0F) | 0x10);
        len >>= 4;
      } while (len > 0);
      return len_len;
    }
    uint32_t append_text_tail(uint32_t grp_no, uint8_t *val, uint32_t len, bool append0 = false) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (append0)
        grp_data_vec.push_back(0);
      return ptr;
    }
    uint32_t append_text_val(uint32_t grp_no, uint8_t *val, uint32_t len, bool append0 = false) {
      byte_vec& grp_data_vec = get_data(grp_no);
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (append0)
        grp_data_vec.push_back(0);
      return grp_data_vec.size() - 2;
    }
    uint32_t append_bin_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len, char data_type = LPDT_BIN) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      if (data_type == LPDT_TEXT || data_type == LPDT_BIN)
        gen::append_vint32(grp_data_vec, len);
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      return ptr;
    }
    uint32_t append_bin15_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      get_set_len_len(len, &grp_data_vec);
      for (uint32_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      // grp_data_vec.push_back(0);
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
      return 514 + grp_data.size() * 4 + inner_tries.size() * 4;
    }
    uint32_t get_data_size() {
      uint32_t data_size = 0;
      for (size_t i = 0; i < grp_data.size(); i++)
        data_size += grp_data[i].size();
      for (size_t i = 0; i < inner_tries.size(); i++)
        data_size += freq_grp_vec[i + inner_trie_start_grp].grp_size;
      return data_size;
    }
    uint32_t get_ptrs_size() {
      return ptrs.size();
    }
    uint32_t get_total_size() {
      if (enc_type == 't')
        return grp_data_size + get_ptrs_size() + ptr_lookup_tbl_sz + 6 * 4 + 4;
      return grp_data_size + get_ptrs_size() + idx2_ptrs_map.size() + ptr_lookup_tbl_sz + 6 * 4 + 4;
    }
    void set_ptr_lkup_tbl_ptr_width(uint8_t width) {
      ptr_lkup_tbl_ptr_width = width;
    }
    typedef leopard::uniq_info *(*get_info_fn) (leopard::node *cur_node, std::vector<leopard::uniq_info *>& info_vec);
    static leopard::uniq_info *get_tails_info_fn(leopard::node *cur_node, std::vector<leopard::uniq_info *>& info_vec) {
      return (leopard::uniq_info *) info_vec[cur_node->get_tail()];
    }
    static leopard::uniq_info *get_vals_info_fn(leopard::node *cur_node, std::vector<leopard::uniq_info *>& info_vec) {
      return (leopard::uniq_info *) info_vec[cur_node->get_col_val()];
    }
    void build(uint32_t node_count, byte_ptr_vec& all_node_sets, get_info_fn get_info_func,
          std::vector<leopard::uniq_info *>& info_vec, bool is_tail, bool no_pt, bool dessicat,
          char encoding_type = 'u', int col_trie_size = 0) {
      no_primary_trie = no_pt;
      dessicate = dessicat;
      enc_type = encoding_type;
      if (encoding_type != 't' && col_trie_size == 0 && freq_grp_vec.size() > 2) {
        ptr_lkup_tbl_ptr_width = 4;
        build_ptr_lookup_tbl(all_node_sets, get_info_func, is_tail, info_vec);
      }
      ptr_lookup_tbl_loc = 6 * 4 + 4;
      if (encoding_type == 't' || col_trie_size > 0 || freq_grp_vec.size() == 2)
        ptr_lookup_tbl_sz = 0;
      else {
        if (dessicate)
          ptr_lookup_tbl_sz = 0;
        else // TODO: PTR LT gets created unnecessarily for last level of tail tries
          ptr_lookup_tbl_sz = gen::get_lkup_tbl_size2(node_count, nodes_per_ptr_block, ptr_lkup_tbl_ptr_width + (nodes_per_ptr_block / nodes_per_ptr_block_n - 1) * 2);
      }
      grp_ptrs_loc = ptr_lookup_tbl_loc + ptr_lookup_tbl_sz;
      if (encoding_type == 't' || col_trie_size > 0) {
        idx2_ptr_count = idx2_ptrs_map_loc = 0;
        grp_data_loc = grp_ptrs_loc + ptrs.size();
        grp_data_size = col_trie_size;
      } else {
        idx2_ptr_count = get_idx2_ptrs_count();
        idx2_ptrs_map_loc = grp_ptrs_loc + ptrs.size();
        grp_data_loc = idx2_ptrs_map_loc + idx2_ptrs_map.size();
        grp_data_size = get_hdr_size() + get_data_size();
      }
      if (dessicate)
        ptr_lookup_tbl_loc = 0;
    }
    #define CODE_LT_BIT_LEN 0
    #define CODE_LT_CODE_LEN 1
    void write_code_lookup_tbl(bool is_tail, FILE* fp) {
      write_code_lt(is_tail, CODE_LT_BIT_LEN, fp);
      write_code_lt(is_tail, CODE_LT_CODE_LEN, fp);
    }
    void write_code_lt(bool is_tail, int which, FILE* fp) {
      for (int i = 0; i < 256; i++) {
        uint8_t code_i = i;
        bool code_found = false;
        for (size_t j = 1; j < freq_grp_vec.size(); j++) {
          uint8_t code_len = freq_grp_vec[j].code_len;
          uint8_t code = freq_grp_vec[j].code;
          if ((code_i >> (8 - code_len)) == code) {
            if (which == CODE_LT_BIT_LEN) {
              int bit_len = freq_grp_vec[j].grp_log2;
              fputc(bit_len, fp);
            }
            if (which == CODE_LT_CODE_LEN) {
              fputc((j - 1) | (code_len << 4), fp);
            }
            code_found = true;
            break;
          }
        }
        if (!code_found) {
          //printf("Code not found: %d", i);
          //fputc(0, fp);
          fputc(0, fp);
        }
      }
    }
    void write_grp_data(uint32_t offset, bool is_tail, FILE* fp) {
      int grp_count = grp_data.size() + inner_tries.size();
      fputc(grp_count, fp);
      if (inner_trie_start_grp > 0) {
        fputc(inner_trie_start_grp - 1, fp);
        //fputc(freq_grp_vec[grp_count].grp_log2, fp);
      } else if (freq_grp_vec.size() == 2)
        fputc(freq_grp_vec[1].grp_log2, fp);
      else
        fputc(0, fp);
      write_code_lookup_tbl(is_tail, fp);
      uint32_t total_data_size = 0;
      for (size_t i = 0; i < grp_data.size(); i++) {
        gen::write_uint32(offset + grp_count * 4 + total_data_size, fp);
        total_data_size += grp_data[i].size();
      }
      for (size_t i = 0; i < inner_tries.size(); i++) {
        gen::write_uint32(offset + grp_count * 4 + total_data_size, fp);
        total_data_size += freq_grp_vec[i + inner_trie_start_grp].grp_size;
      }
      for (size_t i = 0; i < grp_data.size(); i++) {
        fwrite(grp_data[i].data(), 1, grp_data[i].size(), fp);
      }
      for (size_t i = 0; i < inner_tries.size(); i++) {
        inner_tries[i]->fp = fp;
        inner_tries[i]->write_trie();
      }
    }
    void write_ptr_lookup_tbl(FILE *fp) {
      fwrite(ptr_lookup_tbl.data(), 1, ptr_lookup_tbl.size(), fp);
    }
    void write_ptrs(FILE *fp) {
      fwrite(ptrs.data(), 1, ptrs.size(), fp);
    }
    void append_val_ptr(leopard::node *cur_node, get_info_fn get_info_func,
          std::vector<leopard::uniq_info *>& info_vec) {
      if ((cur_node->get_flags() & NFLAG_LEAF) == 0)
        return;
      leopard::uniq_info *vi = get_info_func(cur_node, info_vec);
      freq_grp *fg = get_freq_grp(vi->grp_no);
      // if (cur_node->node_id < 500)
      //   std::cout << "node_id: " << cur_node->node_id << "grp no: " << (int) vi->grp_no << ", bitlen: " << fg->grp_log2 << ", ptr: " << vi->ptr << std::endl;
      append_ptr_bits(fg->code, fg->code_len);
      append_ptr_bits(vi->ptr, fg->grp_log2 - fg->code_len);
    }
    struct block_ptr {
      uint32_t ptr;
      uint8_t ptr_len;
      uint8_t code;
      uint8_t code_len;
    };
    typedef std::vector<block_ptr> block_ptr_vec;
    bool cmp_blk_ptr_vecs(block_ptr_vec& blk_ptrs1, block_ptr_vec& blk_ptrs2) {
      return false;
      if (blk_ptrs1.size() != blk_ptrs2.size())
        return false;
      if (memcmp(blk_ptrs1.data(), blk_ptrs2.data(), sizeof(block_ptr) * blk_ptrs1.size()) == 0)
        return true;
      return false;
    }
    void append_blk_ptrs(block_ptr_vec& block_ptrs) {
      for (size_t iptr = 0; iptr < block_ptrs.size(); iptr++) {
        block_ptr *bp = &block_ptrs[iptr];
        if (bp->code_len != 0 || bp->ptr_len != 0) {
          append_ptr_bits(bp->code, bp->code_len);
          append_ptr_bits(bp->ptr, bp->ptr_len);
        }
      }
    }
    void build_ptr_lookup_tbl(byte_ptr_vec& all_node_sets, get_info_fn get_info_func, bool is_tail,
          std::vector<leopard::uniq_info *>& info_vec) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      uint32_t bit_count4 = 0;
      int pos4 = 0;
      int u16_arr_count = (nodes_per_ptr_block / nodes_per_ptr_block_n);
      u16_arr_count--;
      uint16_t bit_counts[u16_arr_count + 1];
      block_ptr_vec prv_blk_ptrs;
      block_ptr_vec block_ptrs;
      memset(bit_counts, '\0', u16_arr_count * 2 + 2);
      ptr_lookup_tbl.clear();
      if (!is_tail) {
        ptrs.clear();
        ptrs.push_back(0);
        last_byte_bits = 8;
      }
      if (!dessicate) {
        if (ptr_lkup_tbl_ptr_width == 4)
          gen::append_uint32(bit_count, ptr_lookup_tbl);
        else
          gen::append_uint24(bit_count, ptr_lookup_tbl);
      }
      leopard::node_iterator ni(all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        if (node_id && (node_id % nodes_per_ptr_block_n) == 0) {
          if (is_tail) {
            if (bit_count4 > 65535)
              std::cout << "UNEXPECTED: PTR_LOOKUP_TBL bit_count3 > 65k" << std::endl;
            bit_counts[pos4] = bit_count4;
            append_blk_ptrs(block_ptrs);
          } else {
            if (cmp_blk_ptr_vecs(block_ptrs, prv_blk_ptrs)) {
              if (pos4 == 0) {
                bit_count4 = 0;
                bit_counts[0] = 0;
              } else {
                bit_count4 = bit_counts[pos4 - 1];
                bit_counts[pos4] = bit_count4;
              }
            } else {
              if (bit_count4 > 65535)
                std::cout << "UNEXPECTED: PTR_LOOKUP_TBL bit_count3 > 65k" << std::endl;
              bit_counts[pos4] = bit_count4;
              append_blk_ptrs(block_ptrs);
            }
          }
          pos4++;
          prv_blk_ptrs = block_ptrs;
          block_ptrs.clear();
        }
        if (node_id && (node_id % nodes_per_ptr_block) == 0) {
          if (!dessicate) {
            for (int j = 0; j < u16_arr_count; j++)
              gen::append_uint16(bit_counts[j], ptr_lookup_tbl);
          }
          bit_count += bit_counts[u16_arr_count];
          if (!dessicate) {
            if (ptr_lkup_tbl_ptr_width == 4)
              gen::append_uint32(bit_count, ptr_lookup_tbl);
            else
              gen::append_uint24(bit_count, ptr_lookup_tbl);
          }
          bit_count4 = 0;
          pos4 = 0;
          memset(bit_counts, '\0', u16_arr_count * 2 + 2);
        }
        if (is_tail) {
          if (cur_node_flags & NFLAG_TAIL) {
            leopard::uniq_info *vi = get_info_func(&cur_node, info_vec);
            freq_grp& fg = freq_grp_vec[vi->grp_no];
            bit_count4 += fg.grp_log2;
          }
        } else {
          if (cur_node_flags & NFLAG_LEAF) {
            leopard::uniq_info *vi = get_info_func(&cur_node, info_vec);
            freq_grp& fg = freq_grp_vec[vi->grp_no];
            block_ptrs.push_back({vi->ptr, (uint8_t) (fg.grp_log2 - fg.code_len), fg.code, fg.code_len});
            bit_count4 += fg.grp_log2;
          }
        }
        node_id++;
        // if (!is_tail)
        //   append_val_ptr(&cur_node, get_info_func, info_vec);
        cur_node = ni.next();
      }
      prv_blk_ptrs.resize(block_ptrs.size());
      if (!cmp_blk_ptr_vecs(block_ptrs, prv_blk_ptrs))
        append_blk_ptrs(block_ptrs);
      if (!is_tail)
        append_ptr_bits(0x00, 8); // read beyond protection
      if (!dessicate) {
        for (int j = 0; j < u16_arr_count; j++)
          gen::append_uint16(bit_counts[j], ptr_lookup_tbl);
      }
      bit_count += bit_counts[u16_arr_count];
      if (!dessicate) {
        if (ptr_lkup_tbl_ptr_width == 4)
          gen::append_uint32(bit_count, ptr_lookup_tbl);
        else
          gen::append_uint24(bit_count, ptr_lookup_tbl);
        for (int j = 0; j < u16_arr_count; j++)
          gen::append_uint16(bit_counts[j], ptr_lookup_tbl);
      }
    }
    void write_ptrs_data(char data_type, uint8_t flags, bool is_tail, FILE *fp) {
      fputc(ptr_lkup_tbl_ptr_width, fp);
      fputc(data_type, fp);
      fputc(enc_type, fp);
      fputc(flags, fp);
      gen::write_uint32(max_len, fp);
      gen::write_uint32(ptr_lookup_tbl_loc, fp);
      gen::write_uint32(grp_data_loc, fp);
      gen::write_uint32(idx2_ptr_count, fp);
      gen::write_uint32(idx2_ptrs_map_loc, fp);
      gen::write_uint32(grp_ptrs_loc, fp);

      if (enc_type == 't') {
        write_ptrs(fp);
      } else {
        write_ptr_lookup_tbl(fp);
        write_ptrs(fp);
        byte_vec *idx2_ptrs_map = get_idx2_ptrs_map();
        fwrite(idx2_ptrs_map->data(), 1, idx2_ptrs_map->size(), fp);
        write_grp_data(grp_data_loc + 514, is_tail, fp); // group count, 512 lookup tbl, tail locs, tails
      }
      gen::gen_printf("Data size: %u, Ptrs size: %u, LkupTbl size: %u\nIdxMap size: %u, Total size: %u\n",
        get_data_size(), get_ptrs_size(), ptr_lookup_tbl_sz, get_idx2_ptrs_map()->size(), get_total_size());
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
        } else if (inner_trie_start_grp > 0 && i >= inner_trie_start_grp && !is_val) {
          if (fg->count < fg->grp_limit)
            fg->grp_log2 = ceil(log2(fg->count));
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

class tail_val_maps {
  private:
    builder_fwd *bldr;
    gen::byte_blocks& uniq_tails;
    leopard::uniq_info_vec& uniq_tails_rev;
    //leopard::uniq_info_vec uniq_tails_fwd;
    ptr_groups ptr_grps;
    gen::byte_blocks& uniq_vals;
    leopard::uniq_info_vec& uniq_vals_fwd;
    int start_nid, end_nid;
  public:
    tail_val_maps(builder_fwd *_bldr, gen::byte_blocks& _uniq_tails, leopard::uniq_info_vec& _uniq_tails_rev, gen::byte_blocks& _uniq_vals, leopard::uniq_info_vec& _uniq_vals_fwd)
        : bldr (_bldr), uniq_tails (_uniq_tails), uniq_tails_rev (_uniq_tails_rev), uniq_vals (_uniq_vals), uniq_vals_fwd (_uniq_vals_fwd) {
      ptr_grps.init(bldr, _bldr->opts.step_bits_idx, _bldr->opts.step_bits_rest);
    }
    ~tail_val_maps() {
      for (size_t i = 0; i < uniq_tails_rev.size(); i++)
        delete uniq_tails_rev[i];
      for (size_t i = 0; i < uniq_vals_fwd.size(); i++)
        delete uniq_vals_fwd[i];
    }
    struct sort_data {
      uint8_t *data;
      uint32_t len;
      uint32_t freq;
      uint32_t ns_id;
      uint8_t node_idx;
    };

    uint8_t *get_tail(gen::byte_blocks& all_tails, leopard::node n, uint32_t& len) {
      uint8_t *v = all_tails[n.get_tail()];
      size_t vlen;
      len = gen::read_vint32(v, &vlen);
      v += vlen;
      return v;
    }

    const double idx_cost_frac_cutoff = 0.1;
    uint32_t make_uniq_freq(leopard::uniq_info_vec& uniq_arr_vec, leopard::uniq_info_vec& uniq_freq_vec, uint32_t tot_freq_count, uint32_t& last_data_len, uint8_t& start_bits, uint8_t& grp_no) {
      clock_t t = clock();
      uniq_freq_vec = uniq_arr_vec;
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.end(), [](const struct leopard::uniq_info *lhs, const struct leopard::uniq_info *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });

      uint32_t sum_freq = 0;
      if (start_bits < 7) {
        for (size_t i = 0; i < uniq_freq_vec.size(); i++) {
          leopard::uniq_info *vi = uniq_freq_vec[i];
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

      grp_no = 1;
      sum_freq = 0;
      uint32_t freq_idx = 0;
      last_data_len = 0;
      uint32_t cutoff_bits = start_bits;
      uint32_t nxt_idx_limit = pow(2, cutoff_bits);
      for (size_t i = 0; i < uniq_freq_vec.size(); i++) {
        leopard::uniq_info *vi = uniq_freq_vec[i];
        if (last_data_len >= nxt_idx_limit) {
          double cost_frac = last_data_len + nxt_idx_limit * 3;
          cost_frac /= (sum_freq * cutoff_bits / 8);
          if (cost_frac > idx_cost_frac_cutoff)
            break;
          grp_no++;
          sum_freq = 0;
          freq_idx = 0;
          last_data_len = 0;
          cutoff_bits += bldr->opts.step_bits_idx;
          nxt_idx_limit = pow(2, cutoff_bits);
        }
        last_data_len += vi->len;
        last_data_len++;
        sum_freq += vi->freq_count;
        freq_idx++;
      }

      if (grp_no >= bldr->opts.max_groups) {
        cutoff_bits = start_bits;
      }

      grp_no = 0;
      last_data_len = 0;
      uint32_t cumu_freq_idx = 0;
      if (cutoff_bits > start_bits) {
        grp_no = 1;
        freq_idx = 0;
        last_data_len = 0;
        uint32_t next_bits = start_bits;
        nxt_idx_limit = pow(2, next_bits);
        for (cumu_freq_idx = 0; cumu_freq_idx < uniq_freq_vec.size(); cumu_freq_idx++) {
          leopard::uniq_info *vi = uniq_freq_vec[cumu_freq_idx];
          if (freq_idx == nxt_idx_limit) {
            next_bits += bldr->opts.step_bits_idx;
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

      // grp_no = 0;
      // uint32_t cumu_freq_idx = 0;
      //printf("%.1f\t%d\t%u\t%u\n", ceil(log2(freq_idx)), freq_idx, ftot, tail_len_tot);
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.begin() + cumu_freq_idx, [](const struct leopard::uniq_info *lhs, const struct leopard::uniq_info *rhs) -> bool {
        return (lhs->grp_no == rhs->grp_no) ? (lhs->arr_idx > rhs->arr_idx) : (lhs->grp_no < rhs->grp_no);
      });
      std::sort(uniq_freq_vec.begin() + cumu_freq_idx, uniq_freq_vec.end(), [](const struct leopard::uniq_info *lhs, const struct leopard::uniq_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / (lhs->len == 0 ? 1 : lhs->len);
        uint32_t rhs_freq = rhs->freq_count / (rhs->len == 0 ? 1 : rhs->len);
        lhs_freq = ceil(log10(lhs_freq));
        rhs_freq = ceil(log10(rhs_freq));
        return (lhs_freq == rhs_freq) ? (lhs->arr_idx > rhs->arr_idx) : (lhs_freq > rhs_freq);
      });
      t = gen::print_time_taken(t, "Time taken for uniq_freq: ");
      return cumu_freq_idx;

    }

    void check_remaining_text(leopard::uniq_info_vec& uniq_freq_vec, gen::byte_blocks& uniq_data, bool is_tail) {

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
        leopard::uniq_info *ti = uniq_freq_vec[freq_idx];
        freq_idx++;
        if ((ti->flags & MDX_SUFFIX_FULL) || (ti->flags & MDX_PREFIX_FULL))
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
    void build_tail_maps(byte_ptr_vec& all_node_sets, uint32_t tot_freq_count, uint32_t _max_len) {

      clock_t t = clock();

      leopard::uniq_info_vec uniq_tails_freq;
      uint8_t grp_no;
      uint32_t last_data_len;
      uint8_t start_bits = 7;
      uint32_t cumu_freq_idx = make_uniq_freq((leopard::uniq_info_vec&) uniq_tails_rev, (leopard::uniq_info_vec&) uniq_tails_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.set_idx_info(start_bits, grp_no, 3); //last_data_len > 65535 ? 3 : 2);
      ptr_grps.set_max_len(_max_len);

      uint32_t freq_idx = cumu_freq_idx;
      while (freq_idx < uniq_tails_freq.size()) {
        leopard::uniq_info *ti = uniq_tails_freq[freq_idx];
        last_data_len += ti->len;
        last_data_len++;
        freq_idx++;
      }

      freq_idx = 0;
      bool is_bin = false;
      if (uniq_tails_rev[0]->flags & LPDU_BIN)
        is_bin = true;
      if (!is_bin) {
        leopard::uniq_info *prev_ti = uniq_tails_freq[freq_idx];
        while (freq_idx < uniq_tails_freq.size()) {
          leopard::uniq_info *ti = uniq_tails_freq[freq_idx];
          freq_idx++;
          int cmp_ret = gen::compare_rev(uniq_tails[prev_ti->pos], prev_ti->len, uniq_tails[ti->pos], ti->len);
          if (cmp_ret == 0)
            continue;
          uint32_t cmp = abs(cmp_ret);
          cmp--;
          bool partial_suffix = bldr->opts.partial_sfx_coding;
          if (cmp < bldr->opts.sfx_min_tail_len)
            partial_suffix = false;
          if (!bldr->opts.idx_partial_sfx_coding && freq_idx < cumu_freq_idx)
            partial_suffix = false;
          if (cmp == ti->len || partial_suffix) {
            ti->flags |= (cmp == ti->len ? MDX_SUFFIX_FULL : MDX_SUFFIX_PARTIAL);
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
            prev_ti->flags |= MDX_HAS_SUFFIX;
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
      uint32_t sfx_set_max = bldr->opts.sfx_set_max_dflt;
      uint32_t sfx_set_count = 0;
      uint32_t sfx_set_tot_cnt = 0;
      uint32_t sfx_set_tot_len = 0;
      while (freq_idx < uniq_tails_freq.size()) {
        leopard::uniq_info *ti = uniq_tails_freq[freq_idx];
        last_data_len -= ti->len;
        last_data_len--;
        uint32_t it_nxt_limit = ptr_grps.check_next_grp(grp_no, cur_limit, ti->len);
        if (bldr->opts.inner_tries && it_nxt_limit != cur_limit && 
              it_nxt_limit >= inner_trie_min_size && last_data_len >= inner_trie_min_size * 2) {
          break;
        }
        freq_idx++;
        if (is_bin) {
          uint32_t bin_len = ti->len;
          uint32_t len_len = ptr_grps.get_set_len_len(bin_len);
          bin_len += len_len;
          uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, bin_len, tot_freq_count);
          ti->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_tails[ti->pos], ti->len);
          ti->grp_no = grp_no;
          ptr_grps.update_current_grp(grp_no, bin_len, ti->freq_count);
          cur_limit = new_limit;
          continue;
        } else if (ti->flags & MDX_SUFFIX_FULL) {
          savings_full += ti->len;
          savings_full++;
          savings_count_full++;
          leopard::uniq_info *link_ti = uniq_tails_rev[ti->link_arr_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, link_ti->len + 1, tot_freq_count);
            link_ti->grp_no = grp_no;
            ptr_grps.update_current_grp(link_ti->grp_no, link_ti->len + 1, link_ti->freq_count);
            link_ti->ptr = ptr_grps.append_text_tail(grp_no, uniq_tails[link_ti->pos], link_ti->len, true);
          }
          //cur_limit = ptr_grps.next_grp(grp_no, cur_limit, 0);
          ptr_grps.update_current_grp(link_ti->grp_no, 0, ti->freq_count);
          ti->ptr = link_ti->ptr + link_ti->len - ti->len;
          ti->grp_no = link_ti->grp_no;
        } else {
          if (ti->flags & MDX_SUFFIX_PARTIAL) {
            uint32_t cmp = ti->cmp;
            uint32_t remain_len = ti->len - cmp;
            uint32_t len_len = ptr_grps.get_set_len_len(cmp);
            remain_len += len_len;
            uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, remain_len, tot_freq_count);
            if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
              ti->cmp_min = 0;
              if (sfx_set_len == 1)
                sfx_set_len += cmp;
              sfx_set_len += remain_len;
              sfx_set_count++;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              ptr_grps.update_current_grp(grp_no, remain_len, ti->freq_count);
              remain_len -= len_len;
              ti->ptr = ptr_grps.append_text_tail(grp_no, uniq_tails[ti->pos], remain_len);
              byte_vec& tail_data = ptr_grps.get_data(grp_no);
              ptr_grps.get_set_len_len(cmp, &tail_data);
            } else {
              // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
              sfx_set_len = 1;
              sfx_set_tot_len += ti->len;
              sfx_set_count = 1;
              sfx_set_tot_cnt++;
              sfx_set_max = bldr->opts.sfx_set_max_dflt;
              if (ti->len > sfx_set_max)
                sfx_set_max = ti->len * 2;
              ptr_grps.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
              ti->ptr = ptr_grps.append_text_tail(grp_no, uniq_tails[ti->pos], ti->len, true);
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_tails[ti->tail_pos]);
            cur_limit = new_limit;
          } else {
            // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
            sfx_set_len = 1;
            sfx_set_tot_len += ti->len;
            sfx_set_count = 1;
            sfx_set_tot_cnt++;
            sfx_set_max = bldr->opts.sfx_set_max_dflt;
            if (ti->len > sfx_set_max)
              sfx_set_max = ti->len * 2;
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, ti->len + 1, tot_freq_count);
            ptr_grps.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
            ti->ptr = ptr_grps.append_text_tail(grp_no, uniq_tails[ti->pos], ti->len, true);
          }
          ti->grp_no = grp_no;
        }
      }
      gen::gen_printf("Tail Savings full: %u, %u\nSavings Partial: %u, %u / Sfx set: %u, %u\n", savings_full, savings_count_full, savings_partial, savings_count_partial, sfx_set_tot_len, sfx_set_tot_cnt);

      if (bldr->opts.inner_tries && freq_idx < uniq_tails_freq.size()) {
        builder_fwd *inner_trie = bldr->new_instance();
        cur_limit = ptr_grps.next_grp(grp_no, cur_limit, uniq_tails_freq[freq_idx]->len, tot_freq_count, true);
        ptr_grps.inner_trie_start_grp = grp_no;
        uint32_t trie_entry_idx = 0;
        ptr_grps.inner_tries.push_back(inner_trie);
        while (freq_idx < uniq_tails_freq.size()) {
          leopard::uniq_info *ti = uniq_tails_freq[freq_idx];
          uint8_t rev[ti->len];
          uint8_t *ti_data = uniq_tails[ti->pos];
          for (uint32_t j = 0; j < ti->len; j++)
            rev[j] = ti_data[ti->len - j - 1];
          inner_trie->insert(rev, ti->len, nullptr, 0, freq_idx);
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
        uint32_t trie_size = inner_trie->build();
        leopard::node_iterator ni(inner_trie->get_memtrie()->all_node_sets, 0);
        leopard::node n = ni.next();
        //int leaf_id = 0;
        uint32_t node_id = 0;
        while (n != nullptr) {
          uint32_t col_val_pos = n.get_col_val();
          if (n.get_flags() & NFLAG_LEAF) {
            leopard::uniq_info *ti = uniq_tails_freq[col_val_pos];
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
        leopard::uniq_info *ti = uniq_tails_freq[freq_idx];
        ti->ptr = ptr_grps.append_ptr2_idx_map(ti->grp_no, ti->ptr);
      }

      // check_remaining_text(uniq_tails_freq, uniq_tails, true);

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

      ptr_grps.build_freq_codes();
      ptr_grps.show_freq_codes();

      gen::print_time_taken(t, "Time taken for build_tail_maps(): ");

    }

    #define pfx_set_max_dflt 128
    void build_text_val_maps(uint32_t tot_freq_count, uint32_t _max_len) {

      clock_t t = clock();

      leopard::uniq_info_vec uniq_vals_freq;
      uint8_t grp_no;
      uint32_t last_data_len;
      uint8_t start_bits = 1;
      uint32_t cumu_freq_idx = make_uniq_freq((leopard::uniq_info_vec&) uniq_vals_fwd, (leopard::uniq_info_vec&) uniq_vals_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.reset();
      ptr_grps.set_idx_info(start_bits, grp_no, last_data_len > 65535 ? 3 : 2);
      ptr_grps.set_max_len(_max_len);

      uint32_t freq_idx = 0;
      bool is_bin = false;
      if (uniq_vals_fwd[0]->flags & LPDU_BIN) {
        gen::gen_printf("Given content not text.\n");
        is_bin = true;
      }
      if (!is_bin) {
        leopard::uniq_info *prev_ti = uniq_vals_freq[freq_idx];
        while (freq_idx < uniq_vals_freq.size()) {
          leopard::uniq_info *ti = uniq_vals_freq[freq_idx];
          freq_idx++;
          if (ti->flags & LPDU_NULL || ti->flags & LPDU_EMPTY) {
            if (freq_idx < uniq_vals_freq.size())
              prev_ti = uniq_vals_freq[freq_idx];
            continue;
          }
          int cmp_ret = gen::compare(uniq_vals[prev_ti->pos], prev_ti->len, uniq_vals[ti->pos], ti->len);
          if (cmp_ret == 0)
            continue;
          uint32_t cmp = abs(cmp_ret);
          cmp--;
          if (cmp == ti->len || cmp > 1) { // (freq_idx >= cumu_freq_idx && cmp > 1)) {
            ti->flags |= (cmp == ti->len ? MDX_PREFIX_FULL : MDX_PREFIX_PARTIAL);
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
            prev_ti->flags |= MDX_HAS_PREFIX;
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
      uint32_t pfx_set_len = 0;
      uint32_t pfx_set_max = pfx_set_max_dflt;
      uint32_t pfx_set_count = 0;
      uint32_t pfx_set_tot_cnt = 0;
      uint32_t pfx_set_tot_len = 0;
      while (freq_idx < uniq_vals_freq.size()) {
        leopard::uniq_info *ti = uniq_vals_freq[freq_idx];
        freq_idx++;
        byte_vec& val_data = ptr_grps.get_data(grp_no);
        if (is_bin) {
          uint32_t bin_len = ti->len;
          uint32_t len_len = ptr_grps.get_set_len_len(bin_len);
          bin_len += len_len;
          uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, bin_len, tot_freq_count);
          ti->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_vals[ti->pos], ti->len);
          ti->grp_no = grp_no;
          ptr_grps.update_current_grp(grp_no, bin_len, ti->freq_count);
          cur_limit = new_limit;
          continue;
        } else if (ti->flags & MDX_PREFIX_FULL) {
          savings_full += ti->len;
          savings_full++;
          savings_count_full++;
          leopard::uniq_info *link_ti = uniq_vals_fwd[ti->link_arr_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, link_ti->len + 1, tot_freq_count);
            link_ti->grp_no = grp_no;
            ptr_grps.update_current_grp(link_ti->grp_no, link_ti->len + 1, link_ti->freq_count);
            link_ti->ptr = ptr_grps.append_text_val(grp_no, uniq_vals[link_ti->pos], link_ti->len, true);
          }
          //cur_limit = ptr_grps.next_grp(grp_no, cur_limit, 0);
          ptr_grps.update_current_grp(link_ti->grp_no, 0, ti->freq_count);
          ti->ptr = link_ti->ptr - link_ti->len + ti->len;
          ti->grp_no = link_ti->grp_no;
        } else {
          if (ti->flags & MDX_PREFIX_PARTIAL) {
            uint32_t cmp = ti->cmp;
            uint32_t remain_len = ti->len - cmp;
            uint32_t len_len = ptr_grps.get_set_len_len(cmp);
            remain_len += len_len;
            uint32_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, remain_len, tot_freq_count);
            if (pfx_set_len + remain_len <= pfx_set_max && cur_limit == new_limit) {
              ti->cmp_min = 0;
              // if (pfx_set_len == 1)
              //   pfx_set_len += cmp;
              pfx_set_len += remain_len;
              pfx_set_count++;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              ptr_grps.update_current_grp(grp_no, remain_len, ti->freq_count);
              remain_len -= len_len;
              ti->ptr = ptr_grps.append_text_val(grp_no, uniq_vals[ti->pos + cmp], remain_len) + 1;
              ptr_grps.get_set_len_len(cmp, &val_data);
            } else {
              // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
              pfx_set_len = 1;
              pfx_set_tot_len += ti->len;
              pfx_set_count = 1;
              pfx_set_tot_cnt++;
              pfx_set_max = pfx_set_max_dflt;
              if (ti->len > pfx_set_max)
                pfx_set_max = ti->len * 2;
              ptr_grps.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
              ti->ptr = ptr_grps.append_text_val(grp_no, uniq_vals[ti->pos], ti->len, true);
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp, ti->cmp_min, ti->val_ptr, remain_len, ti->val_len, ti->val_len, uniq_vals[ti->val_pos]);
            cur_limit = new_limit;
          } else {
            // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, pfx_set_count, pfx_set_freq, pfx_set_len);
            pfx_set_len = 1;
            pfx_set_tot_len += ti->len;
            pfx_set_count = 1;
            pfx_set_tot_cnt++;
            pfx_set_max = pfx_set_max_dflt;
            if (ti->len > pfx_set_max)
              pfx_set_max = ti->len * 2;
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, ti->len + 1, tot_freq_count);
            ptr_grps.update_current_grp(grp_no, ti->flags & LPDU_NULL || ti->flags & LPDU_EMPTY ? 0 : ti->len + 1, ti->freq_count);
            if (ti->flags & LPDU_NULL)
              ti->ptr = 0;
            else if (ti->flags & LPDU_EMPTY)
              ti->ptr = 1;
            else
              ti->ptr = ptr_grps.append_text_val(grp_no, uniq_vals[ti->pos], ti->len, true);
          }
          ti->grp_no = grp_no;
        }
      }
      gen::gen_printf("Val Savings full: %u, %u\nSavings Partial: %u, %u / Pfx set: %u, %u\n", savings_full, savings_count_full, savings_partial, savings_count_partial, pfx_set_tot_len, pfx_set_tot_cnt);

      for (freq_idx = 0; freq_idx < cumu_freq_idx; freq_idx++) {
        leopard::uniq_info *ti = uniq_vals_freq[freq_idx];
        ti->ptr = ptr_grps.append_ptr2_idx_map(ti->grp_no, ti->ptr);
      }

      check_remaining_text(uniq_vals_freq, uniq_vals, false);

      // clock_t tt = clock();

      // int cmpr_blk_size = 262144;
      // size_t total_size = 0;
      // size_t tot_cmpr_size = 0;
      // uint8_t *cmpr_buf = (uint8_t *) malloc(cmpr_blk_size * 1.2);
      // for (int g = 1; g <= grp_no; g++) {
      //   byte_vec& gd = ptr_grps.get_data(g);
      //   // if (gd.size() > cmpr_blk_size) {
      //     int cmpr_blk_count = gd.size() / cmpr_blk_size + 1;
      //     for (int b = 0; b < cmpr_blk_count; b++) {
      //       size_t input_size = (b == cmpr_blk_count - 1 ? gd.size() % cmpr_blk_size : cmpr_blk_size);
      //       size_t cmpr_size = gen::compress_block(CMPR_TYPE_ZSTD, gd.data() + (b * cmpr_blk_size), input_size, cmpr_buf);
      //       total_size += input_size;
      //       tot_cmpr_size += cmpr_size;
      //       printf("Grp_no: %d, grp_size: %lu, blk_count: %d, In size: %lu, cmpr size: %lu\n", g, gd.size(), cmpr_blk_count, input_size, cmpr_size);
      //     }
      //   // } else {
      //   //   total_size += gd.size();
      //   //   tot_cmpr_size += gd.size();
      //   // }
      // }
      // printf("Total Input size: %lu, Total cmpr size: %lu\n", total_size, tot_cmpr_size);
      // gen::print_time_taken(tt, "Time taken for compress (): ");

      ptr_grps.build_freq_codes(true);
      ptr_grps.show_freq_codes();
      gen::print_time_taken(t, "Time taken for build_val_maps(): ");

    }

    void build_val_maps(uint32_t tot_freq_count, uint32_t _max_len, char data_type) {
      clock_t t = clock();
      uint32_t last_data_len;
      uint8_t start_bits = 1;
      uint8_t grp_no;
      leopard::uniq_info_vec uniq_vals_freq;
      uint32_t cumu_freq_idx = make_uniq_freq(uniq_vals_fwd, uniq_vals_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.reset();
      ptr_grps.set_idx_info(start_bits, grp_no, last_data_len > 65535 ? 3 : 2);
      ptr_grps.set_max_len(_max_len);
      uint32_t freq_idx = 0;
      uint32_t cur_limit = pow(2, start_bits);
      grp_no = 1;
      // gen::word_matcher wm(uniq_vals);
      ptr_grps.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      ptr_grps.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      while (freq_idx < uniq_vals_freq.size()) {
        leopard::uniq_info *vi = uniq_vals_freq[freq_idx];
        freq_idx++;
        uint8_t len_of_len = 0;
        if (data_type == LPDT_TEXT || data_type == LPDT_BIN)
          len_of_len = gen::get_vlen_of_uint32(vi->len);
        uint32_t len_plus_len = vi->len + len_of_len;
        cur_limit = ptr_grps.next_grp(grp_no, cur_limit, len_plus_len, tot_freq_count);
        vi->grp_no = grp_no;
        if (vi->flags & LPDU_NULL) {
          vi->ptr = 0;
          len_plus_len = 0;
        } else if (vi->flags & LPDU_EMPTY) {
          vi->ptr = 1;
          len_plus_len = 0;
        } else {
          vi->ptr = ptr_grps.append_bin_to_grp_data(grp_no, uniq_vals[vi->pos], vi->len, data_type);
          // if (data_type == 't')
          //   wm.add_all_combis(vi->pos, vi->len, vi->arr_idx);
        }
        ptr_grps.update_current_grp(grp_no, len_plus_len, vi->freq_count);
      }
      // wm.process_combis();
      for (freq_idx = 0; freq_idx < cumu_freq_idx; freq_idx++) {
        leopard::uniq_info *vi = uniq_vals_freq[freq_idx];
        vi->ptr = ptr_grps.append_ptr2_idx_map(vi->grp_no, vi->ptr);
      }
      ptr_grps.build_freq_codes(true);
      ptr_grps.show_freq_codes();
      t = gen::print_time_taken(t, "Time taken for build_val_maps(): ");
    }

    void write_tail_ptrs_data(byte_ptr_vec& all_node_sets, FILE *fp) {
      ptr_grps.write_ptrs_data(LPDT_BIN, 1, true, fp);
    }

    void write_val_ptrs_data(byte_ptr_vec& all_node_sets, char data_type, char encoding_type, uint8_t flags, FILE *fp) {
      ptr_grps.write_ptrs_data(data_type, flags, false, fp);
    }

    uint32_t get_tail_ptr(uint32_t grp_no, leopard::uniq_info *ti) {
      uint32_t ptr = ti->ptr;
      if (grp_no <= ptr_grps.get_idx_limit()) {
        byte_vec& idx2_ptr_map = *(ptr_grps.get_idx2_ptrs_map());
        uint32_t pos = ptr_grps.idx_map_arr[grp_no - 1] + ptr * ptr_grps.get_idx_ptr_size();
        ptr = ptr_grps.get_idx_ptr_size() == 2 ? gen::read_uint16(idx2_ptr_map, pos) : gen::read_uint24(idx2_ptr_map, pos);
      }
      return ptr;
    }

    uint32_t read_len(byte_vec& tail, uint32_t ptr, uint8_t& len_len) {
      len_len = 1;
      if (tail[ptr] < 15)
        return tail[ptr];
      uint32_t ret = 0;
      while (tail[++ptr] & 0x80) {
        ret <<= 7;
        ret |= (tail[ptr] & 0x7F);
        len_len++;
      }
      len_len++;
      ret <<= 4;
      ret |= (tail[ptr] & 0x0F);
      return ret + 15;
    }

    uint8_t get_first_byte(gen::byte_blocks& all_tails, leopard::node *n) {
      if (uniq_tails_rev.size() == 0) {
        uint32_t tail_len;
        uint8_t *tail = get_tail(all_tails, *n, tail_len);
        return *tail;
      }
      if ((n->get_flags() & NFLAG_TAIL) == 0)
        return n->get_byte();
      leopard::uniq_info *ti = uniq_tails_rev[n->get_tail()];
      uint32_t grp_no = ti->grp_no;
      uint32_t ptr = get_tail_ptr(grp_no, ti);
      byte_vec& tail = ptr_grps.get_data(grp_no);
      return *(tail.data() + ptr);
    }

    std::string get_tail_str(gen::byte_blocks& all_tails, leopard::node *n) {
      if (uniq_tails_rev.size() == 0) {
        uint32_t tail_len;
        uint8_t *v = get_tail(all_tails, *n, tail_len);
        return std::string((const char *) v, tail_len);
      }
      leopard::uniq_info *ti = uniq_tails_rev[n->get_tail()];
      uint32_t grp_no = ti->grp_no;
      uint32_t tail_ptr = get_tail_ptr(grp_no, ti);
      uint32_t ptr = tail_ptr;
      byte_vec& tail = ptr_grps.get_data(grp_no);
      std::string ret;
      int byt = tail[ptr++];
      while (byt > 31) {
        ret.append(1, byt);
        byt = tail[ptr++];
      }
      if (tail[--ptr] == 0)
        return ret;
      uint8_t len_len = 0;
      uint32_t sfx_len = read_len(tail, ptr, len_len);
      uint32_t ptr_end = tail_ptr;
      ptr = tail_ptr;
      do {
        byt = tail[ptr--];
      } while (byt != 0);
      do {
        byt = tail[ptr--];
      } while (byt > 31);
      ptr++;
      std::string prev_str;
      byt = tail[++ptr];
      while (byt != 0) {
        prev_str.append(1, byt);
        byt = tail[++ptr];
      }
      std::string last_str;
      while (ptr < ptr_end) {
        byt = tail[++ptr];
        while (byt > 31) {
          last_str.append(1, byt);
          byt = tail[++ptr];
        }
        uint32_t prev_sfx_len = read_len(tail, ptr, len_len);
        last_str.append(prev_str.substr(prev_str.length()-prev_sfx_len));
        ptr += len_len;
        ptr--;
        prev_str = last_str;
        last_str.clear();
      }
      ret.append(prev_str.substr(prev_str.length()-sfx_len));
      return ret;
    }

    leopard::uniq_info_vec *get_uniq_tails_rev() {
      return &uniq_tails_rev;
    }
    gen::byte_blocks *get_uniq_tails() {
      return &uniq_tails;
    }
    gen::byte_blocks *get_uniq_vals() {
      return &uniq_vals;
    }
    leopard::uniq_info_vec *get_uniq_vals_fwd() {
      return &uniq_vals_fwd;
    }
    ptr_groups *get_tail_grp_ptrs() {
      return &ptr_grps;
    }
    ptr_groups *get_val_grp_ptrs() {
      return &ptr_grps;
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
      if (opts.trie_leaf_count > 0)
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
    uint32_t end_loc;
    tail_val_maps tail_vals;
    uint32_t column_count;
    char *names;
    char *column_encoding;
    char *column_types;
    uint16_t *names_positions;
    uint16_t names_len;
    uint32_t *val_table;
    builder *col_trie_builder;
    builder *tail_trie_builder;
    trie_parts tp;
    builder(const char *out_file = NULL, const char *_names = "kv_tbl,key,value", const int _column_count = 2,
        const char *_column_types = "tt", const char *_column_encoding = "uu", int _trie_level = 0,
        bool _maintain_seq = true, bool _no_primary_trie = false,
        bldr_options _opts = dflt_opts)
        : memtrie(_column_count, _column_types, _column_encoding, _maintain_seq, _no_primary_trie, _opts.sort_nodes_on_freq),
          tail_vals (this, memtrie.uniq_tails, memtrie.uniq_tails_rev, memtrie.uniq_vals, memtrie.uniq_vals_fwd),
          builder_fwd (_opts, _maintain_seq, _no_primary_trie) {
      trie_level = _trie_level;
      tp = {};
      col_trie_builder = NULL;
      tail_trie_builder = NULL;
      column_count = _column_count;
      val_table = new uint32_t[_column_count];
      column_encoding = new char[_column_count];
      memset(column_encoding, 'u', _column_count);
      *column_encoding = 't'; // first letter is for key
      memcpy(column_encoding, _column_encoding, gen::min(strlen(_column_encoding), _column_count));
      column_types = new char[_column_count];
      memset(column_types, '*', _column_count);
      memcpy(column_types, _column_types, gen::min(strlen(_column_types), _column_count));
      set_names(_names, _column_types, column_encoding);
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
    }

    virtual ~builder() {
      delete [] names;
      delete [] val_table;
      delete [] names_positions;
      delete [] column_encoding;
      delete [] column_types;
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

    void set_names(const char *_names, const char *_column_types, const char *_column_encoding) {
      names_len = strlen(_names) + column_count * 2 + 3;
      names = new char[names_len];
      memset(names, '*', column_count);
      memcpy(names, _column_types, gen::min(strlen(_column_types), column_count));
      names[column_count] = ',';
      memcpy(names + column_count + 1, _column_encoding, column_count);
      names[column_count * 2 + 1] = ',';
      memcpy(names + column_count * 2 + 2, _names, strlen(_names));
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

    size_t size() {
      return memtrie.key_count;
    }

    uint8_t append_tail_ptr(leopard::node *cur_node) {
      if ((cur_node->get_flags() & NFLAG_TAIL) == 0)
        return cur_node->get_byte();
      if (opts.tail_tries) {
        leopard::uniq_info *ti = (*tail_vals.get_uniq_tails_rev())[cur_node->get_tail()];
        return ti->ptr & 0xFF;
      }
      uint8_t node_val;
      uint32_t ptr = 0;
      ptr_groups *ptr_grps = tail_vals.get_tail_grp_ptrs();
        leopard::uniq_info *ti = (*tail_vals.get_uniq_tails_rev())[cur_node->get_tail()];
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
      ptr_groups *ptr_grps = tail_vals.get_tail_grp_ptrs();
      leopard::uniq_info *ti = (*tail_vals.get_uniq_tails_rev())[cur_node->get_tail()];
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

    void make_delta_values() {
      gen::byte_blocks *delta_vals = new gen::byte_blocks();
      delta_vals->push_back("\0", 2);
      int64_t prev_val = 0;
      uint32_t node_id = 0;
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NODE_SET_LEAP) {
          cur_node = ni.next();
          continue;
        }
        node_id = ni.get_node_id();
        if ((node_id % nodes_per_bv_block_n) == 0)
          prev_val = 0;
        if (cur_node.get_flags() & NFLAG_LEAF) {
          uint32_t val_pos = cur_node.get_col_val();
          int64_t col_val = gen::read_svint60((*memtrie.all_vals)[val_pos]);
          int64_t delta_val = col_val;
          // std::cout << node_id << ", " << val_pos << ", ";
          if (node_id % nodes_per_bv_block_n)
            delta_val -= prev_val;
          prev_val = col_val;
          // std::cout << col_val << ": " << delta_val << std::endl;
          val_pos = delta_vals->append_svint60(delta_val);
          cur_node.set_col_val(val_pos);
        }
        node_id++;
        if ((node_id % nodes_per_bv_block_n) == 0)
          prev_val = 0;
        cur_node = ni.next();
      }
      std::cout << "All vals len: " << delta_vals->size() << std::endl;
      delete memtrie.all_vals;
      memtrie.all_vals = delta_vals;
    }

    uint32_t build_col_trie() {
      uint32_t col_trie_size = col_trie_builder->build();
      std::vector<leopard::val_sequence>& col_trie_val_seq = col_trie_builder->memtrie.val_seq;
      uint32_t max_node_id = 0;
      for (size_t seq_idx = 0; seq_idx < col_trie_val_seq.size(); seq_idx++) {
        leopard::val_sequence& val_seq_obj = col_trie_val_seq[seq_idx];
        leopard::node_set_handler col_trie_ns(col_trie_builder->memtrie.all_node_sets, val_seq_obj.node_set_id);
        leopard::node_set_header *col_trie_ns_hdr = col_trie_ns.get_ns_hdr();
        int64_t col_trie_node_id = col_trie_ns_hdr->node_id + val_seq_obj.node_idx;
        if (col_trie_ns_hdr->flags & NODE_SET_LEAP)
          col_trie_node_id++;
        if (max_node_id < col_trie_node_id)
          max_node_id = col_trie_node_id;
        leopard::val_sequence& this_val_seq = memtrie.val_seq[seq_idx];
        leopard::node_set_handler cur_ns(memtrie.all_node_sets, this_val_seq.node_set_id);
        leopard::node cur_node = cur_ns[this_val_seq.node_idx];
        cur_node.set_col_val(col_trie_node_id);
      }
      byte_vec *ptr_grps = tail_vals.get_val_grp_ptrs()->get_ptrs();
      int bit_len = ceil(log2(max_node_id + 1));
      tail_vals.get_val_grp_ptrs()->set_ptr_lkup_tbl_ptr_width(bit_len);
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

    typedef struct {
      uint32_t ptr_pos;
      uint32_t limit;
      uint32_t cmp;
    } ptr_seq;

    uint32_t build_words() {
      gen::byte_blocks *words = memtrie.wm.get_words();
      std::vector<uint32_t> *word_positions = memtrie.wm.get_word_positions();
      gen::combi_freq_vec word_freq_vec;
      gen::combi_freq_ptr_vec word_freq_ptr_vec;
      memtrie.wm.make_uniq_words(word_freq_vec, word_freq_ptr_vec);
      int words_grp_count = memtrie.wm.grp_count;
      builder *word_tries[words_grp_count];
      for (int i = 0; i < words_grp_count; i++) {
        word_tries[i] = new builder(NULL, "word_trie,key", 1, "t", "u", 0, false, false, word_tries_dflt_opts);
        word_tries[i]->fp = fp;
      }
      for (size_t i = 0; i < word_positions->size(); i++) {
        uint32_t wp = word_positions->at(i);
        uint8_t *word_info = (*words)[wp];
        uint32_t fp = gen::read_uint32(word_info);
        gen::combi_freq *cf = &word_freq_vec[fp];
        uint8_t *word = (*words)[cf->pos];
        word_tries[cf->grp]->insert(word, cf->len, nullptr, 0, fp);
      }
      std::vector<uint32_t> idx_sizes;
      uint32_t total_trie_size = 0;
      for (int i = 0; i < words_grp_count; i++) {
        uint32_t trie_size = word_tries[i]->build();
        idx_sizes.push_back(trie_size);
        total_trie_size += trie_size;
        uint32_t leaf_id = 0;
        leopard::node_iterator ni(word_tries[i]->memtrie.all_node_sets, 0);
        leopard::node n = ni.next();
        while (n != nullptr) {
          uint32_t col_val_pos = n.get_col_val();
          if (n.get_flags() & NFLAG_LEAF) {
            // printf("%u, %u\n", col_val_pos, leaf_id);
            gen::combi_freq *cf = &word_freq_vec[col_val_pos];
            cf->ptr = leaf_id++;
          }
          n = ni.next();
        }
      }
      // 11 - word ptrs ovint
      // 01 - NULL so many times
      // 10 - Empty so many times
      // 00 - Repeat previous so many times
      byte_vec ptrs;
      byte_vec line_ptrs;
      byte_vec ptr_lkup_tbl;
      std::vector<ptr_seq> ptr_pos;
      int line_no = 0;
      int rpt_count = 0;
      int tot_rpt_count = 0;
      size_t last_line_sz = 0;
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node n = ni.next();
      while (n != nullptr) {
        if (n.get_flags() & NFLAG_LEAF) {
          uint32_t col_val_pos = n.get_col_val();
          if (col_val_pos < 3) {
            rpt_count = 0;
            if ((ni.get_node_id() % nodes_per_bv_block_n) == 0)
              gen::append_uint32(ptrs.size(), ptr_lkup_tbl);
            ptrs.push_back(col_val_pos == 1 ? '\x40' : '\x80');
            n = ni.next();
            line_no++;
            continue;
          }
          col_val_pos -= 3;
          uint8_t *word_info;
          do {
            uint32_t wp = word_positions->at(col_val_pos);
            word_info = (*words)[wp];
            uint32_t fp = gen::read_uint32(word_info);
            gen::combi_freq *cf = &word_freq_vec[fp];
            gen::append_vint32(line_ptrs, cf->ptr, cf->grp + 1);
            col_val_pos++;
          } while (word_info[4] == 0);
          if ((ni.get_node_id() % nodes_per_bv_block_n) == 0)
            gen::append_uint32(ptrs.size(), ptr_lkup_tbl);
          else {
            if (last_line_sz == line_ptrs.size() && memcmp(line_ptrs.data(),
                  ptrs.data() + ptrs.size() - line_ptrs.size() - rpt_count,
                  line_ptrs.size()) == 0) {
              rpt_count++;
              tot_rpt_count++;
              ptrs.push_back('\0');
              line_ptrs.clear();
              n = ni.next();
              line_no++;
              continue;
            }
          }
          rpt_count = 0;
          last_line_sz = line_ptrs.size();
          gen::append_ovint32(ptrs, line_ptrs.size(), 2, '\xC0');
          ptrs.insert(ptrs.end(), line_ptrs.begin(), line_ptrs.end());
          line_ptrs.clear();
          line_no++;
        }
        n = ni.next();
      }
      printf("Rpt count: %d\n", tot_rpt_count);
      gen::append_uint32(ptrs.size(), ptr_lkup_tbl);
      uint32_t ptr_lkup_tbl_ptr_width = 4;
      fputc(ptr_lkup_tbl_ptr_width, fp);
      fputc(LPDT_WORDS, fp); // data type
      fputc(LPDT_WORDS, fp); // encoding type
      fputc(0, fp); // flags
      uint32_t hdr_size = 6 * 4 + 4;
      uint32_t ptr_lookup_tbl_sz = gen::get_lkup_tbl_size2(line_no, nodes_per_ptr_block_n, ptr_lkup_tbl_ptr_width);
      uint32_t ptr_lookup_tbl_loc = hdr_size;
      uint32_t grp_ptrs_loc = ptr_lookup_tbl_loc + ptr_lookup_tbl_sz;
      uint32_t idx2_ptr_count = 0;
      uint32_t idx2_ptrs_map_loc = grp_ptrs_loc + ptrs.size();
      uint32_t grp_data_loc = idx2_ptrs_map_loc + 0;
      uint32_t grp_data_size = 2 + words_grp_count * 4 + total_trie_size;
      gen::write_uint32(memtrie.max_val_len, fp);
      gen::write_uint32(ptr_lookup_tbl_loc, fp); // ptr_lookup_tbl_loc
      gen::write_uint32(grp_data_loc, fp); // grp_data_loc
      gen::write_uint32(idx2_ptr_count, fp); // idx2_ptr_count
      gen::write_uint32(idx2_ptrs_map_loc, fp); // idx2_ptrs_map_loc
      gen::write_uint32(grp_ptrs_loc, fp); // grp_ptrs_loc
      fwrite(ptr_lkup_tbl.data(), 1, ptr_lkup_tbl.size(), fp);
      fwrite(ptrs.data(), 1, ptrs.size(), fp);
      // fwrite(idx2_ptrs_map->data(), idx2_ptrs_map->size(), 1, fp);
      fputc(words_grp_count, fp);
      fputc(0, fp); // grp_log2 (?)
      total_trie_size = 0;
      for (int i = 0; i < words_grp_count; i++) {
        gen::write_uint32(2 + words_grp_count * 4 + total_trie_size, fp);
        total_trie_size += idx_sizes[i];
      }
      for (int i = 0; i < words_grp_count; i++) {
        word_tries[i]->write_trie();
        delete word_tries[i];
      }
      gen::gen_printf("Header size: %u\n", hdr_size);
      gen::gen_printf("Total trie size: %u\n", grp_data_size);
      gen::gen_printf("Ptrs size: %lu\n", ptrs.size());
      gen::gen_printf("Lookup table size: %u\n", ptr_lookup_tbl_sz);
      return grp_data_size + ptrs.size() + ptr_lookup_tbl_sz + hdr_size;
    }

    uint32_t store_col_val() {
      gen::word_matcher wm(*memtrie.all_vals);
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NODE_SET_LEAP) {
          cur_node = ni.next();
          continue;
        }
        // uint32_t col_val_pos = cur_node.get_col_val();
        // uint8_t *v = (*memtrie.all_vals)[col_val_pos];
        // int8_t vlen;
        // uint32_t len = gen::read_vint32(v, &vlen);
        // wm.add_words(col_val_pos + vlen, len, cur_ns_idx);
        cur_node = ni.next();
      }
      //wm.make_uniq_words();
      //wm.process_combis();
      return 0;
    }

    uint32_t build_and_write_col_val() {
      clock_t t = clock();
      char encoding_type = column_encoding[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1)];
      if (encoding_type == 's') {
        return store_col_val();
      }
      if (encoding_type == 'w') {
        uint32_t val_size = build_words();
        gen::gen_printf("Total val size: %u\n", val_size);
        if (memtrie.cur_col_idx > 0) {
          uint32_t prev_val_loc = val_table[memtrie.cur_col_idx - 1];
          val_table[memtrie.cur_col_idx] = prev_val_loc + memtrie.prev_val_size;
        }
        memtrie.prev_val_size = val_size;
        return val_size;
      }
      if (memtrie.cur_seq_idx > 0 || encoding_type == 't') {
        gen::gen_printf("\nCol: %s, ", names + names_positions[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1) + 2]);
        char data_type = column_types[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1)];
        gen::gen_printf("Type: %c, Enc: %c. ", data_type, encoding_type);
        if (encoding_type == 'd')
          make_delta_values();
        if (encoding_type == 't') {
          uint32_t col_trie_size = build_col_trie();
          ptr_groups *ptr_grps = tail_vals.get_val_grp_ptrs();
          ptr_grps->set_max_len(col_trie_builder->memtrie.max_key_len);
          ptr_grps->build(memtrie.node_count, memtrie.all_node_sets, ptr_groups::get_vals_info_fn, 
              memtrie.uniq_vals_fwd, false, no_primary_trie, opts.dessicate, encoding_type, col_trie_size);
        } else {
          leopard::val_sort_callbacks val_sort_cb(memtrie.all_node_sets, *memtrie.all_vals, memtrie.uniq_vals);
          uint32_t tot_freq_count = leopard::uniq_maker::make_uniq(memtrie.all_node_sets, *memtrie.all_vals,
            memtrie.uniq_vals, memtrie.uniq_vals_fwd, val_sort_cb, memtrie.max_val_len, 0, data_type);
          if (data_type == 't')
            tail_vals.build_text_val_maps(tot_freq_count, memtrie.max_val_len);
          else
            tail_vals.build_val_maps(tot_freq_count, memtrie.max_val_len, data_type);

          uint32_t rpt_count = 0;
          uint32_t prev1_val_pos, prev2_val_pos, prev3_val_pos;
          prev1_val_pos = prev2_val_pos = prev3_val_pos = UINT32_MAX;
          leopard::node_iterator ni(memtrie.all_node_sets, 0);
          leopard::node cur_node = ni.next();
          while (cur_node != nullptr) {
            if (cur_node.get_flags() & NODE_SET_LEAP) {
              cur_node = ni.next();
              continue;
            }
            leopard::uniq_info *ui = get_vi(&cur_node);
            if (ui->pos == prev1_val_pos) // && ui->pos == prev2_val_pos && ui->pos == prev3_val_pos)
              rpt_count++;
            prev3_val_pos = prev2_val_pos;
            prev2_val_pos = prev1_val_pos;
            prev1_val_pos = ui->pos;
            cur_node = ni.next();
          }
          printf("Rpt count: %u\n", rpt_count);

          ptr_groups *ptr_grps = tail_vals.get_val_grp_ptrs();
          ptr_grps->build(memtrie.node_count, memtrie.all_node_sets, ptr_groups::get_vals_info_fn, 
              memtrie.uniq_vals_fwd, false, no_primary_trie, opts.dessicate, encoding_type);
        }
      }
      uint32_t val_size = write_col_val();
      if (encoding_type == 't')
        col_trie_builder->write_trie(NULL);
      t = gen::print_time_taken(t, "Time taken for build_and_write_col_val: ");
      return val_size;
    }

    void reset_for_next_col() {
      memtrie.cur_col_idx++;
      char encoding_type = column_encoding[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1)];
      if (encoding_type == 't') {
        if (col_trie_builder != NULL)
          delete col_trie_builder;
        col_trie_builder = new builder(NULL, "col_trie,key", 1, "*", "*", 0, true, false, opts);
        col_trie_builder->fp = fp;
        return memtrie.reset_for_next_col(&col_trie_builder->memtrie);
      }
      return memtrie.reset_for_next_col();
    }

    bool insert_col_val(const void *val, const int val_len) {
      memtrie.insert_col_val(val, val_len);
      return true;
    }

    builder_fwd *new_instance() {
      bldr_options inner_trie_opts = inner_tries_dflt_opts;
      inner_trie_opts.trie_leaf_count = 0;
      inner_trie_opts.leaf_lt = false;
      inner_trie_opts.max_inner_tries = opts.max_inner_tries;
      if (opts.max_inner_tries <= trie_level + 1) {
        inner_trie_opts.inner_tries = false;
      }
      builder *ret = new builder(NULL, "inner_trie,key", 1, "*", "*", trie_level + 1, false, false, inner_trie_opts);
      ret->fp = fp;
      return ret;
    }

    uint32_t build_tail_trie(uint32_t tot_freq_count) {
      bldr_options tt_opts = tail_tries_dflt_opts;
      tt_opts.trie_leaf_count = 0;
      tt_opts.leaf_lt = false;
      tt_opts.tail_tries = true;
      tt_opts.max_inner_tries = opts.max_inner_tries;
      tail_trie_builder = new builder(NULL, "tail_trie,key", 1, "t", "u", trie_level + 1, false, false, tt_opts);
      for (size_t i = 0; i < memtrie.uniq_tails_rev.size(); i++) {
        leopard::uniq_info *ti = memtrie.uniq_tails_rev[i];
        uint8_t rev[ti->len];
        uint8_t *ti_data = memtrie.uniq_tails[ti->pos];
        for (uint32_t j = 0; j < ti->len; j++)
          rev[j] = ti_data[ti->len - j - 1];
        tail_trie_builder->insert(rev, ti->len, nullptr, 0, i);
      }
      uint32_t trie_size = tail_trie_builder->build();
      int bit_len = ceil(log2(tail_trie_builder->memtrie.node_count + 1)) - 8;
      tail_vals.get_tail_grp_ptrs()->set_ptr_lkup_tbl_ptr_width(bit_len);
      gen::gen_printf("Tail trie bit_len: %d [log(%u) - 8]\n", bit_len, tail_trie_builder->memtrie.node_count);
      uint32_t node_id = 0;
      leopard::node_iterator ni_tt(tail_trie_builder->memtrie.all_node_sets, 0);
      leopard::node cur_node = ni_tt.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_LEAF) {
          leopard::uniq_info *ti = memtrie.uniq_tails_rev[cur_node.get_col_val()];
          ti->ptr = node_id;
        }
        node_id++;
        cur_node = ni_tt.next();
      }
      byte_vec *tail_ptrs = tail_vals.get_tail_grp_ptrs()->get_ptrs();
      gen::int_bit_vector int_bv(tail_ptrs, bit_len, memtrie.uniq_tails_rev.size());
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_TAIL) {
          leopard::uniq_info *ti = memtrie.uniq_tails_rev[cur_node.get_tail()];
          int_bv.append(ti->ptr >> 8);
        }
        cur_node = ni.next();
      }
      return trie_size;
    }

    uint32_t build_trie() {
      clock_t t = clock();
      leopard::tail_sort_callbacks tail_sort_cb(memtrie.all_node_sets, *memtrie.all_tails, memtrie.uniq_tails);
      uint32_t tot_freq_count = leopard::uniq_maker::make_uniq(memtrie.all_node_sets, *memtrie.all_tails,
          memtrie.uniq_tails, memtrie.uniq_tails_rev, tail_sort_cb, memtrie.max_tail_len, trie_level, LPDT_BIN);
      uint32_t tail_trie_size = 0;
      if (memtrie.uniq_tails_rev.size() > 0) {
        if (opts.tail_tries && opts.max_inner_tries >= trie_level + 1 && memtrie.uniq_tails_rev.size() > 255) {
          tail_trie_size = build_tail_trie(tot_freq_count);
        } else {
          opts.tail_tries = false;
          tail_vals.build_tail_maps(memtrie.all_node_sets, tot_freq_count, memtrie.max_tail_len);
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
          leopard::uniq_info_vec *uniq_tails_rev = tail_vals.get_uniq_tails_rev();
          leopard::uniq_info *ti = (*uniq_tails_rev)[cur_node.get_tail()];
          if (ti->flags & MDX_SUFFIX_FULL)
            sfx_full_count++;
          if (ti->flags & MDX_SUFFIX_PARTIAL)
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
      tail_vals.get_tail_grp_ptrs()->build(node_count, memtrie.all_node_sets, ptr_groups::get_tails_info_fn, 
              memtrie.uniq_tails_rev, true, no_primary_trie, opts.dessicate, tail_trie_size == 0 ? 'u' : 't', tail_trie_size);
      uint32_t tail_size = tail_vals.get_tail_grp_ptrs()->get_total_size();
      end_loc = (8 + tail_size + trie.size()); // + trie_flags.size());
      gen::print_time_taken(t, "Time taken for build_trie(): ");
      return end_loc;
    }
    uint32_t write_trie_tail_ptrs_data(FILE *fp) {
      uint32_t tail_size = tail_vals.get_tail_grp_ptrs()->get_total_size();
      gen::gen_printf("\nTrie size: %u, Tail size: %u\n", trie.size(), tail_size);
      gen::write_uint32(tail_size, fp);
      gen::write_uint32(trie_flags.size(), fp);
      gen::gen_printf("Tail stats - ");
      tail_vals.write_tail_ptrs_data(memtrie.all_node_sets, fp);
      if (opts.tail_tries && tail_trie_builder != nullptr) {
        tail_trie_builder->fp = fp;
        tail_trie_builder->write_trie(NULL);
      }
      //fwrite(trie_flags.data(), 1, trie_flags.size(), fp);
      fwrite(trie.data(), 1, trie.size(), fp);
      return 8 + tail_size + trie.size(); // + trie_flags.size();
    }
    uint32_t write_val_ptrs_data(char data_type, char encoding_type, uint8_t flags, FILE *fp_val) {
      uint32_t val_fp_offset = 0;
      if (get_uniq_val_count() > 0 || encoding_type == 't') {
        gen::gen_printf("Stats - ");
        tail_vals.write_val_ptrs_data(memtrie.all_node_sets, data_type, encoding_type, flags, fp_val);
        val_fp_offset += tail_vals.get_val_grp_ptrs()->get_total_size();
      }
      return val_fp_offset;
    }
    size_t trie_data_ptr_size() {
      size_t ret = 8 + trie.size() + tail_vals.get_tail_grp_ptrs()->get_total_size(); // + trie_flags.size();
      //if (get_uniq_val_count() > 0)
      //  ret += tail_vals.get_val_grp_ptrs()->get_total_size();
      return ret;
    }
    size_t get_uniq_val_count() {
      return tail_vals.get_uniq_vals_fwd()->size();
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, void *val) {
      return memtrie.get(key, key_len, in_size_out_value_len, val);
    }

    bool put(const uint8_t *key, int key_len, const void *value, int value_len) {
      return insert(key, key_len, value, value_len);
    }

    bool insert(const uint8_t *key, int key_len) {
      return memtrie.insert(key, key_len, NULL, 0);
    }

    bool insert(const uint8_t *key, int key_len, const void *val, int val_len, uint32_t val_pos = UINT32_MAX) {
      if (col_trie_builder == nullptr && column_count > 1 && column_encoding[1] == 't') {
        col_trie_builder = new builder(NULL, "col_trie,key", 1, "*", "*", 0, true, false, opts);
        col_trie_builder->fp = fp;
        memtrie.col_trie = &col_trie_builder->memtrie;
      }
      return memtrie.insert(key, key_len, val, val_len, val_pos);
    }

    void set_node_id() {
      uint32_t node_id = 0;
      for (size_t i = 0; i < memtrie.all_node_sets.size(); i++) {
        leopard::node_set_header *ns_hdr = (leopard::node_set_header *) memtrie.all_node_sets[i];
        ns_hdr->node_id = node_id;
        if (ns_hdr->last_node_idx > 4 && trie_level == 0 && opts.dart) {
          node_id++; // && (ns_hdr->last_node_idx + 1) >= stats.min_len 
          memtrie.node_count++;
          ns_hdr->flags |= NODE_SET_LEAP;
        }
        node_id += ns_hdr->last_node_idx;
        node_id++;
        // if (ns_hdr->last_node_idx < 5)
        //   continue;
        // printf("nid: %u\t", node_id);
        // leopard::node cur_node;
        // cur_node = (uint8_t *) (ns_hdr + 1);
        // uint8_t prev_b = cur_node.get_byte();
        // uint8_t start_b = prev_b;
        // printf("%d", prev_b);
        // cur_node.next();
        // int max_cont = 0;
        // for (int k = 1; k <= ns_hdr->last_node_idx; k++) {
        //   uint8_t b = cur_node.get_byte();
        //   if (b - 1 != prev_b) {
        //     if (start_b == prev_b)
        //       printf(",%d", b);
        //     else {
        //       printf("-%d,%d", prev_b, b);
        //       if (prev_b - start_b > max_cont)
        //         max_cont = prev_b - start_b;
        //     }
        //     start_b = b;
        //   }
        //   prev_b = b;
        //   cur_node.next();
        // }
        // if (start_b != prev_b) {
        //   printf("-%d", prev_b);
        //   if (prev_b - start_b > max_cont)
        //     max_cont = prev_b - start_b;
        // }
        // printf("\tmax: %d\t%d\n", max_cont, ns_hdr->last_node_idx);
      }
    }

    uint32_t build_cache(bool build_fwd_cache, bool build_rev_cache, uint32_t& max_node_id) {
      clock_t t = clock();
      uint32_t cache_count = 64;
      while (cache_count < memtrie.key_count / 2048)
        cache_count <<= 1;
      //cache_count *= 2;
      if (build_fwd_cache) {
        for (int i = 0; i < opts.fwd_cache_multipler; i++)
          cache_count <<= 1;
        f_cache = new fwd_cache[cache_count + 1]();
        f_cache_freq = new uint32_t[cache_count]();
      }
      if (build_rev_cache) {
        for (int i = 0; i < opts.rev_cache_multipler; i++)
          cache_count <<= 1;
        r_cache = new nid_cache[cache_count + 1]();
        r_cache_freq = new uint32_t[cache_count]();
      }
      build_cache(1, 0, cache_count - 1, 1, build_fwd_cache, build_rev_cache);
      max_node_id = 0;
      int sum_freq = 0;
      for (uint32_t i = 0; i < cache_count; i++) {
        if (build_fwd_cache) {
          fwd_cache *fc = &f_cache[i];
          uint32_t cche_node_id = gen::read_uint24(&fc->child_node_id1);
          if (max_node_id < cche_node_id)
            max_node_id = cche_node_id;
          sum_freq += f_cache_freq[i];
        }
        if (build_rev_cache) {
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

    uint32_t build_cache(uint32_t ns_id, uint32_t parent_node_id, uint32_t cache_mask, uint32_t level, bool build_fwd_cache, bool build_rev_cache) {
      if (ns_id == 0)
        return 1;
      if (memtrie.max_level < level)
        memtrie.max_level = level;
      uint32_t node_id_limit = memtrie.node_count;
      leopard::node_set_handler ns(memtrie.all_node_sets, ns_id);
      leopard::node n = ns.first_node();
      leopard::node_set_header *ns_hdr = ns.get_ns_hdr();
      uint32_t cur_node_id = ns_hdr->node_id + (ns_hdr->flags & NODE_SET_LEAP ? 1 : 0);
      uint32_t freq_count = 1;
      for (int i = 0; i <= ns_hdr->last_node_idx; i++) {
        uint32_t node_freq = build_cache(n.get_child(), cur_node_id, cache_mask, level + 1, build_fwd_cache, build_rev_cache);
        freq_count += node_freq;
        if (n.get_child() > 0 && (n.get_flags() & NFLAG_TAIL) == 0) {
          uint8_t node_byte = n.get_byte();
          leopard::node_set_handler child_nsh(memtrie.all_node_sets, n.get_child());
          uint32_t child_node_id = child_nsh.get_ns_hdr()->node_id;
          if (build_fwd_cache) {
            int node_offset = i + (ns_hdr->flags & NODE_SET_LEAP ? 1 : 0);
            uint32_t cache_loc = (ns_hdr->node_id ^ (ns_hdr->node_id << MDX_CACHE_SHIFT) ^ node_byte) & cache_mask;
            fwd_cache *fc = f_cache + cache_loc;
            if (f_cache_freq[cache_loc] < node_freq && child_node_id < node_id_limit && ns_hdr->node_id < (1 << 24) && child_node_id < (1 << 24) && node_offset < 256) {
              f_cache_freq[cache_loc] = node_freq;
              gen::copy_uint24(ns_hdr->node_id, &fc->parent_node_id1);
              gen::copy_uint24(child_node_id, &fc->child_node_id1);
              fc->node_offset = node_offset;
              fc->node_byte = node_byte;
            }
          }
        }
        if (build_rev_cache) {
          uint32_t cache_loc = cur_node_id & cache_mask;
          nid_cache *rc = r_cache + cache_loc;
          if (r_cache_freq[cache_loc] < node_freq && parent_node_id < (1 << 24) && cur_node_id < node_id_limit && cur_node_id < (1 << 24)) {
            r_cache_freq[cache_loc] = node_freq;
            gen::copy_uint24(parent_node_id, &rc->parent_node_id1);
            gen::copy_uint24(cur_node_id, &rc->child_node_id1);
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
        for (int k = 0; k <= len; k++) {
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

    void write_sec_cache(bldr_min_pos_stats& stats, uint32_t sec_cache_size, FILE *fp) {
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        for (int j = 0; j <= 255; j++) {
          uint8_t min_len = min_pos[i][j];
          if (min_len == 0xFF)
            min_len = 0;
          min_len++;
          fputc(min_len, fp);
        }
      }
    }

    uint32_t build() {

      clock_t t = clock();

      gen::gen_printf("Key count: %u\n", memtrie.key_count);

      tp = {};
      tp.opts_loc = 4 + 10 + 27 * 4; // 122
      if (!no_primary_trie) {
        memtrie.sort_node_sets();
        set_node_id();
        tp.min_stats = make_min_positions();
        tp.trie_tail_ptrs_data_sz = build_trie();

        if (trie_level > 0) {
          opts.fwd_cache = false;
          opts.rev_cache = true;
        }
        if (opts.fwd_cache) {
          tp.fwd_cache_count = build_cache(true, false, tp.fwd_cache_max_node_id);
          tp.fwd_cache_size = tp.fwd_cache_count * 8; // 8 = parent_node_id (3) + child_node_id (3) + node_offset (1) + node_byte (1)
        } else
          tp.fwd_cache_max_node_id = 0;
        if (opts.rev_cache) {
          tp.rev_cache_count = build_cache(false, true, tp.rev_cache_max_node_id);
          tp.rev_cache_size = tp.rev_cache_count * 6; // 6 = parent_node_id (3) + child_node_id (3)
        } else
          tp.rev_cache_max_node_id = 0;
        tp.sec_cache_count = decide_min_stat_to_use(tp.min_stats);
        tp.sec_cache_size = 0;
        if (opts.dart)
          tp.sec_cache_size = (tp.min_stats.max_len - tp.min_stats.min_len + 1) * 256;

        if (trie_level == 0) {
          tp.term_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);
          tp.child_rank_lt_sz = tp.term_rank_lt_sz;
          tp.term_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count, sel_divisor, 3);
          tp.child_select_lt_sz = 6;
          if (memtrie.node_set_count > 1)
            tp.child_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count - 1, sel_divisor, 3);
          tp.louds_rank_lt_sz = 0;
          tp.louds_sel1_lt_sz = 0;
        } else {
          tp.louds_rank_lt_sz = gen::get_lkup_tbl_size2(louds.get_highest() + 1, nodes_per_bv_block, width_of_bv_block);
          tp.louds_sel1_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, sel_divisor, 3);
          tp.term_rank_lt_sz = 0;
          tp.child_rank_lt_sz = 0;
          tp.term_select_lt_sz = 0;
          tp.child_select_lt_sz = 0;
        }

        tp.leaf_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);
        tp.leaf_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.key_count, sel_divisor, 3);
        if (opts.tail_tries || tail_vals.get_tail_grp_ptrs()->get_grp_count() == 2)
          tp.tail_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);

        if (opts.dessicate) {
          tp.term_rank_lt_sz = tp.child_rank_lt_sz = tp.leaf_rank_lt_sz = 0;
          tp.term_select_lt_sz = tp.child_select_lt_sz = tp.leaf_select_lt_sz = 0;
          tp.louds_rank_lt_sz = tp.louds_sel1_lt_sz = 0;
        }
        if (!opts.leaf_lt || opts.trie_leaf_count == 0) {
          tp.leaf_select_lt_sz = 0;
          tp.leaf_rank_lt_sz = 0;
        }

        tp.fwd_cache_loc = tp.opts_loc + opts_size;
        tp.rev_cache_loc = tp.fwd_cache_loc + tp.fwd_cache_size;
        tp.sec_cache_loc = tp.rev_cache_loc + tp.rev_cache_size;

        if (trie_level == 0) {
          tp.child_select_lkup_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.term_select_lkup_loc = tp.child_select_lkup_loc + tp.child_select_lt_sz;
          uint32_t total_rank_lt_size = tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.leaf_rank_lt_sz + tp.tail_rank_lt_sz;
          tp.term_rank_lt_loc = tp.term_select_lkup_loc + tp.term_select_lt_sz;
          tp.child_rank_lt_loc = tp.term_rank_lt_loc + width_of_bv_block;
          tp.trie_flags_loc = tp.term_rank_lt_loc + total_rank_lt_size;
          tp.tail_rank_lt_loc = tp.tail_rank_lt_sz == 0 ? 0 : tp.term_rank_lt_loc + width_of_bv_block * 3;
          tp.louds_rank_lt_loc = tp.term_rank_lt_loc; // dummy
          tp.louds_sel1_lt_loc = tp.term_select_lkup_loc; // dummy
          tp.trie_tail_ptrs_data_loc = tp.trie_flags_loc + trie_flags.size();
        } else {
          tp.louds_sel1_lt_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.louds_rank_lt_loc = tp.louds_sel1_lt_loc + tp.louds_sel1_lt_sz;
          tp.trie_flags_loc = tp.louds_rank_lt_loc + tp.louds_rank_lt_sz;
          tp.tail_rank_lt_loc = tp.trie_flags_loc + louds.size_bytes();
          tp.term_rank_lt_loc = tp.child_rank_lt_loc = tp.louds_rank_lt_loc; // dummy
          tp.term_select_lkup_loc = tp.child_select_lkup_loc = tp.louds_sel1_lt_loc; // dummy
          tp.trie_tail_ptrs_data_loc = tp.tail_rank_lt_loc + tp.tail_rank_lt_sz;
        }

        tp.leaf_rank_lt_loc = tp.term_rank_lt_loc + width_of_bv_block * 2;
        tp.tail_flags_loc = tp.trie_tail_ptrs_data_loc + tp.trie_tail_ptrs_data_sz;
        tp.leaf_select_lkup_loc = tp.tail_flags_loc + trie_flags_tail.size();

        if (!opts.dart)
          tp.sec_cache_loc = 0;
      } else {
        tp.leaf_select_lkup_loc = tp.opts_loc + opts_size;
        tp.tail_rank_lt_sz = 0;
      }

      tp.names_loc = tp.leaf_select_lkup_loc + tp.leaf_select_lt_sz;
      tp.names_sz = (column_count + 2) * sizeof(uint16_t) + names_len;
      tp.col_val_table_loc = tp.names_loc + tp.names_sz;
      int val_count = column_count - (no_primary_trie ? 0 : 1);
      tp.col_val_table_sz = val_count * sizeof(uint32_t);
      tp.col_val_loc0 = tp.col_val_table_loc + tp.col_val_table_sz;
      tp.total_idx_size = tp.opts_loc + opts_size +
                (trie_level > 0 ? louds.raw_data()->size() * sizeof(uint64_t) : trie_flags.size()) +
                trie_flags_tail.size() +
                tp.fwd_cache_size + tp.rev_cache_size + tp.sec_cache_size +
                (trie_level == 0 ?
                  (tp.term_select_lt_sz + tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.child_select_lt_sz) :
                  (tp.louds_sel1_lt_sz + tp.louds_rank_lt_sz)) +
                tp.leaf_select_lt_sz +
                tp.leaf_rank_lt_sz + tp.tail_rank_lt_sz +
                tp.names_sz + tp.col_val_table_sz;
      tp.total_idx_size += trie_data_ptr_size();

      if (opts.dessicate) {
        tp.term_select_lkup_loc = tp.term_rank_lt_loc = tp.child_rank_lt_loc = 0;
        tp.leaf_select_lkup_loc = tp.leaf_rank_lt_loc = tp.child_select_lkup_loc = 0;
      }

      gen::print_time_taken(t, "Time taken for build(): ");

      return tp.total_idx_size;

    }

    void open_file() {
      if (fp != NULL)
        fclose(fp);
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

      size_t ftell_diff = ftell(fp);

      fputc(0xA5, fp); // magic byte
      fputc(0x01, fp); // version 1.0
      fputc(0, fp); // reserved
      fputc(0, fp);

      int val_count = column_count - (no_primary_trie ? 0 : 1);
      gen::write_uint16(val_count, fp);
      gen::write_uint32(tp.names_loc, fp);
      gen::write_uint32(tp.col_val_table_loc, fp);

      gen::write_uint32(memtrie.node_count, fp);
      gen::write_uint32(tp.opts_loc, fp);
      gen::write_uint32(memtrie.node_set_count, fp);
      gen::write_uint32(memtrie.key_count, fp);
      gen::write_uint32(memtrie.max_key_len, fp);
      gen::write_uint32(memtrie.max_val_len, fp);
      gen::write_uint16(memtrie.max_tail_len, fp);
      gen::write_uint16(memtrie.max_level, fp);
      gen::write_uint32(tp.fwd_cache_count, fp);
      gen::write_uint32(tp.rev_cache_count, fp);
      gen::write_uint32(tp.fwd_cache_max_node_id, fp);
      gen::write_uint32(tp.rev_cache_max_node_id, fp);
      fwrite(&tp.min_stats, 1, 4, fp);
      gen::write_uint32(tp.fwd_cache_loc, fp);
      gen::write_uint32(tp.rev_cache_loc, fp);
      gen::write_uint32(tp.sec_cache_loc, fp);

      if (trie_level == 0) {
        gen::write_uint32(tp.term_select_lkup_loc, fp);
        gen::write_uint32(tp.term_rank_lt_loc, fp);
        gen::write_uint32(tp.child_select_lkup_loc, fp);
        gen::write_uint32(tp.child_rank_lt_loc, fp);
      } else {
        gen::write_uint32(tp.louds_sel1_lt_loc, fp);
        gen::write_uint32(tp.louds_rank_lt_loc, fp);
        gen::write_uint32(tp.louds_sel1_lt_loc, fp);
        gen::write_uint32(tp.louds_rank_lt_loc, fp);
      }
      gen::write_uint32(tp.leaf_select_lkup_loc, fp);
      gen::write_uint32(tp.leaf_rank_lt_loc, fp);
      gen::write_uint32(tp.tail_rank_lt_loc, fp);
      gen::write_uint32(tp.trie_tail_ptrs_data_loc, fp);
      gen::write_uint32(tp.louds_rank_lt_loc, fp);
      gen::write_uint32(tp.louds_sel1_lt_loc, fp);
      gen::write_uint32(tp.trie_flags_loc, fp);
      gen::write_uint32(tp.tail_flags_loc, fp);

      fwrite(&opts, 1, opts_size, fp);

      if (!no_primary_trie) {
        write_fwd_cache(fp);
        write_rev_cache(fp);
        if (tp.sec_cache_size > 0)
          write_sec_cache(tp.min_stats, tp.sec_cache_size, fp);
        if (!opts.dessicate) {
          if (trie_level > 0) {
            write_louds_select_lt(fp);
            write_louds_rank_lt(fp);
          } else {
            write_bv_select_lt(BV_LT_TYPE_CHILD, fp);
            write_bv_select_lt(BV_LT_TYPE_TERM, fp);
            write_bv_rank_lt(BV_LT_TYPE_TERM | BV_LT_TYPE_CHILD | (tp.leaf_rank_lt_sz == 0 ? 0 : BV_LT_TYPE_LEAF) | (tp.tail_rank_lt_sz == 0 ? 0 : BV_LT_TYPE_TAIL), fp);
          }
        }
        if (trie_level > 0) {
          fwrite(louds.raw_data()->data(), 1, louds.raw_data()->size() * sizeof(uint64_t), fp);
          if (tp.tail_rank_lt_sz > 0)
            write_bv_rank_lt(BV_LT_TYPE_TAIL, fp);
        } else
          fwrite(trie_flags.data(), 1, trie_flags.size(), fp);

        write_trie_tail_ptrs_data(fp);

        if (!opts.dessicate) {
          fwrite(trie_flags_tail.data(), 1, trie_flags_tail.size(), fp);
          if (opts.leaf_lt && opts.trie_leaf_count > 0)
            write_bv_select_lt(BV_LT_TYPE_LEAF, fp);
        }
      }

      if (column_count > (no_primary_trie ? 0 : 1))
        val_table[0] = tp.col_val_loc0;
      write_names(fp);
      write_col_val_table(fp);

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

      ftell_diff = ftell(fp) - ftell_diff;
      if (tp.total_idx_size != ftell_diff)
        printf("WARNING: Trie size not matching: %lu, %d -----------------------------------------------------------------------------------------------------------\n", ftell_diff, trie_level);

      return tp.total_idx_size;

    }

    void write_kv(const char *filename = NULL) {
      write_trie(filename);
      memtrie.prev_val_size = 0;
      if (column_count == 1 && !no_primary_trie)
        return;
      char encoding_type = column_encoding[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1)];
      if (memtrie.all_vals->size() > 2 || encoding_type == 't' || encoding_type == 'w') { // TODO: What if column contains only NULL and ""
        memtrie.prev_val_size = build_and_write_col_val();
      }
    }

    void write_names(FILE *fp) {
      int name_count = column_count + 2;
      for (int i = 0; i < name_count; i++)
        gen::write_uint16(names_positions[i], fp);
      fwrite(names, 1, names_len, fp);
    }

    void write_col_val_table(FILE *fp) {
      int val_count = column_count - (no_primary_trie ? 0 : 1);
      for (int i = 0; i < val_count; i++)
        gen::write_uint32(val_table[i], fp);
    }

    uint32_t write_col_val() {
      char data_type = column_types[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1)];
      char encoding_type = column_encoding[memtrie.cur_col_idx + (no_primary_trie ? 0 : 1)];
      uint32_t val_size = write_val_ptrs_data(data_type, encoding_type, 1, fp); // TODO: fix flags
      if (memtrie.cur_col_idx > 0) {
        uint32_t prev_val_loc = val_table[memtrie.cur_col_idx - 1];
        val_table[memtrie.cur_col_idx] = prev_val_loc + memtrie.prev_val_size;
      }
      memtrie.prev_val_size = val_size;
      return val_size;
    }

    void write_final_val_table() {
      if (column_count == 1 && !no_primary_trie) {
        close_file();
        return;
      }
      fseek(fp, tp.col_val_table_loc, SEEK_SET);
      write_col_val_table(fp);
      int val_count = column_count - (no_primary_trie ? 0 : 1);
      gen::gen_printf("Val count: %d, tbl:", val_count);
      for (int i = 0; i < val_count; i++)
        gen::gen_printf(" %u", val_table[i]);
      gen::gen_printf("\nCol sizes:");
      for (int i = 1; i < val_count; i++)
        gen::gen_printf(" %u", val_table[i] - val_table[i - 1]);
      gen::gen_printf("\nTotal size: %u\n", val_table[memtrie.cur_col_idx] + memtrie.prev_val_size);
      close_file();
    }

    // struct nodes_ptr_grp {
    //   uint32_t node_id;
    //   uint32_t ptr;
    // };

    void write_bv_n(uint32_t node_id, bool to_write, uint32_t& count, uint32_t& count_n, uint8_t *bit_counts_n, uint8_t& pos_n, FILE *fp) {
      if (!to_write)
        return;
      int u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n) - 1;
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        gen::write_uint32(count, fp);
        fwrite(bit_counts_n, 1, u8_arr_count, fp);
        for (size_t i = 0; i < pos_n; i++)
          count += bit_counts_n[i];
        count += count_n;
        count_n = 0;
        memset(bit_counts_n, 0, u8_arr_count + 1);
        pos_n = 0;
      } else if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
        bit_counts_n[pos_n] = count_n;
        count_n = 0;
        pos_n++;
      }
    }

    void write_bv_rank_lt(uint8_t which, FILE *fp) {
      uint32_t node_id = 0;
      int u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      u8_arr_count--;
      uint32_t count_tail = 0;
      uint32_t count_term = 0;
      uint32_t count_child = 0;
      uint32_t count_leaf = 0;
      uint32_t count_tail_n = 0;
      uint32_t count_term_n = 0;
      uint32_t count_child_n = 0;
      uint32_t count_leaf_n = 0;
      uint8_t bit_counts_tail_n[u8_arr_count + 1];
      uint8_t bit_counts_term_n[u8_arr_count + 1];
      uint8_t bit_counts_child_n[u8_arr_count + 1];
      uint8_t bit_counts_leaf_n[u8_arr_count + 1];
      uint8_t pos_tail_n = 0;
      uint8_t pos_term_n = 0;
      uint8_t pos_child_n = 0;
      uint8_t pos_leaf_n = 0;
      memset(bit_counts_tail_n, 0, u8_arr_count + 1);
      memset(bit_counts_term_n, 0, u8_arr_count + 1);
      memset(bit_counts_child_n, 0, u8_arr_count + 1);
      memset(bit_counts_leaf_n, 0, u8_arr_count + 1);
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n, pos_term_n, fp);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n, pos_child_n, fp);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n, pos_leaf_n, fp);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n, pos_tail_n, fp);
        count_tail_n += (cur_node_flags & NFLAG_TAIL ? 1 : 0);
        count_term_n += (cur_node_flags & NFLAG_TERM ? 1 : 0);
        count_child_n += (cur_node_flags & NFLAG_CHILD ? 1 : 0);
        count_leaf_n += (cur_node_flags & NFLAG_LEAF ? 1 : 0);
        node_id++;
        cur_node = ni.next();
      }
      node_id = nodes_per_bv_block; // just to make it write the last blocks
      for (size_t i = 0; i < 2; i++) {
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n, pos_term_n, fp);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n, pos_child_n, fp);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n, pos_leaf_n, fp);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n, pos_tail_n, fp);
      }
    }

    void write_louds_rank_lt(FILE *fp) {
      uint32_t count = 0;
      uint32_t count_n = 0;
      int u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      u8_arr_count--;
      uint8_t bit_counts_n[u8_arr_count + 1];
      uint8_t pos_n = 0;
      memset(bit_counts_n, 0, u8_arr_count + 1);
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        write_bv_n(i, true, count, count_n, bit_counts_n, pos_n, fp);
        count_n += (louds[i] ? 1 : 0);
      }
      bit_count = nodes_per_bv_block; // just to make it write last blocks
      write_bv_n(bit_count, true, count, count_n, bit_counts_n, pos_n, fp);
      write_bv_n(bit_count, true, count, count_n, bit_counts_n, pos_n, fp);
    }

    void write_fwd_cache(FILE *fp) {
      fwrite(f_cache, 1, tp.fwd_cache_count * sizeof(fwd_cache), fp);
    }

    void write_rev_cache(FILE *fp) {
      fwrite(r_cache, 1, tp.rev_cache_count * sizeof(nid_cache), fp);
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

    void write_bv_select_lt(int which, FILE *fp) {
      uint32_t node_id = 0;
      uint32_t one_count = 0;
      gen::write_uint24(0, fp);
      leopard::node_iterator ni(memtrie.all_node_sets, 0);
      leopard::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        if (node_qualifies_for_select(&cur_node, cur_node_flags, which)) {
          if (one_count && (one_count % sel_divisor) == 0) {
            uint32_t val_to_write = node_id / nodes_per_bv_block;
            gen::write_uint24(val_to_write, fp);
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", one_count, val_to_write);
          }
          one_count++;
        }
        node_id++;
        cur_node = ni.next();
      }
      gen::write_uint24(memtrie.node_count/nodes_per_bv_block, fp);
    }

    void write_louds_select_lt(FILE *fp) {
      uint32_t one_count = 0;
      gen::write_uint24(0, fp);
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        if (louds[i]) {
          if (one_count && (one_count % sel_divisor) == 0) {
            uint32_t val_to_write = i / nodes_per_bv_block;
            gen::write_uint24(val_to_write, fp);
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", one_count, val_to_write);
          }
          one_count++;
        }
      }
      gen::write_uint24(bit_count / nodes_per_bv_block, fp);
    }

    leopard::uniq_info *get_ti(leopard::node *n) {
      tail_val_maps *tm = &tail_vals;
      leopard::uniq_info_vec *uniq_tails_rev = tm->get_uniq_tails_rev();
      return (*uniq_tails_rev)[n->get_tail()];
    }

    uint32_t get_tail_ptr(leopard::node *cur_node) {
      leopard::uniq_info *ti = get_ti(cur_node);
      return ti->ptr;
    }

    leopard::uniq_info *get_vi(leopard::node *n) {
      tail_val_maps *tm = &tail_vals;
      leopard::uniq_info_vec *uniq_vals_fwd = tm->get_uniq_vals_fwd();
      return (*uniq_vals_fwd)[n->get_col_val()];
    }

    uint32_t get_node_id_from_sequence(uint32_t ins_seq_id) {
      return memtrie.get_node_id_from_sequence(ins_seq_id);
    }

};

}

// 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx
// 16 - followed by pointer
// 0 2-15/18-31 bytes 16 ptrs bytes 1

// 0 0000 - terminator
// 0 0001 to 0014 - length of suffix and terminator
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
