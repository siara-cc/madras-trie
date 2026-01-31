#ifndef __DV1_BUILDER_POINTER_GROUPS__
#define __DV1_BUILDER_POINTER_GROUPS__

#include "madras/dv1/common.hpp"

#include "madras/dv1/builder/output_writer.hpp"
#include "madras/dv1/builder/builder_interfaces.hpp"

namespace madras { namespace dv1 {

class ptr_groups {
  private:
    uint8_t max_groups;
    output_writer &output;
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
    uint8_t flags;
    uintxx_t next_idx;
    uintxx_t ptr_lookup_tbl_sz;
    uintxx_t ptr_lookup_tbl_loc;
    uintxx_t grp_data_loc;
    uintxx_t grp_data_size;
    uintxx_t grp_ptrs_loc;
    uintxx_t idx2_ptrs_map_loc;
    uintxx_t null_bv_loc;
    uintxx_t idx2_ptr_count;
    uint64_t tot_ptr_bit_count;
    uintxx_t max_len;
    uintxx_t reserved32_1;
    uintxx_t null_count;
    int64_t min_val;
    int64_t max_val;
  public:
    byte_ptr_vec& all_node_sets;
    std::vector<trie_builder_fwd *> inner_tries;
    size_t inner_trie_start_grp;
    uniq_info_base rpt_ui;
    ptr_groups(byte_ptr_vec& _all_node_sets, output_writer &_output)
          : all_node_sets (_all_node_sets), output (_output) {
      reset();
    }
    ~ptr_groups() {
      reset_inner_tries();
    }
    void init(uint8_t _max_groups, int _step_bits_idx, int _step_bits_rest) {
      max_groups = _max_groups;
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
    uintxx_t get_idx_limit() {
      return idx_limit;
    }
    uintxx_t idx_map_arr[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    void set_idx_info(int _start_bits, uint8_t new_idx_limit, uint8_t _idx_ptr_size) {
      start_bits = _start_bits;
      idx_limit = new_idx_limit;
      idx_ptr_size = _idx_ptr_size;
      for (uintxx_t i = 1; i <= idx_limit; i++) {
        idx_map_arr[i] = idx_map_arr[i - 1] + pow(2, _start_bits) * idx_ptr_size;
        _start_bits += step_bits_idx;
        //gen::gen_printf("idx_map_arr[%d] = %d\n", i, idx_map_arr[i]);
      }
    }
    void set_max_len(uintxx_t _max_len) {
      max_len = _max_len;
    }
    void set_null(size_t idx) {
      null_bv.set(idx, true);
    }
    void set_stats(uint8_t _flags, uintxx_t _null_ct, int64_t _min_val, int64_t _max_val) {
      flags = _flags;
      null_count = _null_ct;
      min_val = _min_val;
      max_val = _max_val;
    }
    void print_stats() {
      gen::gen_printf("\nflags: %d, null_count: %u\n", flags, null_count);
      gen::gen_printf("min_val: %lld\n", min_val);
      gen::gen_printf("max_val: %lld\n\n", max_val);
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
    uintxx_t *get_idx_map_arr() {
      return idx_map_arr;
    }
    uintxx_t get_idx2_ptrs_count() {
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
    uintxx_t check_next_grp(uint8_t grp_no, uintxx_t cur_limit, uintxx_t len) {
      if (grp_no >= max_groups)
        return cur_limit;
      if (grp_no <= idx_limit) {
        if (next_idx == cur_limit)
          return pow(2, log2(cur_limit) + step_bits_idx);
      } else {
        if ((freq_grp_vec[grp_no].grp_size + len) >= cur_limit)
          return pow(2, log2(cur_limit) + step_bits_rest);
      }
      return cur_limit;
    }
    uintxx_t next_grp(uint8_t& grp_no, uintxx_t cur_limit, uintxx_t len, uintxx_t tot_freq_count, bool force_next_grp = false, bool just_add = false) {
      if (grp_no >= max_groups) // reeval curlimit?
        return cur_limit;
      bool next_grp = force_next_grp;
      if (!next_grp) {
        if (grp_no <= idx_limit) {
          if (next_idx == cur_limit) {
            next_grp = true;
            next_idx = 0;
          }
        } else {
          if ((freq_grp_vec[grp_no].grp_size + len) >= cur_limit)
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
    void update_current_grp(uintxx_t grp_no, int32_t len, int32_t freq, uintxx_t count = 0) {
      freq_grp_vec[grp_no].grp_size += len;
      freq_grp_vec[grp_no].freq_count += freq;
      if (count > 0)
        freq_grp_vec[grp_no].count += count;
      else
        freq_grp_vec[grp_no].count += (len < 0 ? -1 : 1);
      next_idx++;
    }
    uintxx_t append_ptr2_idx_map(uintxx_t grp_no, uintxx_t _ptr) {
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
    uintxx_t get_set_len_len(uintxx_t len, byte_vec *vec = NULL) {
      uintxx_t len_len = 0;
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
    uintxx_t append_text(uintxx_t grp_no, const uint8_t *val, uintxx_t len, bool append0 = false) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uintxx_t ptr = grp_data_vec.size();
      for (uintxx_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (append0)
        grp_data_vec.push_back(15);
      return ptr;
    }
    uintxx_t append_bin_to_grp_data(uintxx_t grp_no, uint8_t *val, uintxx_t len, char data_type = MST_BIN) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uintxx_t ptr = grp_data_vec.size();
      if (data_type == MST_TEXT || data_type == MST_BIN)
        gen::append_vint32(grp_data_vec, len);
      for (uintxx_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      return ptr;
    }
    uintxx_t append_bin15_to_grp_data(uintxx_t grp_no, uint8_t *val, uintxx_t len, char data_type = MST_BIN) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uintxx_t ptr = grp_data_vec.size();
      if (data_type == MST_TEXT || data_type == MST_BIN) {
        get_set_len_len(len, &grp_data_vec);
        ptr = grp_data_vec.size() - 1;
      }
      for (uintxx_t k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      // grp_data_vec.push_back(0);
      // uint64_t u64;
      // allflic48::simple_decode(grp_data_vec.data() + ptr, 1, &u64);
      // printf("Grp: %d, u64: %llu, len: %u, %u\n", grp_no, u64, len, ptr);
      return ptr;
    }
    int append_ptr_bits(uintxx_t given_ptr, int bits_to_append) {
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
    uintxx_t get_hdr_size() {
      return 520 + grp_data.size() * 8 + inner_tries.size() * 8;
    }
    uintxx_t get_data_size() {
      uintxx_t data_size = 0;
      for (size_t i = 0; i < grp_data.size(); i++)
        data_size += gen::size_align8(grp_data[i].size());
      for (size_t i = 0; i < inner_tries.size(); i++)
        data_size += freq_grp_vec[i + inner_trie_start_grp].grp_size;
      return data_size;
    }
    uintxx_t get_ptrs_size() {
      return ptrs.size();
    }
    uintxx_t get_total_size() {
      if (enc_type == MSE_TRIE || enc_type == MSE_TRIE_2WAY) {
        return gen::size_align8(grp_data_size) + get_ptrs_size() +
                  null_bv.size_bytes() +
                  gen::size_align8(ptr_lookup_tbl_sz) + 11 * 8 + 16;
      }
      return gen::size_align8(grp_data_size) + get_ptrs_size() +
        null_bv.size_bytes() + gen::size_align8(idx2_ptrs_map.size()) +
        gen::size_align8(ptr_lookup_tbl_sz) + 11 * 8 + 16; // aligned to 8
    }
    void set_ptr_lkup_tbl_ptr_width(uint8_t width) {
      ptr_lkup_tbl_ptr_width = width;
    }
    typedef uniq_info_base *(*get_info_fn) (memtrie::node *cur_node, uniq_info_vec& info_vec);
    static uniq_info_base *get_tails_info_fn(memtrie::node *cur_node, uniq_info_vec& info_vec) {
      return (uniq_info *) info_vec[cur_node->get_tail()];
    }
    static uniq_info_base *get_vals_info_fn(memtrie::node *cur_node, uniq_info_vec& info_vec) {
      return (uniq_info *) info_vec[cur_node->get_col_val()];
    }
    void build(uintxx_t node_count, get_info_fn get_info_func,
          uniq_info_vec& info_vec, bool is_tail, uint16_t _pk_col_count, bool dessicat,
          char encoding_type = 'u', char _data_type = '*', int col_trie_size = 0) {
      pk_col_count = _pk_col_count;
      dessicate = dessicat;
      enc_type = encoding_type;
      data_type = _data_type;
      if (encoding_type != MSE_TRIE && encoding_type != MSE_TRIE_2WAY && col_trie_size == 0 && (freq_grp_vec.size() > 2 || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB)) {
        ptr_lkup_tbl_ptr_width = gen::bytes_needed(tot_ptr_bit_count);
        if (ptr_lkup_tbl_ptr_width < 3)
          ptr_lkup_tbl_ptr_width = 3;
        if (!dessicate)
          build_ptr_lookup_tbl(get_info_func, is_tail, info_vec, encoding_type);
        if (encoding_type != MSE_STORE && encoding_type != MSE_VINTGB) {
          if (!is_tail && encoding_type != MSE_WORDS && encoding_type != MSE_WORDS_2WAY)
            build_val_ptrs(get_info_func, info_vec);
        }
      }
      if (freq_grp_vec.size() <= 2 && !is_tail && col_trie_size == 0 && encoding_type != MSE_WORDS && encoding_type != MSE_WORDS_2WAY && encoding_type != MSE_STORE && encoding_type != MSE_VINTGB)
        build_val_ptrs(get_info_func, info_vec); // flat
      ptr_lookup_tbl_loc = 11 * 8 + 16;
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
      //printf("Ptr sz: %lu, Data sz: %u, lt size: %lu, lt sz: %u\n", ptrs.size(), grp_data_size, ptr_lookup_tbl.size(), ptr_lookup_tbl_sz);
    }
    #define CODE_LT_BIT_LEN 0
    #define CODE_LT_CODE_LEN 1
    void write_code_lookup_tbl(bool is_tail) {
      write_code_lt(is_tail, CODE_LT_BIT_LEN);
      write_code_lt(is_tail, CODE_LT_CODE_LEN);
    }
    void write_code_lt(bool is_tail, int which) {
      for (int i = 0; i < 256; i++) {
        uint8_t code_i = i;
        bool code_found = false;
        for (size_t j = 1; j < freq_grp_vec.size(); j++) {
          uint8_t code_len = freq_grp_vec[j].code_len;
          uint8_t code = freq_grp_vec[j].code;
          if ((code_i >> (8 - code_len)) == code) {
            if (which == CODE_LT_BIT_LEN) {
              int bit_len = freq_grp_vec[j].grp_log2;
              output.write_byte(bit_len);
            }
            if (which == CODE_LT_CODE_LEN) {
              output.write_byte((j - 1) | (code_len << 4));
            }
            code_found = true;
            break;
          }
        }
        if (!code_found) {
          //printf("Code not found: %d", i);
          //output.write_byte(0);
          output.write_byte(0);
        }
      }
    }
    void write_grp_data(uintxx_t offset, bool is_tail) {
      int grp_count = grp_data.size() + inner_tries.size();
      output.write_byte(grp_count);
      if (inner_trie_start_grp > 0) {
        output.write_byte(inner_trie_start_grp - 1);
        //output.write_byte(freq_grp_vec[grp_count].grp_log2);
      } else if (freq_grp_vec.size() == 2)
        output.write_byte(freq_grp_vec[1].grp_log2);
      else
        output.write_byte(0);
      output.write_bytes((const uint8_t *) "      ", 6); // padding
      write_code_lookup_tbl(is_tail);
      uintxx_t total_data_size = 0;
      for (size_t i = 0; i < grp_data.size(); i++) {
        output.write_u64(offset + grp_count * 8 + total_data_size);
        total_data_size += gen::size_align8(grp_data[i].size());
      }
      for (size_t i = 0; i < inner_tries.size(); i++) {
        uintxx_t grp_data_loc = offset + grp_count * 8 + total_data_size;
        //printf("grp_data_loc: %lu, %u\n", i + inner_trie_start_grp, grp_data_loc);
        output.write_u64(grp_data_loc);
        total_data_size += freq_grp_vec[i + inner_trie_start_grp].grp_size;
      }
      for (size_t i = 0; i < grp_data.size(); i++) {
        output.write_bytes(grp_data[i].data(), grp_data[i].size());
        output.write_align8(grp_data[i].size());
      }
      for (size_t i = 0; i < inner_tries.size(); i++) {
        inner_tries[i]->set_fp(output.get_fp());
        inner_tries[i]->set_out_vec(output.get_out_vec());
        if (auto trie_map = dynamic_cast<trie_map_builder_fwd *>(inner_tries[i])) {
          trie_map->write_kv(false);
        } else {
          inner_tries[i]->write_trie();
        }
      }
    }
    void write_ptr_lookup_tbl() {
      output.write_bytes(ptr_lookup_tbl.data(), ptr_lookup_tbl.size());
      output.write_align8(ptr_lookup_tbl_sz);
    }
    void write_ptrs() {
      output.write_bytes(ptrs.data(), ptrs.size());
    }
    void write_null_bv() {
      output.write_bytes((const uint8_t *) null_bv.raw_data()->data(), null_bv.size_bytes());
    }
    void build_val_ptrs(get_info_fn get_info_func, uniq_info_vec& info_vec) {
      ptrs.clear();
      last_byte_bits = 64;
      gen::append_uint64(0, ptrs);
      memtrie::node_iterator ni(all_node_sets, pk_col_count == 0 ? 1 : 0);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if ((cur_node.get_flags() & NFLAG_LEAF) != 0) {
          uintxx_t col_val = cur_node.get_col_val();
          if (col_val != UINTXX_MAX) {
            if (col_val > UINTXX_MAX - 65) {
              freq_grp& fg = freq_grp_vec[rpt_grp_no];
              append_ptr_bits(fg.code, fg.code_len);
              append_ptr_bits(UINTXX_MAX - col_val, fg.grp_log2 - fg.code_len);
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
    void append_plt_count(uintxx_t bit_count) {
      gen::append_byte_vec(ptr_lookup_tbl, (uint8_t *) &bit_count, ptr_lkup_tbl_ptr_width);
      // if (ptr_lkup_tbl_ptr_width == 4)
      //   gen::append_uint32(bit_count, ptr_lookup_tbl);
      // else
      //   gen::append_uint24(bit_count, ptr_lookup_tbl);
    }
    void append_plt_count16(uintxx_t *bit_counts, size_t u16_arr_count, char encoding_type) {
      for (size_t j = 0; j < u16_arr_count; j++) {
        if (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB) {
          if (bit_counts[j] > 16777215) std::cout << "UNEXPECTED: PTR_LOOKUP_TBL bit_count4 overflow: " << bit_counts[j] << std::endl;
          gen::append_uint24(bit_counts[j], ptr_lookup_tbl);
        } else
          gen::append_uint16(bit_counts[j], ptr_lookup_tbl);
      }
    }
    void build_ptr_lookup_tbl(get_info_fn get_info_func, bool is_tail,
          uniq_info_vec& info_vec, char encoding_type) {
      uintxx_t node_id = 0;
      uintxx_t bit_count = 0;
      uintxx_t bit_count4 = 0;
      size_t pos4 = 0;
      size_t u16_arr_count = (nodes_per_ptr_block / nodes_per_ptr_block_n);
      u16_arr_count--;
      std::vector<uintxx_t> bit_counts(u16_arr_count + 1);
      memset(bit_counts.data(), '\0', u16_arr_count * 4 + 4);
      ptr_lookup_tbl.clear();
      append_plt_count(bit_count);
      memtrie::node_iterator ni(all_node_sets, pk_col_count == 0 ? 1 : 0);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        if (node_id && (node_id % nodes_per_ptr_block_n) == 0) {
          if (bit_count4 > (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB ? 16777215 : 65535))
            std::cout << "UNEXPECTED: PTR_LOOKUP_TBL bit_count4 overflow: " << bit_count4 << std::endl;
          // printf("nid: %u, bit count: %u\n", node_id, bit_count4);
          if (encoding_type == MSE_VINTGB && (bit_count4 == 0 || (pos4 > 0 && (bit_count4 - bit_counts[pos4 - 1] == 0))))
            bit_count4 += (data_type == MST_INT ? 3 : 4);
          bit_counts[pos4] = bit_count4;
          pos4++;
        }
        if (node_id && (node_id % nodes_per_ptr_block) == 0) {
          append_plt_count16(bit_counts.data(), u16_arr_count, encoding_type);
          bit_count += bit_counts[u16_arr_count];
          append_plt_count(bit_count);
          bit_count4 = 0;
          pos4 = 0;
          memset(bit_counts.data(), '\0', u16_arr_count * 4 + 4);
        }
        if (cur_node_flags & (is_tail ? NFLAG_TAIL : NFLAG_LEAF)) {
          if (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB) {
            bit_count4 += cur_node.get_col_val();
            //printf("gcv: %u, bc4: %u\n", cur_node.get_col_val(), bit_count4);
          } else {
            uintxx_t col_val = cur_node.get_col_val();
            if (col_val != UINTXX_MAX) {
              uniq_info_base *vi;
              if (col_val > UINTXX_MAX - 65)
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
      append_plt_count16(bit_counts.data(), u16_arr_count, encoding_type);
      bit_count += bit_counts[u16_arr_count];
      append_plt_count(bit_count);
      append_plt_count16(bit_counts.data(), u16_arr_count, encoding_type);
    }
    void write_ptrs_data(bool is_tail) {
      //size_t ftell_start = ftell(fp);
      output.write_byte(ptr_lkup_tbl_ptr_width);
      output.write_byte(data_type);
      output.write_byte(enc_type);
      output.write_byte(flags);
      output.write_byte(len_grp_no - 1);
      output.write_byte(rpt_grp_no - 1);
      output.write_byte(rpt_seq_grp_no);
      output.write_byte(sug_col_width);
      output.write_byte(sug_col_height);
      output.write_byte(idx_limit);
      output.write_byte(start_bits);
      output.write_byte(step_bits_idx);
      output.write_byte(step_bits_rest);
      output.write_byte(idx_ptr_size);
      output.write_byte(reserved8_1);
      output.write_byte(reserved8_2);

      output.write_u64(null_bv.raw_data()->size());
      output.write_u64(max_len);
      output.write_u64(ptr_lookup_tbl_loc);
      output.write_u64(grp_data_loc);
      output.write_u64(idx2_ptr_count);
      output.write_u64(idx2_ptrs_map_loc);
      output.write_u64(grp_ptrs_loc);
      output.write_u64(null_bv_loc);
      output.write_u64(null_count);
      output.write_u64(min_val);
      output.write_u64(max_val);

      if (enc_type == MSE_TRIE || enc_type == MSE_TRIE_2WAY) {
        write_ptrs();
      } else {
        if (freq_grp_vec.size() > 2 || enc_type == MSE_STORE || enc_type == MSE_VINTGB)
          write_ptr_lookup_tbl();
        write_ptrs();
        write_null_bv();
        byte_vec *idx2_ptrs_map = get_idx2_ptrs_map();
        output.write_bytes(idx2_ptrs_map->data(), idx2_ptrs_map->size());
        output.write_align8(idx2_ptrs_map->size());
        write_grp_data(grp_data_loc + 520, is_tail); // group count, 512 lookup tbl, tail locs, tails
        output.write_align8(grp_data_size);
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
      uintxx_t sums[4];
      memset(sums, 0, sizeof(uintxx_t) * 4);
      for (size_t i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        uintxx_t byts = (fg->grp_log2 * fg->freq_count) >> 3;
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
        fg->grp_log2 = gen::bits_needed(fg->grp_size);
        if (!is_val) {
          if (fg->grp_log2 > 8)
            fg->grp_log2 -= 8;
          else
            fg->grp_log2 = 0;
        }
        flat_ptr_bv.init(&ptrs, fg->grp_log2, fg->freq_count);
        return;
      }
      std::vector<uintxx_t> freqs;
      for (size_t i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      gen::huffman<uintxx_t> _huffman(freqs);
      tot_ptr_bit_count = 0;
      for (size_t i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        uintxx_t len;
        fg->code = (uint8_t) _huffman.get_code(i - 1, len);
        fg->code_len = len;
        if (i <= idx_limit) {
          fg->grp_log2 = gen::bits_needed(fg->grp_limit - 1);
        } else if (inner_trie_start_grp > 0 && i >= inner_trie_start_grp) {
          if (fg->count < fg->grp_limit)
            fg->grp_log2 = gen::bits_needed(fg->count == 1 ? 2 : fg->count);
          else
            fg->grp_log2 = gen::bits_needed(fg->grp_limit);
        } else {
          fg->grp_log2 = gen::bits_needed(fg->grp_size);
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

}}

#endif
