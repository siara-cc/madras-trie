#ifndef __DV1_READER_TAIL_PTRS_HPP
#define __DV1_READER_TAIL_PTRS_HPP

#include "cmn.hpp"
#include "pointers.hpp"
#include "interfaces.hpp"
#include "rank_select.hpp"

namespace madras { namespace dv1 {

class tail_ptr_map {
  private:
    // __fq1 __fq2 tail_ptr_map(tail_ptr_map const&);
    // __fq1 __fq2 tail_ptr_map& operator=(tail_ptr_map const&);
  public:
    __fq1 __fq2 tail_ptr_map() {
    }
    __fq1 __fq2 ~tail_ptr_map() {
    }
    __fq1 __fq2 static uintxx_t read_len(uint8_t *t) {
      do {
        t++;
      } while ((*t > 15 && *t < 32) && (*t & 0x08) == 0);
      t--;
      uintxx_t ret;
      read_len_bw(t, ret);
      return ret;
    }
    __fq1 __fq2 static uint8_t *read_len_bw(uint8_t *t, uintxx_t& out_len) {
      out_len = 0;
      while (*t > 15 && *t < 32) {
        out_len <<= 3;
        out_len += (*t & 0x07);
        if (*t-- & 0x08)
          break;
      }
      return t;
    }
    __fq1 __fq2 void read_suffix(uint8_t *out_str, uint8_t *t, uintxx_t sfx_len) {
      while (*t < 15 || *t > 31)
        t--;
      while (*t != 15) {
        uintxx_t prev_sfx_len;
        t = read_len_bw(t, prev_sfx_len);
        while (sfx_len > prev_sfx_len) {
          sfx_len--;
          *out_str++ = *(t + prev_sfx_len - sfx_len);
        }
        while (*t < 15 || *t > 31)
          t--;
      }
      while (sfx_len > 0) {
        *out_str++ = *(t - sfx_len);
        sfx_len--;
      }
    }
    __fq1 __fq2 bool compare_bin(uint8_t *data, uint8_t *tail, input_ctx& in_ctx) {
      uintxx_t bin_len;
      read_len_bw(tail++, bin_len);
      if (in_ctx.key_pos + bin_len > in_ctx.key_len) {
        if (*tail == in_ctx.key[in_ctx.key_pos])
          in_ctx.key_pos++;
        return false;
      }
      if (cmn::memcmp(tail, in_ctx.key + in_ctx.key_pos, bin_len) == 0) {
        in_ctx.key_pos += bin_len;
        return true;
      }
      if (*tail == in_ctx.key[in_ctx.key_pos])
        in_ctx.key_pos++;
      return false;
    }
    __fq1 __fq2 void get_tail_suffix(uint8_t *t, uint8_t *data, uintxx_t tail_ptr, gen::byte_str& tail_str) {
      uintxx_t sfx_len = read_len(t);
      read_suffix(tail_str.data() + tail_str.length(), data + tail_ptr - 1, sfx_len);
      tail_str.set_length(tail_str.length() + sfx_len);
    }
    __fq1 __fq2 void get_bin_data(uint8_t *t, gen::byte_str &tail_str) {
      uintxx_t bin_len;
      read_len_bw(t++, bin_len);
      while (bin_len--)
        tail_str.append(*t++);
    }
    __fq1 __fq2 bool compare_suffix(uint8_t *data, uintxx_t tail_ptr, uint8_t *tail, input_ctx& in_ctx) {
      uintxx_t sfx_len = read_len(tail);
      uint8_t stack_buf[FAST_STACK_BUF];
      uint8_t* heap_buf = nullptr;
      uint8_t *sfx_buf = get_fast_buffer<uint8_t>(sfx_len, stack_buf, heap_buf);
      BufferGuard<uint8_t> guard(heap_buf);
      read_suffix(sfx_buf, data + tail_ptr - 1, sfx_len);
      if (cmn::memcmp(sfx_buf, in_ctx.key + in_ctx.key_pos, sfx_len) == 0) {
        in_ctx.key_pos += sfx_len;
        return true;
      }
      return false;
    }
    __fq1 __fq2 inline bool compare_tail_data(uint8_t *data, uintxx_t tail_ptr, input_ctx& in_ctx) {
      uint8_t *tail = data + tail_ptr;
      if ((uint8_t)(*tail - 15) > 16) { // faster than `(*tail < 15 || *tail > 31)`
        do {
          if (in_ctx.key_pos >= in_ctx.key_len || *tail != in_ctx.key[in_ctx.key_pos])
            return false;
          tail++;
          in_ctx.key_pos++;
        } while ((uint8_t)(*tail - 15) > 16);
        if (*tail == 15)
          return true;
        return compare_suffix(data, tail_ptr, tail, in_ctx);
      }
      return compare_bin(data, tail, in_ctx);
    }
    __fq1 __fq2 inline void get_tail_data(uint8_t *data, uintxx_t tail_ptr, gen::byte_str& tail_str) {
      uint8_t *t = data + tail_ptr;
      if ((uint8_t)(*t - 15) > 16) { // faster than (*t < 15 || *t > 31)
        uint8_t byt = *t;
        do {
          t++;
          tail_str.append(byt);
          byt = *t;
        } while ((uint8_t)(byt - 15) > 16);
        if (byt == 15)
          return;
        get_tail_suffix(t, data, tail_ptr, tail_str);
        return;
      }
      get_bin_data(t, tail_str);
    }
};

class tail_ptr_flat_map : public tail_ptr_map {
  private:
    // __fq1 __fq2 tail_ptr_flat_map(tail_ptr_flat_map const&);
    // __fq1 __fq2 tail_ptr_flat_map& operator=(tail_ptr_flat_map const&);
  public:
    gen::int_bv_reader int_ptr_bv;
    bvlt_rank *tail_lt;
    uint8_t *trie_loc;
    inner_trie_fwd *inner_trie;
    uint8_t *data;
    __fq1 __fq2 tail_ptr_flat_map() {
      inner_trie = nullptr;
    }
    __fq1 __fq2 ~tail_ptr_flat_map() {
      if (inner_trie != nullptr)
        delete inner_trie;
    }
    __fq1 __fq2 void init(inner_trie_fwd *_dict_obj, bvlt_rank *_tail_lt, uint8_t *_trie_loc, uint8_t *tail_loc) {
      tail_lt = _tail_lt;
      trie_loc = _trie_loc;
      uint8_t encoding_type = tail_loc[TV_ENC_TYPE_LOC];
      uint8_t *data_loc = tail_loc + cmn::read_uint64(tail_loc + TV_GRP_DATA_LOC);
      uint8_t *ptrs_loc = tail_loc + cmn::read_uint64(tail_loc + TV_GRP_PTRS_LOC);

      uint8_t ptr_lt_ptr_width = tail_loc[TV_PLT_PTR_WIDTH_LOC];
      inner_trie = nullptr;
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        inner_trie = _dict_obj->new_instance(data_loc);
        int_ptr_bv.init(ptrs_loc, ptr_lt_ptr_width);
      } else {
        uint8_t group_count = data_loc[TVG_GRP_COUNT_LOC];
        if (group_count == 1)
          int_ptr_bv.init(ptrs_loc, data_loc[TVG_PTR_WIDTH_LOC]);
        uint8_t *data_idx_start = data_loc + 8 + 512;
        data = tail_loc + cmn::read_uint64(data_idx_start);
      }
    }
    __fq1 __fq2 inline uintxx_t get_tail_ptr(uintxx_t node_id, uintxx_t& ptr_bit_count) {
      if (ptr_bit_count == UINTXX_MAX)
        ptr_bit_count = tail_lt->rank1(node_id);
      return (int_ptr_bv[ptr_bit_count++] << 8) | trie_loc[node_id];
    }
    __fq1 __fq2 inline bool compare_tail(uintxx_t node_id, input_ctx& in_ctx, uintxx_t& ptr_bit_count) {
      uintxx_t tail_ptr = get_tail_ptr(node_id, ptr_bit_count);
      if (inner_trie != nullptr)
        return inner_trie->compare_trie_tail(tail_ptr, in_ctx);
      return compare_tail_data(data, tail_ptr, in_ctx);
    }
    __fq1 __fq2 inline void get_tail_str(uintxx_t node_id, gen::byte_str& tail_str) {
      uintxx_t tail_ptr = UINTXX_MAX; // avoid a stack entry
      tail_ptr = get_tail_ptr(node_id, tail_ptr);
      if (inner_trie != nullptr) {
        inner_trie->copy_trie_tail(tail_ptr, tail_str);
        return;
      }
      get_tail_data(data, tail_ptr, tail_str);
    }
};

class tail_ptr_group_map : public tail_ptr_map, public ptr_group_map {
  private:
    // __fq1 __fq2 tail_ptr_group_map(tail_ptr_group_map const&);
    // __fq1 __fq2 tail_ptr_group_map& operator=(tail_ptr_group_map const&);
  public:
    __fq1 __fq2 void scan_ptr_bits_tail(uintxx_t node_id, uintxx_t& ptr_bit_count) {
      uintxx_t node_id_from = node_id - (node_id % nodes_per_ptr_block_n);
      uint64_t bm_ptr = ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * multiplier];
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      while (node_id_from < node_id) {
        if (bm_mask & bm_ptr)
          ptr_bit_count += code_lt_bit_len[trie_loc[node_id_from]];
        node_id_from++;
        bm_mask <<= 1;
      }
    }
    __fq1 __fq2 void get_ptr_bit_count_tail(uintxx_t node_id, uintxx_t& ptr_bit_count) {
      ptr_bit_count = ptr_reader.get_ptr_block2(node_id);
      scan_ptr_bits_tail(node_id, ptr_bit_count);
    }
    __fq1 __fq2 uintxx_t get_tail_ptr(uintxx_t node_id, uintxx_t& ptr_bit_count, uint8_t& grp_no) {
      uint8_t node_byte = trie_loc[node_id];
      uint8_t code_len = code_lt_code_len[node_byte];
      grp_no = code_len & 0x0F;
      code_len >>= 4;
      uintxx_t ptr = node_byte & (0xFF >> code_len);
      uint8_t bit_len = code_lt_bit_len[node_byte];
      if (bit_len > 0) {
        if (ptr_bit_count == UINTXX_MAX)
          get_ptr_bit_count_tail(node_id, ptr_bit_count);
        ptr |= (ptr_reader.read(ptr_bit_count, bit_len) << (8 - code_len));
      }
      if (grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      // printf("Grp: %u, ptr: %u\n", grp_no, ptr);
      return ptr;
    }
    __fq1 __fq2 bool compare_tail(uintxx_t node_id, input_ctx& in_ctx, uintxx_t& ptr_bit_count) {
      uint8_t grp_no;
      uintxx_t tail_ptr = get_tail_ptr(node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_data[grp_no];
      if (*tail != 0)
        return inner_tries[grp_no]->compare_trie_tail(tail_ptr, in_ctx);
      return compare_tail_data(tail, tail_ptr, in_ctx);
    }
    __fq1 __fq2 void get_tail_str(uintxx_t node_id, gen::byte_str& tail_str) {
      //ptr_bit_count = UINTXX_MAX;
      uint8_t grp_no;
      uintxx_t tail_ptr = UINTXX_MAX; // avoid a stack entry
      tail_ptr = get_tail_ptr(node_id, tail_ptr, grp_no);
      uint8_t *tail = grp_data[grp_no];
      if (*tail != 0) {
        inner_tries[grp_no]->copy_trie_tail(tail_ptr, tail_str);
        return;
      }
      get_tail_data(tail, tail_ptr, tail_str);
    }
    __fq1 __fq2 tail_ptr_group_map() {
    }
};

}}

#endif
