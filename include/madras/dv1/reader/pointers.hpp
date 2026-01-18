#ifndef __DV1_READER_POINTERS_HPP
#define __DV1_READER_POINTERS_HPP

#include "cmn.hpp"
#include "interfaces.hpp"
#include "lt_builder.hpp"

namespace madras { namespace dv1 {

class ptr_bits_reader {
  protected:
    // __fq1 __fq2 ptr_bits_reader(ptr_bits_reader const&);
    // __fq1 __fq2 ptr_bits_reader& operator=(ptr_bits_reader const&);
    uint8_t *ptrs_loc;
    uint8_t *ptr_lt_loc;
    uintxx_t lt_ptr_width;
    bool release_lt_loc;
  public:
    __fq1 __fq2 uintxx_t read(uintxx_t& ptr_bit_count, int bits_to_read) {
      uint64_t *ptr_loc = (uint64_t *) ptrs_loc + ptr_bit_count / 64;
      int bit_pos = (ptr_bit_count % 64);
      uint64_t ret = (*ptr_loc++ << bit_pos);
      ptr_bit_count += static_cast<int>(bits_to_read);
      if (bit_pos + bits_to_read <= 64)
        return (uintxx_t) (ret >> (64 - bits_to_read));
      return (uintxx_t) ((ret | (*ptr_loc >> (64 - bit_pos))) >> (64 - bits_to_read));
    }
    __fq1 __fq2 uint8_t read8(uintxx_t ptr_bit_count) {
      uint64_t *ptr_loc = (uint64_t *) ptrs_loc + ptr_bit_count / 64;
      int bit_pos = (ptr_bit_count % 64);
      if (bit_pos <= 56)
        return *ptr_loc >> (56 - bit_pos);
      uint8_t ret = *ptr_loc++ << (bit_pos - 56);
      return ret | (*ptr_loc >> (64 - (bit_pos % 8)));
    }
    __fq1 __fq2 uintxx_t get_ptr_block2(uintxx_t node_id) {
      uint8_t *block_ptr = ptr_lt_loc + (node_id / nodes_per_ptr_block) * (ptr_lt_blk_width2 + lt_ptr_width);
      uintxx_t ptr_bit_count = gen::read_uint64(block_ptr) & (UINT64_MAX >> (64 - lt_ptr_width * 8));
      // memcpy(&ptr_bit_count, block_ptr, lt_ptr_width); // improve perf
      uintxx_t pos = (node_id / nodes_per_ptr_block_n) % (nodes_per_ptr_block / nodes_per_ptr_block_n);
      if (pos)
        ptr_bit_count += cmn::read_uint16(block_ptr + lt_ptr_width + --pos * 2);
      return ptr_bit_count;
    }
    __fq1 __fq2 uintxx_t get_ptr_block3(uintxx_t node_id) {
      uint8_t *block_ptr = ptr_lt_loc + (node_id / nodes_per_ptr_block) * (ptr_lt_blk_width3 + lt_ptr_width);
      uintxx_t ptr_bit_count = gen::read_uint64(block_ptr) & (UINT64_MAX >> (64 - lt_ptr_width * 8));
      uintxx_t pos = (node_id / nodes_per_ptr_block_n) % (nodes_per_ptr_block / nodes_per_ptr_block_n);
      if (pos) {
        uintxx_t pbc_block_n = cmn::read_uint24(block_ptr + lt_ptr_width + --pos * 3);
        ptr_bit_count += pbc_block_n;
      }
      return ptr_bit_count;
    }
    __fq1 __fq2 ptr_bits_reader() {
      release_lt_loc = false;
    }
    __fq1 __fq2 ~ptr_bits_reader() {
      if (release_lt_loc)
        delete [] ptr_lt_loc;
    }
    __fq1 __fq2 void init(uint8_t *_ptrs_loc, uint8_t *_lt_loc, uintxx_t _lt_ptr_width, bool _release_lt_loc) {
      ptrs_loc = _ptrs_loc;
      ptr_lt_loc = _lt_loc;
      lt_ptr_width = _lt_ptr_width;
      release_lt_loc = _release_lt_loc;
    }
    __fq1 __fq2 uint8_t *get_ptrs_loc() {
      return ptrs_loc;
    }
};

class ptr_group_map {
  protected:
    uint8_t *code_lt_bit_len;
    uint8_t *code_lt_code_len;
    uint8_t **grp_data;
    ptr_bits_reader ptr_reader;

    uint8_t *trie_loc;
    uint64_t *ptr_bm_loc;

    uintxx_t *idx_map_arr = nullptr;
    uint8_t *idx2_ptrs_map_loc;
    __fq1 __fq2 uintxx_t read_ptr_from_idx(uintxx_t grp_no, uintxx_t ptr) {
      return cmn::read_uint24(idx2_ptrs_map_loc + idx_map_arr[grp_no] + ptr * 3);
      //ptr = cmn::read_uintx(idx2_ptrs_map_loc + idx_map_start + ptr * idx2_ptr_size, idx_ptr_mask);
    }

  public:
    uint8_t data_type;
    uint8_t encoding_type;
    uint8_t flags;
    uint8_t inner_trie_start_grp;
    uint8_t group_count;
    uint8_t rpt_grp_no;
    int8_t grp_idx_limit;
    uintxx_t max_len;
    inner_trie_fwd *dict_obj;
    inner_trie_fwd **inner_tries;

    ptr_bits_reader *get_ptr_reader() {
      return &ptr_reader;
    }

    uint8_t *get_grp_data(size_t grp_no) {
      return grp_data[grp_no];
    }

    __fq1 __fq2 ptr_group_map() {
      trie_loc = nullptr;
      grp_data = nullptr;
      inner_tries = nullptr;
      max_len = 0;
    }

    __fq1 __fq2 virtual ~ptr_group_map() {
      if (grp_data != nullptr)
        delete [] grp_data;
      if (inner_tries != nullptr) {
        for (int i = inner_trie_start_grp; i < group_count; i++) {
          if (inner_tries[i] != nullptr)
            delete inner_tries[i];
        }
        delete [] inner_tries;
      }
      if (idx_map_arr != nullptr)
        delete [] idx_map_arr;
    }

    __fq1 __fq2 void init_ptr_grp_map(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint64_t *_tf_ptr_loc,
                uint8_t *data_loc, uintxx_t _key_count, uintxx_t _node_count, bool is_tail) {

      dict_obj = _dict_obj;
      trie_loc = _trie_loc;

      ptr_bm_loc = _tf_ptr_loc;

      data_type = data_loc[TV_DATA_TYPE_LOC];
      encoding_type = data_loc[TV_ENC_TYPE_LOC];
      rpt_grp_no = data_loc[TV_RPT_GRP_NO_LOC];
      flags = data_loc[TV_FLAGS_LOC];
      max_len = cmn::read_uint64(data_loc + TV_MAX_LEN_LOC);
      uint8_t *ptr_lt_loc = data_loc + cmn::read_uint64(data_loc + TV_PLT_LOC);

      uint8_t *grp_data_loc = data_loc + cmn::read_uint64(data_loc + TV_GRP_DATA_LOC);
      // uintxx_t idx2_ptr_count = cmn::read_uint32(data_loc + 32);
      // idx2_ptr_size = idx2_ptr_count & 0x80000000 ? 3 : 2;
      // idx_ptr_mask = idx2_ptr_size == 3 ? 0x00FFFFFF : 0x0000FFFF;
      uint8_t start_bits = data_loc[TV_START_BITS_LOC];
      grp_idx_limit = data_loc[TV_IDX_LIMIT_LOC];
      uint8_t idx_step_bits = data_loc[TV_STEP_BITS_IDX_LOC];
      idx2_ptrs_map_loc = data_loc + cmn::read_uint64(data_loc + TV_IDX2_PTR_MAP_LOC);

      uint8_t *ptrs_loc = data_loc + cmn::read_uint64(data_loc + TV_GRP_PTRS_LOC);

      uint8_t ptr_lt_ptr_width = data_loc[TV_PLT_PTR_WIDTH_LOC];
      bool release_ptr_lt = false;
      if (encoding_type != MSE_TRIE && encoding_type != MSE_TRIE_2WAY) {
        group_count = grp_data_loc[TVG_GRP_COUNT_LOC];
        inner_trie_start_grp = grp_data_loc[TVG_INNER_TRIE_START_GRP_LOC];
        code_lt_bit_len = grp_data_loc + 8;
        code_lt_code_len = code_lt_bit_len + 256;
        uint8_t *grp_data_idx_start = code_lt_bit_len + 512;
        grp_data = new uint8_t*[group_count]();
        inner_tries = new inner_trie_fwd*[group_count]();
        for (int i = 0; i < group_count; i++) {
          grp_data[i] = data_loc;
          grp_data[i] += cmn::read_uint64(grp_data_idx_start + i * 8);
          if (*(grp_data[i]) != 0)
            inner_tries[i] = dict_obj->new_instance(grp_data[i]);
        }
        int _start_bits = start_bits;
        idx_map_arr = new uintxx_t[grp_idx_limit + 1]();
        for (int i = 1; i <= grp_idx_limit; i++) {
          idx_map_arr[i] = idx_map_arr[i - 1] + (1 << _start_bits) * 3;
          _start_bits += idx_step_bits;
        }
        if (ptr_lt_loc == data_loc && group_count > 1) {
          ptr_lt_loc = lt_builder::create_ptr_lt(trie_loc, _bm_loc, 1, _tf_ptr_loc, ptrs_loc, _key_count, _node_count, code_lt_bit_len, is_tail, ptr_lt_ptr_width, _dict_obj->trie_level);
          release_ptr_lt = true;
        }
        ptr_reader.init(ptrs_loc, ptr_lt_loc, ptr_lt_ptr_width, release_ptr_lt);
      }

    }

};

}}

#endif
