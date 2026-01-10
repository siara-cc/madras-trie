#ifndef __DV1_READER_LT_BUILDER__
#define __DV1_READER_LT_BUILDER__

#include "cmn.hpp"

namespace madras_dv1 {

    class lt_builder {
  private:
    // __fq1 __fq2 lt_builder(lt_builder const&);
    // __fq1 __fq2 lt_builder& operator=(lt_builder const&);
    __fq1 __fq2 static uint8_t read8(uint8_t *ptrs_loc, uintxx_t& ptr_bit_count) {
      uint64_t *ptr_loc = (uint64_t *) ptrs_loc + ptr_bit_count / 64;
      int bit_pos = (ptr_bit_count % 64);
      if (bit_pos <= 56)
        return *ptr_loc >> (56 - bit_pos);
      uint8_t ret = *ptr_loc++ << (bit_pos - 56);
      return ret | (*ptr_loc >> (64 - (bit_pos % 8)));
    }
  public:
    __fq1 __fq2 static uint8_t *create_ptr_lt(uint8_t *trie_loc, uint64_t *bm_loc, uint8_t multiplier, uint64_t *bm_ptr_loc, uint8_t *ptrs_loc, uintxx_t key_count, uintxx_t node_count, uint8_t *code_lt_bit_len, bool is_tail, uint8_t ptr_lt_ptr_width, uint8_t trie_level) {
      uintxx_t node_id = 0;
      uintxx_t bit_count = 0;
      uintxx_t bit_count4 = 0;
      size_t pos4 = 0;
      size_t u16_arr_count = (nodes_per_ptr_block / nodes_per_ptr_block_n);
      u16_arr_count--;
      uint16_t *bit_counts = new uint16_t[u16_arr_count + 1];
      memset(bit_counts, 0, u16_arr_count * 2 + 2);
      size_t lt_size = gen::get_lkup_tbl_size2(node_count, nodes_per_ptr_block, ptr_lt_ptr_width + (nodes_per_ptr_block / nodes_per_ptr_block_n - 1) * 2);
      uint8_t *ptr_lt_begin = new uint8_t[lt_size];
      uint8_t *ptr_lt_loc = ptr_lt_begin;
      uint64_t bm_mask, bm_ptr;
      bm_mask = bm_init_mask << node_id;
      bm_ptr = UINT64_MAX;
      uintxx_t ptr_bit_count = 0;
      if (ptr_lt_ptr_width == 4) {
        gen::copy_uint32(bit_count, ptr_lt_loc); ptr_lt_loc += 4;
      } else {
        gen::copy_uint24(bit_count, ptr_lt_loc); ptr_lt_loc += 3;
      }
      do {
        if ((node_id % nodes_per_bv_block_n) == 0) {
          bm_mask = bm_init_mask;
          if (is_tail) {
            if (trie_level == 0)
              bm_ptr = bm_loc[(node_id / nodes_per_bv_block_n) * multiplier];
            else
              bm_ptr = bm_ptr_loc[node_id / nodes_per_bv_block_n];
          } else {
            if (key_count > 0)
              bm_ptr = bm_ptr_loc[(node_id / nodes_per_bv_block_n) * multiplier];
          }
        }
        if (node_id && (node_id % nodes_per_ptr_block_n) == 0) {
          if (bit_count4 > 65535)
            printf("UNEXPECTED: PTR_LOOKUP_TBL bit_count3 > 65k\n");
          bit_counts[pos4] = bit_count4;
          pos4++;
        }
        if (node_id && (node_id % nodes_per_ptr_block) == 0) {
          for (size_t j = 0; j < u16_arr_count; j++) {
            gen::copy_uint16(bit_counts[j], ptr_lt_loc);
            ptr_lt_loc += 2;
          }
          bit_count += bit_counts[u16_arr_count];
          if (ptr_lt_ptr_width == 4) {
            gen::copy_uint32(bit_count, ptr_lt_loc); ptr_lt_loc += 4;
          } else {
            gen::copy_uint24(bit_count, ptr_lt_loc); ptr_lt_loc += 3;
          }
          bit_count4 = 0;
          pos4 = 0;
          memset(bit_counts, '\0', u16_arr_count * 2 + 2);
        }
        if (is_tail) {
          if (bm_mask & bm_ptr)
            bit_count4 += code_lt_bit_len[trie_loc[node_id]];
        } else {
          if (bm_mask & bm_ptr) {
            uint8_t code = read8(ptrs_loc, ptr_bit_count);
            ptr_bit_count += code_lt_bit_len[code];
            bit_count4 += code_lt_bit_len[code];
          }
        }
        node_id++;
        bm_mask <<= 1;
      } while (node_id < node_count);
      for (size_t j = 0; j < u16_arr_count; j++) {
        gen::copy_uint16(bit_counts[j], ptr_lt_loc);
        ptr_lt_loc += 2;
      }
      bit_count += bit_counts[u16_arr_count];
      if (ptr_lt_ptr_width == 4) {
        gen::copy_uint32(bit_count, ptr_lt_loc); ptr_lt_loc += 4;
      } else {
        gen::copy_uint24(bit_count, ptr_lt_loc); ptr_lt_loc += 3;
      }
      for (size_t j = 0; j < u16_arr_count; j++) {
        gen::copy_uint16(bit_counts[j], ptr_lt_loc);
        ptr_lt_loc += 2;
      }
      delete [] bit_counts;
      return ptr_lt_begin;
    }
    __fq1 __fq2 static uint8_t *write_bv3(uintxx_t node_id, uintxx_t& count, uintxx_t& count3, uint8_t *buf3, uint8_t& pos3, uint8_t *lt_pos) {
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
    __fq1 __fq2 static uint8_t *create_rank_lt_from_trie(int lt_type, uintxx_t node_count, uint64_t *bm_flags_loc) {
      uintxx_t node_id = 0;
      uintxx_t count = 0;
      uintxx_t count3 = 0;
      uint8_t buf3[3];
      uint8_t pos3 = 0;
      memset(buf3, 0, 3);
      size_t lt_size = gen::get_lkup_tbl_size2(node_count, nodes_per_bv_block, 7);
      uint8_t *lt_pos = new uint8_t[lt_size];
      uint8_t *lt_rank_loc = lt_pos;
      uint64_t bm_flags, bm_mask;
      bm_mask = bm_init_mask << node_id;
      bm_flags = bm_flags_loc[node_id / nodes_per_bv_block_n];
      gen::copy_uint32(0, lt_pos); lt_pos += 4;
      do {
        if ((node_id % nodes_per_bv_block_n) == 0) {
          bm_mask = bm_init_mask;
          bm_flags = bm_flags_loc[node_id / nodes_per_bv_block_n];
        }
        lt_pos = write_bv3(node_id, count, count3, buf3, pos3, lt_pos);
        uintxx_t ct = (bm_mask & bm_flags ? 1 : 0);
        count += ct;
        count3 += ct;
        node_id++;
        bm_mask <<= 1;
      } while (node_id < node_count);
      memcpy(lt_pos, buf3, 3); lt_pos += 3;
      // extra (guard)
      gen::copy_uint32(count, lt_pos); lt_pos += 4;
      memcpy(lt_pos, buf3, 3); lt_pos += 3;
      return lt_rank_loc;
    }
    __fq1 __fq2 static uint8_t *create_select_lt_from_trie(int lt_type, uintxx_t key_count, uintxx_t node_set_count, uintxx_t node_count, uint64_t *bm_flags_loc) {
      uintxx_t node_id = 0;
      uintxx_t sel_count = 0;
      size_t lt_size = 0;
      if (lt_type == BV_LT_TYPE_LEAF)
        lt_size = gen::get_lkup_tbl_size2(key_count, sel_divisor, 3);
      else if (lt_type == BV_LT_TYPE_CHILD) {
        lt_size = 0;
        if (node_set_count > 1)
          lt_size = gen::get_lkup_tbl_size2(node_set_count - 1, sel_divisor, 3);
      } else
        lt_size = gen::get_lkup_tbl_size2(node_set_count, sel_divisor, 3);

      uint8_t *lt_pos = new uint8_t[lt_size];
      uint8_t *lt_sel_loc = lt_pos;
      uint64_t bm_flags, bm_mask;
      bm_mask = bm_init_mask << node_id;
      bm_flags = bm_flags_loc[node_id / nodes_per_bv_block_n];
      gen::copy_uint24(0, lt_pos); lt_pos += 3;
      do {
        if ((node_id % nodes_per_bv_block_n) == 0) {
          bm_mask = bm_init_mask;
          bm_flags = bm_flags_loc[node_id / nodes_per_bv_block_n];
        }
        bool node_qualifies_for_select = (bm_mask & bm_flags) ? true : false;
        if (node_qualifies_for_select) {
          if (sel_count && (sel_count % sel_divisor) == 0) {
            uintxx_t val_to_write = node_id / nodes_per_bv_block;
            gen::copy_uint24(val_to_write, lt_pos); lt_pos += 3;
            if (val_to_write > (1 << 24))
              printf("WARNING: %" PRIuXX "\t%" PRIuXX "\n", sel_count, val_to_write);
          }
          sel_count++;
        }
        node_id++;
        bm_mask <<= 1;
      } while (node_id < node_count);
      gen::copy_uint24(node_count / nodes_per_bv_block, lt_pos);
      return lt_sel_loc;
    }
};

}

#endif
