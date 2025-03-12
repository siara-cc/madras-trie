#ifndef STATIC_TRIE_H
#define STATIC_TRIE_H

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/stat.h> 

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>
#include <nmmintrin.h>
#endif
#ifndef _WIN32
#include <sys/mman.h>
#endif
#include <stdint.h>

#include "common_dv1.hpp"
#include "../../ds_common/src/bv.hpp"
#include "../../ds_common/src/vint.hpp"
#include "../../ds_common/src/gen.hpp"

#include "../../flavic48/src/flavic48.hpp"

// Function qualifiers
#ifndef __fq1
#define __fq1
#endif

#ifndef __fq2
#define __fq2
#endif

// Attribute qualifiers
#ifndef __gq1
#define __gq1
#endif

#ifndef __gq2
#define __gq2
#endif

namespace madras_dv1 {

#define TF_PTR 0
#define TF_TERM 1
#define TF_CHILD 2
#define TF_LEAF 3

class iter_ctx {
  private:
    // __fq1 __fq2 iter_ctx(iter_ctx const&);
    // __fq1 __fq2 iter_ctx& operator=(iter_ctx const&);
  public:
    int32_t cur_idx;
    uint16_t key_len;
    uint8_t *key;
    uint32_t *node_path;
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
        node_path = new uint32_t[max_level];
        last_tail_len = new uint16_t[max_level];
      }
      memset(node_path, 0, max_level * sizeof(uint32_t));
      memset(last_tail_len, 0, max_level * sizeof(uint16_t));
      node_path[0] = 1;
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

struct min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
};

class cmn {
  public:
    __fq1 __fq2 static uint32_t read_uint16(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      return *((uint16_t *) ptr); // faster endian dependent
      #else
      uint32_t ret = *ptr++;
      ret |= (*ptr << 8);
      return ret;
      #endif
    }
    __fq1 __fq2 static uint32_t read_uint24(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      return *((uint32_t *) ptr) & 0x00FFFFFF; // faster endian dependent
      #else
      uint32_t ret = *ptr++;
      ret |= (*ptr++ << 8);
      ret |= (*ptr << 16);
      return ret;
      #endif
    }
    __fq1 __fq2 static uint32_t read_uint32(uint8_t *ptr) {
      // #ifndef __CUDA_ARCH__
      return *((uint32_t *) ptr);
      // TODO: fix
      // #else
      // uint32_t ret = *ptr++;
      // ret |= (*ptr++ << 8);
      // ret |= (*ptr++ << 16);
      // ret |= (*ptr << 24);
      // return ret;
      // #endif
    }
    __fq1 __fq2 static uint64_t read_uint64(uint8_t *ptr) {
      // #ifndef __CUDA_ARCH__
      return *((uint64_t *) ptr);
      // #else
      // uint64_t ret = *ptr++;
      // ret |= (*ptr++ << 8);
      // ret |= (*ptr++ << 16);
      // ret |= (*ptr++ << 24);
      // ret |= (*ptr++ << 32);
      // ret |= (*ptr++ << 40);
      // ret |= (*ptr++ << 48);
      // ret |= (*ptr << 56);
      // return ret;
      // #endif
    }
    __fq1 __fq2 static int memcmp(const void *ptr1, const void *ptr2, size_t num) {
      #ifndef __CUDA_ARCH__
      return std::memcmp(ptr1, ptr2, num);
      #else
      const unsigned char *a = (const unsigned char *)ptr1;
      const unsigned char *b = (const unsigned char *)ptr2;
      for (size_t i = 0; i < num; ++i) {
          if (a[i] != b[i]) {
              return (a[i] < b[i]) ? -1 : 1;
          }
      }
      return 0;
      #endif
    }
    __fq1 __fq2 static uint32_t read_vint32(const uint8_t *ptr, size_t *vlen = NULL) {
      uint32_t ret = 0;
      size_t len = 5; // read max 5 bytes
      do {
        ret <<= 7;
        ret += *ptr & 0x7F;
        len--;
      } while ((*ptr++ >> 7) && len);
      if (vlen != NULL)
        *vlen = 5 - len;
      return ret;
    }
};

class lt_builder {
  private:
    // __fq1 __fq2 lt_builder(lt_builder const&);
    // __fq1 __fq2 lt_builder& operator=(lt_builder const&);
    __fq1 __fq2 static uint8_t read8(uint8_t *ptrs_loc, uint32_t& ptr_bit_count) {
      uint64_t *ptr_loc = (uint64_t *) ptrs_loc + ptr_bit_count / 64;
      int bit_pos = (ptr_bit_count % 64);
      if (bit_pos <= 56)
        return *ptr_loc >> (56 - bit_pos);
      uint8_t ret = *ptr_loc++ << (bit_pos - 56);
      return ret | (*ptr_loc >> (64 - (bit_pos % 8)));
    }
  public:
    __fq1 __fq2 static uint8_t *create_ptr_lt(uint8_t *trie_loc, uint64_t *bm_loc, uint8_t multiplier, uint64_t *bm_ptr_loc, uint8_t *ptrs_loc, uint32_t key_count, uint32_t node_count, uint8_t *code_lt_bit_len, bool is_tail, uint8_t ptr_lt_ptr_width, uint8_t trie_level) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      uint32_t bit_count4 = 0;
      size_t pos4 = 0;
      size_t u16_arr_count = (nodes_per_ptr_block / nodes_per_ptr_block_n);
      u16_arr_count--;
      uint16_t *bit_counts = new uint16_t[u16_arr_count + 1];
      memset(bit_counts, 0, u16_arr_count * 2 + 2);
      uint32_t lt_size = gen::get_lkup_tbl_size2(node_count, nodes_per_ptr_block, ptr_lt_ptr_width + (nodes_per_ptr_block / nodes_per_ptr_block_n - 1) * 2);
      uint8_t *ptr_lt_begin = new uint8_t[lt_size];
      uint8_t *ptr_lt_loc = ptr_lt_begin;
      uint64_t bm_mask, bm_ptr;
      bm_mask = bm_init_mask << node_id;
      bm_ptr = UINT64_MAX;
      uint32_t ptr_bit_count = 0;
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
    __fq1 __fq2 static uint8_t *write_bv3(uint32_t node_id, uint32_t& count, uint32_t& count3, uint8_t *buf3, uint8_t& pos3, uint8_t *lt_pos) {
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
    __fq1 __fq2 static uint8_t *create_rank_lt_from_trie(int lt_type, uint32_t node_count, uint64_t *bm_flags_loc) {
      uint32_t node_id = 0;
      uint32_t count = 0;
      uint32_t count3 = 0;
      uint8_t buf3[3];
      uint8_t pos3 = 0;
      memset(buf3, 0, 3);
      uint32_t lt_size = gen::get_lkup_tbl_size2(node_count, nodes_per_bv_block, 7);
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
        uint32_t ct = (bm_mask & bm_flags ? 1 : 0);
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
    __fq1 __fq2 static uint8_t *create_select_lt_from_trie(int lt_type, uint32_t key_count, uint32_t node_set_count, uint32_t node_count, uint64_t *bm_flags_loc) {
      uint32_t node_id = 0;
      uint32_t sel_count = 0;
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
            uint32_t val_to_write = node_id / nodes_per_bv_block;
            gen::copy_uint24(val_to_write, lt_pos); lt_pos += 3;
            if (val_to_write > (1 << 24))
              printf("WARNING: %u\t%u\n", sel_count, val_to_write);
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

__gq1 __gq2 static const uint8_t bit_count[256] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
__gq1 __gq2 static const uint8_t select_lookup_tbl[8][256] = {{
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

class leapfrog {
  private:
    // __fq1 __fq2 leapfrog(leapfrog const&);
    // __fq1 __fq2 leapfrog& operator=(leapfrog const&);
  public:
    __fq1 __fq2 leapfrog() {};
    __fq1 __fq2 virtual ~leapfrog() {};
    __fq1 __fq2 virtual void find_pos(uint32_t& node_id, const uint8_t *trie_loc, uint8_t key_byte) = 0;
};

class leapfrog_asc : public leapfrog {
  private:
    // __fq1 __fq2 leapfrog_asc(leapfrog_asc const&);
    // __fq1 __fq2 leapfrog_asc& operator=(leapfrog_asc const&);
    uint8_t *min_pos_loc;
    min_pos_stats min_stats;
  public:
    __fq1 __fq2 void find_pos(uint32_t& node_id, const uint8_t *trie_loc, uint8_t key_byte) {
      uint8_t min_offset = min_pos_loc[((trie_loc[node_id] - min_stats.min_len) * 256) + key_byte];
      node_id += min_offset;
    }
    __fq1 __fq2 leapfrog_asc() {
    }
    __fq1 __fq2 void init(min_pos_stats _stats, uint8_t *_min_pos_loc) {
      min_stats = _stats;
      min_pos_loc = _min_pos_loc;
    }
};

class leapfrog_rnd : public leapfrog {
  private:
    // __fq1 __fq2 leapfrog_rnd(leapfrog_rnd const&);
    // __fq1 __fq2 leapfrog_rnd& operator=(leapfrog_rnd const&);
  public:
    __fq1 __fq2 void find_pos(uint32_t& node_id, const uint8_t *trie_loc, uint8_t key_byte) {
        int len = trie_loc[node_id++];
      #if defined(__SSE4_2__)
        __m128i to_locate = _mm_set1_epi8(key_byte);
        node_id -= 16;
        do {
          node_id += 16;
          __m128i segment = _mm_loadu_si128((__m128i *)(trie_loc + node_id));
          int pos = _mm_cmpestri(to_locate, 16, segment, 16, _SIDD_CMP_EQUAL_EACH);
          if (pos < (len < 16 ? len : 16)) {
            node_id += pos;
            return;
          }
          len -= 16;
        } while (len > 0);
        // node_id -= 32;
        // do {
        //   node_id += 32;
        //   int bitfield = _mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_set1_epi8(key_byte),
        //                           _mm256_loadu_si256((__m256i *)(trie_loc + node_id))));
        //   if (bitfield) {
        //     int pos = __builtin_ctz(bitfield);
        //     if (pos < len) {
        //       node_id += pos;
        //       return;
        //     }
        //   }
        //   len -= 32;
        // } while (len > 0);
      #else
        while (len-- && trie_loc[node_id] != key_byte)
            node_id++;
      #endif
    }
    __fq1 __fq2 leapfrog_rnd() {
    }
    __fq1 __fq2 void init() {
    }
};

class bvlt_rank {
  private:
    // __fq1 __fq2 bvlt_rank(bvlt_rank const&);
    // __fq1 __fq2 bvlt_rank& operator=(bvlt_rank const&);
  protected:
    uint64_t *bm_loc;
    uint8_t *lt_rank_loc;
    uint8_t multiplier;
    uint8_t lt_width;
  public:
    __fq1 __fq2 bool operator[](size_t pos) {
      return ((bm_loc[multiplier * (pos / 64)] >> (pos % 64)) & 1) != 0;
    }
    __fq1 __fq2 bool is_set1(size_t pos) {
      return ((bm_loc[pos / 64] >> (pos % 64)) & 1) != 0;
    }
    __fq1 __fq2 uint32_t rank1(uint32_t bv_pos) {
      uint8_t *rank_ptr = lt_rank_loc + bv_pos / nodes_per_bv_block * lt_width;
      uint32_t rank = cmn::read_uint32(rank_ptr);
      #if nodes_per_bv_block == 512
      int pos = (bv_pos / nodes_per_bv_block_n) % width_of_bv_block_n;
      if (pos > 0) {
        rank_ptr += 4;
        //while (pos--)
        //  rank += *rank_ptr++;
        rank += (rank_ptr[pos] + (((uint32_t)(*rank_ptr) << pos) & 0x100));
      }
      #else
      int pos = (bv_pos / nodes_per_bv_block_n) % (width_of_bv_block_n + 1);
      if (pos > 0)
        rank += rank_ptr[4 + pos - 1];
      #endif
      uint64_t mask = (bm_init_mask << (bv_pos % nodes_per_bv_block_n)) - 1;
      uint64_t bm = bm_loc[(bv_pos / nodes_per_bv_block_n) * multiplier];
      // return rank + __popcountdi2(bm & (mask - 1));
      return rank + static_cast<uint32_t>(__builtin_popcountll(bm & mask));
    }
    __fq1 __fq2 uint8_t *get_rank_loc() {
      return lt_rank_loc;
    }
    __fq1 __fq2 bvlt_rank() {
    }
    __fq1 __fq2 void init(uint8_t *_lt_rank_loc, uint64_t *_bm_loc, uint8_t _multiplier, uint8_t _lt_unit_count) {
      lt_rank_loc = _lt_rank_loc;
      bm_loc = _bm_loc;
      multiplier = _multiplier;
      lt_width = _lt_unit_count * width_of_bv_block;
    }
};

struct input_ctx {
  const uint8_t *key;
  uint32_t key_len;
  uint32_t key_pos;
  uint32_t node_id;
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
    __fq1 __fq2 virtual bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) = 0;
    __fq1 __fq2 virtual bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) = 0;
    __fq1 __fq2 virtual inner_trie_fwd *new_instance(uint8_t *mem) = 0;
};

class ptr_bits_reader {
  protected:
    // __fq1 __fq2 ptr_bits_reader(ptr_bits_reader const&);
    // __fq1 __fq2 ptr_bits_reader& operator=(ptr_bits_reader const&);
    uint8_t *ptrs_loc;
    uint8_t *ptr_lt_loc;
    bool release_lt_loc;
  public:
    __fq1 __fq2 uint32_t read(uint32_t& ptr_bit_count, int bits_to_read) {
      uint64_t *ptr_loc = (uint64_t *) ptrs_loc + ptr_bit_count / 64;
      int bit_pos = (ptr_bit_count % 64);
      uint64_t ret = (*ptr_loc++ << bit_pos);
      ptr_bit_count += static_cast<int>(bits_to_read);
      if (bit_pos + bits_to_read <= 64)
        return ret >> (64 - bits_to_read);
      return (ret | (*ptr_loc >> (64 - bit_pos))) >> (64 - bits_to_read);
    }
    __fq1 __fq2 uint8_t read8(uint32_t ptr_bit_count) {
      uint64_t *ptr_loc = (uint64_t *) ptrs_loc + ptr_bit_count / 64;
      int bit_pos = (ptr_bit_count % 64);
      if (bit_pos <= 56)
        return *ptr_loc >> (56 - bit_pos);
      uint8_t ret = *ptr_loc++ << (bit_pos - 56);
      return ret | (*ptr_loc >> (64 - (bit_pos % 8)));
    }
    __fq1 __fq2 uint32_t get_ptr_block_t(uint32_t node_id) {
      uint8_t *block_ptr = ptr_lt_loc + (node_id / nodes_per_ptr_block) * ptr_lt_blk_width;
      uint32_t ptr_bit_count = cmn::read_uint32(block_ptr);
      uint32_t pos = (node_id / nodes_per_ptr_block_n) % (nodes_per_ptr_block / nodes_per_ptr_block_n);
      if (pos)
        ptr_bit_count += cmn::read_uint16(block_ptr + 4 + --pos * 2);
      return ptr_bit_count;
    }
    __fq1 __fq2 ptr_bits_reader() {
      release_lt_loc = false;
    }
    __fq1 __fq2 ~ptr_bits_reader() {
      if (release_lt_loc)
        delete [] ptr_lt_loc;
    }
    __fq1 __fq2 void init(uint8_t *_ptrs_loc, uint8_t *_lt_loc, uint32_t _lt_ptr_width, bool _release_lt_loc) {
      ptrs_loc = _ptrs_loc;
      ptr_lt_loc = _lt_loc;
      release_lt_loc = _release_lt_loc;
    }
    __fq1 __fq2 uint32_t get_ptr_block_t_words(uint32_t node_id) {
      uint8_t *block_ptr = ptr_lt_loc + (node_id / nodes_per_ptr_block) * ptr_lt_blk_width_words;
      uint32_t ptr_bit_count = cmn::read_uint32(block_ptr);
      uint32_t pos = (node_id / nodes_per_ptr_block_n) % (nodes_per_ptr_block / nodes_per_ptr_block_n);
      if (pos)
        ptr_bit_count += cmn::read_uint24(block_ptr + 4 + --pos * 3);
      return ptr_bit_count;
    }
    __fq1 __fq2 uint8_t *get_ptrs_loc() {
      return ptrs_loc;
    }
};

class tail_ptr_map {
  private:
    // __fq1 __fq2 tail_ptr_map(tail_ptr_map const&);
    // __fq1 __fq2 tail_ptr_map& operator=(tail_ptr_map const&);
  public:
    __fq1 __fq2 tail_ptr_map() {
    }
    __fq1 __fq2 virtual ~tail_ptr_map() {
    }
    __fq1 __fq2 virtual bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) = 0;
    __fq1 __fq2 virtual void get_tail_str(uint32_t node_id, gen::byte_str& tail_str) = 0;
    __fq1 __fq2 static uint32_t read_len(uint8_t *t) {
      while (*t > 15 && *t < 32)
        t++;
      t--;
      uint32_t ret;
      read_len_bw(t, ret);
      return ret;
    }
    __fq1 __fq2 static uint8_t *read_len_bw(uint8_t *t, uint32_t& out_len) {
      out_len = 0;
      while (*t > 15 && *t < 32) {
        out_len <<= 3;
        out_len += (*t & 0x07);
        if (*t-- & 0x08)
          break;
      }
      return t;
    }
    __fq1 __fq2 void read_suffix(uint8_t *out_str, uint8_t *t, uint32_t sfx_len) {
      while (*t < 15 || *t > 31)
        t--;
      while (*t != 15) {
        uint32_t prev_sfx_len;
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
    inline __fq1 __fq2 bool compare_tail_data(uint8_t *data, uint32_t tail_ptr, input_ctx& in_ctx) {
      uint8_t *tail = data + tail_ptr;
      if (*tail < 15 || *tail > 31) {
        do {
          if (in_ctx.key_pos >= in_ctx.key_len || *tail != in_ctx.key[in_ctx.key_pos])
            return false;
          tail++;
          in_ctx.key_pos++;
        } while (*tail < 15 || *tail > 31);
        if (*tail == 15)
          return true;
        uint32_t sfx_len = read_len(tail);
        #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
        uint8_t *sfx_buf = new uint8_t[sfx_len];
        #else
        uint8_t sfx_buf[sfx_len];
        #endif
        read_suffix(sfx_buf, data + tail_ptr - 1, sfx_len);
        if (cmn::memcmp(sfx_buf, in_ctx.key + in_ctx.key_pos, sfx_len) == 0) {
          in_ctx.key_pos += sfx_len;
          #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
          delete [] sfx_buf;
          #endif
          return true;
        }
        #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
        delete [] sfx_buf;
        #endif
      } else {
        uint32_t bin_len;
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
      return false;
    }
    __fq1 __fq2 void get_tail_data(uint8_t *data, uint32_t tail_ptr, gen::byte_str& tail_str) {
      uint8_t *t = data + tail_ptr;
      if (*t < 15 || *t > 31) {
        uint8_t byt = *t;
        while (byt < 15 || byt > 31) {
          t++;
          tail_str.append(byt);
          byt = *t;
        }
        if (byt == 15)
          return;
        uint32_t sfx_len = read_len(t);
        read_suffix(tail_str.data() + tail_str.length(), data + tail_ptr - 1, sfx_len);
        tail_str.set_length(tail_str.length() + sfx_len);
      } else {
        uint32_t bin_len;
        read_len_bw(t++, bin_len);
        while (bin_len--)
          tail_str.append(*t++);
      }
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
    uint8_t multiplier;
    uint8_t group_count;
    uint8_t rpt_grp_no;
    int8_t grp_idx_limit;
    uint8_t inner_trie_start_grp;

    uint32_t *idx_map_arr = nullptr;
    uint8_t *idx2_ptrs_map_loc;
    __fq1 __fq2 uint32_t read_ptr_from_idx(uint32_t grp_no, uint32_t ptr) {
      return cmn::read_uint24(idx2_ptrs_map_loc + idx_map_arr[grp_no] + ptr * 3);
      //ptr = cmn::read_uintx(idx2_ptrs_map_loc + idx_map_start + ptr * idx2_ptr_size, idx_ptr_mask);
    }

  public:
    uint8_t data_type;
    uint8_t encoding_type;
    uint8_t flags;
    uint32_t max_len;
    inner_trie_fwd *dict_obj;
    inner_trie_fwd **inner_tries;

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

    __fq1 __fq2 void init_ptr_grp_map(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier, uint64_t *_tf_ptr_loc,
                uint8_t *data_loc, uint32_t _key_count, uint32_t _node_count, bool is_tail) {

      dict_obj = _dict_obj;
      trie_loc = _trie_loc;

      ptr_bm_loc = _multiplier == 1 ? _tf_ptr_loc : _bm_loc;
      multiplier = _multiplier;

      data_type = data_loc[1];
      encoding_type = data_loc[2];
      rpt_grp_no = data_loc[5];
      flags = data_loc[3];
      max_len = cmn::read_uint32(data_loc + 20);
      uint8_t *ptr_lt_loc = data_loc + cmn::read_uint32(data_loc + 24);

      uint8_t *grp_data_loc = data_loc + cmn::read_uint32(data_loc + 28);
      uint32_t idx2_ptr_count = cmn::read_uint32(data_loc + 32);
      // idx2_ptr_size = idx2_ptr_count & 0x80000000 ? 3 : 2;
      // idx_ptr_mask = idx2_ptr_size == 3 ? 0x00FFFFFF : 0x0000FFFF;
      uint8_t start_bits = data_loc[10];
      grp_idx_limit = data_loc[9];
      uint8_t idx_step_bits = data_loc[11];
      idx2_ptrs_map_loc = data_loc + cmn::read_uint32(data_loc + 36);

      uint8_t *ptrs_loc = data_loc + cmn::read_uint32(data_loc + 40);

      uint8_t ptr_lt_ptr_width = data_loc[0];
      bool release_ptr_lt = false;
      if (encoding_type != MSE_TRIE && encoding_type != MSE_TRIE_2WAY) {
        group_count = *grp_data_loc;
        if (*grp_data_loc == 0xA5)
          group_count = 1;
        inner_trie_start_grp = grp_data_loc[1];
        code_lt_bit_len = grp_data_loc + 8;
        code_lt_code_len = code_lt_bit_len + 256;
        uint8_t *grp_data_idx_start = code_lt_bit_len + 512;
        grp_data = new uint8_t*[group_count]();
        inner_tries = new inner_trie_fwd*[group_count]();
        for (int i = 0; i < group_count; i++) {
          grp_data[i] = data_loc;
          grp_data[i] += cmn::read_uint32(grp_data_idx_start + i * 4);
          if (*(grp_data[i]) != 0)
            inner_tries[i] = dict_obj->new_instance(grp_data[i]);
        }
        int _start_bits = start_bits;
        idx_map_arr = new uint32_t[grp_idx_limit + 1]();
        for (int i = 1; i <= grp_idx_limit; i++) {
          idx_map_arr[i] = idx_map_arr[i - 1] + (1 << _start_bits) * 3;
          _start_bits += idx_step_bits;
        }
        if (ptr_lt_loc == data_loc && group_count > 1) {
          ptr_lt_loc = lt_builder::create_ptr_lt(trie_loc, _bm_loc, _multiplier, _tf_ptr_loc, ptrs_loc, _key_count, _node_count, code_lt_bit_len, is_tail, ptr_lt_ptr_width, _dict_obj->trie_level);
          release_ptr_lt = true;
        }
        ptr_reader.init(ptrs_loc, ptr_lt_loc, ptr_lt_ptr_width, release_ptr_lt);
      }

    }

};

class tail_ptr_group_map : public tail_ptr_map, public ptr_group_map{
  private:
    // __fq1 __fq2 tail_ptr_group_map(tail_ptr_group_map const&);
    // __fq1 __fq2 tail_ptr_group_map& operator=(tail_ptr_group_map const&);
  public:
    __fq1 __fq2 void scan_ptr_bits_tail(uint32_t node_id, uint32_t& ptr_bit_count) {
      uint32_t node_id_from = node_id - (node_id % nodes_per_ptr_block_n);
      uint64_t bm_ptr = ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * multiplier];
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      while (node_id_from < node_id) {
        if (bm_mask & bm_ptr)
          ptr_bit_count += code_lt_bit_len[trie_loc[node_id_from]];
        node_id_from++;
        bm_mask <<= 1;
      }
    }
    __fq1 __fq2 void get_ptr_bit_count_tail(uint32_t node_id, uint32_t& ptr_bit_count) {
      ptr_bit_count = ptr_reader.get_ptr_block_t(node_id);
      scan_ptr_bits_tail(node_id, ptr_bit_count);
    }
    __fq1 __fq2 uint32_t get_tail_ptr(uint32_t node_id, uint32_t& ptr_bit_count, uint8_t& grp_no) {
      uint8_t node_byte = trie_loc[node_id];
      uint8_t code_len = code_lt_code_len[node_byte];
      grp_no = code_len & 0x0F;
      code_len >>= 4;
      uint32_t ptr = node_byte & (0xFF >> code_len);
      uint8_t bit_len = code_lt_bit_len[node_byte];
      if (bit_len > 0) {
        if (ptr_bit_count == UINT32_MAX)
          get_ptr_bit_count_tail(node_id, ptr_bit_count);
        ptr |= (ptr_reader.read(ptr_bit_count, bit_len) << (8 - code_len));
      }
      if (grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      // printf("Grp: %u, ptr: %u\n", grp_no, ptr);
      return ptr;
    }
    __fq1 __fq2 bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) {
      uint8_t grp_no;
      uint32_t tail_ptr = get_tail_ptr(node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_data[grp_no];
      if (*tail != 0)
        return inner_tries[grp_no]->compare_trie_tail(tail_ptr, in_ctx);
      return compare_tail_data(tail, tail_ptr, in_ctx);
    }
    __fq1 __fq2 void get_tail_str(uint32_t node_id, gen::byte_str& tail_str) {
      //ptr_bit_count = UINT32_MAX;
      uint8_t grp_no;
      uint32_t tail_ptr = UINT32_MAX; // avoid a stack entry
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
    __fq1 __fq2 virtual ~tail_ptr_flat_map() {
      if (inner_trie != nullptr)
        delete inner_trie;
    }
    __fq1 __fq2 void init(inner_trie_fwd *_dict_obj, bvlt_rank *_tail_lt, uint8_t *_trie_loc, uint8_t *tail_loc) {
      tail_lt = _tail_lt;
      trie_loc = _trie_loc;
      uint8_t encoding_type = tail_loc[2];
      uint8_t *data_loc = tail_loc + cmn::read_uint32(tail_loc + 28);
      uint8_t *ptrs_loc = tail_loc + cmn::read_uint32(tail_loc + 40);

      uint8_t ptr_lt_ptr_width = tail_loc[0];
      inner_trie = nullptr;
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        inner_trie = _dict_obj->new_instance(data_loc);
        int_ptr_bv.init(ptrs_loc, ptr_lt_ptr_width);
      } else {
        uint8_t group_count = *data_loc;
        if (group_count == 1)
          int_ptr_bv.init(ptrs_loc, data_loc[1]);
        uint8_t *data_idx_start = data_loc + 8 + 512;
        data = tail_loc + cmn::read_uint32(data_idx_start);
      }
    }
    __fq1 __fq2 uint32_t get_tail_ptr(uint32_t node_id, uint32_t& ptr_bit_count) {
      if (ptr_bit_count == UINT32_MAX)
        ptr_bit_count = tail_lt->rank1(node_id);
      return (int_ptr_bv[ptr_bit_count++] << 8) | trie_loc[node_id];
    }
    __fq1 __fq2 bool compare_tail(uint32_t node_id, input_ctx& in_ctx, uint32_t& ptr_bit_count) {
      uint32_t tail_ptr = get_tail_ptr(node_id, ptr_bit_count);
      if (inner_trie != nullptr)
        return inner_trie->compare_trie_tail(tail_ptr, in_ctx);
      return compare_tail_data(data, tail_ptr, in_ctx);
    }
    __fq1 __fq2 void get_tail_str(uint32_t node_id, gen::byte_str& tail_str) {
      uint32_t tail_ptr = UINT32_MAX;
      tail_ptr = get_tail_ptr(node_id, tail_ptr); // avoid a stack entry
      if (inner_trie != nullptr) {
        inner_trie->copy_trie_tail(tail_ptr, tail_str);
        return;
      }
      get_tail_data(data, tail_ptr, tail_str);
    }
};

class GCFC_rev_cache {
  private:
    // __fq1 __fq2 GCFC_rev_cache(GCFC_rev_cache const&);
    // __fq1 __fq2 GCFC_rev_cache& operator=(GCFC_rev_cache const&);
    nid_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    __fq1 __fq2 bool try_find(uint32_t& node_id, input_ctx& in_ctx) {
      if (node_id >= max_node_id)
        return false;
      nid_cache *cche = cche0 + (node_id & cache_mask);
      if (node_id == cmn::read_uint24(&cche->child_node_id1)) {
        if (cche->tail0_len == 0)
          node_id = cmn::read_uint24(&cche->parent_node_id1);
        else {
          if (in_ctx.key_pos + cche->tail0_len > in_ctx.key_len)
            return false;
          if (cmn::memcmp(in_ctx.key + in_ctx.key_pos, &cche->parent_node_id1, cche->tail0_len) != 0)
            return false;
          in_ctx.key_pos += cche->tail0_len;
          node_id = 0;
        }
        return true;
      }
      return false;
    }
    __fq1 __fq2 bool try_find(uint32_t& node_id, gen::byte_str& tail_str) {
      if (node_id >= max_node_id)
        return false;
      nid_cache *cche = cche0 + (node_id & cache_mask);
      if (node_id == cmn::read_uint24(&cche->child_node_id1)) {
        if (cche->tail0_len == 0)
          node_id = cmn::read_uint24(&cche->parent_node_id1);
        else {
          tail_str.append(&cche->parent_node_id1, cche->tail0_len);
          node_id = 0;
        }
        return true;
      }
      return false;
    }
    __fq1 __fq2 GCFC_rev_cache() {
    }
    __fq1 __fq2 void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (nid_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class bvlt_select : public bvlt_rank {
  private:
    // __fq1 __fq2 bvlt_select(bvlt_select const&);
    // __fq1 __fq2 bvlt_select& operator=(bvlt_select const&);
  protected:
    uint8_t *lt_sel_loc1;
    uint32_t bv_bit_count;
  public:
    __fq1 __fq2 uint32_t bin_srch_lkup_tbl(uint32_t first, uint32_t last, uint32_t given_count) {
      while (first + 1 < last) {
        const uint32_t middle = (first + last) >> 1;
        if (given_count < cmn::read_uint32(lt_rank_loc + middle * lt_width))
          last = middle;
        else
          first = middle;
      }
      return first;
    }
    __fq1 __fq2 uint32_t select1(uint32_t target_count) {
      if (target_count == 0)
        return 0;
      uint8_t *select_loc = lt_sel_loc1 + target_count / sel_divisor * 3;
      uint32_t block = cmn::read_uint24(select_loc);
      // uint32_t end_block = cmn::read_uint24(select_loc + 3);
      // if (block + 4 < end_block)
      //   block = bin_srch_lkup_tbl(block, end_block, target_count);
      uint8_t *block_loc = lt_rank_loc + block * lt_width;
      while (cmn::read_uint32(block_loc) < target_count) {
        block++;
        block_loc += lt_width;
      }
      block--;
      uint32_t bv_pos = block * nodes_per_bv_block;
      block_loc -= lt_width;
      uint32_t remaining = target_count - cmn::read_uint32(block_loc);
      if (remaining == 0)
        return bv_pos;
      block_loc += 4;
      #if nodes_per_bv_block == 256
      size_t pos_n = 0;
      while (block_loc[pos_n] < remaining && pos_n < width_of_bv_block_n)
        pos_n++;
      if (pos_n > 0) {
        remaining -= block_loc[pos_n - 1];
        bv_pos += (nodes_per_bv_block_n * pos_n);
      }
      #else
      // size_t pos_n = 0;
      // while (block_loc[pos_n] < remaining && pos_n < width_of_bv_block_n) {
      //   bv_pos += nodes_per_bv_block_n;
      //   remaining -= block_loc[pos_n];
      //   pos_n++;
      // }
      size_t pos_n = 1;
      while (get_count(block_loc, pos_n) < remaining && pos_n < width_of_bv_block_n)
        pos_n++;
      if (pos_n-- > 1) {
        remaining -= get_count(block_loc, pos_n);
        bv_pos += (nodes_per_bv_block_n * pos_n);
      }
      #endif
      if (remaining == 0)
        return bv_pos;
      uint64_t bm = bm_loc[(bv_pos / 64) * multiplier];
      return bv_pos + bm_select1(remaining, bm);
    }
    __fq1 __fq2 inline uint32_t get_count(uint8_t *block_loc, size_t pos_n) {
      return (block_loc[pos_n] + (((uint32_t)(*block_loc) << pos_n) & 0x100));
    }
    __fq1 __fq2 inline uint32_t bm_select1(uint32_t remaining, uint64_t bm) {

      #ifdef __BMI2__
      uint64_t isolated_bit = _pdep_u64(1ULL << (remaining - 1), bm);
      size_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      // if (bit_loc == 65) {
      //   printf("WARNING: UNEXPECTED bit_loc=65, bit_loc: %u\n", remaining);
      //   return 64;
      // }
      #endif

      #ifndef __BMI2__
      size_t bit_loc = 0;
      while (bit_loc < 64) {
        uint8_t next_count = bit_count[(bm >> bit_loc) & 0xFF];
        if (next_count >= remaining)
          break;
        bit_loc += 8;
        remaining -= next_count;
      }
      if (remaining > 0) // && remaining <= 256)
        bit_loc += select_lookup_tbl[remaining - 1][(bm >> bit_loc) & 0xFF];
      #endif

      // size_t bit_loc = 0;
      // do {
      //   bit_loc = __builtin_ffsll(bm);
      //   remaining--;
      //   bm &= ~(1ULL << (bit_loc - 1));
      // } while (remaining > 0);

      // size_t bit_loc = 0;
      // uint64_t bm_mask = bm_init_mask << bit_loc;
      // while (remaining > 0) {
      //   if (bm & bm_mask)
      //     remaining--;
      //   bit_loc++;
      //   bm_mask <<= 1;
      // }
      return bit_loc;
    }
    __fq1 __fq2 uint8_t *get_select_loc1() {
      return lt_sel_loc1;
    }
    __fq1 __fq2 bvlt_select() {
    }
    __fq1 __fq2 void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc1, uint32_t _bv_bit_count, uint64_t *_bm_loc, uint8_t _multiplier, uint8_t _lt_width) {
      bvlt_rank::init(_lt_rank_loc, _bm_loc, _multiplier, _lt_width);
      lt_sel_loc1 = _lt_sel_loc1;
      bv_bit_count = _bv_bit_count;
    }

};

struct trie_flags {
  uint64_t bm_ptr;
  uint64_t bm_term;
  uint64_t bm_child;
  uint64_t bm_leaf;
};

class inner_trie : public inner_trie_fwd {
  private:
    // __fq1 __fq2 inner_trie(inner_trie const&);
    // __fq1 __fq2 inner_trie& operator=(inner_trie const&);
  protected:
    uint32_t node_count;
    
    uint8_t lt_not_given;
    uint8_t *trie_loc;
    tail_ptr_map *tail_map;
    bvlt_select child_lt;
    bvlt_select term_lt;
    GCFC_rev_cache rev_cache;

  public:
    __fq1 __fq2 bool compare_trie_tail(uint32_t node_id, input_ctx& in_ctx) {
      do {
        if (tail_lt.is_set1(node_id)) {
          uint32_t ptr_bit_count = UINT32_MAX;
          if (!tail_map->compare_tail(node_id, in_ctx, ptr_bit_count))
            return false;
        } else {
          if (in_ctx.key_pos >= in_ctx.key_len || trie_loc[node_id] != in_ctx.key[in_ctx.key_pos])
            return false;
          in_ctx.key_pos++;
        }
        if (!rev_cache.try_find(node_id, in_ctx))
          node_id = child_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    __fq1 __fq2 bool copy_trie_tail(uint32_t node_id, gen::byte_str& tail_str) {
      do {
        if (tail_lt.is_set1(node_id)) {
          tail_map->get_tail_str(node_id, tail_str);
        } else
          tail_str.append(trie_loc[node_id]);
        if (!rev_cache.try_find(node_id, tail_str))
          node_id = child_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    __fq1 __fq2 inner_trie() {
    }
    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      inner_trie *it = new inner_trie();
      it->trie_level = mem[3];
      it->load_inner_trie(mem);
      return it;
    }
    __fq1 __fq2 void load_inner_trie(uint8_t *trie_bytes) {

      tail_map = nullptr;
      trie_loc = nullptr;

      node_count = cmn::read_uint32(trie_bytes + 16);
      uint32_t node_set_count = cmn::read_uint32(trie_bytes + 24);
      uint32_t key_count = cmn::read_uint32(trie_bytes + 28);
      if (key_count > 0) {
        uint32_t rev_cache_count = cmn::read_uint32(trie_bytes + 48);
        uint32_t rev_cache_max_node_id = cmn::read_uint32(trie_bytes + 56);
        uint8_t *rev_cache_loc = trie_bytes + cmn::read_uint32(trie_bytes + 68);
        rev_cache.init(rev_cache_loc, rev_cache_count, rev_cache_max_node_id);

        uint8_t *term_select_lkup_loc = trie_bytes + cmn::read_uint32(trie_bytes + 76);
        uint8_t *term_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 80);
        uint8_t *child_select_lkup_loc = trie_bytes + cmn::read_uint32(trie_bytes + 84);
        uint8_t *child_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 88);

        uint8_t *tail_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 100);
        uint8_t *trie_tail_ptrs_data_loc = trie_bytes + cmn::read_uint32(trie_bytes + 104);

        uint32_t tail_size = cmn::read_uint32(trie_tail_ptrs_data_loc);
        //uint32_t trie_flags_size = cmn::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 8;
        trie_loc = tails_loc + tail_size;

        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 116));
        uint64_t *tf_ptr_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 120));

        bldr_options *opts = (bldr_options *) (trie_bytes + MDX_HEADER_SIZE);
        uint8_t multiplier = opts->trie_leaf_count > 0 ? 4 : 3;

        uint8_t encoding_type = tails_loc[2];
        uint8_t *tail_data_loc = tails_loc + cmn::read_uint32(tails_loc + 28);
        if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || *tail_data_loc == 1) {
          tail_ptr_flat_map *tail_flat_map = new tail_ptr_flat_map();  // Release where?
          tail_flat_map->init(this, &tail_lt, trie_loc, tails_loc);
          tail_map = tail_flat_map;
        } else {
          tail_ptr_group_map *tail_grp_map = new tail_ptr_group_map();
          tail_grp_map->init_ptr_grp_map(this, trie_loc, tf_loc, trie_level == 0 ? multiplier : 1, trie_level == 0 ? tf_loc : tf_ptr_loc, tails_loc, key_count, node_count, true);
          tail_map = tail_grp_map;
        }

        lt_not_given = 0;
        if (term_select_lkup_loc == trie_bytes) term_select_lkup_loc = nullptr;
        if (term_lt_loc == trie_bytes) term_lt_loc = nullptr;
        if (child_select_lkup_loc == trie_bytes) child_select_lkup_loc = nullptr;
        if (child_lt_loc == trie_bytes) child_lt_loc = nullptr;
        if (tail_lt_loc == trie_bytes) tail_lt_loc = nullptr; // TODO: to build if dessicated?
        if (term_lt_loc == nullptr) {
          lt_not_given |= BV_LT_TYPE_TERM;
          term_lt_loc = lt_builder::create_rank_lt_from_trie(BV_LT_TYPE_TERM, node_count, tf_loc);
          term_select_lkup_loc = lt_builder::create_select_lt_from_trie(BV_LT_TYPE_TERM, key_count, node_set_count, node_count, tf_loc);
        }
        if (child_lt_loc == nullptr) {
          lt_not_given |= BV_LT_TYPE_CHILD;
          child_lt_loc = lt_builder::create_rank_lt_from_trie(BV_LT_TYPE_CHILD, node_count, tf_loc);
          child_select_lkup_loc = lt_builder::create_select_lt_from_trie(BV_LT_TYPE_CHILD, key_count, node_set_count, node_count, tf_loc);
        }
        uint8_t bvlt_block_count = tail_lt_loc == nullptr ? 2 : 3;

        if (trie_level == 0) {
          term_lt.init(term_lt_loc, term_select_lkup_loc, node_count, tf_loc + TF_TERM, multiplier, bvlt_block_count);
          child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, tf_loc + TF_CHILD, multiplier, bvlt_block_count);
        } else {
          child_lt.init(child_lt_loc, child_select_lkup_loc, node_count * 2, tf_loc, 1, 1);
        }
        tail_lt.init(tail_lt_loc, trie_level == 0 ? tf_loc + TF_PTR : tf_ptr_loc, trie_level == 0 ? multiplier : 1, trie_level == 0 ? 3 : 1);
      }

    }

    __fq1 __fq2 virtual ~inner_trie() {
      if (tail_map != nullptr) {
        delete tail_map;
      }
      if (lt_not_given & BV_LT_TYPE_CHILD) {
        delete [] child_lt.get_rank_loc();
        delete [] child_lt.get_select_loc1();
      }
      if (lt_not_given & BV_LT_TYPE_TERM) {
        delete [] term_lt.get_rank_loc();
        delete [] term_lt.get_select_loc1();
      }
    }

};

class GCFC_fwd_cache {
  private:
    // __fq1 __fq2 GCFC_fwd_cache(GCFC_fwd_cache const&);
    // __fq1 __fq2 GCFC_fwd_cache& operator=(GCFC_fwd_cache const&);
    fwd_cache *cche0;
    uint32_t max_node_id;
    uint32_t cache_mask;
  public:
    __fq1 __fq2 int try_find(input_ctx& in_ctx) {
      if (in_ctx.node_id >= max_node_id)
        return -1;
      uint8_t key_byte = in_ctx.key[in_ctx.key_pos];
      do {
        uint32_t parent_node_id = in_ctx.node_id;
        uint32_t cache_idx = (parent_node_id ^ (parent_node_id << MDX_CACHE_SHIFT) ^ key_byte) & cache_mask;
        fwd_cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = cmn::read_uint24(&cche->parent_node_id1);
        if (parent_node_id == cache_node_id && cche->node_byte == key_byte) {
          in_ctx.key_pos++;
          if (in_ctx.key_pos < in_ctx.key_len) {
            in_ctx.node_id = cmn::read_uint24(&cche->child_node_id1);
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
    __fq1 __fq2 GCFC_fwd_cache() {
    }
    __fq1 __fq2 void init(uint8_t *_loc, uint32_t _count, uint32_t _max_node_id) {
      cche0 = (fwd_cache *) _loc;
      max_node_id = _max_node_id;
      cache_mask = _count - 1;
    }
};

class static_trie : public inner_trie {
  protected:
    bvlt_select *leaf_lt;
    GCFC_fwd_cache fwd_cache;
    trie_flags *trie_flags_loc;
    leapfrog *leaper;
    uint32_t key_count;
    uint8_t *trie_bytes;
    uint16_t max_tail_len;
    uint16_t max_level;

  private:
    // __fq1 __fq2 static_trie(static_trie const&); // todo: restore? can't return beacuse of this
    // __fq1 __fq2 static_trie& operator=(static_trie const&);
    size_t max_key_len;
  protected:
    bldr_options *opts;
  public:
    __fq1 __fq2 bool lookup(input_ctx& in_ctx) {
      in_ctx.key_pos = 0;
      in_ctx.node_id = 1;
      trie_flags *tf;
      uint64_t bm_mask;
      do {
        int ret = fwd_cache.try_find(in_ctx);
        bm_mask = bm_init_mask << (in_ctx.node_id % nodes_per_bv_block_n);
        tf = trie_flags_loc + in_ctx.node_id / nodes_per_bv_block_n;
        if (ret == 0)
          return bm_mask & tf->bm_leaf;
        if (leaper != nullptr) {
          if ((bm_mask & tf->bm_leaf) == 0 && (bm_mask & tf->bm_child) == 0) {
            leaper->find_pos(in_ctx.node_id, trie_loc, in_ctx.key[in_ctx.key_pos]);
            bm_mask = bm_init_mask << (in_ctx.node_id % nodes_per_bv_block_n);
            tf = trie_flags_loc + in_ctx.node_id / nodes_per_bv_block_n;
          }
        }
        uint32_t ptr_bit_count = UINT32_MAX;
        do {
          if ((bm_mask & tf->bm_ptr) == 0) {
            if (in_ctx.key[in_ctx.key_pos] == trie_loc[in_ctx.node_id]) {
              in_ctx.key_pos++;
              break;
            }
            #ifdef MDX_IN_ORDER
              if (in_ctx.key[in_ctx.key_pos] < trie_loc[in_ctx.node_id])
                return false;
            #endif
          } else {
            uint32_t prev_key_pos = in_ctx.key_pos;
            if (tail_map->compare_tail(in_ctx.node_id, in_ctx, ptr_bit_count))
              break;
            if (prev_key_pos != in_ctx.key_pos)
              return false;
            }
          if (bm_mask & tf->bm_term)
            return false;
          in_ctx.node_id++;
          bm_mask <<= 1;
          if (bm_mask == 0) {
            bm_mask = bm_init_mask;
            tf++;
          }
        } while (1);
        if (in_ctx.key_pos == in_ctx.key_len) {
          return 0 != (bm_mask & tf->bm_leaf);
        }
        if ((bm_mask & tf->bm_child) == 0)
          return false;
        in_ctx.node_id = term_lt.select1(child_lt.rank1(in_ctx.node_id) + 1);
      } while (1);
      return false;
    }

    __fq1 __fq2 bool reverse_lookup(uint32_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      leaf_id++;
      uint32_t node_id = leaf_lt->select1(leaf_id) - 1;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, to_reverse);
    }

    __fq1 __fq2 bool reverse_lookup_from_node_id(uint32_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      gen::byte_str tail(ret_key, max_key_len);
      do {
        if (tail_lt[node_id]) {
          size_t prev_len = tail.length();
          tail_map->get_tail_str(node_id, tail);
          reverse_byte_str(tail.data() + prev_len, tail.length() - prev_len);
        } else {
          tail.append(trie_loc[node_id]);
        }
        if (!rev_cache.try_find(node_id, tail))
          node_id = child_lt.select1(term_lt.rank1(node_id)) - 1;
      } while (node_id != 0);
      if (to_reverse)
        reverse_byte_str(tail.data(), tail.length());
      *in_size_out_key_len = tail.length();
      return true;
    }

    __fq1 __fq2 void reverse_byte_str(uint8_t *str, size_t len) {
      size_t i = len / 2;
      while (i--) {
        uint8_t b = str[i];
        size_t im = len - i - 1;
        str[i] = str[im];
        str[im] = b;
      }
    }

    __fq1 __fq2 uint32_t leaf_rank1(uint32_t node_id) {
      return leaf_lt->rank1(node_id);
    }

    __fq1 __fq2 bool is_leaf(uint32_t node_id) {
      return (*leaf_lt)[node_id];
    }

    __fq1 __fq2 uint32_t leaf_select1(uint32_t leaf_id) {
      return leaf_lt->select1(leaf_id);
    }

    __fq1 __fq2 void push_to_ctx(iter_ctx& ctx, gen::byte_str& tail, uint32_t node_id) {
      ctx.cur_idx++;
      tail.clear();
      update_ctx(ctx, tail, node_id);
    }

    __fq1 __fq2 void insert_arr(uint32_t *arr, int arr_len, int pos, uint32_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void insert_arr(uint16_t *arr, int arr_len, int pos, uint16_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void update_ctx(iter_ctx& ctx, gen::byte_str& tail, uint32_t node_id) {
      ctx.node_path[ctx.cur_idx] = node_id;
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = tail.length();
      memcpy(ctx.key + ctx.key_len, tail.data(), tail.length());
      ctx.key_len += tail.length();
    }

    __fq1 __fq2 void clear_last_tail(iter_ctx& ctx) {
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
    }

    __fq1 __fq2 uint32_t pop_from_ctx(iter_ctx& ctx, gen::byte_str& tail) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      return read_from_ctx(ctx, tail);
    }

    __fq1 __fq2 uint32_t read_from_ctx(iter_ctx& ctx, gen::byte_str& tail) {
      uint32_t node_id = ctx.node_path[ctx.cur_idx];
      return node_id;
    }

    __fq1 __fq2 int next(iter_ctx& ctx, uint8_t *key_buf) {
      gen::byte_str tail;
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
      uint8_t *tail_bytes = new uint8_t[max_tail_len + 1];
      #else
      uint8_t tail_bytes[max_tail_len + 1];
      #endif
      tail.set_buf_max_len(tail_bytes, max_tail_len);
      uint32_t node_id = read_from_ctx(ctx, tail);
      while (node_id < node_count) {
        if (leaf_lt != nullptr && !(*leaf_lt)[node_id] && !child_lt[node_id]) {
          node_id++;
          continue;
        }
        if (leaf_lt == nullptr || (*leaf_lt)[node_id]) {
          if (ctx.to_skip_first_leaf) {
            if (!child_lt[node_id]) {
              while (term_lt[node_id]) {
                if (ctx.cur_idx == 0) {
                  #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
                  delete [] tail_bytes;
                  #endif
                  return -2;
                }
                node_id = pop_from_ctx(ctx, tail);
              }
              node_id++;
              update_ctx(ctx, tail, node_id);
              ctx.to_skip_first_leaf = false;
              continue;
            }
          } else {
            tail.clear();
            if (tail_lt[node_id])
              tail_map->get_tail_str(node_id, tail);
            else
              tail.append(trie_loc[node_id]);
            update_ctx(ctx, tail, node_id);
            memcpy(key_buf, ctx.key, ctx.key_len);
            ctx.to_skip_first_leaf = true;
            #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
            delete [] tail_bytes;
            #endif
            return ctx.key_len;
          }
        }
        ctx.to_skip_first_leaf = false;
        if (child_lt[node_id]) {
          tail.clear();
          if (tail_lt[node_id])
            tail_map->get_tail_str(node_id, tail);
          else
            tail.append(trie_loc[node_id]);
          update_ctx(ctx, tail, node_id);
          node_id = term_lt.select1(child_lt.rank1(node_id) + 1);
          push_to_ctx(ctx, tail, node_id);
        }
      }
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
      delete [] tail_bytes;
      #endif
      return -2;
    }

    __fq1 __fq2 uint32_t get_max_level() {
      return max_level;
    }

    __fq1 __fq2 uint32_t get_key_count() {
      return key_count;
    }

    __fq1 __fq2 uint32_t get_node_count() {
      return node_count;
    }

    __fq1 __fq2 uint32_t get_max_key_len() {
      return max_key_len;
    }

    __fq1 __fq2 uint32_t get_max_tail_len() {
      return max_tail_len;
    }

    __fq1 __fq2 void insert_into_ctx(iter_ctx& ctx, gen::byte_str& tail, uint32_t node_id) {
      insert_arr(ctx.node_path, ctx.cur_idx, 0, node_id);
      insert_arr(ctx.last_tail_len, ctx.cur_idx, 0, (uint16_t) tail.length());
      memmove(ctx.key + tail.length(), ctx.key, ctx.key_len);
      for (size_t i = 0; i < tail.length(); i++)
        ctx.key[i] = tail[i];
      ctx.key_len += tail.length();
      ctx.cur_idx++;
    }

    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      static_trie *it = new static_trie();
      it->trie_level = mem[3];
      it->load_static_trie(mem);
      return it;
    }

    __fq1 __fq2 size_t find_first(const uint8_t *prefix, int prefix_len, iter_ctx& ctx, bool for_next = false) {
      input_ctx in_ctx;
      in_ctx.key = prefix;
      in_ctx.key_len = prefix_len;
      in_ctx.node_id = 0;
      lookup(in_ctx);
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
      uint8_t *tail_buf = new uint8_t[max_tail_len];
      #else
      uint8_t tail_buf[max_tail_len];
      #endif
      ctx.cur_idx = 0;
      gen::byte_str tail(tail_buf, max_tail_len);
      do {
        if (tail_lt[in_ctx.node_id])
          tail_map->get_tail_str(in_ctx.node_id, tail);
        else
          tail.append(trie_loc[in_ctx.node_id]);
        insert_into_ctx(ctx, tail, in_ctx.node_id);
        // printf("[%.*s]\n", (int) tail.length(), tail.data());
        // if (!rev_cache.try_find(in_ctx.node_id, tail))
          in_ctx.node_id = child_lt.select1(term_lt.rank1(in_ctx.node_id)) - 1;
        tail.clear();
      } while (in_ctx.node_id != 0);
      ctx.cur_idx--;
        // for (int i = 0; i < ctx.key_len; i++)
        //   printf("%c", ctx.key[i]);
        // printf("\nlsat tail len: %d\n", ctx.last_tail_len[ctx.cur_idx]);
      if (for_next) {
        ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
        ctx.last_tail_len[ctx.cur_idx] = 0;
        ctx.to_skip_first_leaf = false;
      }
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
      delete [] tail_buf;
      #endif
      return in_ctx.node_id;
    }

    __fq1 __fq2 bvlt_select *get_leaf_lt() {
      return leaf_lt;
    }

    __fq1 __fq2 uint8_t *get_trie_loc() {
      return trie_loc;
    }

    __fq1 __fq2 uint8_t *get_trie_bytes() {
      return trie_bytes;
    }

    __fq1 __fq2 uint8_t *get_null_value(size_t& null_value_len) {
      uint8_t *nv_loc = trie_bytes + cmn::read_uint32(trie_bytes + 124);
      null_value_len = *nv_loc++;
      return nv_loc;
    }

    __fq1 __fq2 uint8_t *get_empty_value(size_t& empty_value_len) {
      uint8_t *ev_loc = trie_bytes + cmn::read_uint32(trie_bytes + 128);
      empty_value_len = *ev_loc++;
      return ev_loc;
    }

    __fq1 __fq2 void load_static_trie(uint8_t *_trie_bytes = nullptr) {

      if (_trie_bytes != nullptr)
        trie_bytes = _trie_bytes;

      load_inner_trie(trie_bytes);
      opts = (bldr_options *) (trie_bytes + MDX_HEADER_SIZE);
      key_count = cmn::read_uint32(trie_bytes + 28);
      if (key_count > 0) {
        max_tail_len = cmn::read_uint16(trie_bytes + 40) + 1;

        uint32_t node_set_count = cmn::read_uint32(trie_bytes + 24);
        uint8_t *leaf_select_lkup_loc = trie_bytes + cmn::read_uint32(trie_bytes + 92);
        uint8_t *leaf_lt_loc = trie_bytes + cmn::read_uint32(trie_bytes + 96);
        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 116));
        trie_flags_loc = (trie_flags *) tf_loc;

        if (leaf_select_lkup_loc == trie_bytes) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == trie_bytes) leaf_lt_loc = nullptr;
        if (opts->trie_leaf_count > 0) {
          if (leaf_lt_loc == nullptr) {
            lt_not_given |= BV_LT_TYPE_LEAF;
            leaf_lt_loc = lt_builder::create_rank_lt_from_trie(BV_LT_TYPE_LEAF, node_count, tf_loc);
            leaf_select_lkup_loc = lt_builder::create_select_lt_from_trie(BV_LT_TYPE_LEAF, key_count, node_set_count, node_count, tf_loc);
          }
          leaf_lt = new bvlt_select();
          leaf_lt->init(leaf_lt_loc, leaf_select_lkup_loc, node_count, tf_loc + 3, 4, 1);
        }
      }

      if (key_count > 0) {
        max_key_len = cmn::read_uint32(trie_bytes + 32);
        max_level = cmn::read_uint16(trie_bytes + 42);
        uint32_t fwd_cache_count = cmn::read_uint32(trie_bytes + 44);
        uint32_t fwd_cache_max_node_id = cmn::read_uint32(trie_bytes + 52);
        uint8_t *fwd_cache_loc = trie_bytes + cmn::read_uint32(trie_bytes + 64);
        fwd_cache.init(fwd_cache_loc, fwd_cache_count, fwd_cache_max_node_id);

        min_pos_stats min_stats;
        memcpy(&min_stats, trie_bytes + 60, 4);
        uint8_t *min_pos_loc = trie_bytes + cmn::read_uint32(trie_bytes + 72);
        if (min_pos_loc == trie_bytes)
          min_pos_loc = nullptr;
        if (min_pos_loc != nullptr) {
          if (opts->sort_nodes_on_freq)
            leaper = new leapfrog_rnd();
          else {
            leaper = new leapfrog_asc();
            ((leapfrog_asc *) leaper)->init(min_stats, min_pos_loc);
          }
        }

      }

    }

    __fq1 __fq2 static_trie() {
      init_vars();
      trie_level = 0;
    }

    __fq1 __fq2 void init_vars() {

      trie_bytes = nullptr;

      max_key_len = max_level = 0;
      leaper = nullptr;

      max_tail_len = 0;
      tail_map = nullptr;
      trie_loc = nullptr;
      leaf_lt = nullptr;

    }

    __fq1 __fq2 virtual ~static_trie() {
      if (leaper != nullptr)
        delete leaper;
      if (lt_not_given & BV_LT_TYPE_LEAF) {
        delete [] leaf_lt->get_rank_loc();
        delete [] leaf_lt->get_select_loc1();
      }
      if (leaf_lt != nullptr)
        delete leaf_lt;
    }

};

class val_ptr_group_map : public ptr_group_map {
  private:
    // __fq1 __fq2 val_ptr_group_map(val_ptr_group_map const&);
    // __fq1 __fq2 val_ptr_group_map& operator=(val_ptr_group_map const&);
    std::vector<uint8_t> prev_val;
    uint32_t node_count;
    uint32_t key_count;
    gen::int_bv_reader int_ptr_bv;
    static_trie *col_trie;
  public:
    __fq1 __fq2 uint32_t scan_ptr_bits_words(uint32_t node_id, uint32_t ptr_bit_count) {
      uint32_t node_id_from = node_id - (node_id % nodes_per_ptr_block_n);
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      uint64_t bm_leaf = UINT64_MAX;
      if (key_count > 0)
        bm_leaf = ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * multiplier];
      size_t to_skip = 0;
      uint32_t last_pbc = ptr_bit_count;
      while (node_id_from < node_id) {
        if (bm_leaf & bm_mask) {
          if (to_skip == 0) {
            uint8_t code = ptr_reader.read8(ptr_bit_count);
            uint8_t grp_no = code_lt_code_len[code] & 0x0F;
            if (grp_no != rpt_grp_no)
              last_pbc = ptr_bit_count;
            uint8_t code_len = code_lt_code_len[code] >> 4;
            uint8_t bit_len = code_lt_bit_len[code];
            ptr_bit_count += code_len;
            uint32_t count = ptr_reader.read(ptr_bit_count, bit_len - code_len);
            if (grp_no == rpt_grp_no) {
              to_skip = count - 1; // todo: -1 not needed
              printf("To skip: %lu\n", to_skip);
            } else {
              while (count--) {
                code = ptr_reader.read8(ptr_bit_count);
                ptr_bit_count += code_lt_bit_len[code];
              }
            }
          } else
            to_skip--;
        }
        node_id_from++;
        bm_mask <<= 1;
      }
      if (to_skip > 0)
        ptr_bit_count = last_pbc;
      return ptr_bit_count;
    }
    __fq1 __fq2 uint8_t *get_word_val(uint32_t node_id, size_t *in_size_out_value_len, void *ret_val, uint32_t *p_ptr_bit_count = nullptr) {
      uint32_t ptr_bit_count = UINT32_MAX;
      if (p_ptr_bit_count == nullptr)
        p_ptr_bit_count = &ptr_bit_count;
      if (*p_ptr_bit_count == UINT32_MAX) {
        uint32_t pbc = ptr_reader.get_ptr_block_t_words(node_id);
        *p_ptr_bit_count = scan_ptr_bits_words(node_id, pbc);
        // printf("scan: %u, ", *p_ptr_bit_count);
      }
      uint8_t code = ptr_reader.read8(*p_ptr_bit_count);
      uint8_t bit_len = code_lt_bit_len[code];
      uint8_t grp_no = code_lt_code_len[code] & 0x0F;
      uint8_t code_len = code_lt_code_len[code] >> 4;
      *p_ptr_bit_count += code_len;
      uint32_t word_count = ptr_reader.read(*p_ptr_bit_count, bit_len - code_len);
      size_t val_len;
      *in_size_out_value_len = 0;
      for (size_t i = 0; i < word_count; i++) {
        code = ptr_reader.read8(*p_ptr_bit_count);
        bit_len = code_lt_bit_len[code];
        grp_no = code_lt_code_len[code] & 0x0F;
        code_len = code_lt_code_len[code] >> 4;
        *p_ptr_bit_count += code_len;
        uint32_t it_leaf_id = ptr_reader.read(*p_ptr_bit_count, bit_len - code_len);
        ((static_trie *) inner_tries[grp_no])->reverse_lookup(it_leaf_id, &val_len,
            (uint8_t *) ret_val + *in_size_out_value_len, true);
        *in_size_out_value_len += val_len;
      }
      // printf("pbc: %u\n", *p_ptr_bit_count);
      return  (uint8_t *) ret_val;
    }

    __fq1 __fq2 void get_col_trie_val(uint32_t node_id, size_t *in_size_out_value_len, void *ret_val) {
      uint32_t ptr_pos = node_id;
      if (key_count > 0)
        ptr_pos = ((static_trie *) dict_obj)->get_leaf_lt()->rank1(node_id);
      uint32_t col_trie_node_id = int_ptr_bv[ptr_pos];
      // printf("Col trie node_id: %u\n", col_trie_node_id);
      col_trie->reverse_lookup_from_node_id(col_trie_node_id, in_size_out_value_len, (uint8_t *) ret_val, true);
    }

    __fq1 __fq2 uint32_t scan_ptr_bits_val(uint32_t node_id, uint32_t ptr_bit_count) {
      uint32_t node_id_from = node_id - (node_id % nodes_per_ptr_block_n);
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      uint64_t bm_leaf = UINT64_MAX;
      if (key_count > 0)
        bm_leaf = ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * multiplier];
      while (node_id_from < node_id) {
        if (bm_leaf & bm_mask) {
          uint8_t code = ptr_reader.read8(ptr_bit_count);
          ptr_bit_count += code_lt_bit_len[code];
        }
        node_id_from++;
        bm_mask <<= 1;
      }
      return ptr_bit_count;
    }
    __fq1 __fq2 uint32_t get_ptr_bit_count_val(uint32_t node_id) {
      uint32_t ptr_bit_count = ptr_reader.get_ptr_block_t(node_id);
      return scan_ptr_bits_val(node_id, ptr_bit_count);
    }

    __fq1 __fq2 bool next_val(uint32_t node_id, size_t *in_size_out_value_len, uint8_t *ret_val) {
      if (node_id >= node_count)
        return false;
      uint32_t ptr_bit_count = UINT32_MAX;
      if (key_count == 0) {
        get_val(node_id, in_size_out_value_len, ret_val, &ptr_bit_count);
        return true;
      }
      do {
        // if ((*ptr_flags)[node_id]) { // TODO: Fix
          get_val(node_id, in_size_out_value_len, ret_val, &ptr_bit_count);
          return true;
        // }
        node_id++;
      } while (node_id < node_count);
      return false;
    }

    __fq1 __fq2 uint8_t *get_val_loc(uint32_t node_id, uint32_t *p_ptr_bit_count = nullptr, uint8_t *p_grp_no = nullptr) {
      uint8_t grp_no = 0;
      if (p_grp_no == nullptr)
        p_grp_no = &grp_no;
      uint32_t ptr_bit_count = UINT32_MAX;
      if (p_ptr_bit_count == nullptr)
        p_ptr_bit_count = &ptr_bit_count;
      if (group_count == 1) {
        *p_grp_no = 0;
        uint32_t ptr_pos = node_id;
        if (key_count > 0)
          ptr_pos = ((static_trie *) dict_obj)->get_leaf_lt()->rank1(node_id);
        uint32_t ptr = int_ptr_bv[ptr_pos];
        if (*p_grp_no < grp_idx_limit)
          ptr = read_ptr_from_idx(*p_grp_no, ptr);
        return grp_data[0] + ptr;
      }
      if (*p_ptr_bit_count == UINT32_MAX)
        *p_ptr_bit_count = get_ptr_bit_count_val(node_id);
      uint8_t code = ptr_reader.read8(*p_ptr_bit_count);
      uint8_t bit_len = code_lt_bit_len[code];
      *p_grp_no = code_lt_code_len[code] & 0x0F;
      uint8_t code_len = code_lt_code_len[code] >> 4;
      *p_ptr_bit_count += code_len;
      uint32_t ptr = ptr_reader.read(*p_ptr_bit_count, bit_len - code_len);
      if (*p_grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(*p_grp_no, ptr);
      return grp_data[*p_grp_no] + ptr;
    }

    __fq1 __fq2 void get_delta_val(uint32_t node_id, size_t *in_size_out_value_len, void *ret_val) {
      uint8_t *val_loc;
      uint32_t ptr_bit_count = UINT32_MAX;
      uint32_t delta_node_id = node_id / nodes_per_bv_block_n;
      delta_node_id *= nodes_per_bv_block_n;
      uint64_t bm_mask = bm_init_mask;
      uint64_t bm_leaf = UINT64_MAX;
      if (key_count > 0)
        bm_leaf = ptr_bm_loc[(node_id / nodes_per_bv_block_n) * multiplier];
      int64_t col_val = 0;
      do {
        // printf("Delta Node-id: %u\n", delta_node_id);
        if (bm_mask & bm_leaf) {
          val_loc = get_val_loc(delta_node_id, &ptr_bit_count);
          if (val_loc != nullptr) {
            int64_t delta_val = gen::read_svint60(val_loc);
            // printf("Delta node id: %u, value: %lld\n", delta_node_id, delta_val);
            col_val += delta_val;
          }
        }
        bm_mask <<= 1;
      } while (delta_node_id++ < node_id);
      *in_size_out_value_len = 8;
      if (data_type == MST_INT) {
        *((int64_t *) ret_val) = col_val;
        return;
      }
      double dbl = static_cast<double>(col_val);
      dbl /= gen::pow10(data_type - MST_DEC0);
      *((double *)ret_val) = dbl;
    }

    __fq1 __fq2 void convert_back(uint8_t *val_loc, void *ret_val, size_t& ret_len) {
      ret_len = 8;
      switch (data_type) {
        case MST_INT:
        case MST_DECV ... MST_DEC9:
        case MST_DATE_US ... MST_DATETIME_ISOT_MS: {
          if (*val_loc == 0x00) {
            uint8_t *null_val = ((static_trie *) dict_obj)->get_null_value(ret_len);
            memcpy(ret_val, null_val, ret_len);
            return;
          }
          int64_t i64 = gen::read_svint60(val_loc);
          if (data_type >= MST_DEC0 && data_type <= MST_DEC9) {
            double dbl = static_cast<double>(i64);
            dbl /= gen::pow10(data_type - MST_DEC0);
            *((double *)ret_val) = dbl;
          } else
            memcpy(ret_val, &i64, sizeof(int64_t));
        } break;
      }
    }

    __fq1 __fq2 const uint8_t *get_val(uint32_t node_id, size_t *in_size_out_value_len, void *ret_val, uint32_t *p_ptr_bit_count = nullptr) {
      if (ret_val == nullptr || in_size_out_value_len == nullptr)
        return nullptr;
      uint8_t *val_loc;
      uint8_t grp_no = 0;
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        get_col_trie_val(node_id, in_size_out_value_len, ret_val);
        if (data_type != MST_TEXT && data_type != MST_BIN)
          convert_back((uint8_t *) ret_val, ret_val, *in_size_out_value_len);
        return (const uint8_t *) ret_val;
      } else if (encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY) {
        return get_word_val(node_id, in_size_out_value_len, ret_val, p_ptr_bit_count);
      } else {
        switch (data_type) {
          case MST_BIN: {
            val_loc = get_val_loc(node_id, p_ptr_bit_count, &grp_no);
            uint32_t bin_len;
            const uint8_t *bin_str = get_bin_val(val_loc, bin_len);
            *in_size_out_value_len = bin_len;
            return bin_str;
          } break;
          case MST_TEXT: {
            val_loc = get_val_loc(node_id, p_ptr_bit_count, &grp_no);
            *in_size_out_value_len = 8;
            #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
            uint8_t *val_str_buf = new uint8_t[max_len];
            #else
            uint8_t val_str_buf[max_len];
            #endif
            gen::byte_str val_str(val_str_buf, max_len);
            uint8_t *val_start = grp_data[grp_no];
            if (*val_start != 0)
              inner_tries[grp_no]->copy_trie_tail(val_loc - val_start, val_str);
            else
              get_val_str(val_str, val_loc - val_start, grp_no, max_len);
            size_t val_len = val_str.length();
            *in_size_out_value_len = val_len;
            memcpy(ret_val, val_str.data(), val_len);
            #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__)
            delete [] val_str_buf;
            #endif
            return (uint8_t *) ret_val;
          } break;
          case MST_INT:
          case MST_DECV ... MST_DEC9:
          case MST_DATE_US ... MST_DATETIME_ISOT_MS: {
            if (encoding_type == MSE_DICT_DELTA) {
              get_delta_val(node_id, in_size_out_value_len, ret_val);
            } else {
              val_loc = get_val_loc(node_id, p_ptr_bit_count, &grp_no);
              // printf("%d, %lu, %lu\n", grp_no, p_ptr_bit_count, val_loc-grp_data[grp_no]);
              *in_size_out_value_len = 8;
              convert_back(val_loc, ret_val, *in_size_out_value_len);
            }
            return (uint8_t *) ret_val;
          } break;
        }
      }
      return nullptr;
    }
    __fq1 __fq2 const uint8_t *get_bin_val(uint8_t *val_loc, uint32_t& bin_len) {
      tail_ptr_flat_map::read_len_bw(val_loc, bin_len);
      return val_loc + 1;
    }

    __fq1 __fq2 void get_val_str(gen::byte_str& ret, uint32_t val_ptr, uint8_t grp_no, size_t max_valset_len) {
      uint8_t *val = grp_data[grp_no];
      ret.clear();
      uint8_t *t = val + val_ptr;
      if (*t > 15 && *t < 32) {
        uint32_t bin_len;
        tail_ptr_flat_map::read_len_bw(t++, bin_len);
        while (bin_len--)
          ret.append(*t++);
        return;
      }
      while (*t < 15 || *t > 31)
        t--;
      t++;
      while (*t < 15 || *t > 31)
        ret.append(*t++);
      if (val[val_ptr - 1] < 15 || val[val_ptr - 1] > 31)
        ret.set_length(ret.length() - (t - val - val_ptr));
      if (*t == 15)
        return;
      uint32_t pfx_len = tail_ptr_flat_map::read_len(t);
      memmove(ret.data() + pfx_len, ret.data(), ret.length());
      ret.set_length(ret.length() + pfx_len);
      read_prefix(ret.data() + pfx_len - 1, val + val_ptr - 1, pfx_len);
    }
    __fq1 __fq2 void read_prefix(uint8_t *out_str, uint8_t *t, uint32_t pfx_len) {
      while (*t < 15 || *t > 31)
        t--;
      while (*t != 15) {
        uint32_t prev_pfx_len;
        t = tail_ptr_flat_map::read_len_bw(t, prev_pfx_len);
        while (*t < 15 || *t > 31)
          t--;
        while (pfx_len > prev_pfx_len) {
          *out_str-- = *(t + pfx_len - prev_pfx_len);
          pfx_len--;
        }
      }
      t--;
      while (*t < 15 || *t > 31)
        t--;
      t++;
      while (pfx_len-- > 0)
        *out_str-- = *(t + pfx_len);
    }
    __fq1 __fq2 void init(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier,
                uint8_t *val_loc, uint32_t _key_count, uint32_t _node_count) {
      key_count = _key_count;
      init_ptr_grp_map(_dict_obj, _trie_loc, _bm_loc, _multiplier, _bm_loc, val_loc, _key_count, _node_count, false);
      uint8_t *data_loc = val_loc + cmn::read_uint32(val_loc + 28);
      uint8_t *ptrs_loc = val_loc + cmn::read_uint32(val_loc + 40);
      if (group_count == 1 || val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY)
        int_ptr_bv.init(ptrs_loc, val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY ? val_loc[0] : data_loc[1]);
      col_trie = nullptr;
      if (val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY) {
        col_trie = new static_trie();
        col_trie->load_static_trie(data_loc);
      }
    }
    __fq1 __fq2 static_trie *get_col_trie() {
      return col_trie;
    }
    __fq1 __fq2 val_ptr_group_map() {
      col_trie = nullptr;
    }
    __fq1 __fq2 virtual ~val_ptr_group_map() {
      if (col_trie != nullptr)
        delete col_trie;
    }
};

class cleanup_interface {
  public:
    __fq1 __fq2 virtual ~cleanup_interface() {
    }
    __fq1 __fq2 virtual void release() = 0;
};

class cleanup : public cleanup_interface {
  private:
    // __fq1 __fq2 cleanup(cleanup const&);
    // __fq1 __fq2 cleanup& operator=(cleanup const&);
    uint8_t *bytes;
  public:
    __fq1 __fq2 cleanup() {
    }
    __fq1 __fq2 virtual ~cleanup() {
    }
    __fq1 __fq2 void release() {
      delete [] bytes;
    }
    __fq1 __fq2 void init(uint8_t *_bytes) {
      bytes = _bytes;
    }
};

class static_trie_map : public static_trie {
  private:
    // __fq1 __fq2 static_trie_map(static_trie_map const&); // todo: restore? can't return because of this
    // __fq1 __fq2 static_trie_map& operator=(static_trie_map const&);
    val_ptr_group_map *val_map;
    uint16_t val_count;
    uint16_t pk_col_count;
    size_t max_val_len;
    uint8_t *names_loc;
    char *names_start;
    const char *column_encoding;
    cleanup_interface *cleanup_object;
    bool is_mmapped;
    size_t trie_size;
  public:
    __fq1 __fq2 static_trie_map() {
      val_map = nullptr;
      is_mmapped = false;
      cleanup_object = nullptr;
    }
    __fq1 __fq2 ~static_trie_map() {
      if (is_mmapped)
        map_unmap();
      if (trie_bytes != nullptr) {
        if (cleanup_object != nullptr) {
          cleanup_object->release();
          delete cleanup_object;
          cleanup_object = nullptr;
        }
      }
      if (val_map != nullptr) {
        delete [] val_map;
      }
    }

    __fq1 __fq2 void set_cleanup_object(cleanup_interface *_cleanup_obj) {
      cleanup_object = _cleanup_obj;
    }

    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      static_trie_map *it = new static_trie_map();
      it->trie_level = mem[3];
      it->load_from_mem(mem, 0);
      return it;
    }

    __fq1 __fq2 bool get(input_ctx& in_ctx, size_t *in_size_out_value_len, void *val) {
      bool is_found = lookup(in_ctx);
      if (is_found) {
        if (val_count > 1)
          val_map[1].get_val(in_ctx.node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
    }

    __fq1 __fq2 const uint8_t *get_col_val(uint32_t node_id, int col_val_idx,
          size_t *in_size_out_value_len, void *val = nullptr, uint32_t *p_ptr_bit_count = nullptr) {
      if (col_val_idx < pk_col_count) { // TODO: Extract from composite keys,Convert numbers back
        if (!reverse_lookup_from_node_id(node_id, in_size_out_value_len, (uint8_t *) val))
          return nullptr;
        return (const uint8_t *) val;
      }
      return val_map[col_val_idx].get_val(node_id, in_size_out_value_len, val, p_ptr_bit_count);
    }

    __fq1 __fq2 const char *get_table_name() {
      return names_start + cmn::read_uint16(names_loc + 2);
    }

    __fq1 __fq2 const char *get_column_name(int i) {
      return names_start + cmn::read_uint16(names_loc + (i + 2) * 2);
    }

    __fq1 __fq2 const char *get_column_types() {
      return names_start;
    }

    __fq1 __fq2 char get_column_type(int i) {
      return names_start[i];
    }

    __fq1 __fq2 const char *get_column_encodings() {
      return column_encoding;
    }

    __fq1 __fq2 char get_column_encoding(int i) {
      return column_encoding[i];
    }

    __fq1 __fq2 uint32_t get_max_val_len() {
      return max_val_len;
    }

    __fq1 __fq2 uint32_t get_max_val_len(int col_val_idx) {
      return val_map[col_val_idx].max_len;
    }

    __fq1 __fq2 static_trie *get_col_trie(int col_val_idx) {
      return val_map[col_val_idx].get_col_trie();
    }

    __fq1 __fq2 static_trie_map get_col_trie_map(int col_val_idx) {
      static_trie_map stm;
      uint8_t *tb = get_col_trie(col_val_idx)->get_trie_bytes();
      stm.load_from_mem(tb, 0);
      return stm;
    }

    __fq1 __fq2 uint16_t get_column_count() {
      return val_count;
    }

    __fq1 __fq2 void map_from_memory(uint8_t *mem) {
      load_from_mem(mem, 0);
    }

    __fq1 __fq2 void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    __fq1 __fq2 uint8_t *map_file(const char *filename, off_t& sz) {
#ifdef _WIN32
      load(filename);
      return trie_bytes;
#else
      struct stat buf;
      int fd = open(filename, O_RDONLY);
      if (fd < 0) {
        perror("open: ");
        return nullptr;
      }
      fstat(fd, &buf);
      sz = buf.st_size;
      uint8_t *map_buf = (uint8_t *) mmap(0, sz, PROT_READ, MAP_PRIVATE, fd, 0);
      if (map_buf == MAP_FAILED) {
        perror("mmap: ");
        close(fd);
        return nullptr;
      }
      close(fd);
      return map_buf;
#endif
    }

    __fq1 __fq2 void map_file_to_mem(const char *filename) {
      off_t dict_size;
      trie_bytes = map_file(filename, dict_size);
      //       int len_will_need = (dict_size >> 2);
      //       //madvise(trie_bytes, len_will_need, MADV_WILLNEED);
      // #ifndef _WIN32
      //       mlock(trie_bytes, len_will_need);
      // #endif
      load_into_vars();
      is_mmapped = true;
    }

    __fq1 __fq2 void map_unmap() {
      // #ifndef _WIN32
      //       munlock(trie_bytes, dict_size >> 2);
      //       int err = munmap(trie_bytes, dict_size);
      //       if(err != 0){
      //         printf("UnMapping trie_bytes Failed\n");
      //         return;
      //       }
      // #endif
      trie_bytes = nullptr;
      is_mmapped = false;
    }

    __fq1 __fq2 void load_from_mem(uint8_t *mem, size_t sz) {
      trie_bytes = mem;
      trie_size = sz;
      cleanup_object = nullptr;
      load_into_vars();
    }

    size_t get_size() {
      return trie_size;
    }

    __fq1 __fq2 void load(const char* filename) {

      init_vars();
      struct stat file_stat;
      memset(&file_stat, 0, sizeof(file_stat));
      stat(filename, &file_stat);
      trie_bytes = new uint8_t[file_stat.st_size];
      cleanup_object = new cleanup();
      ((cleanup *)cleanup_object)->init(trie_bytes);

      FILE *fp = fopen(filename, "rb");
      if (fp == nullptr) {
        printf("fopen failed: %s (errno: %d), %ld\n", strerror(errno), errno, (long) file_stat.st_size);
        #ifdef __CUDA_ARCH__
        return;
        #else
        throw errno;
        #endif
      }
      long bytes_read = fread(trie_bytes, 1, file_stat.st_size, fp);
      if (bytes_read != file_stat.st_size) {
        printf("Read error: [%s], %ld, %lu\n", filename, (long) file_stat.st_size, bytes_read);
        #ifdef __CUDA_ARCH__
        return;
        #else
        throw errno;
        #endif
      }
      fclose(fp);

      // int len_will_need = (dict_size >> 1);
      // #ifndef _WIN32
      //       mlock(trie_bytes, len_will_need);
      // #endif
      //madvise(trie_bytes, len_will_need, MADV_WILLNEED);
      //madvise(trie_bytes + len_will_need, dict_size - len_will_need, MADV_RANDOM);

      load_into_vars();

    }

    __fq1 __fq2 void load_into_vars() {
      load_static_trie();
      pk_col_count = trie_bytes[2];
      val_count = cmn::read_uint32(trie_bytes + 4);
      max_val_len = cmn::read_uint32(trie_bytes + 36);

      uint64_t *tf_leaf_loc = (uint64_t *) (trie_bytes + cmn::read_uint32(trie_bytes + 116));

      names_loc = trie_bytes + cmn::read_uint32(trie_bytes + 8);
      uint8_t *val_table_loc = trie_bytes + cmn::read_uint32(trie_bytes + 12);
      // printf("Val table loc: %lu\n", val_table_loc - trie_bytes);
      names_start = (char *) names_loc + (val_count + 2) * sizeof(uint16_t);
      column_encoding = names_start + cmn::read_uint16(names_loc);

      val_map = nullptr;
      if (val_count > 0) {
        val_map = new val_ptr_group_map[val_count]();
        for (size_t i = pk_col_count; i < val_count; i++) {
          uint64_t vl64 = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
          // printf("Val idx: %lu, %llu\n", i, vl64);
          uint8_t *val_loc = trie_bytes + vl64;
          if (val_loc == trie_bytes)
            continue;
          val_map[i].init(this, trie_loc, tf_leaf_loc + TF_LEAF, opts->trie_leaf_count > 0 ? 4 : 3, val_loc, key_count, node_count);
        }
      }
    }
};

}
#endif
