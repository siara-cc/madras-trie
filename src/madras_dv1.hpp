#ifndef STATIC_TRIE_H
#define STATIC_TRIE_H

#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/stat.h>

#include "common_dv1.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>
#include <nmmintrin.h>
#endif
#if !defined(_WIN32) && !defined(ESP32)
#include <sys/mman.h>
#endif
#include <stdint.h>

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

#define PK_COL_COUNT_LOC 38
#define TRIE_LEVEL_LOC 39

#define MAX_TAIL_LEN_LOC 40
#define MAX_LVL_LOC 42
#define MIN_STAT_LOC 44

#define VAL_COUNT_LOC 48
#define NAMES_LOC 56
#define VAL_TBL_LOC 64
#define NODE_COUNT_LOC 72
#define OPTS_SIZE_LOC 80
#define NS_COUNT_LOC 88
#define KEY_COUNT_LOC 96
#define MAX_KEY_LEN_LOC 104
#define MAX_VAL_LEN_LOC 112

#define FC_COUNT_LOC 120
#define RC_COUNT_LOC 128
#define FC_MAX_NID_LOC 136
#define RC_MAX_NID_LOC 144
#define FCACHE_LOC 152
#define RCACHE_LOC 160
#define SEC_CACHE_LOC 168

#define TERM_SEL_LT_LOC 176
#define TERM_RANK_LT_LOC 184
#define CHILD_SEL_LT_LOC 192
#define CHILD_RANK_LT_LOC 200

#define LOUDS_SEL_LT_LOC 192
#define LOUDS_RANK_LT_LOC 200

#define LEAF_SEL_LT_LOC 208
#define LEAF_RANK_LT_LOC 216
#define TAIL_RANK_LT_LOC 224
#define TRIE_TAIL_PTRS_DATA_LOC 232

#define TRIE_FLAGS_LOC 240
#define TAIL_FLAGS_PTR_LOC 248
#define NULL_VAL_LOC 256
#define EMPTY_VAL_LOC 264

#define TV_PLT_PTR_WIDTH_LOC 0
#define TV_DATA_TYPE_LOC 1
#define TV_ENC_TYPE_LOC 2
#define TV_FLAGS_LOC 3
#define TV_LEN_GRP_NO_LOC 4
#define TV_RPT_GRP_NO_LOC 5
#define TV_RPT_SEQ_GRP_NO_LOC 6
#define TV_SUG_WIDTH_LOC 7
#define TV_SUG_HEIGHT_LOC 8
#define TV_IDX_LIMIT_LOC 9
#define TV_START_BITS_LOC 10
#define TV_STEP_BITS_IDX_LOC 11
#define TV_STEP_BITS_REST_LOC 12
#define TV_IDX_PTR_SIZE_LOC 13

#define TV_NULL_BV_SIZE_LOC 16
#define TV_MAX_LEN_LOC 24
#define TV_PLT_LOC 32
#define TV_GRP_DATA_LOC 40
#define TV_IDX2_PTR_COUNT_LOC 48
#define TV_IDX2_PTR_MAP_LOC 56
#define TV_GRP_PTRS_LOC 64
#define TV_NULL_BV_LOC 72
#define TV_NULL_COUNT_LOC 80
#define TV_MIN_VAL_LOC 88
#define TV_MAX_VAL_LOC 96

#define TVG_GRP_COUNT_LOC 0
#define TVG_INNER_TRIE_START_GRP_LOC 1
#define TVG_PTR_WIDTH_LOC 1

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
    ~val_ctx() {
      unallocate();
    }
    void unallocate() {
      if (to_alloc) {
        if (alloc_len > 0)
          delete [] val->txt_bin;
        delete val_len;
        alloc_len = 0;
        to_alloc = 0;
      }
      if (to_alloc_uxx) {
        delete [] i64_vals;
        delete [] i32_vals;
        delete [] byts;
        to_alloc_uxx = 0;
      }
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
  private:
    // __fq1 __fq2 iter_ctx(iter_ctx const&);
    // __fq1 __fq2 iter_ctx& operator=(iter_ctx const&);
  public:
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
    __fq1 __fq2 static uintxx_t read_uint16(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      return *((uint16_t *) ptr); // faster endian dependent
      #else
      uintxx_t ret = *ptr++;
      ret |= (*ptr << 8);
      return ret;
      #endif
    }
    __fq1 __fq2 static uintxx_t read_uint24(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      return *((uintxx_t *) ptr) & 0x00FFFFFF; // faster endian dependent
      #else
      uintxx_t ret = *ptr++;
      ret |= (*ptr++ << 8);
      ret |= (*ptr << 16);
      return ret;
      #endif
    }
    __fq1 __fq2 static uintxx_t read_uint32(uint8_t *ptr) {
      // #ifndef __CUDA_ARCH__
      return *((uint32_t *) ptr);
      // TODO: fix
      // #else
      // uintxx_t ret = *ptr++;
      // ret |= (*ptr++ << 8);
      // ret |= (*ptr++ << 16);
      // ret |= (*ptr << 24);
      // return ret;
      // #endif
    }
    __fq1 __fq2 static uint64_t read_uint40(uint8_t *ptr) {
      uint64_t ret = *((uint32_t *) ptr);
      return ret | ((uint64_t) ptr[4] << 32);
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
    __fq1 __fq2 static size_t min(size_t v1, size_t v2) {
      return v1 < v2 ? v1 : v2;
    }
    __fq1 __fq2 static uintxx_t read_vint32(const uint8_t *ptr, size_t *vlen = NULL) {
      uintxx_t ret = 0;
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
    __fq1 __fq2 static void convert_back(char data_type, uint8_t *val_loc, mdx_val &ret_val, size_t *ret_len, uint8_t *null_val, size_t null_len) {
      switch (data_type) {
        case MST_INT:
        case MST_DECV: case MST_DEC0: case MST_DEC1: case MST_DEC2:
        case MST_DEC3: case MST_DEC4: case MST_DEC5: case MST_DEC6:
        case MST_DEC7: case MST_DEC8: case MST_DEC9:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISO_MS: case MST_DATETIME_ISOT_MS: {
          *ret_len = 8;
          if (*val_loc == 0x00) {
            ret_val.i64 = INT64_MIN;
            return;
          }
          int64_t i64 = gen::read_svint60(val_loc);
          if (data_type >= MST_DEC0 && data_type <= MST_DEC9) {
            double dbl = static_cast<double>(i64);
            dbl /= allflic48::tens()[data_type - MST_DEC0];
            ret_val.dbl = dbl;
          } else
            memcpy(&ret_val, &i64, sizeof(int64_t));
        } break;
      }
    }
};

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

class leapfrog_asc {
  private:
    // __fq1 __fq2 leapfrog_asc(leapfrog_asc const&);
    // __fq1 __fq2 leapfrog_asc& operator=(leapfrog_asc const&);
    uint8_t *min_pos_loc;
    min_pos_stats min_stats;
  public:
    __fq1 __fq2 void find_pos(uintxx_t& node_id, const uint8_t *trie_loc, uint8_t key_byte) {
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
    __fq1 __fq2 uintxx_t rank1(uintxx_t bv_pos) {
      uint8_t *rank_ptr = lt_rank_loc + bv_pos / nodes_per_bv_block * lt_width;
      uintxx_t rank = cmn::read_uint32(rank_ptr);
      #if nodes_per_bv_block == 512
      int pos = (bv_pos / nodes_per_bv_block_n) % width_of_bv_block_n;
      if (pos > 0) {
        rank_ptr += 4;
        //while (pos--)
        //  rank += *rank_ptr++;
        rank += (rank_ptr[pos] + (((uintxx_t)(*rank_ptr) << pos) & 0x100));
      }
      #else
      int pos = (bv_pos / nodes_per_bv_block_n) % (width_of_bv_block_n + 1);
      if (pos > 0)
        rank += rank_ptr[4 + pos - 1];
      #endif
      uint64_t mask = (bm_init_mask << (bv_pos % nodes_per_bv_block_n)) - 1;
      uint64_t bm = bm_loc[(bv_pos / nodes_per_bv_block_n) * multiplier];
      // return rank + __popcountdi2(bm & (mask - 1));
      #ifdef _WIN32
      return rank + static_cast<uintxx_t>(__popcnt64(bm & mask));
      #else
      return rank + static_cast<uintxx_t>(__builtin_popcountll(bm & mask));
      #endif
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
      if (pos)
        ptr_bit_count += cmn::read_uint24(block_ptr + lt_ptr_width + --pos * 3);
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

class tail_ptr_map {
  private:
    // __fq1 __fq2 tail_ptr_map(tail_ptr_map const&);
    // __fq1 __fq2 tail_ptr_map& operator=(tail_ptr_map const&);
  public:
    __fq1 __fq2 tail_ptr_map() {
    }
    __fq1 __fq2 virtual ~tail_ptr_map() {
    }
    __fq1 __fq2 virtual bool compare_tail(uintxx_t node_id, input_ctx& in_ctx, uintxx_t& ptr_bit_count) = 0;
    __fq1 __fq2 virtual void get_tail_str(uintxx_t node_id, gen::byte_str& tail_str) = 0;
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
    inline __fq1 __fq2 bool compare_tail_data(uint8_t *data, uintxx_t tail_ptr, input_ctx& in_ctx) {
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
        uintxx_t sfx_len = read_len(tail);
        #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
        uint8_t *sfx_buf = new uint8_t[sfx_len];
        #else
        uint8_t sfx_buf[sfx_len];
        #endif
        read_suffix(sfx_buf, data + tail_ptr - 1, sfx_len);
        if (cmn::memcmp(sfx_buf, in_ctx.key + in_ctx.key_pos, sfx_len) == 0) {
          in_ctx.key_pos += sfx_len;
          #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
          delete [] sfx_buf;
          #endif
          return true;
        }
        #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
        delete [] sfx_buf;
        #endif
      } else {
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
      return false;
    }
    __fq1 __fq2 void get_tail_data(uint8_t *data, uintxx_t tail_ptr, gen::byte_str& tail_str) {
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
        uintxx_t sfx_len = read_len(t);
        read_suffix(tail_str.data() + tail_str.length(), data + tail_ptr - 1, sfx_len);
        tail_str.set_length(tail_str.length() + sfx_len);
      } else {
        uintxx_t bin_len;
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
    uint8_t multiplier;
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

    __fq1 __fq2 void init_ptr_grp_map(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier, uint64_t *_tf_ptr_loc,
                uint8_t *data_loc, uintxx_t _key_count, uintxx_t _node_count, bool is_tail) {

      dict_obj = _dict_obj;
      trie_loc = _trie_loc;

      ptr_bm_loc = _multiplier == 1 ? _tf_ptr_loc : _bm_loc;
      multiplier = _multiplier;

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
    __fq1 __fq2 uintxx_t get_tail_ptr(uintxx_t node_id, uintxx_t& ptr_bit_count) {
      if (ptr_bit_count == UINTXX_MAX)
        ptr_bit_count = tail_lt->rank1(node_id);
      return (int_ptr_bv[ptr_bit_count++] << 8) | trie_loc[node_id];
    }
    __fq1 __fq2 bool compare_tail(uintxx_t node_id, input_ctx& in_ctx, uintxx_t& ptr_bit_count) {
      uintxx_t tail_ptr = get_tail_ptr(node_id, ptr_bit_count);
      if (inner_trie != nullptr)
        return inner_trie->compare_trie_tail(tail_ptr, in_ctx);
      return compare_tail_data(data, tail_ptr, in_ctx);
    }
    __fq1 __fq2 void get_tail_str(uintxx_t node_id, gen::byte_str& tail_str) {
      uintxx_t tail_ptr = UINTXX_MAX;
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
    uintxx_t max_node_id;
    uintxx_t cache_mask;
  public:
    __fq1 __fq2 bool try_find(uintxx_t& node_id, input_ctx& in_ctx) {
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
    __fq1 __fq2 bool try_find(uintxx_t& node_id, gen::byte_str& tail_str) {
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
    __fq1 __fq2 void init(uint8_t *_loc, uintxx_t _count, uintxx_t _max_node_id) {
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
    uintxx_t bv_bit_count;
  public:
    __fq1 __fq2 uintxx_t bin_srch_lkup_tbl(uintxx_t first, uintxx_t last, uintxx_t given_count) {
      while (first + 1 < last) {
        const uintxx_t middle = (first + last) >> 1;
        if (given_count < cmn::read_uint32(lt_rank_loc + middle * lt_width))
          last = middle;
        else
          first = middle;
      }
      return first;
    }
    __fq1 __fq2 uintxx_t select1(uintxx_t target_count) {
      if (target_count == 0)
        return 0;
      uint8_t *select_loc = lt_sel_loc1 + target_count / sel_divisor * 3;
      uintxx_t block = cmn::read_uint24(select_loc);
      // uintxx_t end_block = cmn::read_uint24(select_loc + 3);
      // if (block + 4 < end_block)
      //   block = bin_srch_lkup_tbl(block, end_block, target_count);
      uint8_t *block_loc = lt_rank_loc + block * lt_width;
      while (cmn::read_uint32(block_loc) < target_count) {
        block++;
        block_loc += lt_width;
      }
      block--;
      uintxx_t bv_pos = block * nodes_per_bv_block;
      block_loc -= lt_width;
      uintxx_t remaining = target_count - cmn::read_uint32(block_loc);
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
    __fq1 __fq2 inline uintxx_t get_count(uint8_t *block_loc, size_t pos_n) {
      return (block_loc[pos_n] + (((uintxx_t)(*block_loc) << pos_n) & 0x100));
    }
    __fq1 __fq2 inline uintxx_t bm_select1(uintxx_t remaining, uint64_t bm) {

      #ifdef __BMI2__
      uint64_t isolated_bit = _pdep_u64(1ULL << (remaining - 1), bm);
      uintxx_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      // if (bit_loc == 65) {
      //   printf("WARNING: UNEXPECTED bit_loc=65, bit_loc: %u\n", remaining);
      //   return 64;
      // }
      #endif

      #ifndef __BMI2__
      uintxx_t bit_loc = 0;
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
    __fq1 __fq2 void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc1, uintxx_t _bv_bit_count, uint64_t *_bm_loc, uint8_t _multiplier, uint8_t _lt_width) {
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
    uintxx_t node_count;

    uint8_t lt_not_given;
    uint8_t *trie_loc;
    tail_ptr_map *tail_map;
    bvlt_select child_lt;
    bvlt_select term_lt;
    GCFC_rev_cache rev_cache;

  public:
    __fq1 __fq2 bool compare_trie_tail(uintxx_t node_id, input_ctx& in_ctx) {
      do {
        if (tail_lt.is_set1(node_id)) {
          uintxx_t ptr_bit_count = UINTXX_MAX;
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
    __fq1 __fq2 bool copy_trie_tail(uintxx_t node_id, gen::byte_str& tail_str) {
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
      it->trie_level = mem[TRIE_LEVEL_LOC];
      it->load_inner_trie(mem);
      return it;
    }
    __fq1 __fq2 void load_inner_trie(uint8_t *trie_bytes) {

      tail_map = nullptr;
      trie_loc = nullptr;

      node_count = cmn::read_uint64(trie_bytes + NODE_COUNT_LOC);
      uintxx_t node_set_count = cmn::read_uint64(trie_bytes + NS_COUNT_LOC);
      uintxx_t key_count = cmn::read_uint64(trie_bytes + KEY_COUNT_LOC);
      if (key_count > 0) {
        uintxx_t rev_cache_count = cmn::read_uint64(trie_bytes + RC_COUNT_LOC);
        uintxx_t rev_cache_max_node_id = cmn::read_uint64(trie_bytes + RC_MAX_NID_LOC);
        uint8_t *rev_cache_loc = trie_bytes + cmn::read_uint64(trie_bytes + RCACHE_LOC);
        rev_cache.init(rev_cache_loc, rev_cache_count, rev_cache_max_node_id);

        uint8_t *term_select_lkup_loc = trie_bytes + cmn::read_uint64(trie_bytes + TERM_SEL_LT_LOC);
        uint8_t *term_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + TERM_RANK_LT_LOC);
        uint8_t *child_select_lkup_loc = trie_bytes + cmn::read_uint64(trie_bytes + CHILD_SEL_LT_LOC);
        uint8_t *child_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + CHILD_RANK_LT_LOC);

        uint8_t *tail_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + TAIL_RANK_LT_LOC);
        uint8_t *trie_tail_ptrs_data_loc = trie_bytes + cmn::read_uint64(trie_bytes + TRIE_TAIL_PTRS_DATA_LOC);

        uintxx_t tail_size = cmn::read_uint64(trie_tail_ptrs_data_loc);
        //uintxx_t trie_flags_size = cmn::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 16;
        trie_loc = tails_loc + tail_size;

        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TRIE_FLAGS_LOC));
        uint64_t *tf_ptr_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TAIL_FLAGS_PTR_LOC));

        bldr_options *opts = (bldr_options *) (trie_bytes + MDX_HEADER_SIZE);
        uint8_t multiplier = opts->trie_leaf_count > 0 ? 4 : 3;

        uint8_t encoding_type = tails_loc[TV_ENC_TYPE_LOC];
        uint8_t *tail_data_loc = tails_loc + cmn::read_uint64(tails_loc + TV_GRP_DATA_LOC);
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
    uintxx_t max_node_id;
    uintxx_t cache_mask;
  public:
    __fq1 __fq2 int try_find(input_ctx& in_ctx) {
      if (in_ctx.node_id >= max_node_id)
        return -1;
      uint8_t key_byte = in_ctx.key[in_ctx.key_pos];
      do {
        uintxx_t parent_node_id = in_ctx.node_id;
        uintxx_t cache_idx = (parent_node_id ^ (parent_node_id << MDX_CACHE_SHIFT) ^ key_byte) & cache_mask;
        fwd_cache *cche = cche0 + cache_idx;
        uintxx_t cache_node_id = cmn::read_uint24(&cche->parent_node_id1);
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
    __fq1 __fq2 void init(uint8_t *_loc, uintxx_t _count, uintxx_t _max_node_id) {
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
    leapfrog_asc *leaper;
    uintxx_t key_count;
    uint8_t *trie_bytes;
    uint16_t max_tail_len;
    uint16_t max_level;

  private:
    // __fq1 __fq2 static_trie(static_trie const&); // todo: restore? can't return beacuse of this
    // __fq1 __fq2 static_trie& operator=(static_trie const&);
    uintxx_t max_key_len;
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
        uintxx_t ptr_bit_count = UINTXX_MAX;
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
            uintxx_t prev_key_pos = in_ctx.key_pos;
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

    __fq1 __fq2 bool reverse_lookup(uintxx_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
      leaf_id++;
      uintxx_t node_id = leaf_lt->select1(leaf_id) - 1;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, to_reverse);
    }

    __fq1 __fq2 bool reverse_lookup_from_node_id(uintxx_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key, bool to_reverse = true) {
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

    __fq1 __fq2 uintxx_t leaf_rank1(uintxx_t node_id) {
      return leaf_lt->rank1(node_id);
    }

    __fq1 __fq2 bool is_leaf(uintxx_t node_id) {
      return (*leaf_lt)[node_id];
    }

    __fq1 __fq2 uintxx_t leaf_select1(uintxx_t leaf_id) {
      return leaf_lt->select1(leaf_id);
    }

    __fq1 __fq2 void push_to_ctx(iter_ctx& ctx, gen::byte_str& tail, uintxx_t node_id) {
      ctx.cur_idx++;
      tail.clear();
      update_ctx(ctx, tail, node_id);
    }

    __fq1 __fq2 void insert_arr(uintxx_t *arr, int arr_len, int pos, uintxx_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void insert_arr(uint16_t *arr, int arr_len, int pos, uint16_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void update_ctx(iter_ctx& ctx, gen::byte_str& tail, uintxx_t node_id) {
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

    __fq1 __fq2 uintxx_t pop_from_ctx(iter_ctx& ctx, gen::byte_str& tail) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      return read_from_ctx(ctx, tail);
    }

    __fq1 __fq2 uintxx_t read_from_ctx(iter_ctx& ctx, gen::byte_str& tail) {
      uintxx_t node_id = ctx.node_path[ctx.cur_idx];
      return node_id;
    }

    __fq1 __fq2 int next(iter_ctx& ctx, uint8_t *key_buf = nullptr) {
      gen::byte_str tail;
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
      uint8_t *tail_bytes = new uint8_t[max_tail_len + 1];
      #else
      uint8_t tail_bytes[max_tail_len + 1];
      #endif
      tail.set_buf_max_len(tail_bytes, max_tail_len);
      uintxx_t node_id = read_from_ctx(ctx, tail);
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
                  #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
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
            if (key_buf != nullptr)
              memcpy(key_buf, ctx.key, ctx.key_len);
            ctx.to_skip_first_leaf = true;
            #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
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
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
      delete [] tail_bytes;
      #endif
      return -2;
    }

    __fq1 __fq2 uintxx_t get_max_level() {
      return max_level;
    }

    __fq1 __fq2 uintxx_t get_key_count() {
      return key_count;
    }

    __fq1 __fq2 uintxx_t get_node_count() {
      return node_count;
    }

    __fq1 __fq2 uintxx_t get_max_key_len() {
      return max_key_len;
    }

    __fq1 __fq2 uintxx_t get_max_tail_len() {
      return max_tail_len;
    }

    __fq1 __fq2 void insert_into_ctx(iter_ctx& ctx, gen::byte_str& tail, uintxx_t node_id) {
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
      it->trie_level = mem[TRIE_LEVEL_LOC];
      it->load_static_trie(mem);
      return it;
    }

    __fq1 __fq2 uintxx_t find_first(const uint8_t *prefix, size_t prefix_len, iter_ctx& ctx, bool for_next = false) {
      input_ctx in_ctx;
      in_ctx.key = prefix;
      in_ctx.key_len = prefix_len;
      in_ctx.node_id = 0;
      lookup(in_ctx);
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
      uint8_t *tail_buf = new uint8_t[max_tail_len];
      #else
      uint8_t tail_buf[max_tail_len];
      #endif
      ctx.cur_idx = 0;
      gen::byte_str tail(tail_buf, max_tail_len);
      uintxx_t node_id = in_ctx.node_id;
      do {
        if (tail_lt[node_id])
          tail_map->get_tail_str(node_id, tail);
        else
          tail.append(trie_loc[node_id]);
        insert_into_ctx(ctx, tail, node_id);
        // printf("[%.*s]\n", (int) tail.length(), tail.data());
        // if (!rev_cache.try_find(in_ctx.node_id, tail))
          node_id = child_lt.select1(term_lt.rank1(node_id)) - 1;
        tail.clear();
      } while (node_id != 0);
      ctx.cur_idx--;
        // for (int i = 0; i < ctx.key_len; i++)
        //   printf("%c", ctx.key[i]);
        // printf("\nlsat tail len: %d\n", ctx.last_tail_len[ctx.cur_idx]);
      if (for_next) {
        ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
        ctx.last_tail_len[ctx.cur_idx] = 0;
        ctx.to_skip_first_leaf = false;
      }
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
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
      uint8_t *nv_loc = trie_bytes + cmn::read_uint64(trie_bytes + NULL_VAL_LOC);
      null_value_len = *nv_loc++;
      return nv_loc;
    }

    __fq1 __fq2 uint8_t *get_empty_value(size_t& empty_value_len) {
      uint8_t *ev_loc = trie_bytes + cmn::read_uint64(trie_bytes + EMPTY_VAL_LOC);
      empty_value_len = *ev_loc++;
      return ev_loc;
    }

    __fq1 __fq2 void load_static_trie(uint8_t *_trie_bytes = nullptr) {

      if (_trie_bytes != nullptr)
        trie_bytes = _trie_bytes;

      load_inner_trie(trie_bytes);
      opts = (bldr_options *) (trie_bytes + MDX_HEADER_SIZE);
      key_count = cmn::read_uint64(trie_bytes + KEY_COUNT_LOC);
      if (key_count > 0) {
        max_tail_len = cmn::read_uint16(trie_bytes + MAX_TAIL_LEN_LOC) + 1;

        uintxx_t node_set_count = cmn::read_uint64(trie_bytes + NS_COUNT_LOC);
        uint8_t *leaf_select_lkup_loc = trie_bytes + cmn::read_uint64(trie_bytes + LEAF_SEL_LT_LOC);
        uint8_t *leaf_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + LEAF_RANK_LT_LOC);
        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TRIE_FLAGS_LOC));
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
        max_key_len = cmn::read_uint64(trie_bytes + MAX_KEY_LEN_LOC);
        max_level = cmn::read_uint16(trie_bytes + MAX_LVL_LOC);
        uintxx_t fwd_cache_count = cmn::read_uint64(trie_bytes + FC_COUNT_LOC);
        uintxx_t fwd_cache_max_node_id = cmn::read_uint64(trie_bytes + FC_MAX_NID_LOC);
        uint8_t *fwd_cache_loc = trie_bytes + cmn::read_uint64(trie_bytes + FCACHE_LOC);
        fwd_cache.init(fwd_cache_loc, fwd_cache_count, fwd_cache_max_node_id);

        min_pos_stats min_stats;
        memcpy(&min_stats, trie_bytes + MIN_STAT_LOC, 4);
        uint8_t *min_pos_loc = trie_bytes + cmn::read_uint64(trie_bytes + SEC_CACHE_LOC);
        if (min_pos_loc == trie_bytes)
          min_pos_loc = nullptr;
        if (min_pos_loc != nullptr) {
          if (!opts->sort_nodes_on_freq) {
            leaper = new leapfrog_asc();
            leaper->init(min_stats, min_pos_loc);
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

class value_retriever_base : public ptr_group_map {
  public:
    __fq1 __fq2 virtual ~value_retriever_base() {}
    __fq1 __fq2 virtual void load_bm(uintxx_t node_id, val_ctx& vctx) = 0;
    __fq1 __fq2 virtual void init(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier,
            uint8_t *val_loc, uintxx_t _key_count, uintxx_t _node_count) = 0;
    __fq1 __fq2 virtual bool next_val(val_ctx& vctx) = 0;
    __fq1 __fq2 virtual void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) = 0;
    __fq1 __fq2 virtual const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) = 0;
    __fq1 __fq2 virtual static_trie *get_col_trie() = 0;
};

template<char pri_key>
class value_retriever : public value_retriever_base {
  protected:
    std::vector<uint8_t> prev_val;
    uintxx_t node_count;
    uintxx_t key_count;
    gen::int_bv_reader *int_ptr_bv = nullptr;
    gen::bv_reader<uint64_t> null_bv;
    inner_trie_fwd *col_trie = nullptr;
  public:
    __fq1 __fq2 uintxx_t scan_ptr_bits_val(val_ctx& vctx) {
      uintxx_t node_id_from = vctx.node_id - (vctx.node_id % nodes_per_ptr_block_n);
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      uint64_t bm_leaf = UINT64_MAX;
      if (key_count > 0)
        bm_leaf = ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * multiplier];
      vctx.rpt_left = 0;
      uintxx_t last_pbc = vctx.ptr_bit_count;
      while (node_id_from < vctx.node_id) {
        if (bm_leaf & bm_mask) {
          if (vctx.rpt_left == 0) {
            uint8_t code = ptr_reader.read8(vctx.ptr_bit_count);
            uint8_t grp_no = code_lt_code_len[code] & 0x0F;
            if (grp_no != rpt_grp_no && rpt_grp_no > 0)
              last_pbc = vctx.ptr_bit_count;
            uint8_t code_len = code_lt_code_len[code] >> 4;
            uint8_t bit_len = code_lt_bit_len[code];
            vctx.ptr_bit_count += code_len;
            uintxx_t count = ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
            if (rpt_grp_no > 0 && grp_no == rpt_grp_no) {
              vctx.rpt_left = count - 1; // todo: -1 not needed
            }
          } else
            vctx.rpt_left--;
        }
        node_id_from++;
        bm_mask <<= 1;
      }
      if (vctx.rpt_left > 0) {
        vctx.next_pbc = vctx.ptr_bit_count;
        vctx.ptr_bit_count = last_pbc;
        return vctx.ptr_bit_count;
      }
      uint8_t code = ptr_reader.read8(vctx.ptr_bit_count);
      uint8_t grp_no = code_lt_code_len[code] & 0x0F;
      if ((grp_no == rpt_grp_no && rpt_grp_no > 0)) {
        uint8_t code_len = code_lt_code_len[code] >> 4;
        uint8_t bit_len = code_lt_bit_len[code];
        vctx.ptr_bit_count += code_len;
        vctx.rpt_left = ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
        vctx.next_pbc = vctx.ptr_bit_count;
        vctx.ptr_bit_count = last_pbc;
      }
      return vctx.ptr_bit_count;
    }
    __fq1 __fq2 uintxx_t get_ptr_bit_count_val(val_ctx& vctx) {
      vctx.ptr_bit_count = ptr_reader.get_ptr_block2(vctx.node_id);
      return scan_ptr_bits_val(vctx);
    }

    bool is_repeat(val_ctx& vctx) {
      if (vctx.rpt_left > 0) {
        vctx.rpt_left--;
        if (vctx.rpt_left == 0 && vctx.next_pbc != 0) {
          vctx.ptr_bit_count = vctx.next_pbc;
          vctx.next_pbc = 0;
        }
        return true;
      }
      return false;
    }
    __fq1 __fq2 uint8_t *get_val_loc(val_ctx& vctx) {
      if (group_count == 1) {
        vctx.grp_no = 0;
        uintxx_t ptr_pos = vctx.node_id;
        if (key_count > 0)
          ptr_pos = ((static_trie *) dict_obj)->get_leaf_lt()->rank1(vctx.node_id);
        vctx.ptr = (*int_ptr_bv)[ptr_pos];
        // if (vctx.grp_no < grp_idx_limit)
        //   ptr = read_ptr_from_idx(vctx.grp_no, ptr);
        return grp_data[0] + vctx.ptr;
      }
      if (vctx.ptr_bit_count == UINTXX_MAX)
        get_ptr_bit_count_val(vctx);
      uint8_t code = ptr_reader.read8(vctx.ptr_bit_count);
      uint8_t bit_len = code_lt_bit_len[code];
      uint8_t grp_no = code_lt_code_len[code] & 0x0F;
      uint8_t code_len = code_lt_code_len[code] >> 4;
      vctx.ptr_bit_count += code_len;
      uintxx_t rptct_or_ptr = ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
      if (rpt_grp_no > 0 && grp_no == rpt_grp_no) {
        vctx.rpt_left = rptct_or_ptr - 1; // todo: -1 not needed
        return grp_data[vctx.grp_no] + vctx.ptr;
      }
      vctx.grp_no = grp_no;
      vctx.ptr = rptct_or_ptr;
      if (vctx.grp_no < grp_idx_limit)
        vctx.ptr = read_ptr_from_idx(vctx.grp_no, vctx.ptr);
      return grp_data[vctx.grp_no] + vctx.ptr;
    }

    void load_bm(uintxx_t node_id, val_ctx& vctx) {
      if constexpr (pri_key == 'Y') {
        vctx.bm_mask = (bm_init_mask << (node_id % nodes_per_bv_block_n));
        vctx.bm_leaf = UINT64_MAX;
        vctx.bm_leaf = ptr_bm_loc[(node_id / nodes_per_bv_block_n) * multiplier];
      }
    }

    __fq1 __fq2 bool skip_non_leaf_nodes(val_ctx& vctx) {
      if constexpr (pri_key == 'Y') {
        while (vctx.bm_mask && (vctx.bm_leaf & vctx.bm_mask) == 0) {
          vctx.node_id++;
          vctx.bm_mask <<= 1;
        }
      }
      if (vctx.node_id >= node_count)
        return false;
      if constexpr (pri_key == 'N') {
        return true;
      } else {
        if (vctx.bm_mask != 0)
          return true;
        load_bm(vctx.node_id, vctx);
        return skip_non_leaf_nodes(vctx);
      }
    }
    __fq1 __fq2 bool skip_to_next_leaf(val_ctx& vctx) {
      vctx.node_id++;
      if constexpr (pri_key == 'Y') {
        vctx.bm_mask <<= 1;
      }
      return skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 void init(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier,
                uint8_t *val_loc, uintxx_t _key_count, uintxx_t _node_count) {
      key_count = _key_count;
      node_count = _node_count;
      init_ptr_grp_map(_dict_obj, _trie_loc, _bm_loc, _multiplier, _bm_loc, val_loc, _key_count, _node_count, false);
      uint8_t *data_loc = val_loc + cmn::read_uint64(val_loc + TV_GRP_DATA_LOC);
      uint8_t *ptrs_loc = val_loc + cmn::read_uint64(val_loc + TV_GRP_PTRS_LOC);
      uint64_t *null_bv_loc = (uint64_t *) (val_loc + cmn::read_uint64(val_loc + TV_NULL_BV_LOC));
      uintxx_t null_bv_size = cmn::read_uint64(val_loc + TV_NULL_BV_SIZE_LOC);
      null_bv.set_bv(null_bv_loc, null_bv_size);
      if (group_count == 1 || val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY) {
        int_ptr_bv = new gen::int_bv_reader();
        int_ptr_bv->init(ptrs_loc, val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY ? val_loc[0] : data_loc[1]);
      }
      col_trie = nullptr;
      if (val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY) {
        col_trie = _dict_obj->new_instance(data_loc);
      }
    }
    __fq1 __fq2 bool is_null(uintxx_t node_id) {
      return null_bv[node_id];
    }
    __fq1 __fq2 static_trie *get_col_trie() {
      return (static_trie *) col_trie;
    }
    __fq1 __fq2 virtual ~value_retriever() {
      if (int_ptr_bv != nullptr)
        delete int_ptr_bv;
      if (col_trie != nullptr)
        delete col_trie;
    }
};

template<char pri_key>
class stored_val_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~stored_val_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      size_t len_len;
      *vctx.val_len = cmn::read_vint32(vctx.byts, &len_len);
      if (vctx.val != nullptr)
        vctx.val->txt_bin = (vctx.byts + len_len);
      vctx.byts += len_len;
      vctx.byts += *vctx.val_len;
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.ptr_bit_count = Parent::ptr_reader.get_ptr_block3(node_id);
      vctx.byts = Parent::grp_data[0] + vctx.ptr_bit_count + 2;
      size_t len_len;
      uintxx_t vint_len;
      uintxx_t node_id_from = vctx.node_id - (vctx.node_id % nodes_per_ptr_block_n);
      Parent::load_bm(node_id_from, vctx);
      while (node_id_from < vctx.node_id) {
        if (vctx.bm_leaf & vctx.bm_mask) {
          vint_len = cmn::read_vint32(vctx.byts, &len_len);
          // printf("nid from: %u, to: %u, len:%u, len_len:%lu\n", node_id_from, node_id, vint_len, len_len);
          vctx.byts += len_len;
          vctx.byts += vint_len;
        }
        node_id_from++;
        vctx.bm_mask <<= 1;
      }
      // unallocate any allocations as value is read directly
      vctx.unallocate();
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(8, false, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return vctx.val->txt_bin;
    }
};

template<char pri_key>
class col_trie_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~col_trie_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      uintxx_t ptr_pos = vctx.node_id;
      if (Parent::key_count > 0)
        ptr_pos = ((static_trie *) Parent::dict_obj)->get_leaf_lt()->rank1(vctx.node_id);
      uintxx_t col_trie_node_id = (*Parent::int_ptr_bv)[ptr_pos];
      // printf("Col trie node_id: %u\n", col_trie_node_id);
      uint8_t val_loc[16];
      if (Parent::data_type != MST_TEXT && Parent::data_type != MST_BIN)
        vctx.val->txt_bin = val_loc;
      ((static_trie *) Parent::col_trie)->reverse_lookup_from_node_id(col_trie_node_id, vctx.val_len, vctx.val->txt_bin, true);
      if (Parent::data_type != MST_TEXT && Parent::data_type != MST_BIN) {
        size_t null_len;
        uint8_t *null_val = ((static_trie *) Parent::dict_obj)->get_null_value(null_len);
        cmn::convert_back(Parent::data_type, vctx.val->txt_bin, *vctx.val, vctx.val_len, null_val, null_len);
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      Parent::load_bm(node_id, vctx);
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(0, false, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return vctx.val->txt_bin;
    }
};

template<char pri_key>
class words_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~words_retriever() {}
    __fq1 __fq2 uintxx_t scan_ptr_bits_words(uintxx_t node_id, uintxx_t ptr_bit_count) {
      uintxx_t node_id_from = node_id - (node_id % nodes_per_ptr_block_n);
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      uint64_t bm_leaf = UINT64_MAX;
      if (Parent::key_count > 0)
        bm_leaf = Parent::ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * Parent::multiplier];
      size_t to_skip = 0;
      uintxx_t last_pbc = ptr_bit_count;
      while (node_id_from < node_id) {
        if (bm_leaf & bm_mask) {
          if (to_skip == 0) {
            uint8_t code = Parent::ptr_reader.read8(ptr_bit_count);
            uint8_t grp_no = Parent::code_lt_code_len[code] & 0x0F;
            if (grp_no != Parent::rpt_grp_no && Parent::rpt_grp_no > 0)
              last_pbc = ptr_bit_count;
            uint8_t code_len = Parent::code_lt_code_len[code] >> 4;
            uint8_t bit_len = Parent::code_lt_bit_len[code];
            ptr_bit_count += code_len;
            uintxx_t count = Parent::ptr_reader.read(ptr_bit_count, bit_len - code_len);
            if (Parent::rpt_grp_no > 0 && grp_no == Parent::rpt_grp_no) {
              to_skip = count - 1; // todo: -1 not needed
              //printf("To skip: %lu\n", to_skip);
            } else {
              while (count--) {
                code = Parent::ptr_reader.read8(ptr_bit_count);
                ptr_bit_count += Parent::code_lt_bit_len[code];
              }
            }
          } else
            to_skip--;
        }
        node_id_from++;
        bm_mask <<= 1;
      }
      uint8_t code = Parent::ptr_reader.read8(ptr_bit_count);
      uint8_t grp_no = Parent::code_lt_code_len[code] & 0x0F;
      if (to_skip > 0 || (grp_no == Parent::rpt_grp_no && Parent::rpt_grp_no > 0))
        ptr_bit_count = last_pbc;
      return ptr_bit_count;
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      uint8_t code = Parent::ptr_reader.read8(vctx.ptr_bit_count);
      uint8_t bit_len = Parent::code_lt_bit_len[code];
      uint8_t grp_no = Parent::code_lt_code_len[code] & 0x0F;
      uint8_t code_len = Parent::code_lt_code_len[code] >> 4;
      if (grp_no != 0) {
        printf("Group no. ought to be 0: %d\n", grp_no);
      }
      vctx.ptr_bit_count += code_len;
      uintxx_t word_count = Parent::ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
      size_t word_len;
      *vctx.val_len = 0;
      for (size_t i = 0; i < word_count; i++) {
        code = Parent::ptr_reader.read8(vctx.ptr_bit_count);
        bit_len = Parent::code_lt_bit_len[code];
        grp_no = Parent::code_lt_code_len[code] & 0x0F;
        code_len = Parent::code_lt_code_len[code] >> 4;
        vctx.ptr_bit_count += code_len;
        uintxx_t it_leaf_id = Parent::ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
        ((static_trie *) Parent::inner_tries[grp_no])->reverse_lookup(it_leaf_id, &word_len,
            vctx.val->txt_bin + *vctx.val_len, true);
        *vctx.val_len += word_len;
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      Parent::load_bm(node_id, vctx);
      Parent::skip_non_leaf_nodes(vctx);
      uintxx_t pbc = Parent::ptr_reader.get_ptr_block3(vctx.node_id);
      vctx.ptr_bit_count = scan_ptr_bits_words(vctx.node_id, pbc);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(0, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      // printf("pbc: %u\n", *p_ptr_bit_count);
      return ret_val.txt_bin;
    }
};

class block_retriever_base {
  public:
    __fq1 __fq2 virtual ~block_retriever_base() {}
    __fq1 __fq2 virtual void block_operation(uintxx_t node_id, int len, mdx_val &sum_out, int64_t *out = nullptr) = 0;
    __fq1 __fq2 virtual void block_operation32(uintxx_t node_id, int len, mdx_val &sum_out, int32_t *out = nullptr) = 0;
};

template <char type_id, typename T, char operation>
class fast_vint_block_retriever : public block_retriever_base {
  private:
    value_retriever_base *val_retriever;
  public:
    __fq1 __fq2 fast_vint_block_retriever(value_retriever_base *_val_retriever) {
      val_retriever = _val_retriever;
    }
    __fq1 __fq2 void block_operation(uintxx_t node_id, int len, mdx_val &sum_out, int64_t *out = nullptr) {
      int64_t sum_int = 0;
      double sum_dbl = 0;
      int64_t i64;
      int32_t *i32_vals = new int32_t[64];
      int64_t *i64_vals = new int64_t[64];
      uint8_t *byts;
      if constexpr (type_id == '.')
        byts = new uint8_t[64];
      uint8_t *data = val_retriever->get_grp_data(0) + val_retriever->get_ptr_reader()->get_ptr_block3(node_id) + 2;
      size_t out_pos = 0;
      size_t offset;
      if constexpr (type_id == '.')
        offset = 3;
      else
        offset = 2;
      size_t block_size;
      while (len > 0) {
        if ((*data & 0x80) == 0) {
          block_size = allflic48::decode(data + offset, data[1], i32_vals);
          for (size_t i = 0; i < data[1]; i++) {
            i64 = allflic48::zigzag_decode(i32_vals[i]);
            if constexpr (operation == 'a' && type_id == 'i')
               out[out_pos++] = i64;
            else {
              if constexpr (type_id != 'i')
                i64_vals[i] = i64;
              if constexpr (type_id == 'i')
                sum_int += i64;
            }
          }
          if constexpr (type_id == '.')
            memset(byts, '\0', 64);
        } else {
          if constexpr (type_id == '.')
            block_size = allflic48::decode(data + offset, data[1], i64_vals, byts);
          else
            block_size = allflic48::decode(data + offset, data[1], i64_vals);
          for (size_t i = 0; i < data[1]; i++) {
            i64 = allflic48::zigzag_decode(i64_vals[i]);
            if constexpr (operation == 'a' && type_id == 'i')
               out[out_pos++] = i64;
            else {
              if constexpr (type_id != 'i')
                i64_vals[i] = i64;
              if constexpr (type_id == 'i')
                sum_int += i64;
            }
          }
        }
        if constexpr (type_id != 'i') {
          double dbl;
          for (size_t i = 0; i < data[1]; i++) {
            if constexpr (type_id == '.') {
              if (byts[i] == 7) {
                memcpy(&dbl, i64_vals + i, 8);
              } else {
                dbl = static_cast<double>(i64_vals[i]);
                dbl /= allflic48::tens()[data[2]];
              }
            } else {
              dbl = static_cast<double>(i64_vals[i]);
              dbl /= allflic48::tens()[val_retriever->data_type - MST_DEC0];
            }
            if constexpr (operation == 's')
              sum_dbl += dbl;
            if constexpr (operation == 'a') {
              memcpy(out + out_pos, &dbl, 8);
              out_pos++;
            }
          }
        }
        len -= data[1];
        data += block_size;
        data += offset;
      }
      delete [] i32_vals;
      delete [] i64_vals;
      if constexpr (type_id == '.')
        delete[] byts;
      if constexpr (operation == 's') {
        if constexpr (type_id == 'i')
          sum_out.i64 = sum_int;
        else
          sum_out.dbl = sum_dbl;
      }
    }
    __fq1 __fq2 void block_operation32(uintxx_t node_id, int len, mdx_val &sum_out, int32_t *out = nullptr) {
      int64_t sum_int = 0;
      double sum_flt = 0;
      int32_t i32;
      int32_t *i32_vals = new int32_t[64];
      uint8_t *data = val_retriever->get_grp_data(0) + val_retriever->get_ptr_reader()->get_ptr_block3(node_id) + 2;
      size_t out_pos = 0;
      size_t offset;
      if constexpr (type_id == '.')
        offset = 3;
      else
        offset = 2;
      size_t block_size;
      while (len > 0) {
        block_size = allflic48::decode(data + offset, data[1], i32_vals);
        for (size_t i = 0; i < data[1]; i++) {
          i32 = (int32_t) allflic48::zigzag_decode(i32_vals[i]);
          if constexpr (operation == 'a')
              out[out_pos++] = i32;
          else {
            if constexpr (type_id != 'i')
              i32_vals[i] = i32;
            if constexpr (type_id == 'i')
              sum_int += i32;
          }
        }
        if constexpr (type_id != 'i') {
          float flt;
          for (size_t i = 0; i < data[1]; i++) {
            if constexpr (type_id == '.') {
              flt = static_cast<float>(i32_vals[i]);
              flt /= allflic48::tens()[data[2]];
            } else {
              flt = static_cast<float>(i32_vals[i]);
              flt /= allflic48::tens()[val_retriever->data_type - MST_DEC0];
            }
            if constexpr (operation == 's')
              sum_flt += flt;
            if constexpr (operation == 'a')
              memcpy(i32_vals + i, &flt, 4);
          }
        }
        len -= data[1];
        data += block_size;
        data += offset;
      }
      delete [] i32_vals;
      if constexpr (operation == 's') {
        if constexpr (type_id == 'i')
          sum_out.i64 = sum_int;
        else
          sum_out.dbl = sum_flt;
      }
    }
};

template<char pri_key>
class fast_vint_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~fast_vint_retriever() {}
    __fq1 __fq2 void retrieve_block(uintxx_t node_id, val_ctx& vctx) {
      Parent::load_bm(node_id, vctx);
      // if ((node_id / nodes_per_bv_block_n) == (vctx.node_id / nodes_per_bv_block_n))
      //   return;
      // if (vctx.node_id != UINTXX_MAX)
      //   return;
      vctx.node_id = node_id;
      vctx.ptr_bit_count = Parent::ptr_reader.get_ptr_block3(node_id);
      uint8_t *data = Parent::grp_data[0] + vctx.ptr_bit_count + 2;
      vctx.count = data[1];
      vctx.dec_count = data[2]; // only for data_type MST_DECV
      size_t offset = (Parent::data_type == MST_DECV ? 3 : 2);
      if ((*data & 0x80) == 0) {
        allflic48::decode(data + offset, vctx.count, vctx.i32_vals);
        for (size_t i = 0; i < vctx.count; i++)
          vctx.i64_vals[i] = allflic48::zigzag_decode(vctx.i32_vals[i]);
        memset(vctx.byts, '\0', vctx.count); // not setting lens, just 0s
      } else {
        allflic48::decode(data + offset, vctx.count, vctx.i64_vals, vctx.byts);
        for (size_t i = 0; i < vctx.count; i++)
          vctx.i64_vals[i] = allflic48::zigzag_decode(vctx.i64_vals[i]);
      }
    }
    __fq1 __fq2 bool skip_non_leaf_nodes(val_ctx& vctx) {
      if constexpr (pri_key == 'Y') {
        while (vctx.bm_mask && (vctx.bm_leaf & vctx.bm_mask) == 0) {
          vctx.node_id++;
          vctx.bm_mask <<= 1;
        }
      }
      if (vctx.node_id >= Parent::node_count)
        return false;
      if constexpr (pri_key == 'N') {
        if ((vctx.node_id % nodes_per_bv_block_n) == 0)
          retrieve_block(vctx.node_id, vctx);
        return true;
      } else {
        if (vctx.bm_mask != 0)
          return true;
        retrieve_block(vctx.node_id, vctx);
        return skip_non_leaf_nodes(vctx);
      }
    }
    __fq1 __fq2 bool skip_to_next_leaf(val_ctx& vctx) {
      vctx.node_id++;
      if constexpr (pri_key == 'Y') {
        vctx.bm_mask <<= 1;
      }
      return skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      int64_t i64;
      int to_skip;
      if constexpr (pri_key == 'N') {
        to_skip = vctx.node_id % nodes_per_bv_block_n;
      } else {
        #ifdef _WIN32
        to_skip = __popcnt64(vctx.bm_leaf & (vctx.bm_mask - 1));
        #else
        to_skip = __builtin_popcountll(vctx.bm_leaf & (vctx.bm_mask - 1));
        #endif
      }
      i64 = *(vctx.i64_vals + to_skip);
      switch (Parent::data_type) {
        case MST_INT:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISO_MS: case MST_DATETIME_ISOT_MS: {
          *((int64_t *) vctx.val) = i64;
          // printf("%lld\n", i64);
        } break;
        case MST_DECV: {
          if (vctx.byts[to_skip] == 7) {
            *((uint64_t *) vctx.val) = i64;
          } else {
            double dbl = static_cast<double>(i64);
            dbl /= allflic48::tens()[vctx.dec_count];
            *((double *) vctx.val) = dbl;
            // printf("%.2lf\n", *((double *) vctx.val));
          }
        } break;
        case MST_DEC0: case MST_DEC1: case MST_DEC2:
        case MST_DEC3: case MST_DEC4: case MST_DEC5: case MST_DEC6:
        case MST_DEC7: case MST_DEC8: case MST_DEC9: {
          double dbl = static_cast<double>(i64);
          dbl /= allflic48::tens()[Parent::data_type - MST_DEC0];
          *((double *) vctx.val) = dbl;
          // printf("%.2lf\n", *((double *) vctx.val));
        } break;
      }
      i64 = *((int64_t *) vctx.val);
      if (i64 == 0 && Parent::is_null(vctx.node_id))
        *((int64_t *) vctx.val) = INT64_MIN;
      return skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      retrieve_block(node_id, vctx);
      skip_non_leaf_nodes(vctx);
      *vctx.val_len = 8;
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(8, false, true);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class uniq_bin_val_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~uniq_bin_val_retriever() {}
    __fq1 __fq2 const uint8_t *get_bin_val(uint8_t *val_loc, uintxx_t& bin_len) {
      tail_ptr_flat_map::read_len_bw(val_loc, bin_len);
      return val_loc + 1;
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if (!Parent::is_repeat(vctx)) {
        uint8_t *val_loc = Parent::get_val_loc(vctx);
        uintxx_t bin_len;
        get_bin_val(val_loc, bin_len);
        *vctx.val_len = bin_len;
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.init_pbc_vars();
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class uniq_text_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~uniq_text_retriever() {}
    __fq1 __fq2 void get_val_str(gen::byte_str& ret, uintxx_t val_ptr, uint8_t grp_no, size_t max_valset_len) {
      uint8_t *val = Parent::grp_data[grp_no];
      ret.clear();
      uint8_t *t = val + val_ptr;
      if (*t > 15 && *t < 32) {
        uintxx_t bin_len;
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
      uintxx_t pfx_len = tail_ptr_flat_map::read_len(t);
      memmove(ret.data() + pfx_len, ret.data(), ret.length());
      ret.set_length(ret.length() + pfx_len);
      read_prefix(ret.data() + pfx_len - 1, val + val_ptr - 1, pfx_len);
    }
    __fq1 __fq2 void read_prefix(uint8_t *out_str, uint8_t *t, uintxx_t pfx_len) {
      while (*t < 15 || *t > 31)
        t--;
      while (*t != 15) {
        uintxx_t prev_pfx_len;
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
    __fq1 __fq2 const uint8_t *get_text_val(uint8_t *val_loc, uint8_t grp_no, uint8_t *ret_val, size_t& val_len) {

      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
      uint8_t *val_str_buf = new uint8_t[Parent::max_len];
      #else
      uint8_t val_str_buf[Parent::max_len];
      #endif
      gen::byte_str val_str(val_str_buf, Parent::max_len);
      uint8_t *val_start = Parent::grp_data[grp_no];
      if (*val_start != 0)
        Parent::inner_tries[grp_no]->copy_trie_tail((uintxx_t) (val_loc - val_start), val_str);
      else
        get_val_str(val_str, (uintxx_t) (val_loc - val_start), grp_no, Parent::max_len);
      val_len = val_str.length();
      memcpy(ret_val, val_str.data(), val_len);
      #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
      delete [] val_str_buf;
      #endif
      return ret_val;
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if (!Parent::is_repeat(vctx)) {
        uint8_t *val_loc = Parent::get_val_loc(vctx);
        get_text_val(val_loc, vctx.grp_no, vctx.val->txt_bin, *vctx.val_len);
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.init_pbc_vars();
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class uniq_ifp_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~uniq_ifp_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if (!Parent::is_repeat(vctx)) {
        uint8_t *val_loc = Parent::get_val_loc(vctx);
        int64_t i64;
        allflic48::simple_decode(val_loc, 1, &i64);
        i64 = allflic48::zigzag_decode(i64);
        // printf("i64: %lld\n", i64);
        if (Parent::data_type >= MST_DEC0 && Parent::data_type <= MST_DEC9 && i64 != INT64_MIN) {
          double dbl = static_cast<double>(i64);
          dbl /= allflic48::tens()[Parent::data_type - MST_DEC0];
          *((double *)vctx.val) = dbl;
        } else
          memcpy(vctx.val, &i64, sizeof(int64_t));
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.init_pbc_vars();
      *vctx.val_len = 8;
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class delta_val_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~delta_val_retriever() {}
    __fq1 __fq2 void add_delta(val_ctx& vctx) {
      if (Parent::is_repeat(vctx)) {
        vctx.i64 += vctx.i64_delta;
        return;
      }
      uint8_t *val_loc = Parent::get_val_loc(vctx);
      int64_t i64;
      if (val_loc != nullptr) {
        if (*val_loc == 0xF8 && val_loc[1] == 1) {
          i64 = INT64_MIN;
        } else {
          allflic48::simple_decode(val_loc, 1, &i64);
          i64 = allflic48::zigzag_decode(i64);
        }
        // printf("Delta node id: %u, value: %lld\n", delta_node_id, delta_val);
        vctx.i64_delta = i64;
        vctx.i64 += i64;
      }
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if ((vctx.node_id % nodes_per_bv_block_n) == 0)
        vctx.i64 = 0;
      add_delta(vctx);
      if (Parent::data_type == MST_INT || (Parent::data_type >= MST_DATE_US && Parent::data_type <= MST_DATETIME_ISOT_MS)) {
        *((int64_t *) vctx.val) = vctx.i64;
      } else {
        double dbl = static_cast<double>(vctx.i64);
        dbl /= allflic48::tens()[Parent::data_type - MST_DEC0];
        *((double *) vctx.val) = dbl;
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.init_pbc_vars();
      vctx.node_id = node_id / nodes_per_bv_block_n;
      vctx.node_id *= nodes_per_bv_block_n;
      Parent::load_bm(vctx.node_id, vctx);
      vctx.i64 = 0;
      while (vctx.node_id < node_id) {
        // printf("Delta Node-id: %u\n", delta_node_id);
        if (vctx.bm_mask & vctx.bm_leaf)
          add_delta(vctx);
        vctx.bm_mask <<= 1;
        vctx.node_id++;
      }
      *vctx.val_len = 8;
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
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

#define MDX_REV_ST_INIT 0
#define MDX_REV_ST_NEXT 1
#define MDX_REV_ST_END 2
struct rev_nodes_ctx : public input_ctx, public iter_ctx {
  const uint8_t *rev_node_list;
  size_t rev_nl_len;
  uintxx_t rev_nl_pos;
  uintxx_t prev_node_id;
  uint8_t rev_state;
  uint8_t trie_no;
  __fq1 __fq2 rev_nodes_ctx() {
    reset();
  }
  __fq1 __fq2 void init() {
    reset();
  }
  __fq1 __fq2 void init(static_trie *trie) {
    if (is_allocated)
      this->close();
    ((iter_ctx *) this)->init((uint16_t) trie->get_max_key_len(), (uint16_t) trie->get_max_level());
  }
  __fq1 __fq2 void reset() {
    rev_node_list = nullptr;
    prev_node_id = 0;
    node_id = 0;
    trie_no = 0;
    rev_state = MDX_REV_ST_INIT;
  }
};

class static_trie_map : public static_trie {
  private:
    // __fq1 __fq2 static_trie_map(static_trie_map const&); // todo: restore? can't return because of this
    // __fq1 __fq2 static_trie_map& operator=(static_trie_map const&);
    value_retriever_base **val_map;
    uint16_t val_count;
    uint16_t pk_col_count;
    uintxx_t max_val_len;
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
#if !defined(ESP32)
      if (is_mmapped)
        map_unmap();
#endif
      if (trie_bytes != nullptr) {
        if (cleanup_object != nullptr) {
          cleanup_object->release();
          delete cleanup_object;
          cleanup_object = nullptr;
        }
      }
      if (val_map != nullptr) {
        for (size_t i = 0; i < val_count; i++) {
          delete val_map[i];
        }
        delete [] val_map;
      }
    }

    __fq1 __fq2 void set_cleanup_object(cleanup_interface *_cleanup_obj) {
      cleanup_object = _cleanup_obj;
    }

    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      static_trie_map *it = new static_trie_map();
      it->trie_level = mem[TRIE_LEVEL_LOC];
      it->load_from_mem(mem, 0);
      return it;
    }

    __fq1 __fq2 bool get(input_ctx& in_ctx, size_t *in_size_out_value_len, mdx_val &val) {
      bool is_found = lookup(in_ctx);
      if (is_found) {
        if (val_count > 1)
          val_map[1]->get_val(in_ctx.node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
    }

    __fq1 __fq2 const bool next_col_val(int col_val_idx, val_ctx& vctx) {
      return val_map[col_val_idx]->next_val(vctx);
    }

    __fq1 __fq2 const uint8_t *get_col_val(uintxx_t node_id, size_t col_val_idx,
          size_t *in_size_out_value_len, mdx_val &val) {
      if (col_val_idx < pk_col_count) { // TODO: Extract from composite keys,Convert numbers back
        if (!reverse_lookup_from_node_id(node_id, in_size_out_value_len, val.txt_bin))
          return nullptr;
        size_t null_len;
        uint8_t *null_val = get_null_value(null_len);
        cmn::convert_back(get_column_type(col_val_idx), val.txt_bin, val, in_size_out_value_len, null_val, null_len);
        return (const uint8_t *) val.txt_bin;
      }
      return val_map[col_val_idx]->get_val(node_id, in_size_out_value_len, val);
    }

    __fq1 __fq2 const char *get_table_name() {
      return names_start + cmn::read_uint16(names_loc + 2);
    }

    __fq1 __fq2 size_t get_column_size(size_t i) {
      uint8_t *val_table_loc = trie_bytes + cmn::read_uint64(trie_bytes + VAL_TBL_LOC);
      size_t col_start = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
      if (col_start == 0)
        col_start = cmn::read_uint64(val_table_loc);
      size_t col_end = trie_size;
      if (col_end == 0)
        col_end = col_start;
      while (++i < val_count && col_end == 0)
        col_end = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
      return col_end - col_start;
    }

    __fq1 __fq2 const char *get_column_name(size_t i) {
      return names_start + cmn::read_uint16(names_loc + (i + 2) * 2);
    }

    __fq1 __fq2 const char *get_column_types() {
      return names_start;
    }

    __fq1 __fq2 char get_column_type(size_t i) {
      return names_start[i];
    }

    __fq1 __fq2 const char *get_column_encodings() {
      return column_encoding;
    }

    __fq1 __fq2 char get_column_encoding(size_t i) {
      return column_encoding[i];
    }

    __fq1 __fq2 uintxx_t get_max_val_len() {
      return max_val_len;
    }

    __fq1 __fq2 uintxx_t get_max_val_len(size_t col_val_idx) {
      return val_map[col_val_idx]->max_len;
    }

    __fq1 __fq2 static_trie *get_col_trie(size_t col_val_idx) {
      return val_map[col_val_idx]->get_col_trie();
    }

    __fq1 __fq2 static_trie_map *get_col_trie_map(size_t col_val_idx) {
      return (static_trie_map *) val_map[col_val_idx]->get_col_trie();
    }

    __fq1 __fq2 static_trie_map **get_trie_groups(size_t col_val_idx) {
      inner_trie_fwd **inner_tries = val_map[col_val_idx]->inner_tries;
      return (static_trie_map **) inner_tries;
    }

    __fq1 __fq2 size_t get_trie_start_group(size_t col_val_idx) {
      return val_map[col_val_idx]->inner_trie_start_grp;
    }

    __fq1 __fq2 size_t get_group_count(size_t col_val_idx) {
      return val_map[col_val_idx]->group_count;
    }

    __fq1 __fq2 uint16_t get_column_count() {
      return val_count;
    }

    __fq1 __fq2 uint16_t get_pk_col_count() {
      return pk_col_count;
    }

    __fq1 __fq2 value_retriever_base *get_value_retriever(size_t col_val_idx) {
      return val_map[col_val_idx];
    }

    template <char operation>
    __fq1 __fq2 block_retriever_base *get_block_retriever(size_t col_val_idx) {
      if (get_column_encoding(col_val_idx) != MSE_VINTGB)
        return nullptr;
      value_retriever_base *val_retriever = val_map[col_val_idx];
      block_retriever_base *inst;
      switch (val_retriever->data_type) {
        case MST_INT:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS: {
          inst = new fast_vint_block_retriever<'i', int64_t, operation>(val_retriever);
        } break;
        case MST_DECV: {
          inst = new fast_vint_block_retriever<'.', double, operation>(val_retriever);
        } break;
        case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
        case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9: {
          inst = new fast_vint_block_retriever<'x', double, operation>(val_retriever);
        } break;
      }
      return inst;
    }

    __fq1 __fq2 bool read_rev_node_list(uintxx_t ct_node_id, rev_nodes_ctx& rev_ctx) {
      mdx_val mv;
      rev_ctx.rev_node_list = get_col_val(ct_node_id, 1, &rev_ctx.rev_nl_len, mv);
      printf("rev_nl_len: %zu\n", rev_ctx.rev_nl_len);
      if (rev_ctx.rev_node_list == nullptr || rev_ctx.rev_nl_len == 0) {
        rev_ctx.rev_state = MDX_REV_ST_END;
        return false;
      }
      size_t nid_len;
      rev_ctx.rev_nl_pos = 0;
      rev_ctx.node_id = cmn::read_vint32(rev_ctx.rev_node_list, &nid_len) >> 1;
      printf("rev_node id: %" PRIuXX "\n", rev_ctx.node_id);
      rev_ctx.prev_node_id = 0;
      return true;
    }

    __fq1 __fq2 bool get_next_rev_node_id(rev_nodes_ctx& rev_ctx) {
      size_t nid_len;
      uintxx_t nid_start = cmn::read_vint32(rev_ctx.rev_node_list + rev_ctx.rev_nl_pos, &nid_len);
      uintxx_t nid_end = 0;
      bool has_end = (nid_start & 1);
      nid_start = (nid_start >> 1) + rev_ctx.prev_node_id;
      if (has_end) {
        size_t nid_end_len;
        nid_end = cmn::read_vint32(rev_ctx.rev_node_list + rev_ctx.rev_nl_pos + nid_len, &nid_end_len);
        nid_end += nid_start;
        nid_end++;
        nid_len += nid_end_len;
      } else
        nid_end = nid_start;
      printf("nid_start: %" PRIuXX ", nid_end: %" PRIuXX ", prev_nid: %" PRIuXX ", nid: %" PRIuXX "\n", nid_start, nid_end, rev_ctx.prev_node_id, rev_ctx.node_id);
      rev_ctx.node_id++;
      if (rev_ctx.node_id > nid_end) {
        rev_ctx.rev_nl_pos += nid_len;
        rev_ctx.prev_node_id = nid_start + 1;
        if (has_end)
          rev_ctx.prev_node_id = nid_end + 1;
        printf("rev_nl_len: %zu, pos: %" PRIuXX "\n", rev_ctx.rev_nl_len, rev_ctx.rev_nl_pos);
        if (rev_ctx.rev_nl_pos >= rev_ctx.rev_nl_len) {
          rev_ctx.node_id = UINTXX_MAX;
          return false;
        }
        nid_start = cmn::read_vint32(rev_ctx.rev_node_list + rev_ctx.rev_nl_pos, &nid_len);
        rev_ctx.node_id = (nid_start >> 1) + rev_ctx.prev_node_id;
      }
      return true;
    }

    __fq1 __fq2 bool rev_trie_next(rev_nodes_ctx& rev_ctx) {
      if (rev_ctx.rev_state == MDX_REV_ST_END)
        return false;
      if (rev_ctx.rev_state == MDX_REV_ST_INIT) {
        rev_ctx.init(this);
        rev_ctx.rev_state = MDX_REV_ST_NEXT;
        input_ctx *in_ctx = &rev_ctx;
        printf("key: %" PRIuXX ", [%.*s]\n", in_ctx->key_len, (int) in_ctx->key_len, in_ctx->key);
        uintxx_t ct_node_id = find_first(in_ctx->key, in_ctx->key_len, rev_ctx, true);
        printf("ct node id: %" PRIuXX "\n", ct_node_id);
        if (ct_node_id == 0) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        int res = next(rev_ctx);
        printf("res: %d\n", res);
        if (res == -2) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        iter_ctx *it_ctx = &rev_ctx;
        printf("found first: %d, [%.*s]\n", it_ctx->key_len, (int) it_ctx->key_len, it_ctx->key);
        if (it_ctx->key_len < in_ctx->key_len ||
            cmn::memcmp(in_ctx->key, it_ctx->key, cmn::min(it_ctx->key_len, in_ctx->key_len)) != 0) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        ct_node_id = rev_ctx.node_path[rev_ctx.cur_idx];
        printf("ct node id: %" PRIuXX "\n", ct_node_id);
        if (get_column_count() == 1)
          return true;
        if (!read_rev_node_list(ct_node_id, rev_ctx))
          return false;
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        return true;
      } else {
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        if (get_column_count() == 2) {
          if (get_next_rev_node_id(rev_ctx))
            return true;
        }
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        int res = next(rev_ctx);
        if (res == -2) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        input_ctx *in_ctx = &rev_ctx;
        iter_ctx *it_ctx = &rev_ctx;
        printf("found next: %d, [%.*s]\n", it_ctx->key_len, (int) it_ctx->key_len, it_ctx->key);
        if (it_ctx->key_len < in_ctx->key_len ||
            cmn::memcmp(in_ctx->key, it_ctx->key, cmn::min(it_ctx->key_len, in_ctx->key_len)) != 0) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        uintxx_t ct_node_id = rev_ctx.node_path[rev_ctx.cur_idx];
        if (!read_rev_node_list(ct_node_id, rev_ctx))
          return false;
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        return true;
      }
      return false;
    }

    __fq1 __fq2 void map_from_memory(uint8_t *mem) {
      load_from_mem(mem, 0);
    }

    __fq1 __fq2 void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

#if !defined(ESP32)
    __fq1 __fq2 uint8_t* map_file(const char* filename, off_t& sz) {
    #ifdef _WIN32
      HANDLE hFile = CreateFileA(filename, GENERIC_READ, FILE_SHARE_READ,
                                NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL,
                                NULL);
      if (hFile == INVALID_HANDLE_VALUE) {
        fprintf(stderr, "CreateFileA failed: %lu\n", GetLastError());
        return NULL;
      }

      LARGE_INTEGER filesize;
      if (!GetFileSizeEx(hFile, &filesize)) {
        fprintf(stderr, "GetFileSizeEx failed: %lu\n", GetLastError());
        CloseHandle(hFile);
        return NULL;
      }
      sz = static_cast<off_t>(filesize.QuadPart);

      HANDLE hMap = CreateFileMappingA(hFile, NULL, PAGE_READONLY,
                                      0, 0, NULL);
      if (!hMap) {
        fprintf(stderr, "CreateFileMappingA failed: %lu\n", GetLastError());
        CloseHandle(hFile);
        return NULL;
      }

      uint8_t* map_buf = (uint8_t*)MapViewOfFile(hMap, FILE_MAP_READ,
                                                0, 0, 0);
      if (!map_buf) {
        fprintf(stderr, "MapViewOfFile failed: %lu\n", GetLastError());
        CloseHandle(hMap);
        CloseHandle(hFile);
        return NULL;
      }

      CloseHandle(hMap);
      CloseHandle(hFile);
      return map_buf;

    #else
      FILE* fp = fopen(filename, "rb");
      if (!fp) {
        perror("fopen");
        return NULL;
      }

      int fd = fileno(fp);
      struct stat buf;
      if (fstat(fd, &buf) < 0) {
        perror("fstat");
        fclose(fp);
        return NULL;
      }
      sz = buf.st_size;

      uint8_t* map_buf = (uint8_t*)mmap(0, sz, PROT_READ, MAP_PRIVATE,
                                        fd, 0);
      if (map_buf == MAP_FAILED) {
        perror("mmap");
        fclose(fp);
        return NULL;
      }

      fclose(fp);
      return map_buf;
    #endif
    trie_size = sz;

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
    #ifdef _WIN32
      if (trie_bytes) {
        if (!UnmapViewOfFile(trie_bytes)) {
          fprintf(stderr, "UnmapViewOfFile failed: %lu\n", GetLastError());
        }
      }
    #else
      if (trie_bytes) {
        // munlock(trie_bytes, dict_size >> 2);  // optional; ignore errors
        int err = munmap(trie_bytes, trie_size);
        if (err != 0) {
          perror("munmap");
          printf("UnMapping trie_bytes Failed\n");
        }
      }
    #endif
      trie_bytes = nullptr;
      is_mmapped = false;
    }
#endif

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
      size_t bytes_read = fread(trie_bytes, 1, file_stat.st_size, fp);
      if (bytes_read != file_stat.st_size) {
        printf("Read error: [%s], %zu, %zu\n", filename, (size_t) file_stat.st_size, bytes_read);
        #ifdef __CUDA_ARCH__
        return;
        #else
        throw errno;
        #endif
      }
      fclose(fp);
      trie_size = bytes_read;

      // int len_will_need = (dict_size >> 1);
      // #ifndef _WIN32
      //       mlock(trie_bytes, len_will_need);
      // #endif
      //madvise(trie_bytes, len_will_need, MADV_WILLNEED);
      //madvise(trie_bytes + len_will_need, dict_size - len_will_need, MADV_RANDOM);

      load_into_vars();

    }

    value_retriever_base *new_value_retriever(char data_type, char encoding_type) {
      value_retriever_base *val_retriever = nullptr;
      switch (encoding_type) {
        case MSE_TRIE: case MSE_TRIE_2WAY:
          if (pk_col_count > 0)
            val_retriever = new col_trie_retriever<'Y'>();
          else
            val_retriever = new col_trie_retriever<'N'>();
          break;
        case MSE_WORDS: case MSE_WORDS_2WAY:
          if (pk_col_count > 0)
            val_retriever = new words_retriever<'Y'>();
          else
            val_retriever = new words_retriever<'N'>();
          break;
        case MSE_VINTGB:
          if (pk_col_count > 0)
            val_retriever = new fast_vint_retriever<'Y'>();
          else
            val_retriever = new fast_vint_retriever<'N'>();
          break;
        case MSE_STORE:
          if (pk_col_count > 0)
            val_retriever = new stored_val_retriever<'Y'>();
          else
            val_retriever = new stored_val_retriever<'N'>();
          break;
        default:
          switch (data_type) {
            case MST_BIN:
              if (pk_col_count > 0)
                val_retriever = new uniq_bin_val_retriever<'Y'>();
              else
                val_retriever = new uniq_bin_val_retriever<'N'>();
              break;
            case MST_TEXT:
              if (pk_col_count > 0)
                val_retriever = new uniq_text_retriever<'Y'>();
              else
                val_retriever = new uniq_text_retriever<'N'>();
              break;
            case MST_INT:
            case MST_DECV:
            case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
            case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9:
            case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
            case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
            case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS:
              if (encoding_type == MSE_DICT_DELTA) {
                if (pk_col_count > 0)
                  val_retriever = new delta_val_retriever<'Y'>();
                else
                  val_retriever = new delta_val_retriever<'N'>();
              } else {
                if (pk_col_count > 0)
                  val_retriever = new uniq_ifp_retriever<'Y'>();
                else
                  val_retriever = new uniq_ifp_retriever<'N'>();
              }
              break;
        }
      }
      return val_retriever;
    }
    __fq1 __fq2 void load_into_vars() {
      load_static_trie();
      pk_col_count = trie_bytes[PK_COL_COUNT_LOC];
      val_count = cmn::read_uint64(trie_bytes + VAL_COUNT_LOC);
      max_val_len = cmn::read_uint64(trie_bytes + MAX_VAL_LEN_LOC);

      uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TRIE_FLAGS_LOC));

      names_loc = trie_bytes + cmn::read_uint64(trie_bytes + NAMES_LOC);
      uint8_t *val_table_loc = trie_bytes + cmn::read_uint64(trie_bytes + VAL_TBL_LOC);
      // printf("Val table loc: %lu\n", val_table_loc - trie_bytes);
      names_start = (char *) names_loc + (val_count + 2) * sizeof(uint16_t);
      column_encoding = names_start + cmn::read_uint16(names_loc);

      val_map = new value_retriever_base*[val_count]();
      for (size_t i = 0; i < val_count; i++) {
        val_map[i] = new_value_retriever(get_column_type(i), get_column_encoding(i));
        uint64_t vl64 = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
        if (i < pk_col_count) // why can't this be set in the builder?
          vl64 = 0;
        // printf("Val idx: %lu, %llu\n", i, vl64);
        uint8_t *val_loc = trie_bytes + vl64;
        if (val_loc == trie_bytes)
          continue;
        val_map[i]->init(this, trie_loc, tf_loc + TF_LEAF, opts->trie_leaf_count > 0 ? 4 : 3, val_loc, key_count, node_count);
      }
    }
};

}
#endif
