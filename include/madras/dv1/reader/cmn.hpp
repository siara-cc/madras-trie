#ifndef __DV1_READER_COMMON__
#define __DV1_READER_COMMON__

#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/stat.h>

#include "madras/dv1/common.hpp"

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

namespace madras { namespace dv1 {

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

#define DCT_INSERT_AFTER -2
#define DCT_INSERT_BEFORE -3
#define DCT_INSERT_LEAF -4
#define DCT_INSERT_EMPTY -5
#define DCT_INSERT_THREAD -6
#define DCT_INSERT_CONVERT -7
#define DCT_INSERT_CHILD_LEAF -8

class cmn {
  public:
    __fq1 __fq2 static int pop_cnt(uint64_t bm) {
      #ifdef _WIN32
      return __popcnt64(bm);
      #else
      return __builtin_popcountll(bm);
      #endif
    }
    __fq1 __fq2 static uintxx_t read_uint16(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      uint16_t tmp;
      memcpy(&tmp, ptr, sizeof(tmp));   // safe even if unaligned
      return tmp;                       // endian dependent
      #else
      uintxx_t ret = *ptr++;
      ret |= (*ptr << 8);
      return ret;
      #endif
    }
    __fq1 __fq2 static uintxx_t read_uint24(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      uint32_t tmp = 0;
      memcpy(&tmp, ptr, 3);             // safe, partial copy
      return tmp & 0x00FFFFFF;          // endian dependent
      #else
      uintxx_t ret = *ptr++;
      ret |= (*ptr++ << 8);
      ret |= (*ptr << 16);
      return ret;
      #endif
    }
    __fq1 __fq2 static uintxx_t read_uint32(uint8_t *ptr) {
      // #ifndef __CUDA_ARCH__
      uint32_t tmp;
      memcpy(&tmp, ptr, sizeof(tmp));   // safe even if unaligned
      return tmp;
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
      uint32_t lo;
      memcpy(&lo, ptr, sizeof(lo));     // safe
      return (uint64_t)lo | ((uint64_t)ptr[4] << 32);
    }
    __fq1 __fq2 static uint64_t read_uint64(uint8_t *ptr) {
      // #ifndef __CUDA_ARCH__
      uint64_t tmp;
      memcpy(&tmp, ptr, sizeof(tmp));   // safe even if unaligned
      return tmp;
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
    __fq1 __fq2 static void convert_back(char data_type, uint8_t *val_loc, mdx_val &ret_val, size_t *ret_len) {
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
            dbl /= allflic::allflic48::tens()[data_type - MST_DEC0];
            ret_val.dbl = dbl;
          } else
            ret_val.i64 = i64;
        } break;
      }
    }
};

}}

#endif
