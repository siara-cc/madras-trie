#ifndef common_hpp
#define common_hpp

#include <sstream>
#include <stdint.h>

#include "ds_common/src/compiler_util.hpp"

BEGIN_IGNORE_UNUSED_FUNCTION
#include "ds_common/src/bv.hpp"
#include "ds_common/src/vint.hpp"
#include "ds_common/src/gen.hpp"
#include "allflic48/src/allflic48.hpp"
END_IGNORE_UNUSED_FUNCTION

namespace madras_dv1 {

#ifndef UINTXX_WIDTH
#define UINTXX_WIDTH 32
#endif

#if UINTXX_WIDTH == 32
#define uintxx_t uint32_t
#define PRIuXX PRIu32
#define UINTXX_MAX UINT32_MAX
#else
#define uintxx_t uint64_t
#define PRIuXX PRIu64
#define UINTXX_MAX UINT64_MAX
#endif

#define MDX_HEADER_SIZE (36 + 4 + 8 + 29 * 8)

const static uint8_t *NULL_VALUE =  (const uint8_t *) " ";
const static size_t NULL_VALUE_LEN = 1;
const static uint8_t *EMPTY_VALUE = (const uint8_t *) "!";
const static size_t EMPTY_VALUE_LEN = 1;

#if defined(__GNUC__) || defined(__clang__)
#define PACKED_STRUCT __attribute__((packed))
#elif defined(_MSC_VER)
#define PACKED_STRUCT
#pragma pack(push, 1)
#else
#define PACKED_STRUCT
#endif

#define MDX_CACHE_SHIFT 5

// not used
#define MDX_FWD_MRU_NID_CACHE 1
#define MDX_REV_MRU_NID_CACHE 2

// #define nodes_per_bv_block 256
// #define width_of_bv_block 7 // not aligned
// #define width_of_bv_block_n 3
#define nodes_per_bv_block 512
#define width_of_bv_block 12
#define width_of_bv_block_n 8
#define nodes_per_bv_block_n 64

// #define nodes_per_ptr_block 64
// #define nodes_per_ptr_block_n 16
// #define bytes_per_ptr_block_n 24

// #define nodes_per_ptr_block 128
// #define nodes_per_ptr_block_n 32
// #define bytes_per_ptr_block_n 48

#define nodes_per_ptr_block 1984
#define nodes_per_ptr_block_n 64
// ptr_lt_blk_width = (nodes_per_ptr_block/nodes_per_ptr_block_n - 1) * 2
// **Invalid for now** -> Ensure ptr_lt_blk_width is multiple of 4 so it works on CUDA cores
#define ptr_lt_blk_width2 60
#define ptr_lt_blk_width3 90

// #define sel_divisor 256

#define sel_divisor 512

#define bm_init_mask 0x0000000000000001ULL

#define BV_LT_TYPE_LEAF 1
#define BV_LT_TYPE_TERM 2
#define BV_LT_TYPE_CHILD 4
#define BV_LT_TYPE_TAIL 8

#define NFLAG_LEAF 1
#define NFLAG_CHILD 2
#define NFLAG_TAIL 4
#define NFLAG_TERM 8
#define NFLAG_NULL 16
#define NFLAG_EMPTY 32

#define MST_BIN '*'
#define MST_TEXT 't'

#define MST_INT 'i'
#define MST_DECV '.'
#define MST_DEC0 '0'
#define MST_DEC1 '1'
#define MST_DEC2 '2'
#define MST_DEC3 '3'
#define MST_DEC4 '4'
#define MST_DEC5 '5'
#define MST_DEC6 '6'
#define MST_DEC7 '7'
#define MST_DEC8 '8'
#define MST_DEC9 '9'

#define MST_DATE_US '`'
#define MST_DATE_EUR 'a'
#define MST_DATE_ISO 'b'
#define MST_DATETIME_US 'c'
#define MST_DATETIME_EUR 'd'
#define MST_DATETIME_ISO 'e'
#define MST_DATETIME_ISOT 'f'
#define MST_DATETIME_ISO_MS 'g'
#define MST_DATETIME_ISOT_MS 'h'

#define MST_SEC_2WAY 'S'

#define MSE_TRIE 't'
#define MSE_TRIE_2WAY 'T'
#define MSE_TRIE_SINT 's'
#define MSE_WORDS 'w'
#define MSE_WORDS_2WAY 'W'

#define MSE_DICT 'u'
#define MSE_DICT_DELTA 'd'
#define MSE_VINTGB 'v'
#define MSE_STORE 's'
// MSE_STORE is used internally for storing reverse indices
// and not available for user (yet)

// int / float format
// b1 - header
// w - 0=32, 1=64
// r - 0=no, 1=repeats
// v - 
// - - 0=no, 1=negatives
// e - 0=no, 1=float exceptions (w=64)
// sss - scheme
//      000 - vintgb
//      001 - fixed width in iwd (1-8)
//      010 - PFor
//      011 - Prefix deltas
//      100 to 111 - reserved
// b2 - count excluding repeats - vnnnnnnn
// b3 - decimal count 0-32 and base vint len - 0-7

struct PACKED_STRUCT fwd_cache {
  uint8_t parent_node_id1;
  uint8_t parent_node_id2;
  uint8_t parent_node_id3;
  uint8_t node_byte;
  uint8_t child_node_id1;
  uint8_t child_node_id2;
  uint8_t child_node_id3;
  uint8_t node_offset;
};

struct PACKED_STRUCT nid_cache {
  uint8_t child_node_id1;
  uint8_t child_node_id2;
  uint8_t child_node_id3;
  uint8_t tail0_len;
  uint8_t parent_node_id1;
  uint8_t parent_node_id2;
  uint8_t parent_node_id3;
  uint8_t tail_byte1;
  uint8_t tail_byte2;
  uint8_t tail_byte3;
  uint8_t tail_byte4;
  uint8_t tail_byte5;
};

struct PACKED_STRUCT bldr_options {
  uint8_t inner_tries;
  uint8_t fwd_cache;
  uint8_t rev_cache;
  uint8_t leap_frog;
  uint8_t dessicate;
  uint8_t sort_nodes_on_freq;
  uint8_t leaf_lt;
  uint8_t partial_sfx_coding;
  uint8_t idx_partial_sfx_coding;
  uint8_t sfx_min_tail_len;
  uint8_t step_bits_idx;
  uint8_t step_bits_rest;
  uint8_t max_inner_tries;
  uint8_t fwd_cache_multiplier;
  uint8_t rev_cache_multiplier;
  uint8_t trie_leaf_count;
  uint8_t max_groups;
  uint8_t sec_idx_nid_shift_bits;
  uint8_t split_tails_method;
  uint8_t rpt_enable_perc;
  uint8_t opts_count;
  uint8_t align8_padding4;
  uint16_t sfx_set_max_dflt;
}; // 24 bytes

const static bldr_options preset_opts[] = {
  //  it,    fc,   rc,     lf,  dsct, sortn,   llt,    sc, scidx, mt, si, sr,  it, cm, cm, lc, mg, ns, st, re,oc, p, sfx
  {false,  true,  true,  true, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0,  0,  0, 1, 0, 64},
  { true,  true,  true, false, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0,  0,  0, 1, 0, 64},
  { true,  true,  true,  true, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0,  0,  0, 1, 0, 64}
};

const static bldr_options dflt_opts =
  { true,  true,  true, false, false,  true,  true,  true, false,  4,  3,  3,   1,  2,  1,  1, 16,  0,  0, 40, 1, 0, 64};

const static bldr_options word_tries_dflt_opts =
  {false,  true, false, false, false, false,  true,  true, false,  4,  3,  3,   1,  2,  2,  1, 16,  0,  0,  0, 1, 0, 64};
const static bldr_options inner_tries_dflt_opts =
  { true, false,  true, false, false, false, false,  true, false,  4,  3,  3, 127,  2,  3,  1, 16,  0,  0,  0, 1, 0, 64};

class word_split_iface {
  public:
    virtual uintxx_t split_into_words(const uint8_t *str, uintxx_t str_len, uintxx_t *out_word_positions, uintxx_t max_word_count) = 0;
    virtual ~word_split_iface() {
    }
};

class simple_word_splitter : public word_split_iface {
  private:
    uintxx_t min_word_len = 1;
  public:
    void set_min_word_len(uintxx_t _min_len) {
      min_word_len = _min_len;
    }
    uintxx_t split_into_words(const uint8_t *str, uintxx_t str_len, uintxx_t *out_word_positions, uintxx_t max_word_count) {
      uintxx_t word_count = 0;
      uintxx_t last_word_len = 0;
      bool is_prev_non_word = false;
      out_word_positions[word_count++] = 0;
      for (uintxx_t i = 0; i < str_len; i++) {
        uint8_t c = str[i];
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c > 127) {
          if (is_prev_non_word && last_word_len >= min_word_len && word_count < max_word_count) {
            out_word_positions[word_count++] = i;
            is_prev_non_word = false;
            last_word_len = 0;
          }
        } else
          is_prev_non_word = true;
        last_word_len++;
      }
      out_word_positions[word_count] = str_len;
      return word_count;
    }
};
simple_word_splitter dflt_word_splitter;

static int64_t dt_str_to_i64(const uint8_t *dt_txt_db, char col_type) {
  struct tm tm = {0};
  #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
  char *dt_txt = new char[gen::dt_format_lens[col_type - MST_DATE_US] + 1];
  #else
  char dt_txt[gen::dt_format_lens[col_type - MST_DATE_US] + 1];
  #endif
  strncpy(dt_txt, (const char *) dt_txt_db, gen::dt_format_lens[col_type - MST_DATE_US]);
  dt_txt[gen::dt_format_lens[col_type - MST_DATE_US]] = 0;
  // printf("%s, %s\n", dt_txt, dt_formats[col_type - MST_DATE_US]);
  #if defined(_WIN32)
  std::istringstream is(dt_txt);
  is >> std::get_time(&tm, gen::dt_formats[col_type - MST_DATE_US]);
  if (is.fail()) {
    printf("Error parsing date: %s\n", dt_txt);
    delete [] dt_txt;
    return INT64_MIN;
  }
  #else
  char *result = strptime((const char *) dt_txt, gen::dt_formats[col_type - MST_DATE_US], &tm);
  if (result == nullptr || *result != '\0') {
    //printf(" e%lu/%lu", ins_seq_id, sql_col_idx);
    printf("Error parsing date: %s\n", dt_txt);
    #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
      delete [] dt_txt;
    #endif
    return INT64_MIN;
  }
  #endif
  int64_t dt_val = gen::tm_to_epoch_seconds(&tm);
  // if (tm.tm_year < 0)
  //   printf("time_val: %lld, %s, %d-%d-%d\n", dt_val, dt_txt, tm.tm_mday, tm.tm_mon, tm.tm_year);
  if (col_type >= MST_DATE_US && col_type <= MST_DATE_ISO)
    dt_val /= 86400;
  if (col_type == MST_DATETIME_ISO_MS || col_type == MST_DATETIME_ISOT_MS) {
    dt_val *= 1000;
    char *dot_pos = (char *) memchr(dt_txt_db, '.', strnlen((const char *) dt_txt_db, 24));
    if (dot_pos != nullptr)
      dt_val += atoi(dot_pos + 1);
  }
  #if defined(__CUDA_ARCH__) || defined(__EMSCRIPTEN__) || defined(_WIN32)
    delete [] dt_txt;
  #endif
  return dt_val;
}

static size_t dt_i64_to_str(int64_t i64, char *dt_txt, size_t dt_txt_sz, char col_type) {
  int64_t original_epoch = i64;
  // printf("orig epoch: %lld\n", original_epoch);
  if (col_type >= MST_DATE_US && col_type <= MST_DATE_ISO)
    original_epoch *= 86400;
  if (col_type == MST_DATETIME_ISO_MS || col_type == MST_DATETIME_ISOT_MS)
    original_epoch /= 1000;
  struct tm out_tm;
  gen::epoch_seconds_to_tm(original_epoch, &out_tm);
  strftime(dt_txt, dt_txt_sz, gen::dt_formats[col_type - MST_DATE_US], &out_tm);
  size_t val_len = strlen(dt_txt);
  if (col_type == MST_DATETIME_ISO_MS || col_type == MST_DATETIME_ISOT_MS) {
    original_epoch = i64;
    dt_txt[val_len++] = '.';
    dt_txt[val_len++] = '0' + ((original_epoch / 100) % 10);
    dt_txt[val_len++] = '0' + ((original_epoch / 10) % 10);
    dt_txt[val_len++] = '0' + (original_epoch % 10);
    dt_txt[val_len] = 0;
  }
  return val_len;
}
#if defined(_MSC_VER)
#pragma pack(pop)
#endif

typedef struct {
  union {
    double dbl;
    int64_t i64;
    uint8_t *txt_bin;
    bool bool_val;
  };
} mdx_val;

typedef struct {
  union {
    double dbl;
    int64_t i64;
    const uint8_t *txt_bin;
    bool bool_val;
  };
} mdx_val_in;

#undef PACKED_STRUCT

}

#endif
