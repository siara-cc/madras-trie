#ifndef common_hpp
#define common_hpp

namespace madras_dv1 {

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
// ptr_lt_blk_width = 4 + (nodes_per_ptr_block/nodes_per_ptr_block_n - 1) * 2
// Ensure ptr_lt_blk_width is multiple of 4 so it works on CUDA cores
#define ptr_lt_blk_width 64

// #define sel_divisor 256

#define sel_divisor 512

#define bm_init_mask 0x0000000000000001UL

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
#define MST_INT_DELTA 'I'
#define MST_DEC 'f'
#define MST_DEC_DELTA 'F'
#define MST_DATETIME 'd'

#define MSE_TRIE 't'
#define MSE_WORDS 'w'

#define MSE_DATE_US 'a'
#define MSE_DATE_EUR 'b'
#define MSE_DATE_ISO 'c'

#define MSE_DATETIME_US 'd'
#define MSE_DATETIME_EUR 'e'
#define MSE_DATETIME_ISO 'f'
#define MSE_DATETIME_ISOT 'g'
#define MSE_DATETIME_ISO_MS 'h'
#define MSE_DATETIME_ISOT_MS 'i'

#define MSE_DICT 'u'
#define MSE_STORE 's'
#define MSE_VINTGB 'v'
#define MSE_SORTABLE 's'

#define MSE_DECV '0'
#define MSE_DEC1 '1'
#define MSE_DEC2 '2'
#define MSE_DEC3 '3'
#define MSE_DEC4 '4'
#define MSE_DEC5 '5'
#define MSE_DEC6 '6'
#define MSE_DEC7 '7'
#define MSE_DEC8 '8'
#define MSE_DEC9 '9'

// int / float format
// b1 - header
// w - 0=32, 1=64
// d - 0=no, 1=delta
// r - 0=no, 1=repeats
// - - 0=no, 1=negatives
// e - 0=no, 1=float exceptions
// sss - scheme
//      000 - vintgb
//      001 - fixed width in iwd (1-8)
//      010 - PFor
//      011 to 111 - reserved
// b2 - count excluding repeats - nnnnnnnn
// b3 - repeat block of bytes - ppppnnnn
//      pppp - position from last
//             if 0, shift nnnn by 4 and add to next
//      nnnn - number of repetitions
//             if 0, no repeat at pos, see next

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
  uint8_t fwd_cache_multipler;
  uint8_t rev_cache_multipler;
  uint8_t trie_leaf_count;
  uint8_t max_groups;
  uint8_t split_tails_method;
  uint8_t align8_padding1;
  uint8_t align8_padding2;
  uint8_t align8_padding3;
  uint8_t align8_padding4;
  uint16_t sfx_set_max_dflt;
}; // 24 bytes

const static bldr_options preset_opts[] = {
  //  it,    fc,   rc,     lf,  dsct, sortn,   llt,    sc, scidx, mt, si, sr,  it, cm, cm, lc, mg, st, p, p, p, p, sfx
  {false,  true,  true,  true, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0, 0, 0, 0, 0, 64},
  { true,  true,  true, false, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0, 0, 0, 0, 0, 64},
  { true,  true,  true,  true, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0, 0, 0, 0, 0, 64}
};

const static bldr_options dflt_opts =
  { true,  true,  true, false, false,  true,  true,  true, false,  4,  3,  3,   1,  2,  1,  1, 16,  0, 0, 0, 0, 0, 64};

const static bldr_options word_tries_dflt_opts =
  {false,  true, false, false, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  0, 0, 0, 0, 0, 64};
const static bldr_options inner_tries_dflt_opts =
  { true, false,  true, false, false, false, false,  true, false,  4,  3,  3, 127,  2,  3,  1, 16,  0, 0, 0, 0, 0, 64};

#if defined(_MSC_VER)
#pragma pack(pop)
#endif

#undef PACKED_STRUCT

}

#endif
