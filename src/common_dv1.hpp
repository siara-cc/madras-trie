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

#define DCT_BIN '*'
#define DCT_TEXT 't'
#define DCT_WORDS 'w'
#define DCT_RINT32 'a'
#define DCT_RINT64 'b'
#define DCT_UINT32 'c'
#define DCT_DOUBLE 'd'
#define DCT_UINT64 'e'
#define DCT_FLOAT 'f'
#define DCT_INT32 'g'
#define DCT_INT64 'h'
// '0' to 'H' in sequence for coding simplicity and performance
#define DCT_S64_INT '0'
#define DCT_S64_DEC1 '1'
#define DCT_S64_DEC2 '2'
#define DCT_S64_DEC3 '3'
#define DCT_S64_DEC4 '4'
#define DCT_S64_DEC5 '5'
#define DCT_S64_DEC6 '6'
#define DCT_S64_DEC7 '7'
#define DCT_S64_DEC8 '8'
#define DCT_S64_DEC9 '9'
#define DCT_DATETIME_US '@'
#define DCT_DATETIME_EUR 'A'
#define DCT_DATE_EUR 'B'
#define DCT_DATE_US 'C'
#define DCT_DATE_ISO 'D'
#define DCT_DATETIME_ISO 'E'
#define DCT_DATETIME_ISOT 'F'
#define DCT_DATETIME_ISO_MS 'G'
#define DCT_DATETIME_ISOT_MS 'H'
#define DCT_U64_INT 'i'
#define DCT_U64_DEC1 'j'
#define DCT_U64_DEC2 'k'
#define DCT_U64_DEC3 'l'
#define DCT_U64_DEC4 'm'
#define DCT_U64_DEC5 'n'
#define DCT_U64_DEC6 'o'
#define DCT_U64_DEC7 'p'
#define DCT_U64_DEC8 'q'
#define DCT_U64_DEC9 'r'
#define DCT_U15_DEC1 'X'
#define DCT_U15_DEC2 'Z'

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
