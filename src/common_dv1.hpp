#ifndef common_hpp
#define common_hpp

namespace madras_dv1 {

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
// #define width_of_bv_block 7
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

#define nodes_per_ptr_block 2048
#define nodes_per_ptr_block_n 64
// ptr_lt_blk_width = 4 + (nodes_per_ptr_block/nodes_per_ptr_block_n - 1) * 2
#define ptr_lt_blk_width 66

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
  uint8_t tail_tries;
  uint8_t fwd_cache;
  uint8_t rev_cache;
  uint8_t dart;
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
  uint16_t sfx_set_max_dflt;
};
#define opts_size 19

const static bldr_options preset_opts[] = {
  //  it,    tt,    fc,    rc,  dart,  dsct, sortn,   llt,    sc, scidx, mt, si, sr,  it, cm, cm, lc, mg, sfx
  {false, false,  true,  true,  true, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  64},
  { true, false,  true,  true, false, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  64},
  { true, false,  true,  true,  true, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  64}
};

// const static bldr_options dflt_opts =
//   { true, false,  true, false, false, false,  true,  true,  true, false,  4,  3,  3,   1,  2,  2,  1, 16,  64};

const static bldr_options dflt_opts =
  {false,  true,  true,  true, false, false,  true,  true, false, false,  4,  3,  3,   2,  2,  1,  1,   1,  64};

const static bldr_options word_tries_dflt_opts =
  {false, false,  true, false, false, false, false, false,  true, false,  4,  3,  3,   0,  2,  2,  1, 16,  64};
const static bldr_options inner_tries_dflt_opts =
  { true, false, false,  true, false, false, false, false,  true, false,  4,  3,  3, 127,  2,  3,  1, 16,  64};
const static bldr_options tail_tries_dflt_opts =
  {false, false, false,  true, false, false, false, false, false, false,  4,  3,  3, 127,  2,  3,  1,  1,  64};

#if defined(_MSC_VER)
#pragma pack(pop)
#endif

#undef PACKED_STRUCT

}

#endif
