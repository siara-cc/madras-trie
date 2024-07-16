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

#define nodes_per_bv_block 256
#define bytes_per_bv_block 384
#define nodes_per_bv_block3 64
#define bytes_per_bv_block3 96

#define nodes_per_ptr_block 64
#define nodes_per_ptr_block3 16
#define bytes_per_ptr_block3 24

// #define nodes_per_ptr_block 128
// #define nodes_per_ptr_block3 32
// #define bytes_per_ptr_block3 48

// #define nodes_per_ptr_block 256
// #define nodes_per_ptr_block3 64
// #define bytes_per_ptr_block3 96

#define sel_divisor 256

// #define sel_divisor 512

#define BV_LT_TYPE_LEAF 1
#define BV_LT_TYPE_TERM 2
#define BV_LT_TYPE_CHILD 3

#define NFLAG_LEAF '\x01'
#define NFLAG_CHILD '\x02'
#define NFLAG_TAIL '\x04'
#define NFLAG_TERM '\x08'
#define NFLAG_NULL '\x10'
#define NFLAG_EMPTY '\x20'

struct PACKED_STRUCT fwd_cache {
  uint8_t parent_node_id1;
  uint8_t parent_node_id2;
  uint8_t parent_node_id3;
  uint8_t node_offset;
  uint8_t child_node_id1;
  uint8_t child_node_id2;
  uint8_t child_node_id3;
  uint8_t node_byte;
};

struct PACKED_STRUCT rev_cache {
  uint8_t parent_node_id1;
  uint8_t parent_node_id2;
  uint8_t parent_node_id3;
  uint8_t child_node_id1;
  uint8_t child_node_id2;
  uint8_t child_node_id3;
};
#if defined(_MSC_VER)
#pragma pack(pop)
#endif

#undef PACKED_STRUCT

}

#endif
