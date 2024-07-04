#ifndef common_hpp
#define common_hpp

namespace madras_dv1 {

#define nodes_per_bv_block 256
#define bytes_per_bv_block 384
#define nodes_per_bv_block3 64
#define bytes_per_bv_block3 96

#define nodes_per_ptr_block 256
#define nodes_per_ptr_block3 64
#define bytes_per_ptr_block3 96

#define sel_divisor 512

#define BV_LT_TYPE_LEAF 1
#define BV_LT_TYPE_TERM 2
#define BV_LT_TYPE_CHILD 3

#define NFLAG_LEAF '\x01'
#define NFLAG_CHILD '\x02'
#define NFLAG_TAIL '\x04'
#define NFLAG_TERM '\x08'
#define NFLAG_NULL '\x10'
#define NFLAG_EMPTY '\x20'

}

#endif
