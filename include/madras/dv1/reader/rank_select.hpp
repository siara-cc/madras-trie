#ifndef __DV1_READER_RANK_SELECT__
#define __DV1_READER_RANK_SELECT__

#include "cmn.hpp"

namespace madras { namespace dv1 {

#if defined(__x86_64__) && defined(__SSSE3__)
    static const __m128i nibble_lut = _mm_setr_epi8(
      0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4
    );
#endif

static constexpr uint64_t PREFIX_SUM_OVERFLOW[64] = {
#define M01 0x0101010101010101ULL
  0x7F*M01,0x7E*M01,0x7D*M01,0x7C*M01,0x7B*M01,0x7A*M01,0x79*M01,0x78*M01,
  0x77*M01,0x76*M01,0x75*M01,0x74*M01,0x73*M01,0x72*M01,0x71*M01,0x70*M01,
  0x6F*M01,0x6E*M01,0x6D*M01,0x6C*M01,0x6B*M01,0x6A*M01,0x69*M01,0x68*M01,
  0x67*M01,0x66*M01,0x65*M01,0x64*M01,0x63*M01,0x62*M01,0x61*M01,0x60*M01,
  0x5F*M01,0x5E*M01,0x5D*M01,0x5C*M01,0x5B*M01,0x5A*M01,0x59*M01,0x58*M01,
  0x57*M01,0x56*M01,0x55*M01,0x54*M01,0x53*M01,0x52*M01,0x51*M01,0x50*M01,
  0x4F*M01,0x4E*M01,0x4D*M01,0x4C*M01,0x4B*M01,0x4A*M01,0x49*M01,0x48*M01,
  0x47*M01,0x46*M01,0x45*M01,0x44*M01,0x43*M01,0x42*M01,0x41*M01,0x40*M01
#undef M01
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
      return rank + static_cast<uintxx_t>(cmn::pop_cnt(bm & mask));
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
    uint64_t get_bm(uintxx_t node_id) {
      return bm_loc[(node_id / 64) * multiplier];
    }
    __fq1 __fq2 inline uintxx_t get_count(uint8_t *block_loc, size_t pos_n) {
      return (block_loc[pos_n] + (((uintxx_t)(*block_loc) << pos_n) & 0x100));
    }

    static const uint64_t MASK_01 = 0x0101010101010101ULL;

    #if defined(__x86_64__) && defined(__SSSE3__)

    static inline uint64_t popcount_bytes(uint64_t bm) {
        __m128i v = _mm_set1_epi64x((long long)bm);
        __m128i lo = _mm_and_si128(v, _mm_set1_epi8(0x0F));
        __m128i hi = _mm_and_si128(_mm_srli_epi64(v, 4), _mm_set1_epi8(0x0F));
        __m128i plo = _mm_shuffle_epi8(nibble_lut, lo);
        __m128i phi = _mm_shuffle_epi8(nibble_lut, hi);
        return (uint64_t)_mm_cvtsi128_si64(_mm_add_epi8(plo, phi));
    }

    #elif defined(__aarch64__)

    #include <arm_neon.h>
    static inline uint64_t popcount_bytes(uint64_t bm) {
        uint8x8_t v = vcreate_u8(bm);
        uint8x8_t c = vcnt_u8(v);
        uint64_t r = vget_lane_u64(vreinterpret_u64_u8(c), 0);
        return r;
    }

    #else

    static inline uint64_t popcount_bytes(uint64_t bm) {
        // scalar fallback (carry-rippler)
        constexpr uint64_t MASK_0F = 0x0F0F0F0F0F0F0F0FULL;
        constexpr uint64_t MASK_33 = 0x3333333333333333ULL;
        constexpr uint64_t MASK_55 = 0x5555555555555555ULL;
        uint64_t x = bm - ((bm >> 1) & MASK_55);
        x = (x & MASK_33) + ((x >> 2) & MASK_33);
        return ((x + (x >> 4)) & MASK_0F);
    }

    #endif

    __fq1 __fq2 inline uintxx_t bm_select1(uintxx_t remaining, uint64_t bm)
    {
    #if defined(__BMI2__)

        // Your original BMI2 path
        uint64_t isolated_bit = _pdep_u64(1ULL << (remaining - 1), bm);
        uintxx_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
        return bit_loc;

    #elif defined(__aarch64__) || defined(__ARM_NEON__)

      // *** optimized byte-select path ***
      uint64_t counts = popcount_bytes(bm) * MASK_01;

      uint64_t x = (counts + PREFIX_SUM_OVERFLOW[remaining - 1]) &
                  0x8080808080808080ULL;

      uintxx_t byte_index = __builtin_ctzll(x) >> 3;

      remaining -= (uintxx_t)((counts << 8) >> (byte_index * 8)) & 0xFF;

      uint8_t b = (uint8_t)(bm >> (byte_index * 8));
      uintxx_t bit_in_byte = select_lookup_tbl[remaining - 1][b];

      return (byte_index << 3) + bit_in_byte;

    #else

        //
        // Your original non-BMI2 fallback path (kept for safety)
        //

        uintxx_t bit_loc = 0;
        while (bit_loc < 64) {
            uint8_t next_count = bit_count[(bm >> bit_loc) & 0xFF];
            if (next_count >= remaining)
                break;
            bit_loc += 8;
            remaining -= next_count;
        }
        if (remaining > 0)
            bit_loc += select_lookup_tbl[remaining - 1][(bm >> bit_loc) & 0xFF];

        return bit_loc;

    #endif
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

}}

#endif
