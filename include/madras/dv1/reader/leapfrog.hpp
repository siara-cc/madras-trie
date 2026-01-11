#ifndef __DV1_READER_LEAPFROG__
#define __DV1_READER_LEAPFROG__

#include "cmn.hpp"
#include "madras/dv1/common.hpp"

namespace madras { namespace dv1 {

  struct min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
};

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

}}

#endif
