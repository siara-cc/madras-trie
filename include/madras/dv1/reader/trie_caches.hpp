#ifndef __DV1_READER_TRIE_CACHES__
#define __DV1_READER_TRIE_CACHES__

#include "ifaces.hpp"
#include "madras/dv1/common.hpp"

namespace madras { namespace dv1 {

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

}}

#endif
