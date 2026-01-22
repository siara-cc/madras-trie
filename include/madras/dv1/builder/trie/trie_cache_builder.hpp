#ifndef __DV1_BUILDER_TRIE_CACHE__
#define __DV1_BUILDER_TRIE_CACHE__

#include "madras/dv1/common.hpp"

namespace madras { namespace dv1 {

class trie_cache_builder {
  private:
    fwd_cache *f_cache;
    nid_cache *r_cache;
    uintxx_t *f_cache_freq;
    uintxx_t *r_cache_freq;
    output_writer &output;
    memtrie::in_mem_trie &memtrie;
    tail_val_maps tail_maps;
    uint8_t fwd_cache_multiplier;
    uint8_t rev_cache_multiplier;
    uint16_t trie_level;

  public:
    trie_cache_builder(memtrie::in_mem_trie &_memtrie, output_writer &_output, tail_val_maps &_tail_maps)
            : memtrie (_memtrie), output (_output), tail_maps (_tail_maps) {
      f_cache = nullptr;
      r_cache = nullptr;
      f_cache_freq = nullptr;
      r_cache_freq = nullptr;
    }

    ~trie_cache_builder() {
      if (f_cache != nullptr)
        delete [] f_cache;
      if (r_cache != nullptr)
        delete [] r_cache;
      if (f_cache_freq != nullptr)
        delete [] f_cache_freq;
      if (r_cache_freq != nullptr)
        delete [] r_cache_freq;
    }

    void init(uint8_t _fwd_cache_multiplier, uint8_t _rev_cache_multiplier, uint16_t _trie_level) {
      fwd_cache_multiplier = _fwd_cache_multiplier;
      rev_cache_multiplier = _rev_cache_multiplier;
      trie_level = _trie_level;
    }

    #define CACHE_FWD 1
    #define CACHE_REV 2
    uintxx_t build_cache(int which, uintxx_t& max_node_id) {
      clock_t t = clock();
      uintxx_t cache_count = 64;
      while (cache_count < memtrie.key_count / 2048)
        cache_count <<= 1;
      //cache_count *= 2;
      if (which == CACHE_FWD) {
        for (int i = 0; i < fwd_cache_multiplier; i++)
          cache_count <<= 1;
        f_cache = new fwd_cache[cache_count + 1]();
        f_cache_freq = new uintxx_t[cache_count]();
      }
      if (which == CACHE_REV) {
        for (int i = 0; i < rev_cache_multiplier; i++)
          cache_count <<= 1;
        r_cache = new nid_cache[cache_count + 1]();
        r_cache_freq = new uintxx_t[cache_count]();
      }
      uint8_t tail_buf[memtrie.max_key_len];
      gen::byte_str tail_from0(tail_buf, memtrie.max_key_len);
      build_cache(which, 1, 0, cache_count - 1, tail_from0);
      max_node_id = 0;
      int sum_freq = 0;
      for (uintxx_t i = 0; i < cache_count; i++) {
        if (which == CACHE_FWD) {
          fwd_cache *fc = &f_cache[i];
          uintxx_t cche_node_id = gen::read_uint24(&fc->child_node_id1);
          if (max_node_id < cche_node_id)
            max_node_id = cche_node_id;
          sum_freq += f_cache_freq[i];
        }
        if (which == CACHE_REV) {
          nid_cache *rc = &r_cache[i];
          uintxx_t cche_node_id = gen::read_uint24(&rc->child_node_id1);
          if (max_node_id < cche_node_id)
            max_node_id = cche_node_id;
          sum_freq += r_cache_freq[i];
        }
        // printf("NFreq:\t%u\tPNid:\t%u\tCNid:\t%u\tNb:\t%c\toff:\t%u\n", f_cache_freq[i], gen::read_uint24(&fc->parent_node_id1), gen::read_uint24(&fc->child_node_id1), fc->node_byte, fc->node_offset);
      }
      max_node_id++;
      gen::gen_printf("Cache Max node id: %d\n", max_node_id);
      //gen::gen_printf("Sum of cache freq: %d\n", sum_freq);
      gen::print_time_taken(t, "Time taken for build_cache(): ");
      return cache_count;
    }

    uintxx_t build_cache(int which, uintxx_t ns_id, uintxx_t parent_node_id, uintxx_t cache_mask, gen::byte_str& tail_from0) {
      if (ns_id == 0)
        return 1;
      memtrie::node_set_handler ns(memtrie.all_node_sets, ns_id);
      memtrie::node n = ns.first_node();
      uintxx_t cur_node_id = ns.hdr()->node_id + (ns.hdr()->flags & NODE_SET_LEAP ? 1 : 0);
      uintxx_t freq_count = trie_level > 0 ? ns.hdr()->freq : 1;
      size_t parent_tail_len = tail_from0.length();
      for (int i = 0; i <= ns.hdr()->last_node_idx; i++) {
        if (n.get_flags() & NFLAG_TAIL) {
          uniq_info_base *ti = (*tail_maps.get_ui_vec())[n.get_tail()];
          uint8_t *ti_tail = (*tail_maps.get_uniq_data())[ti->pos];
          if (trie_level == 0)
            tail_from0.append(ti_tail, ti->len);
          else {
            for (uint8_t *t = ti_tail + ti->len - 1; t >= ti_tail; t--)
              tail_from0.append(*t);
          }
        } else
          tail_from0.append(n.get_byte());
        uintxx_t node_freq = build_cache(which, n.get_child(), cur_node_id, cache_mask, tail_from0);
        tail_from0.set_length(parent_tail_len);
        freq_count += node_freq;
        if (n.get_child() > 0 && (n.get_flags() & NFLAG_TAIL) == 0) {
          uint8_t node_byte = n.get_byte();
          memtrie::node_set_handler child_nsh(memtrie.all_node_sets, n.get_child());
          uintxx_t child_node_id = child_nsh.hdr()->node_id;
          if (which == CACHE_FWD) {
            int node_offset = i + (ns.hdr()->flags & NODE_SET_LEAP ? 1 : 0);
            uintxx_t cache_loc = (ns.hdr()->node_id ^ (ns.hdr()->node_id << MDX_CACHE_SHIFT) ^ node_byte) & cache_mask;
            fwd_cache *fc = f_cache + cache_loc;
            if (f_cache_freq[cache_loc] < node_freq && ns.hdr()->node_id < (1 << 24) && child_node_id < (1 << 24) && node_offset < 256) {
              f_cache_freq[cache_loc] = node_freq;
              gen::copy_uint24(ns.hdr()->node_id, &fc->parent_node_id1);
              gen::copy_uint24(child_node_id, &fc->child_node_id1);
              fc->node_offset = node_offset;
              fc->node_byte = node_byte;
            }
          }
        }
        if (which == CACHE_REV) {
          uintxx_t cache_loc = cur_node_id & cache_mask;
          nid_cache *rc = r_cache + cache_loc;
          if (r_cache_freq[cache_loc] < node_freq && parent_node_id < (1 << 24) && cur_node_id < (1 << 24)) {
            r_cache_freq[cache_loc] = node_freq;
            gen::copy_uint24(cur_node_id, &rc->child_node_id1);
            rc->tail0_len = 0;
            gen::copy_uint24(parent_node_id, &rc->parent_node_id1);
            if (parent_tail_len < 9) {
              uint8_t *t = &rc->tail0_len + parent_tail_len;
              for (size_t k = 0; k < parent_tail_len; k++)
                *t-- = tail_from0[k];
              rc->tail0_len = parent_tail_len;
            }
          }
        }
        n.next();
        cur_node_id++;
      }
      return freq_count;
    }

    void write_fwd_cache(uintxx_t fwd_cache_count) {
      output.write_bytes((const uint8_t *) f_cache, fwd_cache_count * sizeof(fwd_cache));
    }

    void write_rev_cache(uintxx_t rev_cache_count, uintxx_t rev_cache_size) {
      output.write_bytes((const uint8_t *) r_cache, rev_cache_count * sizeof(nid_cache));
      output.write_align8(rev_cache_size);
    }

};

}}

#endif
