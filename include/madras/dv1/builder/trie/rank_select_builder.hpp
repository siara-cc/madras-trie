#ifndef __DV1_BUILDER_RANK_SELECT__
#define __DV1_BUILDER_RANK_SELECT__

#include "madras/dv1/common.hpp"
#include "madras/dv1/builder/output_writer.hpp"
#include "madras/dv1/builder/trie/memtrie/in_mem_trie.hpp"

namespace madras { namespace dv1 {

class rank_select_builder {
  private:
    output_writer &output;
    memtrie::in_mem_trie &memtrie;

  public:
    rank_select_builder(memtrie::in_mem_trie &_memtrie, output_writer &_output)
            : memtrie (_memtrie), output (_output) {
    }

    void write_bv_n(uintxx_t node_id, bool to_write, uintxx_t& count, uintxx_t& count_n, uint16_t *bit_counts_n, uint8_t& pos_n) {
      if (!to_write)
        return;
      size_t u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        output.write_u32(count);
        for (size_t i = nodes_per_bv_block == 256 ? 1: 0; i < u8_arr_count; i++) {
          output.write_byte(bit_counts_n[i]);
        }
        if (nodes_per_bv_block == 512) {
          for (size_t i = 1; i < pos_n; i++)
            count += bit_counts_n[i];
        }
        count += count_n;
        count_n = 0;
        memset(bit_counts_n, 0xFF, u8_arr_count * sizeof(uint16_t));
        bit_counts_n[0] = 0x1E;
        pos_n = 1;
      } else if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
        bit_counts_n[pos_n] = count_n & 0xFF;
        // uint8_t b0_mask = (0x100 >> pos_n);
        // bit_counts_n[0] &= ~b0_mask;
        // if (count_n > 255)
        //   bit_counts_n[0] |= ((count_n & 0x100) >> pos_n);
        if (nodes_per_bv_block == 512)
          count_n = 0;
        pos_n++;
      }
    }

    void write_bv_rank_lt(uint8_t which, size_t rank_lt_sz) {
      uintxx_t node_id = 0;
      size_t u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      uintxx_t count_tail = 0;
      uintxx_t count_term = 0;
      uintxx_t count_child = 0;
      uintxx_t count_leaf = 0;
      uintxx_t count_tail_n = 0;
      uintxx_t count_term_n = 0;
      uintxx_t count_child_n = 0;
      uintxx_t count_leaf_n = 0;
      std::vector<uint16_t> bit_counts_tail_n(u8_arr_count);
      std::vector<uint16_t> bit_counts_term_n(u8_arr_count);
      std::vector<uint16_t> bit_counts_child_n(u8_arr_count);
      std::vector<uint16_t> bit_counts_leaf_n(u8_arr_count);
      uint8_t pos_tail_n = 1;
      uint8_t pos_term_n = 1;
      uint8_t pos_child_n = 1;
      uint8_t pos_leaf_n = 1;
      memset(bit_counts_tail_n.data(),  0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_term_n.data(),  0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_child_n.data(), 0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_leaf_n.data(),  0xFF, u8_arr_count * sizeof(uint16_t));
      bit_counts_tail_n[0] = 0x1E;
      bit_counts_term_n[0] = 0x1E;
      bit_counts_child_n[0] = 0x1E;
      bit_counts_leaf_n[0] = 0x1E;
      memtrie::node_iterator ni(memtrie.all_node_sets, 0);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n.data(), pos_term_n);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n.data(), pos_child_n);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n.data(), pos_leaf_n);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n.data(), pos_tail_n);
        count_tail_n += (cur_node_flags & NFLAG_TAIL ? 1 : 0);
        count_term_n += (cur_node_flags & NFLAG_TERM ? 1 : 0);
        count_child_n += (cur_node_flags & NFLAG_CHILD ? 1 : 0);
        count_leaf_n += (cur_node_flags & NFLAG_LEAF ? 1 : 0);
        node_id++;
        cur_node = ni.next();
      }
      node_id = nodes_per_bv_block; // just to make it write the last blocks
      for (size_t i = 0; i < 2; i++) {
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n.data(), pos_term_n);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n.data(), pos_child_n);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n.data(), pos_leaf_n);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n.data(), pos_tail_n);
      }
      output.write_align8(rank_lt_sz);
    }

    bool node_qualifies_for_select(memtrie::node *cur_node, uint8_t cur_node_flags, int which) {
      switch (which) {
        case BV_LT_TYPE_TERM:
          return (cur_node_flags & NFLAG_TERM) > 0;
        case BV_LT_TYPE_LEAF:
          return (cur_node_flags & NFLAG_LEAF) > 0;
        case BV_LT_TYPE_CHILD:
          return (cur_node_flags & NFLAG_CHILD) > 0;
      }
      return false;
    }

    void write_bv_select_lt(int which, size_t sel_lt_sz) {
      uintxx_t node_id = 0;
      uintxx_t one_count = 0;
      output.write_u24(0);
      memtrie::node_iterator ni(memtrie.all_node_sets, 0);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t cur_node_flags = 0;
        if ((cur_node.get_flags() & NODE_SET_LEAP) == 0)
          cur_node_flags = cur_node.get_flags();
        if (node_qualifies_for_select(&cur_node, cur_node_flags, which)) {
          one_count++;
          if (one_count && (one_count % sel_divisor) == 0) {
            uintxx_t val_to_write = node_id / nodes_per_bv_block;
            output.write_u24(val_to_write);
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", one_count, val_to_write);
          }
        }
        node_id++;
        cur_node = ni.next();
      }
      output.write_u24(memtrie.node_count/nodes_per_bv_block);
      output.write_align8(sel_lt_sz);
    }

    void write_louds_rank_lt(size_t rank_lt_sz, gen::bit_vector<uint64_t> &louds) {
      uintxx_t count = 0;
      uintxx_t count_n = 0;
      int u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      std::vector<uint16_t> bit_counts_n(u8_arr_count);
      uint8_t pos_n = 1;
      memset(bit_counts_n.data(), 0xFF, u8_arr_count * sizeof(uint16_t));
      bit_counts_n[0] = 0x1E;
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        write_bv_n(i, true, count, count_n, bit_counts_n.data(), pos_n);
        count_n += (louds[i] ? 1 : 0);
      }
      bit_count = nodes_per_bv_block; // just to make it write last blocks
      write_bv_n(bit_count, true, count, count_n, bit_counts_n.data(), pos_n);
      write_bv_n(bit_count, true, count, count_n, bit_counts_n.data(), pos_n);
      output.write_align8(rank_lt_sz);
    }

    void write_louds_select_lt(size_t sel_lt_sz, gen::bit_vector<uint64_t> &louds) {
      uintxx_t one_count = 0;
      output.write_u24(0);
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        if (louds[i]) {
          one_count++;
          if (one_count && (one_count % sel_divisor) == 0) {
            uintxx_t val_to_write = i / nodes_per_bv_block;
            output.write_u24(val_to_write);
            if (val_to_write > (1 << 24))
              gen::gen_printf("WARNING: %u\t%u\n", one_count, val_to_write);
          }
        }
      }
      output.write_u24(bit_count / nodes_per_bv_block);
      output.write_align8(sel_lt_sz);
    }

};

}}

#endif
