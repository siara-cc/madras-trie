#ifndef __DV1_BUILDER_UNIQ_MAKER__
#define __DV1_BUILDER_UNIQ_MAKER__

#include "madras/dv1/common.hpp"
#include "madras/dv1/ds_common/byte_blocks.hpp"
#include "madras/dv1/builder/builder_interfaces.hpp"
#include "madras/dv1/builder/trie/memtrie/in_mem_trie.hpp"

namespace madras { namespace dv1 {

class uniq_maker {
  public:
    static uintxx_t make_uniq(byte_ptr_vec& all_node_sets, gen::byte_blocks& all_data, gen::byte_blocks& uniq_data,
          uniq_info_vec& uniq_vec, sort_callbacks& sic, uintxx_t& max_len, int trie_level = 0, size_t col_idx = 0, size_t column_count = 0, char type = MST_BIN) {
      node_data_vec nodes_for_sort;
      add_to_node_data_vec(nodes_for_sort, all_node_sets, sic, all_data, type);
      return sort_and_reduce(nodes_for_sort, all_data, uniq_data, uniq_vec, sic, max_len, trie_level, type);
    }
    static void add_to_node_data_vec(node_data_vec& nodes_for_sort, byte_ptr_vec& all_node_sets, sort_callbacks& sic,
          gen::byte_blocks& all_data, char type = MST_BIN) {
      for (uintxx_t i = 1; i < all_node_sets.size(); i++) {
        memtrie::node_set_handler cur_ns(all_node_sets, i);
        memtrie::node n = cur_ns.first_node();
        for (size_t k = 0; k <= cur_ns.last_node_idx(); k++) {
          uintxx_t len = 0;
          uint8_t *pos = sic.get_data_and_len(n, len, type);
          if (pos != NULL || len == 1) {
            // printf("%d, [%.*s]\n", len, len, pos);
            nodes_for_sort.push_back((struct node_data) { pos, len, i, (uint8_t) k});
          }
          n.next();
        }
      }
      gen::gen_printf("Nodes for sort size: %lu\n", nodes_for_sort.size());
    }
    static uintxx_t sort_and_reduce(node_data_vec& nodes_for_sort, gen::byte_blocks& all_data,
          gen::byte_blocks& uniq_data, uniq_info_vec& uniq_vec, sort_callbacks& sic, uintxx_t& max_len, int trie_level, char type = MST_BIN) {
      clock_t t = clock();
      if (nodes_for_sort.size() == 0)
        return 0;
      sic.sort_data(nodes_for_sort, trie_level);
      uintxx_t freq_count = 0;
      uintxx_t tot_freq = 0;
      node_data_vec::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->data;
      int prev_val_len = it->len;
      while (it != nodes_for_sort.end()) {
        int cmp = sic.compare(it->data, it->len, prev_val, prev_val_len, trie_level);
        if (cmp != 0) {
          uniq_info *ui_ptr = new uniq_info(0, prev_val_len, uniq_vec.size(), 0);
          ui_ptr->freq_count = freq_count;
          uniq_vec.push_back(ui_ptr);
          tot_freq += freq_count;
          for (int i = 0; i < prev_val_len; i++) {
            uint8_t b = prev_val[i];
            if (b >= 15 && b < 32)
              ui_ptr->flags |= LPDU_BIN;
              //uniq_vec[0]->flags |= LPDU_BIN;
          }
          if (trie_level == 0) // TODO: no need for vals
            uniq_data.push_back(prev_val, prev_val_len);
          else
            uniq_data.push_back_rev(prev_val, prev_val_len);
          ui_ptr->pos = uniq_data.size() - prev_val_len;
          if (max_len < prev_val_len)
            max_len = prev_val_len;
          freq_count = 0;
          prev_val = it->data;
          prev_val_len = it->len;
        }
        freq_count++; // += it->freq;
        sic.set_uniq_pos(it->ns_id, it->node_idx, uniq_vec.size());
        it++;
      }
      uniq_info *ui_ptr = new uniq_info(0, prev_val_len, uniq_vec.size(), 0);
      ui_ptr->freq_count = freq_count;
      uniq_vec.push_back(ui_ptr);
      for (int i = 0; i < prev_val_len; i++) {
        uint8_t b = prev_val[i];
        if (b >= 15 && b < 32)
          ui_ptr->flags |= LPDU_BIN;
          // uniq_vec[0]->flags |= LPDU_BIN;
      }
      if (trie_level == 0)
        uniq_data.push_back(prev_val, prev_val_len);
      else
        uniq_data.push_back_rev(prev_val, prev_val_len);
      ui_ptr->pos = uniq_data.size() - prev_val_len;
      if (max_len < prev_val_len)
        max_len = prev_val_len;
      t = gen::print_time_taken(t, "Time taken for make_uniq: ");
      return tot_freq;
    }
};

}}

#endif
