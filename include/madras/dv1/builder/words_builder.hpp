#ifndef __DV1_BUILDER_WORDS__
#define __DV1_BUILDER_WORDS__

#include "builder_interfaces.hpp"
#include "madras/dv1/builder/trie/memtrie/in_mem_trie.hpp"

namespace madras { namespace dv1 {

typedef struct {
  uint8_t *word_pos;
  uintxx_t word_len;
  uintxx_t word_ptr_pos;
  uintxx_t node_id;
} word_refs;

class word_tries_builder {
  private:
    trie_map_builder_fwd *map_bldr;
    tail_val_maps &val_maps;
    std::vector<uintxx_t> word_ptrs;
    std::vector<word_refs> words_for_sort;
    uintxx_t max_word_count = 0;
    uintxx_t total_word_entries = 0;
    uintxx_t rpt_freq = 0;
    uintxx_t max_rpt_count = 0;

  public:
    word_tries_builder(trie_map_builder_fwd *_map_bldr, tail_val_maps &_val_maps)
                : val_maps (_val_maps) {
      map_bldr = _map_bldr;
      max_word_count = 0;
      total_word_entries = 0;
      rpt_freq = 0;
      max_rpt_count = 0;
    }

    virtual ~word_tries_builder() {
    }

    void add_rev_node_id(byte_vec& rev_nids, uintxx_t node_start, uintxx_t node_end, uintxx_t prev_node_id) {
      if (node_end != UINTXX_MAX) {
        node_end -= node_start;
        node_end--;
      }
      node_start -= prev_node_id;
      // printf("ns: %zu, ne: %zu, pnid: %zu\n", node_start, node_end, prev_node_id);
      gen::append_vint32(rev_nids, (node_start << 1) | (node_end != UINTXX_MAX ? 1 : 0));
      if (node_end != UINTXX_MAX)
         gen::append_vint32(rev_nids, node_end);
    }

    trie_map_builder_fwd *create_word_trie_builder(char enc_type, gen::byte_blocks *rev_col_vals) {
      bldr_options wtb_opts[2];
      wtb_opts[0] = dflt_opts;
      wtb_opts[1] = dflt_opts;
      wtb_opts[0].max_groups = 1;
      wtb_opts[0].max_inner_tries = 2;
      wtb_opts[0].partial_sfx_coding = false;
      wtb_opts[0].sort_nodes_on_freq = false;
      wtb_opts[0].opts_count = 2;
      wtb_opts[0].leap_frog = true;
      wtb_opts[1].inner_tries = 0;
      wtb_opts[1].max_inner_tries = 0;
      trie_map_builder_fwd *ret;
      if (enc_type == MSE_WORDS_2WAY) {
        ret = map_bldr->new_instance("word_trie,key,rev_nids", 2, "t*", "us", 0, 1, wtb_opts);
        ret->set_all_vals(rev_col_vals);
      } else
        ret = map_bldr->new_instance("word_trie,key", 1, "t", "u", 0, 1, wtb_opts);
      return ret;
    }

    uintxx_t build_words(uniq_info_vec& uniq_words_vec, gen::byte_blocks& uniq_words,
              char encoding_type, uint16_t pk_col_count,
              gen::byte_blocks *rev_col_vals, uint8_t sec_idx_nid_shift_bits) {

      clock_t t = clock();

      std::sort(words_for_sort.begin(), words_for_sort.end(), [](const word_refs& lhs, const word_refs& rhs) -> bool {
        int cmp = gen::compare(lhs.word_pos, lhs.word_len, rhs.word_pos, rhs.word_len);
        return (cmp == 0) ? (lhs.node_id < rhs.node_id) : (cmp < 0);
      });

      byte_vec rev_nids;
      rev_col_vals->reset();
      rev_col_vals->push_back("\xFF\xFF", 2);
      uintxx_t prev_node_id = 0;
      uintxx_t node_start = UINTXX_MAX;
      uintxx_t node_end = UINTXX_MAX;

      uintxx_t tot_freq = 0;
      uintxx_t freq_count = 0;
      std::vector<word_refs>::iterator it = words_for_sort.begin();
      uint8_t *prev_val = it->word_pos;
      uintxx_t prev_val_len = it->word_len;
      while (it != words_for_sort.end()) {
        int cmp = gen::compare(it->word_pos, it->word_len, prev_val, prev_val_len);
        if (cmp != 0) {
          uniq_info_base *ui_ptr = new uniq_info_base({0, prev_val_len, (uintxx_t) uniq_words_vec.size()});
          ui_ptr->freq_count = freq_count;
          uniq_words_vec.push_back(ui_ptr);
          tot_freq += freq_count;
          ui_ptr->pos = uniq_words.push_back(prev_val, prev_val_len);
          freq_count = 0;
          prev_val = it->word_pos;
          prev_val_len = it->word_len;
              add_rev_node_id(rev_nids, node_start, node_end, prev_node_id);
              // if (memcmp(prev_val, (const uint8_t *) "Portugal (", gen::min(10, prev_val_len)) == 0) {
              //   printf("ns: %u, ne: %u, pns: %u\n", node_start, node_end, prev_node_id);
              //   printf("Rev nids size: %lu\n", rev_nids.size());
              // }
              ui_ptr->ptr = rev_col_vals->push_back_with_vlen(rev_nids.data(), rev_nids.size());
              // printf("Rev nids ptr\t%u\t%lu\n", ui_ptr->ptr, rev_nids.size());
              rev_nids.clear();
              prev_node_id = 0;
              node_end = UINTXX_MAX;
              node_start = UINTXX_MAX;
        }
        // if (memcmp(it->word_pos, (const uint8_t *) "Himalaya", gen::min(8, it->word_len)) == 0) {
        //   printf("[%.*s], nid: %u\n", (int) it->word_len, it->word_pos, it->node_id);
        // }
        uintxx_t cur_node_id = it->node_id >> sec_idx_nid_shift_bits;
        if (node_start == UINTXX_MAX) {
          node_start = cur_node_id;
        } else if (cur_node_id - (node_end == UINTXX_MAX ? node_start : node_end) < 2) {
          node_end = cur_node_id;
        } else {
          // if (memcmp(prev_val, (const uint8_t *) "Portugal (", gen::min(10, prev_val_len)) == 0) {
          //   printf("ns: %u, ne: %u, pns: %u\n", node_start, node_end, prev_node_id);
          //   printf("Rev nids size: %lu\n", rev_nids.size());
          // }
          add_rev_node_id(rev_nids, node_start, node_end, prev_node_id);
          prev_node_id = (node_end == UINTXX_MAX ? node_start : node_end) + 1;
          node_start = cur_node_id;
          node_end = UINTXX_MAX;
        }
        freq_count++; // += it->freq;
        word_ptrs[it->word_ptr_pos] = uniq_words_vec.size();
        it++;
      }
      add_rev_node_id(rev_nids, node_start, node_end, prev_node_id);
      printf("Size of rev nids: %zu\n", rev_nids.size());
      uniq_info_base *ui_ptr = new uniq_info_base({0, prev_val_len, (uintxx_t) uniq_words_vec.size()});
      ui_ptr->freq_count = freq_count;
      uniq_words_vec.push_back(ui_ptr);
      tot_freq += freq_count;
      ui_ptr->pos = uniq_words.push_back(prev_val, prev_val_len);
      ui_ptr->ptr = rev_col_vals->push_back_with_vlen(rev_nids.data(), rev_nids.size());
      // printf("Rev nids size: %lu\n", rev_nids.size());

      printf("\nTotal words: %zu, uniq words: %zu, rpt_count: %" PRIuXX ", max rpts: %" PRIuXX ", max word count: %" PRIuXX ", word_ptr_size: %zu\n",
            words_for_sort.size(), uniq_words_vec.size(), rpt_freq, max_rpt_count, max_word_count, word_ptrs.size());
      t = gen::print_time_taken(t, "Time taken for make_uniq_words: ");

      uintxx_t last_data_len;
      uint8_t start_bits = 1;
      uint8_t grp_no, len_grp_no, rpt_grp_no;
      rpt_grp_no = 0;
      ptr_groups& ptr_grps = *val_maps.get_grp_ptrs();
      ptr_grps.reset();
      uniq_info_vec uniq_words_freq;
      uintxx_t cumu_freq_idx = val_maps.make_uniq_freq(uniq_words_vec, uniq_words_freq, tot_freq, last_data_len, start_bits, grp_no, true);

      ptr_grps.set_idx_info(start_bits, grp_no, 3);
      ptr_grps.add_freq_grp(freq_grp{0, 0, 0, 0, 0, 0, 0, 0}, true);

      grp_no = 1;
      len_grp_no = grp_no;
      ptr_grps.next_grp(grp_no, pow(2, gen::bits_needed(max_word_count) - 1), 0, tot_freq, true);
      ptr_grps.update_current_grp(len_grp_no, max_word_count, total_word_entries, max_word_count);
      ptr_grps.append_text(len_grp_no, (const uint8_t *) "L", 1);

      if (rpt_freq > 0 && max_rpt_count > 0) {
        rpt_grp_no = grp_no;
        ptr_grps.next_grp(grp_no, pow(2, gen::bits_needed(max_rpt_count) - 1), 0, tot_freq, true);
        ptr_grps.update_current_grp(rpt_grp_no, max_rpt_count, rpt_freq, max_rpt_count);
        ptr_grps.append_text(rpt_grp_no, (const uint8_t *) "R", 1);
      }

      uintxx_t freq_idx = 0;
      uintxx_t max_word_len = 0;
      uintxx_t cur_limit = pow(2, start_bits);

      trie_builder_fwd *cur_word_trie = create_word_trie_builder(encoding_type, rev_col_vals);
      ptr_grps.inner_tries.push_back(cur_word_trie);
      ptr_grps.inner_trie_start_grp = grp_no;
      ptr_grps.add_freq_grp(freq_grp{grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0}, true);
      while (freq_idx < uniq_words_freq.size()) {
        uniq_info_base *vi = uniq_words_freq[freq_idx];
        freq_idx++;
        uintxx_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, 1, tot_freq, false, true);
        if (new_limit != cur_limit) {
          cur_word_trie = create_word_trie_builder(encoding_type, rev_col_vals);
          ptr_grps.inner_tries.push_back(cur_word_trie);
          cur_limit = new_limit;
        }
        if (max_word_len < vi->len)
          max_word_len = vi->len;
        vi->grp_no = grp_no;
        cur_word_trie->insert(uniq_words[vi->pos], vi->len, freq_idx - 1);
        ptr_grps.update_current_grp(grp_no, 1, vi->freq_count);
      }

      ptr_grps.set_grp_nos(len_grp_no, rpt_grp_no, 0);

      uintxx_t sum_trie_sizes = 0;
      for (size_t it_idx = 0; it_idx < ptr_grps.inner_tries.size(); it_idx++) {
        trie_builder_fwd *inner_trie = ptr_grps.inner_tries[it_idx];
        uintxx_t trie_size = inner_trie->build();
        printf("Trie size: %" PRIuXX "\n", trie_size);
        // ptr_groups *it_ptr_grps = inner_trie->get_grp_ptrs();
        // it_ptr_grps->build();
        memtrie::node_iterator ni(inner_trie->get_memtrie()->all_node_sets, 0);
        memtrie::node n = ni.next();
        int leaf_id = 0;
        uintxx_t node_id = 0;
        // printf("Inner trie %lu\n", it_idx);
        while (n != nullptr) {
          uintxx_t col_val_pos = n.get_col_val();
          if (n.get_flags() & NFLAG_LEAF) {
            uniq_info_base *ti = uniq_words_freq[col_val_pos];
            // printf("Ptr: %u\n", ti->ptr);
            if (encoding_type == MSE_WORDS_2WAY)
              n.set_col_val(ti->ptr);
            ti->ptr = leaf_id++;
          }
          n = ni.next();
          node_id++;
        }
        trie_map_builder_fwd *inner_trie_map = dynamic_cast<trie_map_builder_fwd*>(inner_trie);
        trie_size += inner_trie_map->build_kv(false);
        inner_trie_map->set_all_vals(nullptr, false);
        freq_grp *fg = ptr_grps.get_freq_grp(it_idx + ptr_grps.inner_trie_start_grp);
        fg->grp_size = trie_size;
        // fg->grp_limit = node_id;
        // fg->count = node_id;
        sum_trie_sizes += trie_size;
        //printf("Inner Trie size:\t%u\n", trie_size);
      }

      ptr_grps.build_freq_codes(true);
      ptr_grps.show_freq_codes();

      freq_grp *cur_fg;
      memtrie::node_iterator ni(ptr_grps.all_node_sets, pk_col_count == 0 ? 1 : 0);
      memtrie::node n = ni.next();
      while (n != nullptr && (n.get_flags() & NFLAG_LEAF) == 0) {
        n = ni.next();
        n.set_col_val(0);
      }
      uintxx_t ptr_bit_count = 0;
      uintxx_t word_count = 0;
      for (size_t i = 0; i < word_ptrs.size(); i++) {
        if (word_count == 0) {
          word_count = word_ptrs[i];
          if ((word_count & 0x40000000L) == 0) {
            if (ptr_bit_count > 0) {
              if (n.get_flags() & NFLAG_LEAF) {
                n.set_col_val(ptr_bit_count);
                // printf("ptr_bit_count: %u\n", ptr_bit_count);
              }
              do {
                n = ni.next();
                n.set_col_val(0);
              } while (n != nullptr && (n.get_flags() & NFLAG_LEAF) == 0);
              ptr_bit_count = 0;
            }
            cur_fg = ptr_grps.get_freq_grp(len_grp_no);
            ptr_grps.append_ptr_bits(cur_fg->code, cur_fg->code_len);
            ptr_grps.append_ptr_bits(word_count, cur_fg->grp_log2 - cur_fg->code_len);
            ptr_bit_count += cur_fg->grp_log2;
            continue;
          }
        }
        if (word_count & 0x40000000L) {
          uintxx_t word_rpt_count = word_count & 0x3FFFFFFFL;
          cur_fg = ptr_grps.get_freq_grp(rpt_grp_no);
          ptr_grps.append_ptr_bits(cur_fg->code, cur_fg->code_len);
          ptr_grps.append_ptr_bits(word_rpt_count, cur_fg->grp_log2 - cur_fg->code_len);
          ptr_bit_count += cur_fg->grp_log2;
          while (word_rpt_count--) {
            if (n.get_flags() & NFLAG_LEAF) { // should be always true
              n.set_col_val(0);
              n = ni.next();
            }
            while ((n.get_flags() & NFLAG_LEAF) == 0) {
              n.set_col_val(0);
              n = ni.next();
            }
          }
          word_count = 0;
          continue;
        }
        uniq_info_base *vi = uniq_words_vec[word_ptrs[i]];
        cur_fg = ptr_grps.get_freq_grp(vi->grp_no);
        ptr_grps.append_ptr_bits(cur_fg->code, cur_fg->code_len);
        ptr_grps.append_ptr_bits(vi->ptr, cur_fg->grp_log2 - cur_fg->code_len);
        ptr_bit_count += cur_fg->grp_log2;
        // printf("%u\t%u\t%u\t%u\n", vi->grp_no, cur_fg->code_len, cur_fg->grp_log2, vi->ptr);
        word_count--;
      }
      if (ptr_bit_count > 0 && n != nullptr && (n.get_flags() & NFLAG_LEAF))
        n.set_col_val(ptr_bit_count);
      ptr_grps.append_ptr_bits(0x00, 8); // read beyond protection

      printf("Total size of tries: %" PRIuXX ", Ptrs size: %" PRIuXX "\n", sum_trie_sizes, ptr_grps.get_ptrs_size());
      t = gen::print_time_taken(t, "Time taken for build_words(): ");

      return 0;

    }

    void add_words(uintxx_t node_id, uint8_t *word_str, uintxx_t word_str_len, 
            uint8_t *prev_val, uintxx_t prev_val_len, word_split_iface *word_splitter) {
      // first 2 bits of word_count_pos:
      // 00 - count of words
      // 01 - repeat prev val
      // 19 - repeat prev seq
      // 11 - tbd
      // if ((node_id % nodes_per_bv_block_n) != 0 && words_len == prev_val_len &&
      //         std::memcmp(words, prev_val, prev_val_len) == 0) {
      //   rpt_freq++;
      //   size_t last_idx = word_ptrs.size() - 1;
      //   if (word_ptrs[last_idx] == 0) {
      //     word_ptrs.push_back(0x40000001L);
      //     last_idx++;
      //   } else {
      //     // if (word_ptrs[last_idx] & 0x40000000L) { // implied
      //       word_ptrs[last_idx]++;
      //     // }
      //   }
      //   if (max_rpt_count < (word_ptrs[last_idx] & 0x3FFFFFFFL))
      //     max_rpt_count = (word_ptrs[last_idx] & 0x3FFFFFFFL);
      //   return;
      // }
      total_word_entries++;
      uintxx_t last_word_len = 0;
      bool is_prev_non_word = false;
      uintxx_t word_count_pos = word_ptrs.size();
      word_ptrs.push_back(0); // initially 0
      uintxx_t *word_positions = new uintxx_t[word_str_len + 1];
      splitter_result sr = word_splitter->split_into_words(word_str, word_str_len, word_str_len, word_positions);
      word_positions[sr.word_count] = word_str_len;
      uintxx_t ref_id = 0;
      // printf("Word str: %.*s\n", word_str_len, word_str);
      for (size_t i = 0; i < sr.word_count; i++) {
        ref_id = word_ptrs.size();
        word_ptrs.push_back(0);
        uintxx_t word_len = word_positions[i + 1] - word_positions[i];
        // printf("Word: %.*s, len: %zu\n", word_len, word_str + word_positions[i], word_len);
        words_for_sort.push_back(word_refs{word_str + word_positions[i], word_len, ref_id, node_id});
      }
      if (max_word_count < sr.word_count)
        max_word_count = sr.word_count;
      word_ptrs[word_count_pos] = sr.word_count;
    }

};
}}

#endif
