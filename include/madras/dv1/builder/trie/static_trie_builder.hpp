#ifndef __DV1_BUILDER_STATIC_TRIE__
#define __DV1_BUILDER_STATIC_TRIE__

#include "madras/dv1/common.hpp"

#include "madras/dv1/builder/output_writer.hpp"
#include "madras/dv1/builder/pointer_groups.hpp"
#include "madras/dv1/builder/builder_interfaces.hpp"

namespace madras { namespace dv1 {

struct tail_token {
  uintxx_t token_pos;
  uintxx_t token_len;
  uintxx_t fwd_pos;
  uintxx_t cmp_max;
};

struct bldr_min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
  bldr_min_pos_stats() {
    max_b = max_len = 0;
    min_b = min_len = 0xFF;
  }
};

struct trie_parts {
  uintxx_t fwd_cache_count;
  uintxx_t fwd_cache_size;
  uintxx_t fwd_cache_max_node_id;
  uintxx_t rev_cache_count;
  uintxx_t rev_cache_size;
  uintxx_t rev_cache_max_node_id;
  uintxx_t sec_cache_count;
  uintxx_t sec_cache_size;
  uintxx_t louds_rank_lt_loc;
  uintxx_t louds_sel1_lt_loc;
  uintxx_t trie_flags_term_loc;
  uintxx_t trie_flags_child_loc;
  uintxx_t trie_flags_tail_loc;
  uintxx_t trie_flags_leaf_loc;
  uintxx_t louds_rank_lt_sz;
  uintxx_t louds_sel1_lt_sz;
  uintxx_t term_rank_lt_sz;
  uintxx_t child_rank_lt_sz;
  uintxx_t leaf_rank_lt_sz;
  uintxx_t tail_rank_lt_sz;
  uintxx_t term_select_lt_sz;
  uintxx_t child_select_lt_sz;
  uintxx_t leaf_select_lt_sz;
  uintxx_t opts_loc;
  uintxx_t opts_size;
  uintxx_t fwd_cache_loc;
  uintxx_t rev_cache_loc;
  uintxx_t sec_cache_loc;
  uintxx_t term_select_lkup_loc;
  uintxx_t term_rank_lt_loc;
  uintxx_t child_rank_lt_loc;
  uintxx_t trie_tail_ptrs_data_loc;
  uintxx_t trie_tail_ptrs_data_sz;
  uintxx_t leaf_select_lkup_loc;
  uintxx_t leaf_rank_lt_loc;
  uintxx_t tail_rank_lt_loc;
  uintxx_t child_select_lkup_loc;
  uintxx_t names_loc;
  uintxx_t names_sz;
  uintxx_t col_val_table_loc;
  uintxx_t col_val_table_sz;
  uintxx_t col_val_loc0;
  uintxx_t null_val_loc;
  uintxx_t empty_val_loc;
  uintxx_t null_empty_sz;
  uintxx_t total_idx_size;
  bldr_min_pos_stats min_stats;
};

class tail_sort_callbacks : public sort_callbacks {
  private:
    byte_ptr_vec& all_node_sets;
    gen::byte_blocks& all_tails;
    gen::byte_blocks& uniq_tails;
  public:
    tail_sort_callbacks(byte_ptr_vec& _all_node_sets, gen::byte_blocks& _all_tails, gen::byte_blocks& _uniq_tails)
      : all_node_sets (_all_node_sets), all_tails (_all_tails), uniq_tails (_uniq_tails) {
    }
    virtual ~tail_sort_callbacks() {
    }
    uint8_t *get_data_and_len(memtrie::node& n, uintxx_t& len, char type = '*') {
      if (n.get_flags() & NFLAG_TAIL) {
        size_t vlen;
        uint8_t *v = all_tails[n.get_tail()];
        len = gen::read_vint32(v, &vlen);
        v += vlen;
        return v;
      }
      return NULL;
    }
    void set_uniq_pos(uintxx_t ns_id, uint8_t node_idx, size_t pos) {
      memtrie::node_set_handler ns(all_node_sets, ns_id);
      memtrie::node n = ns[node_idx];
      n.set_tail(pos);
    }
    int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2, int trie_level) {
      if (trie_level == 0)
        return gen::compare_rev(v1, len1, v2, len2);
      return gen::compare(v1, len1, v2, len2);
    }
    void sort_data(node_data_vec& nodes_for_sort, int trie_level) {
      clock_t t = clock();
      if (trie_level == 0) {
        std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [](const struct node_data& lhs, const struct node_data& rhs) -> bool {
          return gen::compare_rev(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
        });
      } else {
        std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [](const struct node_data& lhs, const struct node_data& rhs) -> bool {
          return gen::compare(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
        });
      }
      t = gen::print_time_taken(t, "Time taken for sort tails: ");
    }
};

class static_trie_builder : public virtual trie_builder_fwd {
  private:
  public:
    fwd_cache *f_cache;
    nid_cache *r_cache;
    uintxx_t *f_cache_freq;
    uintxx_t *r_cache_freq;
    memtrie::in_mem_trie memtrie;
    byte_vec trie;
    gen::bit_vector<uint64_t> louds;
    byte_vec trie_flags_term;
    byte_vec trie_flags_child;
    byte_vec trie_flags_tail;
    byte_vec trie_flags_leaf;
    uintxx_t end_loc;
    gen::byte_blocks uniq_tails;
    uniq_info_vec uniq_tails_rev;
    tail_val_maps tail_maps;
    bool is_ns_sorted;
    uintxx_t max_level;
    size_t max_val_len;
    uintxx_t column_count;
    trie_builder_fwd *tail_trie_builder;
    uint8_t null_value[15];
    size_t null_value_len;
    uint8_t empty_value[15];
    size_t empty_value_len;
    trie_parts tp = {};
    static_trie_builder(uint16_t _pk_col_count,
          uintxx_t _col_count, size_t _max_val_len, // todo: not member
          uint16_t _trie_level, const bldr_options *_opts = &dflt_opts,
          const uint8_t *_null_value = NULL_VALUE, size_t _null_value_len = NULL_VALUE_LEN,
          const uint8_t *_empty_value = EMPTY_VALUE, size_t _empty_value_len = EMPTY_VALUE_LEN) :
          column_count (_col_count), max_val_len (_max_val_len),
          memtrie(_null_value, _null_value_len, _empty_value, _empty_value_len),
          tail_maps (this, uniq_tails, uniq_tails_rev, memtrie.all_node_sets, output),
          trie_builder_fwd (_opts, _trie_level, _pk_col_count) {
      memcpy(null_value, _null_value, _null_value_len);
      null_value_len = _null_value_len;
      memcpy(empty_value, _empty_value, _empty_value_len);
      empty_value_len = _empty_value_len;
      memtrie.set_print_enabled(gen::is_gen_print_enabled);
      f_cache = nullptr;
      r_cache = nullptr;
      f_cache_freq = nullptr;
      r_cache_freq = nullptr;
      max_level = 0;
      tail_trie_builder = NULL;
      tail_maps.init();
      is_ns_sorted = false;
    }

    virtual ~static_trie_builder() {
      if (tail_trie_builder != NULL)
        delete tail_trie_builder;
      if (f_cache != nullptr)
        delete [] f_cache;
      if (r_cache != nullptr)
        delete [] r_cache;
      if (f_cache_freq != nullptr)
        delete [] f_cache_freq;
      if (r_cache_freq != nullptr)
        delete [] r_cache_freq;
    }

    size_t size() {
      return memtrie.key_count;
    }

    bldr_options *get_opts() {
      return opts;
    }

    memtrie::in_mem_trie *get_memtrie() {
      return &memtrie;
    }

    trie_builder_fwd *new_instance() {
      bldr_options inner_trie_opts = inner_tries_dflt_opts;
      inner_trie_opts.trie_leaf_count = 0;
      inner_trie_opts.leaf_lt = false;
      inner_trie_opts.max_groups = get_opts()->max_groups;
      inner_trie_opts.max_inner_tries = get_opts()->max_inner_tries;
      if (get_opts()->max_inner_tries <= trie_level + 1) {
        inner_trie_opts.inner_tries = false;
      }
      static_trie_builder *ret = new static_trie_builder(1, 1, 0, trie_level + 1, &inner_trie_opts);
      ret->set_fp(output.get_fp());
      ret->set_out_vec(output.get_out_vec());
      return ret;
    }

    // uint64_t avg_freq_all_ns;
    uintxx_t set_ns_freq(uintxx_t ns_id, int level) {
      if (ns_id == 0)
        return 1;
      memtrie::node_set_handler ns(memtrie.all_node_sets, ns_id);
      memtrie::node n = ns.first_node();
      memtrie::node_set_header *ns_hdr = ns.hdr();
      uintxx_t freq_count = 1;
      for (int i = 0; i <= ns_hdr->last_node_idx; i++) {
        uintxx_t node_freq = set_ns_freq(n.get_child(), level + 1);
        freq_count += node_freq;
        n.next();
      }
      // avg_freq_all_ns += freq_count;
      ns_hdr->freq = freq_count; // > 0xFFFF ? 0xFFFF : freq_count;
      return freq_count;
    }

    void split_tails() {
      clock_t t = clock();
      typedef struct {
        uint8_t *part;
        uintxx_t uniq_arr_idx;
        uintxx_t ns_id;
        uint16_t part_len;
        uint8_t node_idx;
      } tail_part;
      typedef struct {
        uint8_t *uniq_part;
        uintxx_t freq_count;
        uint16_t uniq_part_len;
      } tail_uniq_part;
      std::vector<tail_part> tail_parts;
      std::vector<tail_uniq_part> tail_uniq_parts;
      memtrie::node_iterator ni(memtrie.all_node_sets, 1);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uint8_t *tail = (*memtrie.all_tails)[cur_node.get_tail()];
          size_t vlen;
          uintxx_t tail_len = gen::read_vint32(tail, &vlen);
          tail += vlen;
          int last_word_len = 0;
          bool is_prev_non_word = false;
          std::vector<tail_part> tail_parts1;
          for (size_t i = 0; i < tail_len; i++) {
            uint8_t c = tail[i];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c > 127) {
              if (last_word_len >= 5 && is_prev_non_word) {
                if (tail_len - i < 5) {
                  last_word_len += (tail_len - i);
                  break;
                }
                if (i > last_word_len)
                  tail_parts1.push_back((tail_part) {tail + i - last_word_len, 0, (uintxx_t) ni.get_cur_nsh_id(), (uint16_t) last_word_len, ni.get_cur_sib_id()});
                last_word_len = 0;
              }
              is_prev_non_word = false;
            } else {
              is_prev_non_word = true;
            }
            last_word_len++;
          }
          if (last_word_len < tail_len)
            tail_parts1.push_back((tail_part) {tail + tail_len - last_word_len, 0, (uintxx_t) ni.get_cur_nsh_id(), (uint16_t) last_word_len, ni.get_cur_sib_id()});
          if (tail_parts1.size() > 0) {
            tail_parts.insert(tail_parts.end(), tail_parts1.begin(), tail_parts1.end());
            // printf("Tail: [%.*s]\n", (int) tail_len, tail);
            // for (size_t i = 0; i < tail_parts1.size(); i++) {
            //   tail_part *tp = &tail_parts1[i];
            //   printf("[%.*s]\n", tp->part_len, tp->part);
            // }
          }
        }
        cur_node = ni.next();
      }
      gen::gen_printf("Parts count: %lu\n", tail_parts.size());
      if (tail_parts.size() == 0)
        return;
      std::sort(tail_parts.begin(), tail_parts.end(), [](const tail_part& lhs, const tail_part& rhs) -> bool {
        return gen::compare(lhs.part, lhs.part_len, rhs.part, rhs.part_len) < 0;
      });
      tail_part *prev_tp = &tail_parts[0];
      uintxx_t freq_count = 0;
      uintxx_t uniq_arr_idx = 0;
      for (size_t i = 0; i < tail_parts.size(); i++) {
        tail_part *tp = &tail_parts[i];
        tp->uniq_arr_idx = uniq_arr_idx;
        int cmp = gen::compare(tp->part, tp->part_len, prev_tp->part, prev_tp->part_len);
        if (cmp == 0) {
          freq_count++;
        } else {
          tail_uniq_parts.push_back((tail_uniq_part) {prev_tp->part, freq_count, prev_tp->part_len});
          uniq_arr_idx++;
          freq_count = 1;
        }
        prev_tp = tp;
      }
      tail_uniq_parts.push_back((tail_uniq_part) {prev_tp->part, freq_count, prev_tp->part_len});
      std::sort(tail_parts.begin(), tail_parts.end(), [](const tail_part& lhs, const tail_part& rhs) -> bool {
        return lhs.part > rhs.part;
      });
      gen::gen_printf("Uniq Parts count: %lu\n", tail_uniq_parts.size());
      // std::sort(tail_uniq_parts.begin(), tail_uniq_parts.end(), [](const tail_uniq_part& lhs, const tail_uniq_part& rhs) -> bool {
      //   return lhs.freq_count > rhs.freq_count;
      // });
      // for (size_t i = 0; i < tail_uniq_parts.size(); i++) {
      //   tail_uniq_part *tup = &tail_uniq_parts[i];
      //   printf("%u\t[%.*s]\n", tup->freq_count, (int) tup->uniq_part_len, tup->uniq_part);
      // }
      memtrie::node new_node;
      memtrie::node_set_handler new_nsh(memtrie.all_node_sets, 0);
      memtrie::node_set_handler nsh(memtrie.all_node_sets, 0);
      for (size_t i = 0; i < tail_parts.size(); i++) {
        tail_part *tp = &tail_parts[i];
        tail_uniq_part *utp = &tail_uniq_parts[tp->uniq_arr_idx];
        // if (utp->freq_count > 1) {
          nsh.set_pos(tp->ns_id);
          cur_node = nsh[tp->node_idx];
          uint8_t *tail = (*memtrie.all_tails)[cur_node.get_tail()];
          size_t vlen;
          uintxx_t tail_len = gen::read_vint32(tail, &vlen);
          uintxx_t orig_tail_new_len = tp->part - tail - vlen;
          uintxx_t new_tail_len = tail_len - orig_tail_new_len;
          gen::copy_vint32(orig_tail_new_len, tail, vlen);
          //printf("Tail: [%.*s], pos: %u, len: %u, new len: %u, Part: [%.*s], len: %u\n", (int) tail_len, tail + vlen, cur_node.get_tail(), tail_len, orig_tail_new_len, (int) tp->part_len, tp->part, new_tail_len);
          size_t new_tail_pos = memtrie.all_tails->push_back_with_vlen(tp->part, new_tail_len);
          uintxx_t new_ns_pos = memtrie::node_set_handler::create_node_set(memtrie.all_node_sets, 1);
          new_nsh.set_pos(new_ns_pos);
          new_node = new_nsh.first_node();
          new_node.set_flags(cur_node.get_flags() | NFLAG_TERM);
          new_node.set_child(cur_node.get_child());
          new_node.set_col_val(cur_node.get_col_val());
          new_node.set_byte(*tp->part);
          //printf("New tail pos: %lu\n", new_tail_pos);
          new_node.set_tail(new_tail_pos);
          cur_node.set_flags((cur_node.get_flags() | NFLAG_CHILD) & ~NFLAG_LEAF);
          cur_node.set_child(new_ns_pos);
          cur_node.set_col_val(0);
          memtrie.node_set_count++;
          memtrie.node_count++;
        // }
      }
      gen::gen_printf("New NS count: %lu\n", memtrie.all_node_sets.size());
      gen::print_time_taken(t, "Time taken for tail split: ");
    }

    void sort_nodes_on_freq(memtrie::node_set_handler& nsh) {
      memtrie::node cur_node = nsh.first_node();
      memtrie::node_s *ns = cur_node.get_node_struct();
      // uintxx_t avg_freq = 0;
      // int n_count = nsh.last_node_idx() + 1;
      // node_set_handler nsh_child(all_node_sets, ns->child);
      // for (int i = 0; i < n_count; i++) {
      //   if (ns->child > 0) {
      //     avg_freq += nsh_child.hdr()->freq;
      //   } else
      //     avg_freq++;
      //   cur_node.next();
      //   ns = cur_node.get_node_struct();
      //   if (ns != nullptr)
      //     nsh_child.set_pos(ns->child);
      // }
      // avg_freq /= n_count;
      // ns = nsh.first_node().get_node_struct();
      // std::sort(ns, ns + nsh.last_node_idx(), [this,avg_freq,n_count](const node_s& lhs, const node_s& rhs) -> bool {
      std::sort(ns, ns + nsh.last_node_idx(), [this](const memtrie::node_s& lhs, const memtrie::node_s& rhs) -> bool {
        uintxx_t lhs_freq = 1;
        uintxx_t rhs_freq = 1;
        if (lhs.child > 0) {
          memtrie::node_set_handler nsh_l(this->memtrie.all_node_sets, lhs.child);
          lhs_freq = nsh_l.hdr()->freq;
        }
        if (rhs.child > 0) {
          memtrie::node_set_handler nsh_r(this->memtrie.all_node_sets, rhs.child);
          rhs_freq = nsh_r.hdr()->freq;
        }
        // if (lhs_freq >= avg_freq || rhs_freq >= avg_freq || n_count > 16)
        // if (n_count < 16)
          return lhs_freq > rhs_freq;
        // return lhs.b < rhs.b;
      });
      for (size_t i = 0; i <= nsh.last_node_idx(); i++) {
        memtrie::node n = nsh[i];
        n.set_flags(n.get_flags() & ~NFLAG_TERM);
        if (i == nsh.last_node_idx())
          n.set_flags(n.get_flags() | NFLAG_TERM);
        // uintxx_t node_freq = 1;
        // if (n.get_child() > 0) {
        //   node_set_handler nsh_c(this->all_node_sets, n.get_child());
        //   node_freq = nsh_c.hdr()->freq;
        // }
        // printf("%c(%d/%u) ", n.get_byte(), n.get_byte(), node_freq);
      }
      // printf("\n");
    }

    void swap_node_sets(uintxx_t pos_from, uintxx_t pos_to) {
      if (pos_from == pos_to) {
        memtrie.all_node_sets[pos_to][0] |= NODE_SET_SORTED;
        return;
      }
      while (memtrie.all_node_sets[pos_from][0] & NODE_SET_SORTED)
        pos_from = ((memtrie::node_set_header *) memtrie.all_node_sets[pos_from])->swap_pos;
      uint8_t *ns = memtrie.all_node_sets[pos_to];
      memtrie.all_node_sets[pos_to] = memtrie.all_node_sets[pos_from];
      memtrie.all_node_sets[pos_from] = ns;
      memtrie::node_set_header *nsh = (memtrie::node_set_header *) memtrie.all_node_sets[pos_to];
      nsh->swap_pos = pos_from;
      memtrie.all_node_sets[pos_to][0] |= NODE_SET_SORTED;
    }

    void sort_node_sets() {
      clock_t t = clock();
      uintxx_t nxt_set = 0;
      uintxx_t nxt_node = 0;
      uintxx_t node_set_id = 1;
      memtrie::node_set_handler nsh(memtrie.all_node_sets, 0);
      if (get_opts()->sort_nodes_on_freq) {
        // avg_freq_all_ns = 0;
        set_ns_freq(1, 0);
        // printf("Avg freq: %llu\n", avg_freq_all_ns);
        // avg_freq_all_ns /= node_set_count;
        // printf("Avg freq: %llu\n", avg_freq_all_ns);
        sort_nodes_on_freq(nsh);
      }
      memtrie::node n = nsh.first_node();
      while (nxt_set < memtrie.all_node_sets.size()) {
        if (n == NULL) {
          nxt_set++;
          if (nxt_set == memtrie.all_node_sets.size())
            break;
          nsh.set_pos(nxt_set);
          if (get_opts()->sort_nodes_on_freq)
            sort_nodes_on_freq(nsh);
          n = nsh.first_node();
          nxt_node = 0;
          continue;
        }
        uintxx_t nxt_child = n.get_child();
        if (nxt_child == 0) {
          nxt_node++;
          n.next();
          continue;
        }
        n.set_child(node_set_id);
        swap_node_sets(nxt_child, node_set_id);
        node_set_id++;
        nxt_node++;
        n.next();
      }
      is_ns_sorted = true;
      gen::print_time_taken(t, "Time taken for sort_nodes(): ");
    }

    uint8_t append_tail_ptr(memtrie::node *cur_node) {
      if ((cur_node->get_flags() & NFLAG_TAIL) == 0)
        return cur_node->get_byte();
      if (get_opts()->inner_tries && tail_trie_builder != nullptr) {
        uniq_info_base *ti = (*tail_maps.get_ui_vec())[cur_node->get_tail()];
        return ti->ptr & 0xFF;
      }
      uint8_t node_val;
      uintxx_t ptr = 0;
      ptr_groups *ptr_grps = tail_maps.get_grp_ptrs();
        uniq_info_base *ti = (*tail_maps.get_ui_vec())[cur_node->get_tail()];
        uint8_t grp_no = ti->grp_no;
        // if (grp_no == 0 || ti->tail_ptr == 0)
        //   gen::gen_printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
        ptr = ti->ptr;
        freq_grp *fg = ptr_grps->get_freq_grp(grp_no);
        int node_val_bits = 8 - fg->code_len;
        node_val = (fg->code << node_val_bits) | (ptr & ((1 << node_val_bits) - 1));
        ptr >>= node_val_bits;
        ptr_grps->append_ptr_bits(ptr, fg->grp_log2);
      return node_val;
    }

    FILE *fpp;
    void dump_ptr(memtrie::node *cur_node, uintxx_t node_id) {
      if ((node_id % 64) == 0) {
        fputc('\n', fpp);
      }
      if ((cur_node->get_flags() & NFLAG_TAIL) == 0)
        return;
      uintxx_t ptr = 0;
      ptr_groups *ptr_grps = tail_maps.get_grp_ptrs();
      uniq_info_base *ti = (*tail_maps.get_ui_vec())[cur_node->get_tail()];
      uint8_t grp_no = ti->grp_no;
      // if (grp_no == 0 || ti->tail_ptr == 0)
      //   gen::gen_printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
      ptr = ti->ptr;
      freq_grp *fg = ptr_grps->get_freq_grp(grp_no);
      int node_val_bits = 8 - fg->code_len;
      ptr >>= node_val_bits;
      if (fg->grp_log2 > 0)
        fprintf(fpp, "%u\t%" PRIuXX "\t%" PRIuXX "\n", grp_no, fg->grp_log2, ptr);
    }

    uintxx_t build_tail_trie(uintxx_t tot_freq_count) {
      bldr_options tt_opts = inner_tries_dflt_opts;
      tt_opts.trie_leaf_count = 0;
      tt_opts.leaf_lt = false;
      tt_opts.inner_tries = true;
      tt_opts.max_groups = get_opts()->max_groups;
      tt_opts.max_inner_tries = get_opts()->max_inner_tries;
      tail_trie_builder = new static_trie_builder(1, 1, 0, trie_level + 1, &tt_opts);
      for (size_t i = 0; i < uniq_tails_rev.size(); i++) {
        uniq_info_base *ti = uniq_tails_rev[i];
        uint8_t rev[ti->len];
        uint8_t *ti_data = uniq_tails[ti->pos];
        for (uintxx_t j = 0; j < ti->len; j++)
          rev[j] = ti_data[ti->len - j - 1];
        tail_trie_builder->insert(rev, ti->len, i);
      }
      memtrie::node_iterator ni_freq(tail_trie_builder->get_memtrie()->all_node_sets, 0);
      memtrie::node cur_node = ni_freq.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_CHILD) {
          uintxx_t sum_freq = 0;
          memtrie::node_set_handler nsh_children(tail_trie_builder->get_memtrie()->all_node_sets, cur_node.get_child());
          for (size_t i = 0; i <= nsh_children.last_node_idx(); i++) {
            memtrie::node child_node = nsh_children[i];
            if (child_node.get_flags() & NFLAG_LEAF) {
              uniq_info_base *ti = uniq_tails_rev[cur_node.get_col_val()];
              sum_freq += ti->freq_count;
            }
          }
          nsh_children.hdr()->freq = sum_freq;
        }
        cur_node = ni_freq.next();
      }
      uintxx_t trie_size = tail_trie_builder->build();
      int bit_len = gen::bits_needed(tail_trie_builder->get_memtrie()->node_count + 1) - 8;
      tail_maps.get_grp_ptrs()->set_ptr_lkup_tbl_ptr_width(bit_len);
      gen::gen_printf("Tail trie bit_len: %d [log(%u) - 8]\n", bit_len, tail_trie_builder->get_memtrie()->node_count);
      uintxx_t node_id = 0;
      memtrie::node_iterator ni_tt(tail_trie_builder->get_memtrie()->all_node_sets, 0);
      cur_node = ni_tt.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_LEAF) {
          uniq_info_base *ti = uniq_tails_rev[cur_node.get_col_val()];
          ti->ptr = node_id;
        }
        node_id++;
        cur_node = ni_tt.next();
      }
      byte_vec *tail_ptrs = tail_maps.get_grp_ptrs()->get_ptrs();
      gen::int_bit_vector int_bv(tail_ptrs, bit_len, uniq_tails_rev.size());
      memtrie::node_iterator ni(memtrie.all_node_sets, 0);
      cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uniq_info_base *ti = uniq_tails_rev[cur_node.get_tail()];
          int_bv.append(ti->ptr >> 8);
        }
        cur_node = ni.next();
      }
      return trie_size;
    }

    uintxx_t build_trie() {
      clock_t t = clock();
      tail_sort_callbacks tail_sort_cb(memtrie.all_node_sets, *memtrie.all_tails, uniq_tails);
      uintxx_t tot_freq_count = uniq_maker::make_uniq(memtrie.all_node_sets, *memtrie.all_tails,
          uniq_tails, uniq_tails_rev, tail_sort_cb, memtrie.max_tail_len, trie_level, 0, 0, MST_BIN);
      uintxx_t tail_trie_size = 0;
      if (uniq_tails_rev.size() > 0) {
        if (get_opts()->inner_tries && get_opts()->max_groups == 1 && get_opts()->max_inner_tries >= trie_level + 1 && uniq_tails_rev.size() > 255) {
          tail_trie_size = build_tail_trie(tot_freq_count);
        } else {
          //get_opts()->inner_tries = false;
          tail_maps.build_tail_val_maps(true, memtrie.all_node_sets, uniq_tails_rev, uniq_tails, tot_freq_count, memtrie.max_tail_len, 0);
        }
      }
      uintxx_t flag_counts[8];
      uintxx_t char_counts[8];
      memset(flag_counts, '\0', sizeof(uintxx_t) * 8);
      memset(char_counts, '\0', sizeof(uintxx_t) * 8);
      uintxx_t sfx_full_count = 0;
      uintxx_t sfx_partial_count = 0;
      uint64_t bm_leaf = 0;
      uint64_t bm_term = 0;
      uint64_t bm_child = 0;
      uint64_t bm_ptr = 0;
      uint64_t bm_mask = bm_init_mask;
      byte_vec byte_vec64;
      //trie.reserve(node_count + (node_count >> 1));
      uintxx_t ptr_count = 0;
      uintxx_t node_count = 0;
      uintxx_t louds_pos = 0;
      louds.set(louds_pos++, true);
      louds.set(louds_pos++, false);
      memtrie::node_set_handler nsh_children(memtrie.all_node_sets, 1);
      memtrie::node_iterator ni(memtrie.all_node_sets, 0);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        uint8_t node_byte, cur_node_flags;
        if (cur_node.get_flags() & NODE_SET_LEAP) {
          if (get_opts()->sort_nodes_on_freq) {
            size_t no_tail_count = 0;
            memtrie::node n = ni.get_cur_nsh()->first_node();
            for (size_t k = 0; k <= ni.get_cur_nsh()->last_node_idx(); k++) {
              if (n.get_flags() & NFLAG_TAIL)
                break;
              no_tail_count++;
              n.next();
            }
            node_byte = no_tail_count;
            //printf("%lu\n", no_tail_count);
          } else
            node_byte = ni.last_node_idx();
          cur_node_flags = 0;
        } else {
          node_byte = append_tail_ptr(&cur_node);
          cur_node_flags = cur_node.get_flags();
        }
        //dump_ptr(&cur_node, node_count);
        if (node_count && (node_count % 64) == 0) {
          // append_flags(trie_flags, bm_leaf, bm_child, bm_term, bm_ptr);
          if (trie_level == 0) {
            gen::append_uint64(bm_ptr, trie_flags_tail);
            gen::append_uint64(bm_term, trie_flags_term);
            gen::append_uint64(bm_child, trie_flags_child);
            gen::append_uint64(bm_leaf, trie_flags_leaf);
          } else
            gen::append_uint64(bm_ptr, trie_flags_tail);
          trie.insert(trie.end(), byte_vec64.begin(), byte_vec64.end());
          bm_term = 0; bm_child = 0; bm_leaf = 0; bm_ptr = 0;
          bm_mask = 1UL;
          byte_vec64.clear();
        }
        if (cur_node_flags & NFLAG_LEAF)
          bm_leaf |= bm_mask;
        if (cur_node_flags & NFLAG_TERM)
          bm_term |= bm_mask;
        if (cur_node_flags & NFLAG_CHILD)
          bm_child |= bm_mask;
        if (cur_node_flags & NFLAG_TAIL)
          bm_ptr |= bm_mask;
        if (trie_level > 0) {
          if (cur_node_flags & NFLAG_CHILD) {
            nsh_children.set_pos(cur_node.get_child());
            for (size_t ci = 0; ci <= nsh_children.last_node_idx(); ci++)
              louds.set(louds_pos++, true);
          }
          louds.set(louds_pos++, false);
        }
        bm_mask <<= 1;
        byte_vec64.push_back(node_byte);
        node_count++;
        if (cur_node.get_flags() & NODE_SET_LEAP) {
          cur_node = ni.next();
          continue;
        }
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uniq_info_vec *uniq_tails_rev = tail_maps.get_ui_vec();
          uniq_info_base *ti = (*uniq_tails_rev)[cur_node.get_tail()];
          if (ti->flags & MDX_AFFIX_FULL)
            sfx_full_count++;
          if (ti->flags & MDX_AFFIX_PARTIAL)
            sfx_partial_count++;
          if (ti->len > 1)
            char_counts[(ti->len > 8) ? 7 : (ti->len - 2)]++;
          ptr_count++;
        }
        uint8_t flags = (cur_node.get_flags() & NFLAG_LEAF ? 1 : 0) +
          (cur_node.get_child() > 0 ? 2 : 0) + (cur_node.get_flags() & NFLAG_TAIL ? 4 : 0) +
          (cur_node.get_flags() & NFLAG_TERM ? 8 : 0);
        flag_counts[flags & 0x07]++;
        cur_node = ni.next();
      }
      // TODO: write on all cases?
      // append_flags(trie_flags, bm_leaf, bm_child, bm_term, bm_ptr);
      if (trie_level == 0) {
        gen::append_uint64(bm_ptr, trie_flags_tail);
        gen::append_uint64(bm_term, trie_flags_term);
        gen::append_uint64(bm_child, trie_flags_child);
        gen::append_uint64(bm_leaf, trie_flags_leaf);
      } else
        gen::append_uint64(bm_ptr, trie_flags_tail);
      trie.insert(trie.end(), byte_vec64.begin(), byte_vec64.end());
      louds.set(louds_pos++, false);
      for (int i = 0; i < 8; i++) {
        gen::gen_printf("Flag %d: %d\tChar: %d: %d\n", i, flag_counts[i], i + 2, char_counts[i]);
      }
      gen::gen_printf("Tot ptr count: %u, Full sfx count: %u, Partial sfx count: %u\n", ptr_count, sfx_full_count, sfx_partial_count);
      tail_maps.get_grp_ptrs()->build(node_count, ptr_groups::get_tails_info_fn, 
              uniq_tails_rev, true, pk_col_count, get_opts()->dessicate, tail_trie_size == 0 ? 'u' : MSE_TRIE, MST_BIN, tail_trie_size);
      end_loc = trie_data_ptr_size();
      gen::print_time_taken(t, "Time taken for build_trie(): ");
      return end_loc;
    }
    uintxx_t write_trie_tail_ptrs_data() {
      uintxx_t tail_size = tail_maps.get_grp_ptrs()->get_total_size();
      gen::gen_printf("\nTrie: %u, Flags: %u, Tail size: %u\n", trie.size(), trie_flags_tail.size(), tail_size);
      output.write_u64(tail_size);
      output.write_u64(trie_flags_tail.size());
      gen::gen_printf("Tail stats - ");
      tail_maps.get_grp_ptrs()->write_ptrs_data(true);
      if (get_opts()->inner_tries && tail_trie_builder != nullptr) {
        tail_trie_builder->set_fp(output.get_fp());
        tail_trie_builder->set_out_vec(output.get_out_vec());
        tail_trie_builder->write_trie();
      }
      //output.write_bytes(trie_flags.data(), trie_flags.size());
      output.write_bytes(trie.data(), trie.size());
      output.write_align8(trie.size());
      return trie_data_ptr_size();
    }

    size_t trie_data_ptr_size() {
      size_t ret = 16 + gen::size_align8(trie.size()) + tail_maps.get_grp_ptrs()->get_total_size(); // + trie_flags.size();
      //if (get_uniq_val_count() > 0)
      //  ret += val_maps.get_grp_ptrs()->get_total_size();
      return ret;
    }

    memtrie::node_set_vars insert(const uint8_t *key, int key_len, uintxx_t val_pos = UINTXX_MAX) {
      return memtrie.insert(key, key_len, val_pos);
    }

    void set_leaf_seq(uintxx_t ns_id, uintxx_t& seq_idx, std::function<void(uintxx_t, uintxx_t)> set_seq) {
      memtrie::node_set_handler ns(memtrie.all_node_sets, ns_id);
      memtrie::node n = ns.first_node();
      for (size_t i = 0; i <= ns.last_node_idx(); i++) {
        if (n.get_flags() & NFLAG_LEAF)
          set_seq(n.get_col_val(), seq_idx++);
        if (n.get_flags() & NFLAG_CHILD)
          set_leaf_seq(n.get_child(), seq_idx, set_seq);
        n.next();
      }
    }

    void set_level(uintxx_t ns_id, uintxx_t level) {
      if (max_level < level)
        max_level = level;
      memtrie::node_set_handler ns(memtrie.all_node_sets, ns_id);
      memtrie::node n = ns.first_node();
      for (size_t i = 0; i <= ns.last_node_idx(); i++) {
        if (n.get_flags() & NFLAG_CHILD)
          set_level(n.get_child(), level + 1);
        n.next();
      }
    }

    void set_node_id() {
      size_t num_leap = 0;
      size_t num_leap_no_tail = 0;
      uintxx_t node_id = 0;
      memtrie::node_set_handler nsh(memtrie.all_node_sets, 0);
      for (size_t i = 0; i < memtrie.all_node_sets.size(); i++) {
        nsh.set_pos(i);
        memtrie::node_set_header *ns_hdr = nsh.hdr();
        ns_hdr->node_id = node_id;
        if (ns_hdr->last_node_idx > 4 && trie_level == 0 && get_opts()->leap_frog) {
          if (!get_opts()->sort_nodes_on_freq) {
            ns_hdr->flags |= NODE_SET_LEAP;
            node_id++;
            memtrie.node_count++;
          }
          num_leap++;
          size_t no_tail_count = 0;
          memtrie::node n = nsh.first_node();
          for (size_t k = 0; k <= nsh.last_node_idx(); k++) {
            if (n.get_flags() & NFLAG_TAIL)
              break;
            no_tail_count++;
            n.next();
          }
          if (no_tail_count >= 6) {
            num_leap_no_tail++;
            if (get_opts()->sort_nodes_on_freq) {
              ns_hdr->flags |= NODE_SET_LEAP;
              node_id++;
              memtrie.node_count++;
            }
          }
        }
        node_id += ns_hdr->last_node_idx;
        node_id++;
      }
      // printf("Num leap: %lu, no tail: %lu\n", num_leap, num_leap_no_tail);
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
        for (int i = 0; i < get_opts()->fwd_cache_multiplier; i++)
          cache_count <<= 1;
        f_cache = new fwd_cache[cache_count + 1]();
        f_cache_freq = new uintxx_t[cache_count]();
      }
      if (which == CACHE_REV) {
        for (int i = 0; i < get_opts()->rev_cache_multiplier; i++)
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

    uint8_t min_pos[256][256];
    // uintxx_t min_len_count[256];
    // uintxx_t min_len_last_ns_id[256];
    bldr_min_pos_stats make_min_positions() {
      clock_t t = clock();
      bldr_min_pos_stats stats;
      memset(min_pos, 0xFF, 65536);
      // memset(min_len_count, 0, sizeof(uintxx_t) * 256);
      // memset(min_len_last_ns_id, 0, sizeof(uintxx_t) * 256);
      for (size_t i = 1; i < memtrie.all_node_sets.size(); i++) {
        memtrie::node_set_handler cur_ns(memtrie.all_node_sets, i);
        uint8_t len = cur_ns.last_node_idx();
        if (stats.min_len > len)
          stats.min_len = len;
        if (stats.max_len < len)
          stats.max_len = len;
        // if (i < memtrie.node_set_count / 4) {
        //   min_len_count[len]++;
        //   min_len_last_ns_id[len] = i;
        // }
        memtrie::node cur_node = cur_ns.first_node();
        for (size_t k = 0; k <= len; k++) {
          uint8_t b = cur_node.get_byte();
          if (min_pos[len][b] > k)
             min_pos[len][b] = k;
          if (stats.min_b > b)
            stats.min_b = b;
          if (stats.max_b < b)
            stats.max_b = b;
          cur_node.next();
        }
      }
      gen::print_time_taken(t, "Time taken for make_min_positions(): ");
      // for (int i = 0; i < 256; i++) {
      //   if (min_len_count[i] > 0)
      //     printf("%d\t%u\t%u\n", i, min_len_count[i], min_len_last_ns_id[i]);
      // }
      return stats;
    }

    uintxx_t decide_min_stat_to_use(bldr_min_pos_stats& stats) {
      clock_t t = clock();
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        int good_b_count = 0;
        for (int j = stats.min_b; j <= stats.max_b; j++) {
          if (min_pos[i][j] > 0 && min_pos[i][j] != 0xFF)
            good_b_count++;
        }
        if (good_b_count > (i >> 1)) {
          stats.min_len = i;
          break;
        }
      }
      // for (int i = stats.min_len; i <= stats.max_len; i++) {
      //   printf("Len: %d:: ", i);
      //   for (int j = stats.min_b; j <= stats.max_b; j++) {
      //     int min = min_pos[i][j];
      //     if (min != 255)
      //       printf("%c(%d): %d, ", j, j, min);
      //   }
      //   printf("\n\n");
      // }
      gen::print_time_taken(t, "Time taken for decide_min_stat_to_use(): ");
      return 0; //todo
    }

    void write_sec_cache(bldr_min_pos_stats& stats, uintxx_t sec_cache_size) {
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        for (int j = 0; j <= 255; j++) {
          uint8_t min_len = min_pos[i][j];
          if (min_len == 0xFF)
            min_len = 0;
          min_len++;
          output.write_byte(min_len);
        }
      }
    }

    uintxx_t build() {

      tp.opts_loc = MDX_HEADER_SIZE;
      tp.opts_size = sizeof(bldr_options) * opts->opts_count;

      if (pk_col_count == 0)
        memtrie.node_count--;

        if (pk_col_count > 0) {
        if (get_opts()->split_tails_method > 0)
          split_tails();
        sort_node_sets();
        set_node_id();
        set_level(1, 1);
        tp.min_stats = make_min_positions();
        tp.trie_tail_ptrs_data_sz = build_trie();

        if (trie_level > 0) {
          get_opts()->fwd_cache = false;
          get_opts()->rev_cache = true;
        }
        if (get_opts()->fwd_cache) {
          tp.fwd_cache_count = build_cache(CACHE_FWD, tp.fwd_cache_max_node_id);
          tp.fwd_cache_size = tp.fwd_cache_count * 8; // 8 = parent_node_id (3) + child_node_id (3) + node_offset (1) + node_byte (1)
        } else
          tp.fwd_cache_max_node_id = 0;
        if (get_opts()->rev_cache) {
          tp.rev_cache_count = build_cache(CACHE_REV, tp.rev_cache_max_node_id);
          tp.rev_cache_size = tp.rev_cache_count * 12; // 6 = parent_node_id (3) + child_node_id (3)
        } else
          tp.rev_cache_max_node_id = 0;
        tp.sec_cache_count = decide_min_stat_to_use(tp.min_stats);
        tp.sec_cache_size = 0;
        if (get_opts()->leap_frog)
          tp.sec_cache_size = (tp.min_stats.max_len - tp.min_stats.min_len + 1) * 256; // already aligned

        if (trie_level == 0) {
          tp.term_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);
          tp.child_rank_lt_sz = tp.term_rank_lt_sz;
          tp.term_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count + 1, sel_divisor, 3);
          tp.child_select_lt_sz = 8;
          // if (memtrie.node_set_count > 1)
            tp.child_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count, sel_divisor, 3);
          tp.louds_rank_lt_sz = 0;
          tp.louds_sel1_lt_sz = 0;
        } else {
          tp.louds_rank_lt_sz = gen::get_lkup_tbl_size2(louds.get_highest() + 1, nodes_per_bv_block, width_of_bv_block);
          tp.louds_sel1_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count + 1, sel_divisor, 3);
          tp.term_rank_lt_sz = 0;
          tp.child_rank_lt_sz = 0;
          tp.term_select_lt_sz = 0;
          tp.child_select_lt_sz = 0;
        }

        tp.leaf_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);
        tp.leaf_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.key_count + 1, sel_divisor, 3);
        if (tail_maps.get_grp_ptrs()->get_grp_count() <= 2)
          tp.tail_rank_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, width_of_bv_block);

        // if (get_opts()->dessicate) {
        //   tp.term_rank_lt_sz = tp.child_rank_lt_sz = tp.leaf_rank_lt_sz = 0;
        //   tp.term_select_lt_sz = tp.child_select_lt_sz = tp.leaf_select_lt_sz = 0;
        //   tp.louds_rank_lt_sz = tp.louds_sel1_lt_sz = 0;
        // }
        if (!get_opts()->leaf_lt || get_opts()->trie_leaf_count == 0) {
          tp.leaf_select_lt_sz = 0;
          tp.leaf_rank_lt_sz = 0;
        }

        tp.fwd_cache_loc = tp.opts_loc + tp.opts_size;
        tp.rev_cache_loc = tp.fwd_cache_loc + tp.fwd_cache_size;
        tp.sec_cache_loc = tp.rev_cache_loc + gen::size_align8(tp.rev_cache_size);

        if (trie_level == 0) {
          tp.child_select_lkup_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.term_select_lkup_loc = tp.child_select_lkup_loc + gen::size_align8(tp.child_select_lt_sz);
          //uintxx_t total_rank_lt_size = tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.tail_rank_lt_sz;
          tp.term_rank_lt_loc = tp.term_select_lkup_loc + gen::size_align8(tp.term_select_lt_sz);
          tp.child_rank_lt_loc = tp.term_rank_lt_loc + gen::size_align8(tp.term_rank_lt_sz);
          tp.tail_rank_lt_loc = tp.tail_rank_lt_sz == 0 ? 0 : tp.child_rank_lt_loc + gen::size_align8(tp.child_rank_lt_sz);
          tp.trie_flags_term_loc = tp.child_rank_lt_loc + gen::size_align8(tp.child_rank_lt_sz) + gen::size_align8(tp.tail_rank_lt_sz);
          tp.trie_flags_child_loc = tp.trie_flags_term_loc + trie_flags_term.size();
          tp.trie_flags_leaf_loc = tp.trie_flags_child_loc + trie_flags_child.size();
          tp.trie_tail_ptrs_data_loc = tp.trie_flags_leaf_loc + trie_flags_leaf.size();
          tp.louds_rank_lt_loc = tp.term_rank_lt_loc; // dummy
          tp.louds_sel1_lt_loc = tp.term_select_lkup_loc; // dummy
        } else {
          tp.louds_sel1_lt_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.louds_rank_lt_loc = tp.louds_sel1_lt_loc + gen::size_align8(tp.louds_sel1_lt_sz);
          tp.trie_flags_term_loc = tp.louds_rank_lt_loc + gen::size_align8(tp.louds_rank_lt_sz);
          tp.trie_flags_child_loc = tp.trie_flags_term_loc;
          tp.trie_flags_leaf_loc = tp.trie_flags_child_loc + louds.size_bytes();
          tp.tail_rank_lt_loc = tp.trie_flags_leaf_loc + trie_flags_leaf.size();
          tp.trie_tail_ptrs_data_loc = tp.tail_rank_lt_loc + gen::size_align8(tp.tail_rank_lt_sz);
          tp.term_rank_lt_loc = tp.child_rank_lt_loc = gen::size_align8(tp.louds_rank_lt_loc); // All point to louds
          tp.term_select_lkup_loc = tp.child_select_lkup_loc = gen::size_align8(tp.louds_sel1_lt_loc); // All point to louds
        }

        tp.leaf_rank_lt_loc = tp.trie_tail_ptrs_data_loc + tp.trie_tail_ptrs_data_sz;
        tp.trie_flags_tail_loc = tp.leaf_rank_lt_loc + gen::size_align8(tp.leaf_rank_lt_sz);
        tp.leaf_select_lkup_loc = tp.trie_flags_tail_loc + trie_flags_tail.size();

        if (!get_opts()->leap_frog)
          tp.sec_cache_loc = 0;
      } else {
        tp.leaf_select_lkup_loc = tp.opts_loc + tp.opts_size;
        tp.leaf_select_lt_sz = 0;
      }
      tp.total_idx_size = tp.opts_loc + tp.opts_size +
                (trie_level > 0 ? louds.size_bytes() : trie_flags_term.size() +
                trie_flags_child.size() + trie_flags_leaf.size()) +
                trie_flags_tail.size() +
                tp.fwd_cache_size + gen::size_align8(tp.rev_cache_size) + tp.sec_cache_size +
                (trie_level == 0 ? (gen::size_align8(tp.child_select_lt_sz) +
                     gen::size_align8(tp.term_select_lt_sz) +
                     gen::size_align8(tp.term_rank_lt_sz) +
                     gen::size_align8(tp.child_rank_lt_sz)) :
                  (gen::size_align8(tp.louds_sel1_lt_sz) + gen::size_align8(tp.louds_rank_lt_sz))) +
                gen::size_align8(tp.leaf_select_lt_sz) +
                gen::size_align8(tp.leaf_rank_lt_sz) + gen::size_align8(tp.tail_rank_lt_sz);
      if (pk_col_count > 0)
        tp.total_idx_size += trie_data_ptr_size();
      return tp.total_idx_size;
    }

    void write_trie() {
      output.write_bytes((const uint8_t *) "Madras Sorcery Static DB Format 1.0", 36);

      output.write_byte(0xA5); // magic byte
      output.write_byte(0x01); // version 1.0
      output.write_byte(pk_col_count);
      output.write_byte(trie_level);

      output.write_u16(memtrie.max_tail_len);
      output.write_u16(max_level);
      output.write_bytes((const uint8_t *) &tp.min_stats, 4); // todo: check

      int val_count = column_count;
      output.write_u64(val_count);
      output.write_u64(tp.names_loc);
      output.write_u64(tp.col_val_table_loc);
      output.write_u64(memtrie.node_count);
      output.write_u64(tp.opts_size);
      output.write_u64(memtrie.node_set_count);
      output.write_u64(memtrie.key_count);
      output.write_u64(memtrie.max_key_len);
      output.write_u64(max_val_len);
      output.write_u64(tp.fwd_cache_count);
      output.write_u64(tp.rev_cache_count);
      output.write_u64(tp.fwd_cache_max_node_id);
      output.write_u64(tp.rev_cache_max_node_id);
      output.write_u64(tp.fwd_cache_loc);
      output.write_u64(tp.rev_cache_loc);
      output.write_u64(tp.sec_cache_loc);

      if (trie_level == 0) {
        output.write_u64(tp.term_select_lkup_loc);
        output.write_u64(tp.term_rank_lt_loc);
        output.write_u64(tp.child_select_lkup_loc);
        output.write_u64(tp.child_rank_lt_loc);
      } else {
        output.write_u64(tp.louds_sel1_lt_loc);
        output.write_u64(tp.louds_rank_lt_loc);
        output.write_u64(tp.louds_sel1_lt_loc);
        output.write_u64(tp.louds_rank_lt_loc);
      }
      output.write_u64(tp.leaf_select_lkup_loc);
      output.write_u64(tp.leaf_rank_lt_loc);
      output.write_u64(tp.tail_rank_lt_loc);
      output.write_u64(tp.trie_tail_ptrs_data_loc);
      output.write_u64(tp.trie_flags_term_loc);
      output.write_u64(tp.trie_flags_child_loc);
      output.write_u64(tp.trie_flags_leaf_loc);
      output.write_u64(tp.trie_flags_tail_loc);
      output.write_u64(tp.null_val_loc);
      output.write_u64(tp.empty_val_loc);
      output.write_u64(0); // padding

      output.write_bytes((const uint8_t *) opts, tp.opts_size);

      if (pk_col_count > 0) {
        write_fwd_cache();
        write_rev_cache();
        if (tp.sec_cache_size > 0)
          write_sec_cache(tp.min_stats, tp.sec_cache_size);
        // if (!get_opts()->dessicate) {
          if (trie_level > 0) {
            write_louds_select_lt(tp.louds_sel1_lt_sz);
            write_louds_rank_lt(tp.louds_rank_lt_sz);
          } else {
            write_bv_select_lt(BV_LT_TYPE_CHILD, tp.child_select_lt_sz);
            write_bv_select_lt(BV_LT_TYPE_TERM, tp.term_select_lt_sz);
            // write_bv_rank_lt(BV_LT_TYPE_TERM | BV_LT_TYPE_CHILD | (tp.tail_rank_lt_sz == 0 ? 0 : BV_LT_TYPE_TAIL),
            //     tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.tail_rank_lt_sz);
            write_bv_rank_lt(BV_LT_TYPE_TERM, tp.term_rank_lt_sz);
            write_bv_rank_lt(BV_LT_TYPE_CHILD, tp.child_rank_lt_sz);
            if (tp.tail_rank_lt_sz > 0)
              write_bv_rank_lt(BV_LT_TYPE_TAIL, tp.tail_rank_lt_sz);
          }
        // }
        if (trie_level > 0) {
          output.write_bytes((const uint8_t *) louds.raw_data()->data(), louds.raw_data()->size() * sizeof(uint64_t));
          if (tp.tail_rank_lt_sz > 0)
            write_bv_rank_lt(BV_LT_TYPE_TAIL, tp.tail_rank_lt_sz);
        } else {
          output.write_bytes(trie_flags_term.data(), trie_flags_term.size());
          output.write_bytes(trie_flags_child.data(), trie_flags_child.size());
          output.write_bytes(trie_flags_leaf.data(), trie_flags_leaf.size());
        }

        write_trie_tail_ptrs_data();

        // if (!get_opts()->dessicate) {
          if (trie_level == 0 && tp.leaf_rank_lt_sz > 0)
            write_bv_rank_lt(BV_LT_TYPE_LEAF, tp.leaf_rank_lt_sz);
          output.write_bytes(trie_flags_tail.data(), trie_flags_tail.size());
          if (get_opts()->leaf_lt && get_opts()->trie_leaf_count > 0)
            write_bv_select_lt(BV_LT_TYPE_LEAF, tp.leaf_select_lt_sz);
        // }
      }
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
        // if (nodes_per_bv_block == 512) {
        //   for (size_t i = 1; i < pos_n; i++)
        //     count += bit_counts_n[i];
        // }
        count += count_n;
        count_n = 0;
        memset(bit_counts_n, 0xFF, u8_arr_count * sizeof(uint16_t));
        bit_counts_n[0] = 0x1E;
        pos_n = 1;
      } else if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
        bit_counts_n[pos_n] = count_n & 0xFF;
        uint8_t b0_mask = (0x100 >> pos_n);
        bit_counts_n[0] &= ~b0_mask;
        if (count_n > 255)
          bit_counts_n[0] |= ((count_n & 0x100) >> pos_n);
        // if (nodes_per_bv_block == 512)
        //   count_n = 0;
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
      uint16_t bit_counts_tail_n[u8_arr_count];
      uint16_t bit_counts_term_n[u8_arr_count];
      uint16_t bit_counts_child_n[u8_arr_count];
      uint16_t bit_counts_leaf_n[u8_arr_count];
      uint8_t pos_tail_n = 1;
      uint8_t pos_term_n = 1;
      uint8_t pos_child_n = 1;
      uint8_t pos_leaf_n = 1;
      memset(bit_counts_tail_n,  0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_term_n,  0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_child_n, 0xFF, u8_arr_count * sizeof(uint16_t));
      memset(bit_counts_leaf_n,  0xFF, u8_arr_count * sizeof(uint16_t));
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
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n, pos_term_n);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n, pos_child_n);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n, pos_leaf_n);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n, pos_tail_n);
        count_tail_n += (cur_node_flags & NFLAG_TAIL ? 1 : 0);
        count_term_n += (cur_node_flags & NFLAG_TERM ? 1 : 0);
        count_child_n += (cur_node_flags & NFLAG_CHILD ? 1 : 0);
        count_leaf_n += (cur_node_flags & NFLAG_LEAF ? 1 : 0);
        node_id++;
        cur_node = ni.next();
      }
      node_id = nodes_per_bv_block; // just to make it write the last blocks
      for (size_t i = 0; i < 2; i++) {
        write_bv_n(node_id, which & BV_LT_TYPE_TERM, count_term, count_term_n, bit_counts_term_n, pos_term_n);
        write_bv_n(node_id, which & BV_LT_TYPE_CHILD, count_child, count_child_n, bit_counts_child_n, pos_child_n);
        write_bv_n(node_id, which & BV_LT_TYPE_LEAF, count_leaf, count_leaf_n, bit_counts_leaf_n, pos_leaf_n);
        write_bv_n(node_id, which & BV_LT_TYPE_TAIL, count_tail, count_tail_n, bit_counts_tail_n, pos_tail_n);
      }
      output.write_align8(rank_lt_sz);
    }

    void write_louds_rank_lt(size_t rank_lt_sz) {
      uintxx_t count = 0;
      uintxx_t count_n = 0;
      int u8_arr_count = (nodes_per_bv_block / nodes_per_bv_block_n);
      uint16_t bit_counts_n[u8_arr_count];
      uint8_t pos_n = 1;
      memset(bit_counts_n, 0xFF, u8_arr_count * sizeof(uint16_t));
      bit_counts_n[0] = 0x1E;
      size_t bit_count = louds.get_highest() + 1;
      for (size_t i = 0; i < bit_count; i++) {
        write_bv_n(i, true, count, count_n, bit_counts_n, pos_n);
        count_n += (louds[i] ? 1 : 0);
      }
      bit_count = nodes_per_bv_block; // just to make it write last blocks
      write_bv_n(bit_count, true, count, count_n, bit_counts_n, pos_n);
      write_bv_n(bit_count, true, count, count_n, bit_counts_n, pos_n);
      output.write_align8(rank_lt_sz);
    }

    void write_fwd_cache() {
      output.write_bytes((const uint8_t *) f_cache, tp.fwd_cache_count * sizeof(fwd_cache));
    }

    void write_rev_cache() {
      output.write_bytes((const uint8_t *) r_cache, tp.rev_cache_count * sizeof(nid_cache));
      output.write_align8(tp.rev_cache_size);
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

    void write_louds_select_lt(size_t sel_lt_sz) {
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

    uniq_info_base *get_ti(memtrie::node *n) {
      tail_val_maps *tm = &tail_maps;
      uniq_info_vec *uniq_tails_rev = tm->get_ui_vec();
      return (*uniq_tails_rev)[n->get_tail()];
    }

    uintxx_t get_tail_ptr(memtrie::node *cur_node) {
      uniq_info_base *ti = get_ti(cur_node);
      return ti->ptr;
    }

};

}}

#endif
