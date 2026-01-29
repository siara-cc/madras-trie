#ifndef __DV1_BUILDER_STATIC_TRIE__
#define __DV1_BUILDER_STATIC_TRIE__

#include "madras/dv1/common.hpp"

#include "madras/dv1/builder/output_writer.hpp"
#include "madras/dv1/builder/builder_interfaces.hpp"

#include "pointer_groups.hpp"
#include "leapfrog_builder.hpp"
#include "trie_cache_builder.hpp"
#include "rank_select_builder.hpp"

namespace madras { namespace dv1 {

struct tail_token {
  uintxx_t token_pos;
  uintxx_t token_len;
  uintxx_t fwd_pos;
  uintxx_t cmp_max;
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
  uintxx_t trie_flags_loc;
  uintxx_t louds_rank_lt_sz;
  uintxx_t louds_sel1_lt_sz;
  uintxx_t tail_flags_loc;
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
    trie_cache_builder cache_builder;
    rank_select_builder rs_builder;
    leap_frog_builder leap_frog;
    byte_vec trie;
    gen::bit_vector<uint64_t> louds;
    byte_vec trie_flags;
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
          trie_builder_fwd(_opts, _trie_level, _pk_col_count, _null_value, _null_value_len, _empty_value, _empty_value_len),
          leap_frog(memtrie, output), cache_builder(memtrie, output, tail_maps),
          rs_builder(memtrie, output),
          tail_maps (this, uniq_tails, uniq_tails_rev, memtrie.all_node_sets, output) {
      memcpy(null_value, _null_value, _null_value_len);
      null_value_len = _null_value_len;
      memcpy(empty_value, _empty_value, _empty_value_len);
      empty_value_len = _empty_value_len;
      memtrie.set_print_enabled(gen::is_gen_print_enabled);
      max_level = 0;
      tail_trie_builder = NULL;
      tail_maps.init();
      is_ns_sorted = false;
    }

    virtual ~static_trie_builder() {
      if (tail_trie_builder != NULL)
        delete tail_trie_builder;
    }

    size_t size() {
      return memtrie.key_count;
    }

    bldr_options *get_opts() {
      return opts;
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
            gen::append_uint64(bm_ptr, trie_flags);
            gen::append_uint64(bm_term, trie_flags);
            gen::append_uint64(bm_child, trie_flags);
            gen::append_uint64(bm_leaf, trie_flags);
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
        gen::append_uint64(bm_ptr, trie_flags);
        gen::append_uint64(bm_term, trie_flags);
        gen::append_uint64(bm_child, trie_flags);
        gen::append_uint64(bm_leaf, trie_flags);
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
      gen::gen_printf("\nTrie: %u, Flags: %u, Tail size: %u\n", trie.size(), trie_flags.size(), tail_size);
      output.write_u64(tail_size);
      output.write_u64(trie_flags.size());
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

    uintxx_t build() {

      tp.opts_loc = MDX_HEADER_SIZE;
      tp.opts_size = sizeof(bldr_options) * opts->opts_count;

      if (pk_col_count == 0)
        memtrie.node_count--;

        if (pk_col_count > 0) {
        if (get_opts()->split_tails_method > 0)
          memtrie.split_tails();
        sort_node_sets();
        set_node_id();
        set_level(1, 1);
        tp.min_stats = leap_frog.make_min_positions();
        tp.trie_tail_ptrs_data_sz = build_trie();

        if (trie_level > 0) {
          get_opts()->fwd_cache = false;
          get_opts()->rev_cache = true;
        }
        cache_builder.init(get_opts()->fwd_cache_multiplier, get_opts()->rev_cache_multiplier, trie_level);
        if (get_opts()->fwd_cache) {
          tp.fwd_cache_count = cache_builder.build_cache(CACHE_FWD, tp.fwd_cache_max_node_id);
          tp.fwd_cache_size = tp.fwd_cache_count * 8; // 8 = parent_node_id (3) + child_node_id (3) + node_offset (1) + node_byte (1)
        } else
          tp.fwd_cache_max_node_id = 0;
        if (get_opts()->rev_cache) {
          tp.rev_cache_count = cache_builder.build_cache(CACHE_REV, tp.rev_cache_max_node_id);
          tp.rev_cache_size = tp.rev_cache_count * 12; // 6 = parent_node_id (3) + child_node_id (3)
        } else
          tp.rev_cache_max_node_id = 0;
        tp.sec_cache_count = leap_frog.decide_min_stat_to_use(tp.min_stats);
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

        tp.rev_cache_loc = tp.opts_loc + tp.opts_size;
        tp.fwd_cache_loc = tp.rev_cache_loc + gen::size_align8(tp.rev_cache_size);
        tp.sec_cache_loc = tp.fwd_cache_loc + gen::size_align8(tp.fwd_cache_size);

        if (trie_level == 0) {
          tp.child_select_lkup_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.term_select_lkup_loc = tp.child_select_lkup_loc + gen::size_align8(tp.child_select_lt_sz);
          //uintxx_t total_rank_lt_size = tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.tail_rank_lt_sz;
          tp.term_rank_lt_loc = tp.term_select_lkup_loc + gen::size_align8(tp.term_select_lt_sz);
          tp.child_rank_lt_loc = tp.term_rank_lt_loc + gen::size_align8(tp.term_rank_lt_sz);
          tp.tail_rank_lt_loc = tp.tail_rank_lt_sz == 0 ? 0 : tp.child_rank_lt_loc + gen::size_align8(tp.child_rank_lt_sz);
          tp.trie_flags_loc = tp.child_rank_lt_loc + gen::size_align8(tp.child_rank_lt_sz) + gen::size_align8(tp.tail_rank_lt_sz);
          tp.louds_rank_lt_loc = tp.term_rank_lt_loc; // dummy
          tp.louds_sel1_lt_loc = tp.term_select_lkup_loc; // dummy
          tp.trie_tail_ptrs_data_loc = tp.trie_flags_loc + trie_flags.size();
        } else {
          tp.louds_sel1_lt_loc = tp.sec_cache_loc + tp.sec_cache_size;
          tp.louds_rank_lt_loc = tp.louds_sel1_lt_loc + gen::size_align8(tp.louds_sel1_lt_sz);
          tp.trie_flags_loc = tp.louds_rank_lt_loc + gen::size_align8(tp.louds_rank_lt_sz);
          tp.tail_rank_lt_loc = tp.trie_flags_loc + louds.size_bytes();
          tp.term_rank_lt_loc = tp.child_rank_lt_loc = gen::size_align8(tp.louds_rank_lt_loc); // All point to louds
          tp.term_select_lkup_loc = tp.child_select_lkup_loc = gen::size_align8(tp.louds_sel1_lt_loc); // All point to louds
          tp.trie_tail_ptrs_data_loc = tp.tail_rank_lt_loc + gen::size_align8(tp.tail_rank_lt_sz);
        }

        tp.leaf_rank_lt_loc = tp.trie_tail_ptrs_data_loc + tp.trie_tail_ptrs_data_sz;
        tp.tail_flags_loc = tp.leaf_rank_lt_loc + gen::size_align8(tp.leaf_rank_lt_sz);
        tp.leaf_select_lkup_loc = tp.tail_flags_loc + trie_flags_tail.size();

        if (!get_opts()->leap_frog)
          tp.sec_cache_loc = 0;
      } else {
        tp.leaf_select_lkup_loc = tp.opts_loc + tp.opts_size;
        tp.leaf_select_lt_sz = 0;
      }
      tp.total_idx_size = tp.opts_loc + tp.opts_size +
                (trie_level > 0 ? louds.size_bytes() : trie_flags.size()) +
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
      output.write_u64(tp.trie_flags_loc);
      output.write_u64(tp.tail_flags_loc);
      output.write_u64(tp.null_val_loc);
      output.write_u64(tp.empty_val_loc);
      output.write_u64(0); // padding

      output.write_bytes((const uint8_t *) opts, tp.opts_size);

      if (pk_col_count > 0) {
        cache_builder.write_rev_cache(tp.rev_cache_count, tp.rev_cache_size);
        cache_builder.write_fwd_cache(tp.fwd_cache_count);
        if (tp.sec_cache_size > 0)
          leap_frog.write_sec_cache(tp.min_stats, tp.sec_cache_size);
        // if (!get_opts()->dessicate) {
          if (trie_level > 0) {
            rs_builder.write_louds_select_lt(tp.louds_sel1_lt_sz, louds);
            rs_builder.write_louds_rank_lt(tp.louds_rank_lt_sz, louds);
          } else {
            rs_builder.write_bv_select_lt(BV_LT_TYPE_CHILD, tp.child_select_lt_sz);
            rs_builder.write_bv_select_lt(BV_LT_TYPE_TERM, tp.term_select_lt_sz);
            // rs_builder.write_bv_rank_lt(BV_LT_TYPE_TERM | BV_LT_TYPE_CHILD | (tp.tail_rank_lt_sz == 0 ? 0 : BV_LT_TYPE_TAIL),
            //     tp.term_rank_lt_sz + tp.child_rank_lt_sz + tp.tail_rank_lt_sz);
            rs_builder.write_bv_rank_lt(BV_LT_TYPE_TERM, tp.term_rank_lt_sz);
            rs_builder.write_bv_rank_lt(BV_LT_TYPE_CHILD, tp.child_rank_lt_sz);
            if (tp.tail_rank_lt_sz > 0)
              rs_builder.write_bv_rank_lt(BV_LT_TYPE_TAIL, tp.tail_rank_lt_sz);
          }
        // }
        if (trie_level > 0) {
          output.write_bytes((const uint8_t *) louds.raw_data()->data(), louds.raw_data()->size() * sizeof(uint64_t));
          if (tp.tail_rank_lt_sz > 0)
            rs_builder.write_bv_rank_lt(BV_LT_TYPE_TAIL, tp.tail_rank_lt_sz);
        } else
          output.write_bytes(trie_flags.data(), trie_flags.size());

        write_trie_tail_ptrs_data();

        // if (!get_opts()->dessicate) {
          if (trie_level == 0 && tp.leaf_rank_lt_sz > 0)
            rs_builder.write_bv_rank_lt(BV_LT_TYPE_LEAF, tp.leaf_rank_lt_sz);
          output.write_bytes(trie_flags_tail.data(), trie_flags_tail.size());
          if (get_opts()->leaf_lt && get_opts()->trie_leaf_count > 0)
            rs_builder.write_bv_select_lt(BV_LT_TYPE_LEAF, tp.leaf_select_lt_sz);
        // }
      }
    }

};

}}

#endif
