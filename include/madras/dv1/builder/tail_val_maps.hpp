#ifndef __DV1_BUILDER_TAIL_VAL_MAPS__
#define __DV1_BUILDER_TAIL_VAL_MAPS__

#include "madras/dv1/common.hpp"

#include "output_writer.hpp"
#include "tail_val_maps.hpp"
#include "pointer_groups.hpp"
#include "builder_interfaces.hpp"

namespace madras { namespace dv1 {

#define MDX_AFFIX_FULL 0x01
#define MDX_AFFIX_PARTIAL 0x02
#define MDX_AFFIXES 0x03
#define MDX_HAS_AFFIX 0x04
#define MDX_HAS_CHILD 0x10

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

class tail_val_maps {
  private:
    trie_builder_fwd *bldr;
    gen::byte_blocks& uniq_data;
    uniq_info_vec& ui_vec;
    //uniq_info_vec uniq_tails_fwd;
    ptr_groups ptr_grps;
    int start_nid, end_nid;
  public:
    tail_val_maps(trie_builder_fwd *_bldr, gen::byte_blocks& _uniq_data, uniq_info_vec& _ui_vec, output_writer &_output)
        : bldr (_bldr), uniq_data (_uniq_data), ui_vec (_ui_vec), ptr_grps (_output) {
    }
    ~tail_val_maps() {
      for (size_t i = 0; i < ui_vec.size(); i++)
        delete ui_vec[i];
    }

    void init() {
      ptr_grps.init(bldr->get_opts()->max_groups, bldr->get_opts()->step_bits_idx, bldr->get_opts()->step_bits_rest);
    }

    uint8_t *get_tail(gen::byte_blocks& all_tails, memtrie::node n, uintxx_t& len) {
      uint8_t *v = all_tails[n.get_tail()];
      size_t vlen;
      len = gen::read_vint32(v, &vlen);
      v += vlen;
      return v;
    }

    const double idx_cost_frac_cutoff = 0.1;
    uintxx_t make_uniq_freq(uniq_info_vec& uniq_arr_vec, uniq_info_vec& uniq_freq_vec, uintxx_t tot_freq_count, uintxx_t& last_data_len, uint8_t& start_bits, uint8_t& grp_no, bool no_idx_map = false) {
      clock_t t = clock();
      uniq_freq_vec = uniq_arr_vec;
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.end(), [](const struct uniq_info_base *lhs, const struct uniq_info_base *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });

      uintxx_t sum_freq = 0;
      if (start_bits < 7) {
        for (size_t i = 0; i < uniq_freq_vec.size(); i++) {
          uniq_info_base *vi = uniq_freq_vec[i];
          if (i >= pow(2, start_bits)) {
            double bit_width = log2(tot_freq_count / sum_freq);
            if (bit_width < 1.1) {
              gen::gen_printf("i: %d, freq: %u, Bit width: %.9f, start_bits: %d\n", i, sum_freq, bit_width, (int) start_bits);
              break;
            }
            start_bits++;
          }
          if (start_bits > 7) {
            start_bits = 7;
            break;
          }
          sum_freq += vi->freq_count;
        }
      }

      uintxx_t freq_idx;
      uintxx_t cumu_freq_idx = 0;
      grp_no = 0;
      if (!no_idx_map) {
        grp_no = 1;
        sum_freq = 0;
        freq_idx = 0;
        last_data_len = 2;
        uintxx_t cutoff_bits = start_bits;
        uintxx_t nxt_idx_limit = pow(2, cutoff_bits);
        for (size_t i = 0; i < uniq_freq_vec.size(); i++) {
          uniq_info_base *vi = uniq_freq_vec[i];
          last_data_len += vi->len;
          last_data_len++;
          sum_freq += vi->freq_count;
          if (last_data_len >= nxt_idx_limit) {
            double cost_frac = last_data_len + nxt_idx_limit * 3;
            double divisor = sum_freq * cutoff_bits;
            divisor /= 8;
            cost_frac /= (divisor == 0 ? 1 : divisor);
            if (cost_frac > idx_cost_frac_cutoff)
              break;
            grp_no++;
            sum_freq = 0;
            freq_idx = 0;
            last_data_len = 0;
            cutoff_bits += bldr->get_opts()->step_bits_idx;
            nxt_idx_limit = pow(2, cutoff_bits);
          }
          freq_idx++;
        }

        if (grp_no >= bldr->get_opts()->max_groups) {
          cutoff_bits = start_bits;
        }

        grp_no = 0;
        last_data_len = 0;
        if (cutoff_bits > start_bits) {
          grp_no = 1;
          freq_idx = 0;
          uintxx_t next_bits = start_bits;
          nxt_idx_limit = pow(2, next_bits);
          for (cumu_freq_idx = 0; cumu_freq_idx < uniq_freq_vec.size(); cumu_freq_idx++) {
            uniq_info_base *vi = uniq_freq_vec[cumu_freq_idx];
            if (freq_idx == nxt_idx_limit) {
              next_bits += bldr->get_opts()->step_bits_idx;
              if (next_bits >= cutoff_bits) {
                break;
              }
              nxt_idx_limit = pow(2, next_bits);
              grp_no++;
              freq_idx = 0;
              last_data_len = 0;
            }
            vi->grp_no = grp_no;
            vi->ptr = freq_idx;
            last_data_len += vi->len;
            last_data_len++;
            freq_idx++;
          }
        }
        if (grp_no == 0 && start_bits == 1) {
          start_bits = gen::bits_needed(2 + uniq_freq_vec[0]->len + 1);
          if (start_bits == 0)
            start_bits++;
        }
      }

      // grp_no = 0;
      // uintxx_t cumu_freq_idx = 0;
      //printf("%.1f\t%d\t%u\t%u\n", ceil(log2(freq_idx)), freq_idx, ftot, tail_len_tot);
      if (ptr_grps.inner_trie_start_grp == 0) {

          std::sort(
              uniq_freq_vec.begin(),
              uniq_freq_vec.begin() + cumu_freq_idx,
              [](const uniq_info_base* lhs, const uniq_info_base* rhs) {
                  if (lhs->grp_no != rhs->grp_no)
                      return lhs->grp_no < rhs->grp_no;
                  return lhs->arr_idx > rhs->arr_idx;
              }
          );

          std::sort(
              uniq_freq_vec.begin() + cumu_freq_idx,
              uniq_freq_vec.end(),
              [](const uniq_info_base* lhs, const uniq_info_base* rhs) {
                  const uint64_t lhs_len = lhs->len ? lhs->len : 1;
                  const uint64_t rhs_len = rhs->len ? rhs->len : 1;

                  const uint64_t lhs_freq = lhs->freq_count / lhs_len;
                  const uint64_t rhs_freq = rhs->freq_count / rhs_len;

                  const uint64_t lhs_bucket = ceil_log10(lhs_freq);
                  const uint64_t rhs_bucket = ceil_log10(rhs_freq);

                  if (lhs_bucket != rhs_bucket)
                      return lhs_bucket > rhs_bucket;

                  return lhs->arr_idx > rhs->arr_idx;
              }
          );

      }
      t = gen::print_time_taken(t, "Time taken for uniq_freq: ");
      return cumu_freq_idx;

    }

    static inline uint64_t ceil_log10(uint64_t v) {
        // v > 0 guaranteed
        uint64_t p = 1;
        uint64_t r = 0;

        while (p < v) {
            p *= 10;
            ++r;
        }
        return r;
    }

    void check_remaining_text(uniq_info_vec& uniq_freq_vec, gen::byte_blocks& uniq_data, bool is_tail) {

      uintxx_t remain_tot = 0;
      uintxx_t remain_cnt = 0;
      uintxx_t cmp_min_tot = 0;
      uintxx_t cmp_min_cnt = 0;
      uintxx_t free_tot = 0;
      uintxx_t free_cnt = 0;
      //fp = fopen("remain.txt", "w+");
      uintxx_t freq_idx = 0;
      clock_t tt = clock();
      while (freq_idx < uniq_freq_vec.size()) {
        uniq_info *ti = (uniq_info *) uniq_freq_vec[freq_idx];
        freq_idx++;
        if (ti->flags & MDX_AFFIX_FULL)
         continue;
        if ((ti->flags & 0x07) == 0) {
          // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uintxx_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
          //printf("%u\t%u\t%u\t%u\t[%.*s]\n", (uintxx_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
          free_tot += ti->len;
          free_cnt++;
        }
        int remain_len = ti->len - ti->cmp_max;
        if (remain_len > 3) {
          // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uintxx_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
          //fprintf(fp, "%.*s\n", remain_len - 1, uniq_tails.data() + ti->tail_pos + 1);
          remain_tot += remain_len;
          remain_tot--;
          remain_cnt++;
        }
        //fprintf(fp, "%u\t%u\t%u\t%u\n", ti->freq_count, ti->len, ti->arr_idx, ti->cmp_max);
        if (ti->cmp_min != UINTXX_MAX && ti->cmp_min > 4) {
          cmp_min_tot += (ti->cmp_min - 1);
          cmp_min_cnt++;
        }
      }
      tt = gen::print_time_taken(tt, "Time taken for remain: ");
      // wm.process_combis();
      // gen::print_time_taken(tt, "Time taken for process combis: ");

      //fclose(fp);
      gen::gen_printf("Free entries: %u, %u\n", free_tot, free_cnt);
      gen::gen_printf("Remaining: %u, %u\n", remain_tot, remain_cnt);
      gen::gen_printf("Cmp_min_tot: %u, %u\n", cmp_min_tot, cmp_min_cnt);

    }

    constexpr static uintxx_t idx_ovrhds[] = {384, 3072, 24576, 196608, 1572864, 10782081};
    #define inner_trie_min_size 131072
    void build_tail_val_maps(bool is_tail, byte_ptr_vec& all_node_sets, uniq_info_vec& uniq_info_arr, gen::byte_blocks& uniq_data, uintxx_t tot_freq_count, uintxx_t _max_len, uint8_t max_repeats) {

      clock_t t = clock();

      uniq_info_vec uniq_info_arr_freq;
      uint8_t grp_no;
      uintxx_t last_data_len;
      uint8_t start_bits = is_tail ? 7 : 1;
      uintxx_t cumu_freq_idx = make_uniq_freq(uniq_info_arr, uniq_info_arr_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.reset();
      ptr_grps.set_idx_info(start_bits, grp_no, 3); //last_data_len > 65535 ? 3 : 2);
      ptr_grps.set_max_len(_max_len);

      uintxx_t freq_idx = cumu_freq_idx;
      while (freq_idx < uniq_info_arr_freq.size()) {
        uniq_info_base *ti = uniq_info_arr_freq[freq_idx];
        last_data_len += ti->len;
        last_data_len++;
        freq_idx++;
      }

      freq_idx = 0;
      bool is_bin = false;
      if (uniq_info_arr[0]->flags & LPDU_BIN) {
        gen::gen_printf("Tail content not text.\n");
        // is_bin = true;
      }
      if (!is_bin) {
        uniq_info *prev_ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
        while (freq_idx < uniq_info_arr_freq.size()) {
          uniq_info *ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
          freq_idx++;
          if (ti->flags & LPDU_NULL || ti->flags & LPDU_EMPTY || ti->flags & LPDU_BIN) {
            if (freq_idx < uniq_info_arr_freq.size())
              prev_ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
            continue;
          }
          // printf("%lu, [%.*s]\n", ti->len, ti->len, uniq_data[ti->pos]);
          int cmp_ret = (is_tail ? 
            gen::compare_rev(uniq_data[prev_ti->pos], prev_ti->len, uniq_data[ti->pos], ti->len)
            : gen::compare(uniq_data[prev_ti->pos], prev_ti->len, uniq_data[ti->pos], ti->len));
          if (cmp_ret == 0)
            continue;
          uintxx_t cmp = abs(cmp_ret);
          cmp--;
          bool partial_affix = bldr->get_opts()->partial_sfx_coding;
          if (cmp < bldr->get_opts()->sfx_min_tail_len)
            partial_affix = false;
          if (!bldr->get_opts()->idx_partial_sfx_coding && freq_idx < cumu_freq_idx)
            partial_affix = false;
          if (cmp == ti->len || partial_affix) {
            ti->flags |= (cmp == ti->len ? MDX_AFFIX_FULL : MDX_AFFIX_PARTIAL);
            ti->cmp = cmp;
            if (ti->cmp_max < cmp)
              ti->cmp_max = cmp;
            if (prev_ti->cmp_min > cmp) {
              prev_ti->cmp_min = cmp;
              if (cmp == ti->len && prev_ti->cmp >= cmp)
                prev_ti->cmp = cmp - 1;
            }
            if (cmp == ti->len) {
              if (prev_ti->cmp_max < cmp)
                prev_ti->cmp_max = cmp;
            }
            prev_ti->flags |= MDX_HAS_AFFIX;
            ti->link_arr_idx = prev_ti->arr_idx;
          }
          if (cmp != ti->len)
            prev_ti = ti;
        }
      }

      freq_idx = 0;
      grp_no = 1;
      uintxx_t cur_limit = pow(2, start_bits);
      ptr_grps.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      ptr_grps.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      uintxx_t savings_full = 0;
      uintxx_t savings_count_full = 0;
      uintxx_t savings_partial = 0;
      uintxx_t savings_count_partial = 0;
      uintxx_t sfx_set_len = 0;
      uintxx_t sfx_set_max = bldr->get_opts()->sfx_set_max_dflt;
      uintxx_t sfx_set_count = 0;
      uintxx_t sfx_set_tot_cnt = 0;
      uintxx_t sfx_set_tot_len = 0;
      while (freq_idx < uniq_info_arr_freq.size()) {
        uniq_info *ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
        last_data_len -= ti->len;
        last_data_len--;
        uintxx_t it_nxt_limit = ptr_grps.check_next_grp(grp_no, cur_limit, ti->len);
        if (bldr->get_opts()->inner_tries && it_nxt_limit != cur_limit && 
              it_nxt_limit >= inner_trie_min_size && last_data_len >= inner_trie_min_size * 2) {
          break;
        }
        freq_idx++;
        if (is_bin) {
          uintxx_t bin_len = ti->len;
          uintxx_t len_len = ptr_grps.get_set_len_len(bin_len);
          bin_len += len_len;
          uintxx_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, bin_len, tot_freq_count);
          ti->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_data[ti->pos], ti->len);
          ti->grp_no = grp_no;
          ptr_grps.update_current_grp(grp_no, bin_len, ti->freq_count);
          cur_limit = new_limit;
          continue;
        } else if (ti->flags & MDX_AFFIX_FULL) {
          savings_full += ti->len;
          savings_full++;
          savings_count_full++;
          uniq_info *link_ti = (uniq_info *) uniq_info_arr[ti->link_arr_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, link_ti->len + 1, tot_freq_count);
            link_ti->grp_no = grp_no;
            ptr_grps.update_current_grp(link_ti->grp_no, link_ti->len + 1 - (is_tail ? 0 : ti->cmp), link_ti->freq_count);
            link_ti->ptr = ptr_grps.append_text(grp_no, uniq_data[link_ti->pos], link_ti->len - (is_tail ? 0 : ti->cmp), true);
            link_ti->flags &= ~MDX_AFFIX_PARTIAL;
          }
          //cur_limit = ptr_grps.next_grp(grp_no, cur_limit, 0);
          ptr_grps.update_current_grp(link_ti->grp_no, 0, ti->freq_count);
          if (is_tail)
            ti->ptr = link_ti->ptr + link_ti->len - ti->len;
          else
            ti->ptr = link_ti->ptr + ti->len - (link_ti->flags & MDX_AFFIX_PARTIAL ? link_ti->cmp : 0);
          ti->grp_no = link_ti->grp_no;
        } else {
          if (ti->flags & MDX_AFFIX_PARTIAL) {
            uintxx_t cmp = ti->cmp;
            uintxx_t remain_len = ti->len - cmp;
            uintxx_t len_len = ptr_grps.get_set_len_len(cmp);
            remain_len += len_len;
            uintxx_t new_limit = ptr_grps.next_grp(grp_no, cur_limit, remain_len, tot_freq_count);
            byte_vec& tail_val_data = ptr_grps.get_data(grp_no);
            if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
              ti->cmp_min = 0;
              if (sfx_set_len == 1 && is_tail)
                sfx_set_len += cmp;
              sfx_set_len += remain_len;
              sfx_set_count++;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              ptr_grps.update_current_grp(grp_no, remain_len, ti->freq_count);
              remain_len -= len_len;
              ti->ptr = ptr_grps.append_text(grp_no, uniq_data[ti->pos + (is_tail ? 0 : cmp)], remain_len);
              ptr_grps.get_set_len_len(cmp, &tail_val_data);
            } else {
              // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
              sfx_set_len = 1;
              sfx_set_tot_len += ti->len;
              sfx_set_count = 1;
              sfx_set_tot_cnt++;
              sfx_set_max = bldr->get_opts()->sfx_set_max_dflt;
              if (ti->len > sfx_set_max)
                sfx_set_max = ti->len * 2;
              ptr_grps.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
              ti->ptr = ptr_grps.append_text(grp_no, uniq_data[ti->pos], ti->len, true);
              ti->flags &= ~MDX_AFFIX_PARTIAL;
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_data[ti->tail_pos]);
            cur_limit = new_limit;
          } else {
            // gen::gen_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
            sfx_set_len = 1;
            sfx_set_tot_len += ti->len;
            sfx_set_count = 1;
            sfx_set_tot_cnt++;
            sfx_set_max = bldr->get_opts()->sfx_set_max_dflt;
            uintxx_t len_len = 1;
            if (ti->flags & LPDU_BIN)
              len_len = ptr_grps.get_set_len_len(ti->len);
            if (ti->len > sfx_set_max)
              sfx_set_max = ti->len * 2;
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, ti->len + len_len, tot_freq_count); // todo: only if not null or empty
            ptr_grps.update_current_grp(grp_no, (ti->flags & LPDU_NULL || ti->flags & LPDU_EMPTY) ? 0 : ti->len + len_len, ti->freq_count);
            if (ti->flags & LPDU_NULL)
              ti->ptr = 0;
            else if (ti->flags & LPDU_EMPTY)
              ti->ptr = 1;
            else if (ti->flags & LPDU_BIN)
              ti->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_data[ti->pos], ti->len);
            else
              ti->ptr = ptr_grps.append_text(grp_no, uniq_data[ti->pos], ti->len, true);
          }
          ti->grp_no = grp_no;
        }
      }
      gen::gen_printf("Savings full: %u, %u\nSavings Partial: %u, %u / Sfx set: %u, %u\n", savings_full, savings_count_full, savings_partial, savings_count_partial, sfx_set_tot_len, sfx_set_tot_cnt);

      if (ptr_grps.rpt_ui.pos == UINTXX_MAX) { // repeats
        ptr_grps.rpt_ui.freq_count = ptr_grps.rpt_ui.repeat_freq;
        max_repeats++;
        ptr_grps.add_freq_grp((freq_grp) {++grp_no, 0, max_repeats, ptr_grps.rpt_ui.len, ptr_grps.rpt_ui.freq_count, max_repeats, 0, 0}, true);
        ptr_grps.set_grp_nos(0, grp_no, 0);
        ptr_grps.append_text(grp_no, (const uint8_t *) "", 1);
        ptr_grps.rpt_ui.grp_no = grp_no;
      }

      if (bldr->get_opts()->inner_tries && freq_idx < uniq_info_arr_freq.size()) {
        trie_builder_fwd *inner_trie = bldr->new_instance();
        cur_limit = ptr_grps.next_grp(grp_no, cur_limit, uniq_info_arr_freq[freq_idx]->len, tot_freq_count, true);
        ptr_grps.inner_trie_start_grp = grp_no;
        uintxx_t trie_entry_idx = 0;
        ptr_grps.inner_tries.push_back(inner_trie);
        while (freq_idx < uniq_info_arr_freq.size()) {
          uniq_info *ti = (uniq_info *) uniq_info_arr_freq[freq_idx];
          uint8_t rev[ti->len];
          uint8_t *ti_data = uniq_data[ti->pos];
          for (uintxx_t j = 0; j < ti->len; j++)
            rev[j] = ti_data[ti->len - j - 1];
          inner_trie->insert(rev, ti->len, freq_idx);
          ptr_grps.update_current_grp(grp_no, 1, ti->freq_count);
          ti->grp_no = grp_no;
          trie_entry_idx++;
          if (trie_entry_idx == cur_limit) {
            inner_trie = bldr->new_instance();
            cur_limit = ptr_grps.next_grp(grp_no, cur_limit, trie_entry_idx, tot_freq_count, true);
            trie_entry_idx = 0;
            ptr_grps.inner_tries.push_back(inner_trie);
          }
          freq_idx++;
        }
      }

      for (size_t it_idx = 0; it_idx < ptr_grps.inner_tries.size(); it_idx++) {
        trie_builder_fwd *inner_trie = ptr_grps.inner_tries[it_idx];
        // todo: What was this for?
        // memtrie::node_iterator ni_freq(inner_trie->get_memtrie()->all_node_sets, 0);
        // memtrie::node cur_node = ni_freq.next();
        // while (cur_node != nullptr) {
        //   if (cur_node.get_flags() & NFLAG_CHILD) {
        //     uintxx_t sum_freq = 0;
        //     memtrie::node_set_handler nsh_children(inner_trie->get_memtrie()->all_node_sets, cur_node.get_child());
        //     for (size_t i = 0; i <= nsh_children.last_node_idx(); i++) {
        //       memtrie::node child_node = nsh_children[i];
        //       if (child_node.get_flags() & NFLAG_LEAF) {
        //         uniq_info_base *ti = uniq_info_arr_freq[cur_node.get_col_val()];
        //         sum_freq += ti->freq_count;
        //       }
        //     }
        //     nsh_children.hdr()->freq = sum_freq;
        //   }
        //   cur_node = ni_freq.next();
        // }
        uintxx_t trie_size = inner_trie->build();
        memtrie::node_iterator ni(inner_trie->get_memtrie()->all_node_sets, 0);
        memtrie::node n = ni.next();
        //int leaf_id = 0;
        uintxx_t node_id = 0;
        while (n != nullptr) {
          uintxx_t col_val_pos = n.get_col_val();
          if (n.get_flags() & NFLAG_LEAF) {
            uniq_info_base *ti = uniq_info_arr_freq[col_val_pos];
            uint8_t *ti_data = uniq_data[ti->pos];
            ti->ptr = node_id; //leaf_id++;
          }
          n = ni.next();
          node_id++;
        }
        freq_grp *fg = ptr_grps.get_freq_grp(ptr_grps.inner_trie_start_grp + it_idx);
        fg->grp_size = trie_size;
        fg->grp_limit = node_id;
        fg->count = node_id;
        //printf("Inner Trie size:\t%u\n", trie_size);
      }

      for (freq_idx = 0; freq_idx < cumu_freq_idx; freq_idx++) {
        uniq_info_base *ti = uniq_info_arr_freq[freq_idx];
        ti->ptr = ptr_grps.append_ptr2_idx_map(ti->grp_no, ti->ptr);
      }

      // check_remaining_text(uniq_info_arr_freq, uniq_data, true);

      // int cmpr_blk_size = 65536;
      // size_t total_size = 0;
      // size_t tot_cmpr_size = 0;
      // uint8_t *cmpr_buf = (uint8_t *) malloc(cmpr_blk_size * 1.2);
      // for (int g = 1; g <= grp_no; g++) {
      //   byte_vec& gd = ptr_grps.get_data(g);
      //   if (gd.size() > cmpr_blk_size) {
      //     int cmpr_blk_count = gd.size() / cmpr_blk_size + 1;
      //     for (int b = 0; b < cmpr_blk_count; b++) {
      //       size_t input_size = (b == cmpr_blk_count - 1 ? gd.size() % cmpr_blk_size : cmpr_blk_size);
      //       size_t cmpr_size = gen::compress_block(CMPR_TYPE_ZSTD, gd.data() + (b * cmpr_blk_size), input_size, cmpr_buf);
      //       total_size += input_size;
      //       tot_cmpr_size += cmpr_size;
      //       printf("Grp_no: %d, grp_size: %lu, blk_count: %d, In size: %lu, cmpr size: %lu\n", g, gd.size(), cmpr_blk_count, input_size, cmpr_size);
      //     }
      //   } else {
      //     total_size += gd.size();
      //     tot_cmpr_size += gd.size();
      //   }
      // }
      // printf("Total Input size: %lu, Total cmpr size: %lu\n", total_size, tot_cmpr_size);

      ptr_grps.build_freq_codes(!is_tail);
      ptr_grps.show_freq_codes();

      gen::print_time_taken(t, "Time taken for build_tail_val_maps(): ");

    }

    void build_val_maps(uintxx_t tot_freq_count, uintxx_t _max_len, char data_type, uint8_t max_repeats) {
      clock_t t = clock();
      uintxx_t last_data_len;
      uint8_t start_bits = 1;
      uint8_t grp_no;
      uniq_info_vec uniq_vals_freq;
      uintxx_t cumu_freq_idx = make_uniq_freq(ui_vec, uniq_vals_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      ptr_grps.reset();
      ptr_grps.set_idx_info(start_bits, grp_no, 3);
      ptr_grps.set_max_len(_max_len);
      uintxx_t freq_idx = 0;
      uintxx_t cur_limit = pow(2, start_bits);
      grp_no = 1;
      // gen::word_matcher wm(uniq_vals);
      ptr_grps.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      ptr_grps.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      while (freq_idx < uniq_vals_freq.size()) {
        uniq_info_base *vi = uniq_vals_freq[freq_idx];
        freq_idx++;
        uint8_t len_of_len = 0;
        if (data_type == MST_TEXT || data_type == MST_BIN)
          len_of_len = ptr_grps.get_set_len_len(vi->len);
        uintxx_t len_plus_len = vi->len + len_of_len;
        cur_limit = ptr_grps.next_grp(grp_no, cur_limit, len_plus_len, tot_freq_count);
        vi->grp_no = grp_no;
        vi->ptr = ptr_grps.append_bin15_to_grp_data(grp_no, uniq_data[vi->pos], vi->len, data_type);
        // if (data_type == MSE_TRIE)
        //   wm.add_all_combis(vi->pos, vi->len, vi->arr_idx);
        ptr_grps.update_current_grp(grp_no, len_plus_len, vi->freq_count);
      }
      // wm.process_combis();
      for (freq_idx = 0; freq_idx < cumu_freq_idx; freq_idx++) {
        uniq_info_base *vi = uniq_vals_freq[freq_idx];
        vi->ptr = ptr_grps.append_ptr2_idx_map(vi->grp_no, vi->ptr);
      }
      if (ptr_grps.rpt_ui.pos == UINTXX_MAX) { // repeats
        ptr_grps.rpt_ui.freq_count = ptr_grps.rpt_ui.repeat_freq;
        max_repeats++;
        ptr_grps.add_freq_grp((freq_grp) {++grp_no, 0, max_repeats, ptr_grps.rpt_ui.len, ptr_grps.rpt_ui.freq_count, max_repeats, 0, 0}, true);
        ptr_grps.set_grp_nos(0, grp_no, 0);
        ptr_grps.append_text(grp_no, (const uint8_t *) "", 1);
        ptr_grps.rpt_ui.grp_no = grp_no;
      }
      ptr_grps.build_freq_codes(true);
      ptr_grps.show_freq_codes();
      t = gen::print_time_taken(t, "Time taken for build_val_maps(): ");
    }

    uintxx_t get_tail_ptr(uintxx_t grp_no, uniq_info *ti) {
      uintxx_t ptr = ti->ptr;
      if (grp_no <= ptr_grps.get_idx_limit()) {
        byte_vec& idx2_ptr_map = *(ptr_grps.get_idx2_ptrs_map());
        uintxx_t pos = ptr_grps.idx_map_arr[grp_no - 1] + ptr * ptr_grps.get_idx_ptr_size();
        ptr = ptr_grps.get_idx_ptr_size() == 2 ? gen::read_uint16(idx2_ptr_map, pos) : gen::read_uint24(idx2_ptr_map, pos);
      }
      return ptr;
    }

    gen::byte_blocks *get_uniq_data() {
      return &uniq_data;
    }
    uniq_info_vec *get_ui_vec() {
      return &ui_vec;
    }
    ptr_groups *get_grp_ptrs() {
      return &ptr_grps;
    }

};

}}

#endif
