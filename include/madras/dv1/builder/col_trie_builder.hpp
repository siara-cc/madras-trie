#ifndef __DV1_BUILDER_COLTRIE__
#define __DV1_BUILDER_COLTRIE__

#include "builder_interfaces.hpp"
#include "madras/dv1/builder/trie/tail_val_maps.hpp"
#include "madras/dv1/builder/trie/memtrie/in_mem_trie.hpp"

namespace madras { namespace dv1 {

typedef struct {
  uintxx_t link;
  uintxx_t node_start;
  uintxx_t node_end;
} rev_trie_node_map;

class ct_builder {
  private:
    trie_map_builder_fwd *map_bldr;
    trie_map_builder_fwd *col_trie_builder = nullptr;
    tail_val_maps *val_maps;
    uintxx_t rpt_freq = 0;
    uintxx_t max_rpt_count = 0;

  public:
    ct_builder() {
    }

    virtual ~ct_builder() {
      if (col_trie_builder != nullptr)
        delete col_trie_builder;
    }

    void init(trie_map_builder_fwd *_map_bldr, tail_val_maps &_val_maps, char encoding_type, output_writer &ow) {
      map_bldr = _map_bldr;
      val_maps = &_val_maps;
      if (col_trie_builder != nullptr)
        delete col_trie_builder;
      col_trie_builder = new_col_trie_builder(encoding_type == MSE_TRIE_2WAY);
      col_trie_builder->set_fp(ow.get_fp());
      col_trie_builder->set_out_vec(ow.get_out_vec());
      rpt_freq = 0;
      max_rpt_count = 0;
    }

    memtrie::in_mem_trie *get_memtrie() {
      return &col_trie_builder->memtrie;
    }

    void set_all_vals(gen::byte_blocks *all_vals) {
      col_trie_builder->all_vals = all_vals;
    }

    uintxx_t build() {
      return col_trie_builder->build_kv(false);
    }

    void write() {
      col_trie_builder->write_kv(false, nullptr);
    }

    trie_map_builder_fwd *new_col_trie_builder(bool two_way) {
      trie_map_builder_fwd *new_ct_builder;
      //get_opts()->split_tails_method = 0;
      bldr_options ctb_opts[2];
      ctb_opts[0] = dflt_opts;
      ctb_opts[1] = dflt_opts;
      ctb_opts[0].max_groups = 1;
      ctb_opts[0].max_inner_tries = 2;
      ctb_opts[0].partial_sfx_coding = false;
      ctb_opts[0].sort_nodes_on_freq = false;
      if (two_way) {
        ctb_opts[0].opts_count = 2;
        ctb_opts[0].leap_frog = true;
        ctb_opts[1].inner_tries = 0;
        ctb_opts[1].max_inner_tries = 0;
        new_ct_builder = map_bldr->new_instance("col_trie,key,rev_nids", 2, "t*", "us", 0, 1, ctb_opts);
      } else
        new_ct_builder = map_bldr->new_instance("col_trie,key", 1, "t", "u", 0, 1, ctb_opts);
      return new_ct_builder;
    }

    memtrie::node_set_vars insert(const uint8_t *key, int key_len, uintxx_t val_pos = UINTXX_MAX) {
      return col_trie_builder->memtrie.insert(key, key_len, val_pos);
    }

    uintxx_t build_col_trie(memtrie::byte_ptr_vec &all_node_sets, uintxx_t node_count, gen::byte_blocks *val_blocks, std::vector<rev_trie_node_map>& revmap_vec,
                    gen::byte_blocks& col_trie_vals, char column_type, char encoding_type) {
      uintxx_t col_trie_size = col_trie_builder->build();
      printf("Col trie size: %" PRIuXX "\n", col_trie_size);
      if (encoding_type == MSE_TRIE_2WAY) {
        col_trie_vals.push_back("\xFF\xFF", 2);
      }
      uintxx_t max_node_id = 0;
      uintxx_t node_id = 0;
      uintxx_t leaf_id = 1;
      size_t node_map_count = 0;
      memtrie::node n;
      memtrie::node_set_vars nsv;
      memtrie::node_set_handler cur_ns(all_node_sets, 1);
      for (uintxx_t i = 1; i < all_node_sets.size(); i++) {
        cur_ns.set_pos(i);
        if (cur_ns.hdr()->flags & NODE_SET_LEAP) {
          node_id++;
        }
        n = cur_ns.first_node();
        for (size_t k = 0; k <= cur_ns.last_node_idx(); k++) {
          if ((n.get_flags() & NFLAG_LEAF) == 0) {
            n.next();
            node_id++;
            continue;
          }
          size_t len_len = 0;
          size_t data_len = 0; 
          uint8_t num_data[16];
          uint8_t *data_pos = (*val_blocks)[n.get_col_val()];
          switch (column_type) {
            case MST_TEXT:
            case MST_BIN:
            case MST_SEC_2WAY: {
              data_len = gen::read_vint32(data_pos, &len_len);
              data_pos += len_len;
            } break;
            case MST_INT:
            case MST_DECV:
            case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
            case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9:
            case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
            case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
            case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS: {
              data_len = *data_pos & 0x07;
              data_len += 2;
              if (encoding_type != MSE_DICT_DELTA) {
                int64_t i64;
                allflic::allflic48::simple_decode(data_pos, 1, &i64);
                i64 = allflic::allflic48::zigzag_decode(i64);
                // printf("%lld\n", i64);
                if (*data_pos == 0xFF && data_len == 9) {
                  data_len = 1;
                  data_pos = num_data;
                  *num_data = 0;
                } else {
                  data_len = gen::get_svint60_len(i64);
                  data_pos = num_data;
                  gen::copy_svint60(i64, num_data, data_len);
                }
                // data_len = gen::read_svint60_len(data_pos);
              }
            } break;
          }
          if (!col_trie_builder->lookup_memtrie_after_build(data_pos, data_len, nsv))
            printf("Col trie value not found: %zu, [%.*s]!!\n", data_len, (int) data_len, data_pos);
          memtrie::node_set_handler ct_nsh(col_trie_builder->get_memtrie()->all_node_sets, nsv.node_set_pos);
          // printf("CT NSH: %u, node idx: %d, size: %lu\n", nsv.node_set_pos, nsv.cur_node_idx, memtrie->all_node_sets.size());
          memtrie::node ct_node = ct_nsh[nsv.cur_node_idx];
          if (encoding_type == MSE_TRIE_2WAY && (ct_node.get_flags() & NFLAG_MARK) == 0) {
            uintxx_t link = ct_node.get_col_val();
            rev_trie_node_map *ct_nm = &revmap_vec[link];
            rev_trie_node_map *prev_ct_nm;
            uintxx_t prev_link = ct_nm->link;
            ct_nm->link = 0;
            while (prev_link != 0) {
              ct_nm = &revmap_vec[prev_link];
              uintxx_t temp_link = ct_nm->link;
              ct_nm->link = link;
              link = prev_link;
              prev_link = temp_link;
            }
            node_map_count++;
            std::vector<uint8_t> rev_nids;
            // printf("\ncol_val: %u, Next entry 1: %u\n", ct_node.get_col_val(), ct_nm->link);
            uintxx_t prev_node_id = 0;
            while (1) {
              uintxx_t node_start = (ct_nm->node_start - prev_node_id) << 1;
              prev_node_id = ct_nm->node_start + 1;
              gen::append_vint32(rev_nids, node_start | (ct_nm->node_end != UINTXX_MAX ? 1 : 0));
              if (ct_nm->node_end != UINTXX_MAX) {
                gen::append_vint32(rev_nids, ct_nm->node_end - prev_node_id);
                prev_node_id = ct_nm->node_end + 1;
              }
              if (ct_nm->link == 0)
                break;
              // printf("Next entry: %u\n", ct_nm->link);
              ct_nm = &revmap_vec[ct_nm->link];
              node_map_count++;
            }
            ct_node.set_col_val(col_trie_vals.push_back_with_vlen(rev_nids.data(), rev_nids.size()));
            ct_node.set_flags(ct_node.get_flags() | NFLAG_MARK);
          }
          uintxx_t col_trie_node_id = ct_nsh.hdr()->node_id + (ct_nsh.hdr()->flags & NODE_SET_LEAP ? 1 : 0) + nsv.cur_node_idx;
          if (max_node_id < col_trie_node_id)
            max_node_id = col_trie_node_id;
          n.set_col_val(col_trie_node_id);
          node_id++;
          n.next();
        }
      }
      printf("Node map count: %zu\n", node_map_count);
      if (encoding_type == MSE_TRIE_2WAY) {
        col_trie_builder->set_all_vals(&col_trie_vals);
      }
      byte_vec *ptr_grps = val_maps->get_grp_ptrs()->get_ptrs();
      int bit_len = gen::bits_needed(max_node_id + 1);
      val_maps->get_grp_ptrs()->set_ptr_lkup_tbl_ptr_width(bit_len);
      gen::gen_printf("Col trie bit_len: %d [log(%u)]\n", bit_len, max_node_id);
      gen::int_bit_vector int_bv(ptr_grps, bit_len, node_count);
      int counter = 0;
      memtrie::node_iterator ni(all_node_sets, 0);
      memtrie::node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_LEAF) {
          int_bv.append(cur_node.get_col_val());
          counter++;
        }
        cur_node = ni.next();
      }
      gen::gen_printf("Col trie ptr count: %d\n", counter);
      return col_trie_size;
    }

    void add_to_rev_map(memtrie::node_set_vars& nsv, std::vector<rev_trie_node_map>& revmap_vec, uintxx_t nid_shifted) {
      memtrie::node_set_handler nsh_rev(col_trie_builder->memtrie.all_node_sets, nsv.node_set_pos);
      memtrie::node n_rev = nsh_rev[nsv.cur_node_idx];
      uintxx_t link = 0;
      bool to_append = true;
      // printf("nsv: %d, %u, %u, %u\n", nsv.find_state, nsv.node_set_pos, nsv.cur_node_idx, nsh_rev.last_node_idx());
      if (n_rev.get_col_val() > 0 && nsv.find_state == LPD_FIND_FOUND) { // || nsv.find_state == LPD_INSERT_LEAF)) {
        rev_trie_node_map *rev_map = &revmap_vec[n_rev.get_col_val()];
        // printf("Found: %u, %u, %u\n", rev_map->node_start, rev_map->node_end, nid_shifted);
        if (rev_map->node_start == nid_shifted)
          to_append = false;
        else if ((rev_map->node_start == (nid_shifted - 1) && rev_map->node_end == UINTXX_MAX) || rev_map->node_end == (nid_shifted - 1)) {
          // printf("Range: %u to %u\n", rev_map->node_start, nid_shifted);
          rev_map->node_end = nid_shifted;
          to_append = false;
        } else {
          link = n_rev.get_col_val();
          n_rev.set_col_val(revmap_vec.size());
        }
      } else
        n_rev.set_col_val(revmap_vec.size());
      if (to_append) {
        rev_trie_node_map rev_map;
        rev_map.link = link;
        rev_map.node_start = nid_shifted;
        rev_map.node_end = UINTXX_MAX;
        revmap_vec.push_back(rev_map);
      }
    }

};

}}

#endif
