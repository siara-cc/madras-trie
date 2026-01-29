#ifndef builder_H
#define builder_H

#include <fcntl.h>
#include <stdlib.h>
#include <stdarg.h>
#include <cstring>
#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <float.h>
#include <math.h>
#include <time.h>
#include <functional> // for std::function

#include "madras/dv1/common.hpp"
#include "trie/memtrie/in_mem_trie.hpp"

#include "madras/dv1/ds_common/bv.hpp"
#include "madras/dv1/ds_common/gen.hpp"
#include "madras/dv1/ds_common/vint.hpp"
#include "madras/dv1/ds_common/huffman.hpp"

#include "output_writer.hpp"
#include "trie/tail_val_maps.hpp"
#include "builder_interfaces.hpp"
#include "fast_vint_builder.hpp"
#include "words_builder.hpp"
#include "col_trie_builder.hpp"
#include "trie/static_trie_builder.hpp"

namespace madras { namespace dv1 {

class col_val_sort_callbacks : public sort_callbacks {
  private:
    byte_ptr_vec& all_node_sets;
    gen::byte_blocks& all_vals;
    gen::byte_blocks& uniq_vals;
  public:
    col_val_sort_callbacks(byte_ptr_vec& _all_node_sets, gen::byte_blocks& _all_vals, gen::byte_blocks& _uniq_vals)
      : all_node_sets (_all_node_sets), all_vals (_all_vals), uniq_vals (_uniq_vals) {
    }
    virtual ~col_val_sort_callbacks() {
    }
    uint8_t *get_data(gen::byte_blocks& vec, uintxx_t pos, uintxx_t& len, char type = '*') {
      if (pos >= vec.size())
        std::cout << "WARNING:: accessing beyond vec size !!!!!!!!!!!!!!!!!!!!!!!!!!!!!: " << pos << ", " << vec.size() << std::endl;
      uint8_t *v = vec[pos];
      switch (type) {
        case MST_TEXT:
        case MST_BIN: {
          size_t vlen;
          len = gen::read_vint32(v, &vlen);
          v += vlen;
        } break;
        case MST_INT:
        case MST_DECV:
        case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
        case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS:
          len = *v & 0x07;
          len += 2;
          break;
      }
      return v;
    }
    uint8_t *get_data_and_len(memtrie::node& n, uintxx_t& len, char type = '*') {
      len = 0;
      if (n.get_flags() & NFLAG_LEAF) {
        uintxx_t col_val = n.get_col_val();
        if (col_val == 1) {
          len = 1;
          return NULL;
        }
        return get_data(all_vals, n.get_col_val(), len, type);
      }
      return NULL;
    }
    void set_uniq_pos(uintxx_t ns_id, uint8_t node_idx, size_t pos) {
      memtrie::node_set_handler ns(all_node_sets, ns_id);
      memtrie::node n = ns[node_idx];
      n.set_col_val(pos);
    }
    int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2, int trie_level) {
      if (v1 == NULL && v2 == NULL)
        return 0;
      if (v1 == NULL)
        return -1;
      if (v2 == NULL)
        return 1;
      return gen::compare(v1, len1, v2, len2);
    }
    void sort_data(node_data_vec& nodes_for_sort, int trie_level) {
      clock_t t = clock();
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [](const struct node_data& lhs, const struct node_data& rhs) -> bool {
        if (rhs.data == NULL)
          return false;
        if (lhs.data == NULL)
          return true;
        return gen::compare(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
      });
      t = gen::print_time_taken(t, "Time taken for sort vals: ");
    }
};

class builder : public static_trie_builder, public trie_map_builder_fwd {

  private:
    //dfox uniq_basix_map;
    //basix uniq_basix_map;
    //art_tree at;

    //builder(builder const&);
    //builder& operator=(builder const&);

    void append_flags(byte_vec& byv, uint64_t bm_leaf, uint64_t bm_child, uint64_t bm_term, uint64_t bm_ptr) {
      gen::append_uint64(bm_child, byv);
      gen::append_uint64(bm_term, byv);
      gen::append_uint64(bm_ptr, byv);
      if (get_opts()->trie_leaf_count > 0)
        gen::append_uint64(bm_leaf, byv);
    }

  public:
    char *out_filename;
    tail_val_maps val_maps;
    uniq_info_vec uniq_vals_fwd;
    gen::byte_blocks uniq_vals;
    int cur_col_idx;
    int cur_seq_idx;
    bool is_processing_cols;
    std::vector<uintxx_t> rec_pos_vec;
    char *names;
    char *column_encodings;
    char *column_types;
    uint16_t *names_positions;
    uint16_t names_len;
    uintxx_t prev_val_size;
    uint64_t *val_table;
    char *sk_col_positions;
    ct_builder col_trie_builder;
    word_split_iface *word_splitter;
    builder(const char *out_file = NULL, const char *_names = "kv_tbl,key,value", const int _column_count = 2,
        const char *_column_types = "tt", const char *_column_encodings = "uu", int _trie_level = 0,
        uint16_t _pk_col_count = 1, const bldr_options *_opts = &dflt_opts,
        word_split_iface *_ws = &dflt_word_splitter, const char *_sk_col_positions = "",
        const uint8_t *_null_value = NULL_VALUE, size_t _null_value_len = NULL_VALUE_LEN,
        const uint8_t *_empty_value = EMPTY_VALUE, size_t _empty_value_len = EMPTY_VALUE_LEN)
        : val_maps (this, uniq_vals, uniq_vals_fwd, memtrie.all_node_sets, output),
          trie_builder_fwd(_opts, _trie_level, _pk_col_count, _null_value, _null_value_len, _empty_value, _empty_value_len),
          static_trie_builder(_pk_col_count, _column_count, 0,
            _trie_level, _opts, _null_value, _null_value_len, _empty_value, _empty_value_len) {
      word_splitter = _ws;
      is_processing_cols = false;
      val_maps.init();
      if (_sk_col_positions == nullptr)
        _sk_col_positions = "";
      sk_col_positions = new char[strlen(_sk_col_positions) + 1];
      strcpy(sk_col_positions, _sk_col_positions);
      trie_level = _trie_level;
      column_count = _column_count;
      val_table = new uint64_t[_column_count];
      column_encodings = new char[_column_count];
      memset(column_encodings, 'u', _column_count);
      *column_encodings = MSE_TRIE; // first letter is for key
      memcpy(column_encodings, _column_encodings, gen::min(strlen(_column_encodings), _column_count));
      column_types = new char[_column_count];
      memset(column_types, '*', _column_count);
      memcpy(column_types, _column_types, gen::min(strlen(_column_types), _column_count));
      set_names(_names, _column_types, column_encodings);
      //art_tree_init(&at);
      out_filename = NULL;
      if (out_file != NULL)
        set_out_file(out_file);
      cur_col_idx = 0;
      max_val_len = 8;
      rec_pos_vec.push_back(0);
      all_vals = new gen::byte_blocks();
      all_vals->push_back("\0", 2);
      cur_seq_idx = 0;
    }

    virtual ~builder() {
      delete [] names;
      delete [] val_table;
      delete [] names_positions;
      delete [] column_encodings;
      delete [] column_types;
      delete [] sk_col_positions;
      if (out_filename != NULL)
        delete [] out_filename;
      if (all_vals != NULL)
        delete all_vals;
      if (opts != nullptr)
        delete [] opts;
      //close_file(); // TODO: Close for nested tries?
    }

    trie_map_builder_fwd *new_instance(const char *_names = "kv_tbl,key,value", const int _column_count = 2,
        const char *_column_types = "tt", const char *_column_encodings = "uu", int _trie_level = 0,
        uint16_t _pk_col_count = 1, const bldr_options *_opts = &dflt_opts) override {
      return new builder(nullptr, _names, _column_count, _column_types, _column_encodings, _trie_level, _pk_col_count, _opts);
    }

    void close_file() {
      output.close();
    }

    void set_names(const char *_names, const char *_column_types, const char *_column_encodings) {
      names_len = strlen(_names) + strlen(sk_col_positions) + column_count * 2 + 4;
      names = new char[names_len];
      memset(names, '*', column_count);
      memcpy(names, _column_types, gen::min(strlen(_column_types), column_count));
      names[column_count] = ',';
      memcpy(names + column_count + 1, _column_encodings, column_count);
      names[column_count * 2 + 1] = ',';
      memcpy(names + column_count * 2 + 2, _names, strlen(_names));
      names[column_count * 2 + 2 + strlen(_names)] = ',';
      memcpy(names + column_count * 2 + 2 + strlen(_names) + 1, sk_col_positions, strlen(sk_col_positions));
      // std::cout << names << std::endl;
      int pos_count = column_count + 2;
      names_positions = new uint16_t[pos_count];
      int idx = 0;
      int name_str_pos = column_count;
      while (name_str_pos < names_len) {
        if (names[name_str_pos] == ',') {
          names[name_str_pos] = '\0';
          if (idx < pos_count)
            names_positions[idx++] = name_str_pos + 1;
        }
        name_str_pos++;
      }
      name_str_pos--;
      names[column_count] = '\0';
      names[column_count * 2 + 1] = '\0';
      names[name_str_pos] = '\0';
    }

    void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    void set_all_vals(gen::byte_blocks *_all_vals, bool to_delete_prev = true) override {
      if (all_vals != nullptr && to_delete_prev)
        delete all_vals;
      all_vals = _all_vals;
    }

    uniq_info_base *get_ti(memtrie::node *n) {
      tail_val_maps *tm = &tail_maps;
      uniq_info_vec *uniq_tails_rev = tm->get_ui_vec();
      return (*uniq_tails_rev)[n->get_tail()];
    }

    bool lookup_memtrie_after_build(const uint8_t *key, size_t key_len, memtrie::node_set_vars& nsv) override {
      if (key == NULL) {
        key = null_value;
        key_len = null_value_len;
      }
      if (key_len == 0) {
        key = empty_value;
        key_len = empty_value_len;
      }
      nsv.level = 0;
      nsv.key_pos = 0;
      nsv.node_set_pos = 1;
      memtrie::node_set_handler nsh(memtrie.all_node_sets, 1);
      uint8_t key_byte = key[nsv.key_pos];
      memtrie::node n;
      n = nsh.first_node();
      nsv.cur_node_idx = 0;
      do {
        uint8_t trie_byte = n.get_byte();
        if (key_byte != trie_byte) {
          nsv.cur_node_idx++;
          trie_byte = n.next();
          if (n == NULL)
            return false;
          continue;
        }
        if (key_byte == trie_byte) {
          int tail_len = 1;
          uint8_t flags = n.get_flags();
          nsv.cmp = 0;
          if (flags & NFLAG_TAIL) {
            size_t vlen;
            uniq_info_base *ti = get_ti(&n);
            uint8_t *tail = uniq_tails[ti->pos];
            tail_len = ti->len;
            nsv.cmp = gen::compare(tail, tail_len, key + nsv.key_pos, key_len - nsv.key_pos);
          }
          if (nsv.cmp == 0 && nsv.key_pos + tail_len == key_len && (flags & NFLAG_LEAF)) {
            return true;
          }
          if (nsv.cmp == 0 || abs(nsv.cmp) - 1 == tail_len) {
            nsv.key_pos += tail_len;
            if (nsv.key_pos >= key_len) {
              return false;
            }
            if ((flags & NFLAG_CHILD) == 0) {
              return false;
            }
            nsv.node_set_pos = n.get_child();
            nsv.level++;
            nsh.set_pos(nsv.node_set_pos);
            key_byte = key[nsv.key_pos];
            n = nsh.first_node();
            nsv.cur_node_idx = 0;
            continue;
          }
          if (abs(nsv.cmp) - 1 == key_len - nsv.key_pos) {
            return false;
          }
          return false;
        }
        return false;
      } while (n != NULL);
      return false;
    }

    size_t process_repeats(bool to_mark, uint8_t& max_repeats) {
      size_t rpt_count = 0;
      size_t tot_rpt_count = 0;
      ptr_groups *ptr_grps = val_maps.get_grp_ptrs();
      ptr_grps->rpt_ui = {};
      if (to_mark) {
        ptr_grps->rpt_ui.pos = UINTXX_MAX;
        ptr_grps->rpt_ui.len = 0;
        ptr_grps->rpt_ui.arr_idx = 0;
        ptr_grps->rpt_ui.freq_count = 0;
        ptr_grps->rpt_ui.repeat_freq = 0;
        ptr_grps->rpt_ui.grp_no = 0;
        ptr_grps->rpt_ui.flags = 0;
      }
      max_repeats = 0;
      uniq_info_base *prev_ui = nullptr;
      uintxx_t node_id = 0;
      memtrie::node_iterator ni(memtrie.all_node_sets, pk_col_count == 0 ? 1 : 0);
      memtrie::node cur_node = ni.next();
      memtrie::node prev_node = cur_node;
      while (cur_node != nullptr) {
        if ((cur_node.get_flags() & NODE_SET_LEAP) || ((cur_node.get_flags() & NFLAG_LEAF) == 0)) {
          if ((node_id % nodes_per_bv_block_n) == 0) {
            if (rpt_count > 0 && prev_ui != nullptr && to_mark) {
              prev_node.set_col_val(UINTXX_MAX - rpt_count);
              prev_ui->freq_count -= rpt_count;
              ptr_grps->rpt_ui.len += rpt_count;
              ptr_grps->rpt_ui.repeat_freq++;
              if (max_repeats < rpt_count)
                max_repeats = rpt_count;
              rpt_count = 0;
            }
            prev_ui = nullptr;
          }
          cur_node = ni.next();
          node_id++;
          continue;
        }
        uniq_info_base *ui = get_vi(&cur_node);
        if ((node_id % nodes_per_bv_block_n) != 0) {
          if (ui == prev_ui) {
            if (to_mark) {
              cur_node.set_col_val(UINTXX_MAX);
              prev_node = cur_node;
            }
            rpt_count++;
            tot_rpt_count++; // todo: sometimes exceeds leaf_count?
          } else {
            if (rpt_count > 0 && to_mark) {
              prev_node.set_col_val(UINTXX_MAX - rpt_count);
              prev_ui->freq_count -= rpt_count;
              ptr_grps->rpt_ui.len += rpt_count;
              ptr_grps->rpt_ui.repeat_freq++;
              if (max_repeats < rpt_count)
                max_repeats = rpt_count;
              rpt_count = 0;
            }
          }
        } else {
          if (rpt_count > 0 && to_mark) {
            prev_node.set_col_val(UINTXX_MAX - rpt_count);
            prev_ui->freq_count -= rpt_count;
            ptr_grps->rpt_ui.len += rpt_count;
            ptr_grps->rpt_ui.repeat_freq++;
            if (max_repeats < rpt_count)
              max_repeats = rpt_count;
            rpt_count = 0;
          }
        }
        prev_ui = ui;
        cur_node = ni.next();
        node_id++;
      }
      if (rpt_count > 0 && prev_ui != nullptr && to_mark) {
        prev_node.set_col_val(UINTXX_MAX - rpt_count);
        prev_ui->freq_count -= rpt_count;
        ptr_grps->rpt_ui.len += rpt_count;
        ptr_grps->rpt_ui.repeat_freq++;
        if (max_repeats < rpt_count)
          max_repeats = rpt_count;
      }
      return tot_rpt_count;
    }

    void set_min_max(double dbl_val, double &min_dbl, double &max_dbl, uintxx_t &null_count) {
      if (gen::is_negative_zero(dbl_val))
        null_count++;
      else {
        if (min_dbl > dbl_val)
          min_dbl = dbl_val;
        if (max_dbl < dbl_val)
          max_dbl = dbl_val;
      }
    }

    void set_min_max(int64_t int_val, int64_t &min_int, int64_t &max_int, uintxx_t &null_count) {
      if (int_val == INT64_MIN)
        null_count++;
      else {
        if (min_int > int_val)
          min_int = int_val;
        if (max_int < int_val)
          max_int = int_val;
      }
    }

    // FILE *col_trie_fp;
    uintxx_t build_col_val() {
      clock_t t = clock();
      char encoding_type = column_encodings[cur_col_idx];
      col_trie_builder.init(this, val_maps, encoding_type, output);
      gen::gen_printf("\nCol: %s, ", names + names_positions[cur_col_idx + 2]);
      char data_type = column_types[cur_col_idx];
      gen::gen_printf("Type: %c, Enc: %c. ", data_type, encoding_type);
      word_tries_builder words_builder(this, val_maps);
      fast_vint fast_v(data_type);
      gen::byte_blocks *new_vals = nullptr;
      // if (encoding_type == MSE_DICT_DELTA || encoding_type == MSE_TRIE_2WAY)
        new_vals = new gen::byte_blocks();
      bool is_rec_pos_src_leaf_id = false;
      if (pk_col_count == 0 || rec_pos_vec[0] == UINTXX_MAX)
        is_rec_pos_src_leaf_id = true;
      rec_pos_vec[0] = UINTXX_MAX;
      // bool delta_next_block = true;
      node_data_vec nodes_for_sort;
      int64_t min_int = INT64_MAX;
      int64_t max_int = 0;
      double min_dbl = DBL_MAX;
      double max_dbl = 0;
      uintxx_t null_count = 0;
      uintxx_t max_len = 0;
      uint8_t num_data[16];
      uint8_t *data_pos = nullptr;
      size_t len_len = 0;
      uintxx_t data_len = 0;
      std::vector<rev_trie_node_map> revmap_vec;
      revmap_vec.push_back(rev_trie_node_map());
      uint8_t *prev_val = nullptr;
      uintxx_t prev_val_len = 0;
      int64_t prev_ival = 0;
      uintxx_t prev_val_node_id = 0;
      uintxx_t pos = 2;
      uintxx_t node_id = 0;
      uintxx_t leaf_id = 1;
      size_t data_size = 0;
      ptr_groups *ptr_grps = val_maps.get_grp_ptrs();
      ptr_grps->set_stats(0, 0, INT64_MAX, 0);
      memset(&ptr_grps->rpt_ui, '\0', sizeof(struct uniq_info_base));
      fast_v.set_block_data(&ptr_grps->get_data(1));
      fast_v.set_ptr_grps(ptr_grps);
      memtrie::node_iterator ni(memtrie.all_node_sets, pk_col_count == 0 ? 1 : 0);
      memtrie::node n = ni.next();
      memtrie::node prev_node = n;
      while (n != nullptr) {
        if (node_id && (node_id % nodes_per_bv_block_n) == 0) {
          if (encoding_type == MSE_VINTGB) {
            size_t blk_size = fast_v.build_block();
            if (blk_size > (data_type == MST_DECV ? 4 : 3))
              prev_node.set_col_val(blk_size);
            fast_v.reset_block();
            data_size += blk_size;
          }
        }
        if ((n.get_flags() & NFLAG_LEAF) == 0) { // || ni.hdr()->flags & NODE_SET_LEAP
          n = ni.next();
          node_id++;
          continue;
        }
        if (memcmp((*all_vals)[0], "\xFF\xFF", 2) == 0) {
          size_t vlen;
          pos = n.get_col_val();
          data_len = gen::read_vint32((*all_vals)[pos], &vlen);
          // printf("Pos:\t%u\tData len:\t%u\n", pos, data_len);
          data_pos = (*all_vals)[pos + vlen];
          set_min_max(0, min_int, max_int, null_count);
        } else {
          size_t vlen;
          if (pk_col_count > 0 && !is_rec_pos_src_leaf_id)
            rec_pos_vec[leaf_id] = n.get_col_val();
          pos = rec_pos_vec[leaf_id];
          gen::read_vint32((*all_vals)[pos], &vlen);
          pos += vlen;
          for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
            data_pos = (*all_vals)[pos];
            len_len = 0;
            data_len = 0;
            uintxx_t col_val = pos;
            n.set_col_val(col_val);
            // printf("Key: %u, %u, [%.*s]\n", col_val, data_len, (int) data_len, data_pos);
              char col_data_type = column_types[col_idx];
            switch (col_data_type) {
              case MST_TEXT:
              case MST_BIN:
              case MST_SEC_2WAY: {
                data_len = gen::read_vint32(data_pos, &len_len);
                data_pos += len_len;
                pos += len_len;
                pos += data_len;
                if (cur_col_idx == col_idx)
                  set_min_max(memcmp(data_pos, null_value, null_value_len) == 0 ? INT64_MIN : data_len, min_int, max_int, null_count);
              } break;
              case MST_DECV: {
                data_len = 8;
                pos += data_len;
                if (cur_col_idx == col_idx)
                  set_min_max(*((double *) data_pos), min_dbl, max_dbl, null_count);
              } break;
              case MST_INT:
              case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
              case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9:
              case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
              case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
              case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS: {
                data_len = *data_pos & 0x07;
                data_len += 2;
                pos += data_len;
                int64_t i64;
                if (*data_pos == 0xFF && data_len == 9) {
                  i64 = INT64_MIN;
                } else {
                  allflic::allflic48::simple_decode(data_pos, 1, &i64);
                  i64 = allflic::allflic48::zigzag_decode(i64);
                }
                if (cur_col_idx == col_idx) {
                  if (data_type >= MST_DEC0 && data_type <= MST_DEC9) {
                    if (i64 == INT64_MIN)
                      set_min_max(-0.0, min_dbl, max_dbl, null_count);
                    else {
                      double dbl = static_cast<double>(i64);
                      dbl /= allflic::allflic48::tens()[data_type - MST_DEC0];
                      set_min_max(dbl, min_dbl, max_dbl, null_count);
                    }
                  } else
                    set_min_max(i64, min_int, max_int, null_count);
                }
                if (encoding_type != MSE_DICT_DELTA && encoding_type != MSE_VINTGB) {
                  if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
                    if (*data_pos == 0xFF && data_len == 9) {
                      data_len = 1;
                      *num_data = 0;
                    } else {
                      data_len = gen::get_svint60_len(i64);
                      gen::copy_svint60(i64, num_data, data_len);
                    }
                    data_pos = num_data;
                  } else {
                    data_pos = (*new_vals)[new_vals->push_back(data_pos, data_len)];
                  }
                }
              } break;
            }
            if (cur_col_idx == col_idx)
              break;
          }
        }
        switch (encoding_type) {
          case MSE_WORDS:
          case MSE_WORDS_2WAY: {
            words_builder.add_words(node_id, data_pos, data_len, prev_val, prev_val_len, word_splitter);
            prev_val = data_pos;
            prev_val_len = data_len;
          } break;
          case MSE_TRIE:
          case MSE_TRIE_2WAY: {
            uintxx_t nid_shifted = node_id >> get_opts()->sec_idx_nid_shift_bits;
            // printf("Data: [%.*s]\n", data_len, data_pos);
            memtrie::node_set_vars nsv = col_trie_builder.insert(data_pos, data_len);
            if (encoding_type == MSE_TRIE_2WAY) {
              col_trie_builder.add_to_rev_map(nsv, revmap_vec, nid_shifted);
            }
            // fprintf(col_trie_fp, "%.*s\n", (int) data_len, data_pos);
          } break;
          case MSE_VINTGB: {
            n.set_col_val(0);
            fast_v.add(node_id, data_pos, data_len);
          } break;
          case MSE_STORE: {
            uintxx_t ptr = ptr_grps->append_bin_to_grp_data(1, data_pos, data_len, data_type);
            byte_vec& data = ptr_grps->get_data(1);
            n.set_col_val(data.size() - ptr);
          } break;
          case MSE_DICT_DELTA: {
            int64_t i64;
            uint8_t frac_width = allflic::allflic48::simple_decode_single(data_pos, &i64);
            int64_t col_val = allflic::allflic48::zigzag_decode(i64);
            int64_t delta_val = col_val;
            if ((node_id / nodes_per_bv_block_n) == (prev_val_node_id / nodes_per_bv_block_n))
              delta_val -= prev_ival;
            prev_ival = col_val;
            prev_val_node_id = node_id;
            // printf("Node id: %u, delta value: %lld\n", node_id, delta_val);
            uint8_t v64[10];
            i64 = allflic::allflic48::zigzag_encode(delta_val);
            uint8_t *v_end = allflic::allflic48::simple_encode_single(i64, v64, 0);
            data_len = (v_end - v64);
            data_pos = (*new_vals)[new_vals->push_back(v64, data_len)];
            n.set_col_val(data_pos - (*new_vals)[0]);
            nodes_for_sort.push_back((struct node_data) {data_pos, data_len, ni.get_cur_nsh_id(), ni.get_cur_sib_id(), (uint8_t) len_len});
            // printf("Key: %u, %u, [%.*s]\n", col_val, data_len, (int) data_len, data_pos);
          } break;
          default:
            nodes_for_sort.push_back((struct node_data) {data_pos, data_len, ni.get_cur_nsh_id(), ni.get_cur_sib_id(), (uint8_t) len_len});
          // printf("RecNo: %lu, Pos: %u, data_len: %u, vlen: %lu\n", rec_no, pos, data_len, vlen);
        }
        if (max_len < data_len)
          max_len = data_len;
        if (data_type >= MST_DECV && data_type <= MST_DEC9) {
          memcpy(&min_int, &min_dbl, 8);
          memcpy(&max_int, &max_dbl, 8);
        }
        leaf_id++;
        node_id++;
        prev_node = n;
        n = ni.next();
      }
      if (encoding_type == MSE_VINTGB) {
        size_t blk_size = fast_v.build_block();
        if (blk_size > (data_type == MST_DECV ? 4 : 3))
          prev_node.set_col_val(blk_size);
        data_size += blk_size;
      }
      ptr_grps->set_stats(fast_v.isAll32bit ? 1 : 0, null_count, min_int, max_int);
      uintxx_t col_trie_size = 0;
      switch (encoding_type) {
        case MSE_WORDS:
        case MSE_WORDS_2WAY: {
          gen::byte_blocks uniq_words;
          uniq_info_vec uniq_words_vec;
          words_builder.build_words(uniq_words_vec, uniq_words,
                  encoding_type, pk_col_count, new_vals, get_opts()->sec_idx_nid_shift_bits);
          ptr_grps->set_max_len(max_len);
          ptr_grps->build(memtrie.node_count, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type);
        } break;
        case MSE_TRIE:
        case MSE_TRIE_2WAY: {
          // fclose(col_trie_fp);
          col_trie_size = col_trie_builder.build_col_trie(memtrie.all_node_sets, memtrie.node_count, all_vals, revmap_vec, *new_vals, data_type, encoding_type);
          ptr_grps->set_max_len(col_trie_builder.get_memtrie()->max_key_len);
          ptr_grps->build(memtrie.node_count, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type, col_trie_size);
        } break;
        case MSE_VINTGB:
        case MSE_STORE: {
          // ptr_grps->set_null(memtrie.node_count);
          ptr_grps->set_max_len(max_len);
          ptr_grps->build(memtrie.node_count, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type);
        } break;
        default: {
          col_val_sort_callbacks val_sort_cb(memtrie.all_node_sets, *all_vals, uniq_vals);
          uintxx_t tot_freq_count = uniq_maker::sort_and_reduce(nodes_for_sort, *all_vals,
                      uniq_vals, uniq_vals_fwd, val_sort_cb, max_len, 0, data_type);

          uint8_t max_repeats;
          size_t rpt_count = process_repeats(false, max_repeats);
          printf("rpt enable perc: %d, actual perc: %zu\n", get_opts()->rpt_enable_perc, (size_t) (rpt_count * 100 / memtrie.node_count));
          if (get_opts()->rpt_enable_perc < (rpt_count * 100 / memtrie.node_count))
            process_repeats(true, max_repeats);
          printf("Max col len: %" PRIuXX ", Rpt count: %lu, max: %d\n", max_len, rpt_count, max_repeats);

          if (data_type == MST_TEXT)
            val_maps.build_tail_val_maps(false, memtrie.all_node_sets, uniq_vals_fwd, uniq_vals, tot_freq_count, max_len, max_repeats);
          else
            val_maps.build_val_maps(tot_freq_count, max_len, data_type, max_repeats);
          
          ptr_grps->set_max_len(max_len);
          ptr_grps->build(memtrie.node_count, ptr_groups::get_vals_info_fn, 
              uniq_vals_fwd, false, pk_col_count, get_opts()->dessicate, encoding_type, data_type);
        }
      }

      ptr_grps->print_stats();

      uintxx_t val_size = ptr_grps->get_total_size();
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        val_size += col_trie_builder.build();
      }

      if (new_vals != nullptr) {
        delete new_vals;
        if (encoding_type == MSE_TRIE_2WAY) {
          col_trie_builder.set_all_vals(nullptr); // todo: standardize
        }
      }
      t = gen::print_time_taken(t, "Time taken for build_col_val: ");

      return val_size;

    }

    void write_col_val() {
      printf("trie level: %d, col idx: %d\n", trie_level, cur_col_idx);
      char data_type = column_types[cur_col_idx];
      char encoding_type = column_encodings[cur_col_idx];
      write_val_ptrs_data(data_type, encoding_type); // TODO: fix flags
      if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY) {
        col_trie_builder.write();
      }
    }

    void reset_for_next_col() {
      cur_seq_idx = 0;
      cur_col_idx++;
      // if (all_vals != NULL)
      //   delete all_vals;
      // all_vals = new gen::byte_blocks();
      // all_vals->push_back("\0", 2);
      uniq_vals.reset();
      val_maps.get_grp_ptrs()->reset();
      for (size_t i = 0; i < uniq_vals_fwd.size(); i++)
        delete uniq_vals_fwd[i];
      uniq_vals_fwd.resize(0);
    }

    uintxx_t write_val_ptrs_data(char data_type, char encoding_type) {
      uintxx_t val_fp_offset = 0;
      if (get_uniq_val_count() > 0 || encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || encoding_type == MSE_WORDS || encoding_type == MSE_WORDS_2WAY || encoding_type == MSE_STORE || encoding_type == MSE_VINTGB) {
        gen::gen_printf("Stats - %c - ", encoding_type);
        val_maps.get_grp_ptrs()->write_ptrs_data(false);
        val_fp_offset += val_maps.get_grp_ptrs()->get_total_size();
      }
      return val_fp_offset;
    }
    size_t get_uniq_val_count() {
      return val_maps.get_ui_vec()->size();
    }

    uintxx_t build() override {

      clock_t t = clock();

      gen::gen_printf("Key count: %u\n", memtrie.key_count);

      tp = {};

      static_trie_builder::build();

      tp.names_loc = tp.leaf_select_lkup_loc + gen::size_align8(tp.leaf_select_lt_sz);
      tp.names_sz = (column_count + 2) * sizeof(uint16_t) + names_len;
      tp.col_val_table_loc = tp.names_loc + gen::size_align8(tp.names_sz);
      int val_count = column_count;
      tp.col_val_table_sz = val_count * sizeof(uint64_t);
      tp.null_val_loc = tp.col_val_table_loc + gen::size_align8(tp.col_val_table_sz);
      tp.empty_val_loc = tp.null_val_loc + 16;
      tp.col_val_loc0 = tp.empty_val_loc + 16;
      tp.null_empty_sz = 32;
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
                gen::size_align8(tp.leaf_rank_lt_sz) + gen::size_align8(tp.tail_rank_lt_sz) +
                gen::size_align8(tp.names_sz) + gen::size_align8(tp.col_val_table_sz) + tp.null_empty_sz;
      if (pk_col_count > 0)
        tp.total_idx_size += trie_data_ptr_size();

      // if (get_opts()->dessicate) {
      //   tp.term_select_lkup_loc = tp.term_rank_lt_loc = tp.child_rank_lt_loc = 0;
      //   tp.leaf_select_lkup_loc = tp.leaf_rank_lt_loc = tp.child_select_lkup_loc = 0;
      // }

      gen::print_time_taken(t, "Time taken for build(): ");

      return tp.total_idx_size;

    }

    void set_out_file(const char *out_file) {
      if (out_file == NULL)
        return;
      int len = strlen(out_file);
      if (out_filename != NULL)
        delete [] out_filename;
      out_filename = new char[len + 1];
      strcpy(out_filename, out_file);
    }

    void open_file() {
      if (out_filename == nullptr)
        return;
      FILE *fp = fopen(out_filename, "wb+");
      fclose(fp);
      fp = fopen(out_filename, "rb+");
      if (fp == NULL)
        throw errno;
      output.set_fp(fp);
    }

    uintxx_t write_trie(const char *filename = NULL) {

      if (tp.names_loc == 0)
        build();

      clock_t t = clock();
      if (filename != NULL) {
        set_out_file(filename);
        open_file();
      }

      size_t actual_trie_size = output.output_to_file() ? output.get_current_pos() : 0;

      output.reserve(tp.total_idx_size);

      static_trie_builder::write_trie();

      val_table[0] = 0;
      write_names();
      write_col_val_table();
      write_null_empty();

      gen::gen_printf("\nNodes#: %u, Node set#: %u\nTrie bv: %u, Leaf bv: %u, Tail bv: %u\n"
        "Select lt - Term: %u, Child: %u, Leaf: %u\n"
        "Fwd cache: %u, Rev cache: %u, Sec cache: %u\nNode struct size: %u, Max tail len: %u\n",
            memtrie.node_count, memtrie.node_set_count, tp.term_rank_lt_sz + tp.child_rank_lt_sz,
            tp.leaf_rank_lt_sz, tp.tail_rank_lt_sz, tp.term_select_lt_sz, tp.child_select_lt_sz, tp.leaf_select_lt_sz,
            tp.fwd_cache_size, tp.rev_cache_size, tp.sec_cache_size, sizeof(memtrie::node), memtrie.max_tail_len);

      // fp = fopen("nodes.txt", "wb+");
      // // dump_nodes(first_node, fp);
      // find_rpt_nodes(fp);
      // fclose(fp);

      gen::print_time_taken(t, "Time taken for write_trie(): ");
      gen::gen_printf("Idx size: %u\n", tp.total_idx_size);

      actual_trie_size = output.output_to_file() ? (output.get_current_pos() - actual_trie_size) : output.get_current_pos();
      if (!output.output_to_file() && trie_level > 0)
        actual_trie_size = tp.total_idx_size;
      if (tp.total_idx_size != actual_trie_size)
        printf("WARNING: Trie size not matching: %lu, ^%lld, lvl: %d -----------------------------------------------------------------------------------------------------------\n", actual_trie_size, (long long) actual_trie_size - tp.total_idx_size, trie_level);

      return tp.total_idx_size;

    }

    uintxx_t build_kv(bool to_build_trie = true) override {
      is_processing_cols = false;
      uintxx_t kv_size = 0;
      if (to_build_trie)
        kv_size = build();
      is_processing_cols = true;
      prev_val_size = 0;
      uint64_t prev_val_loc = tp.col_val_loc0;
      for (cur_col_idx = 0; cur_col_idx < column_count; ) {
        char encoding_type = column_encodings[cur_col_idx];
        char data_type = column_types[cur_col_idx];
        if (pk_col_count > 0 && cur_col_idx < pk_col_count) {
          val_table[cur_col_idx] = 0;
          cur_col_idx++;
          continue;
        }
        uintxx_t val_size = build_col_val();
        val_table[cur_col_idx] = prev_val_loc;
        prev_val_loc += val_size;
        prev_val_size = val_size;
        break;
      }
      if (!to_build_trie)
        prev_val_loc -= tp.col_val_loc0;
      kv_size += prev_val_loc;
      gen::gen_printf("%s size: %u\n", (to_build_trie ? "Total" : "Val"), kv_size);
      return kv_size;
    }

    void write_kv(bool to_close = true, const char *filename = NULL) override {
      is_processing_cols = false;
      write_trie(filename);
      if (column_count > 1) {
        is_processing_cols = true;
        write_col_val();
      }
      if (to_close) // todo: revisit: only close for trie_level 0?
        write_final_val_table(to_close);
    }

    uintxx_t build_and_write_all(bool to_close = true, const char *filename = NULL) {
      write_trie(filename);
      is_processing_cols = true;
      prev_val_size = 0;
      uint64_t prev_val_loc = tp.col_val_loc0;
      for (cur_col_idx = 0; cur_col_idx < column_count; ) {
        char encoding_type = column_encodings[cur_col_idx];
        char data_type = column_types[cur_col_idx];
        if (pk_col_count > 0 && cur_col_idx < pk_col_count) {
          val_table[cur_col_idx] = 0;
          cur_col_idx++;
          continue;
        }
        // if (all_vals->size() > 2 || encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || encoding_type == 'w') { // TODO: What if column contains only NULL and ""
        uintxx_t val_size = build_col_val();
        write_col_val();
        val_table[cur_col_idx] = prev_val_loc;
          prev_val_loc += val_size;
          prev_val_size = val_size;
          reset_for_next_col();  // cur_col_idx++ happens here (?)
        // }
      }
      write_final_val_table(to_close);
      gen::gen_printf("Total size: %u\n", prev_val_loc);
      return prev_val_loc;
    }

    void write_names() {
      int name_count = column_count + 2;
      for (int i = 0; i < name_count; i++)
        output.write_u16(names_positions[i]);
      output.write_bytes((const uint8_t *) names, names_len);
      output.write_align8(tp.names_sz);
    }

    void write_null_empty() {
      output.write_byte(null_value_len);
      output.write_bytes(null_value, 15);
      output.write_byte(empty_value_len);
      output.write_bytes(empty_value, 15);
    }

    void write_col_val_table() {
      for (size_t i = 0; i < column_count; i++)
        output.write_u64(val_table[i]);
      output.write_align8(tp.col_val_table_sz);
    }

    void write_final_val_table(bool to_close = true) {
      if (!output.output_to_file()) {
        for (size_t i = 0; i < column_count; i++)
          gen::copy_uint64(val_table[i], output.get_output_buf_at(tp.col_val_table_loc + i * 8));
      } else {
        output.seek(tp.col_val_table_loc, SEEK_SET);
        write_col_val_table();
        output.seek(0, SEEK_END);
      }
      int val_count = column_count;
      gen::gen_printf("Val count: %d, tbl:", val_count);
      for (int i = 0; i < val_count; i++)
        gen::gen_printf(" %u", val_table[i]);
      gen::gen_printf("\nCol sizes:");
      uintxx_t total_size = val_table[0];
      for (int i = 1; i < val_count; i++) {
        gen::gen_printf(" %u", val_table[i] - val_table[i - 1]);
        total_size += val_table[i];
      }
      gen::gen_printf("\n");
      if (to_close)
        close_file();
    }

    // struct nodes_ptr_grp {
    //   uintxx_t node_id;
    //   uintxx_t ptr;
    // };

    uniq_info_base *get_vi(memtrie::node *n) {
      tail_val_maps *tm = &val_maps;
      uniq_info_vec *uniq_vals_fwd = tm->get_ui_vec();
      return (*uniq_vals_fwd)[n->get_col_val()];
    }

    void set_fp(FILE *_fp) {
      output.set_fp(_fp);
    }

    void set_out_vec(byte_vec *ov) {
      output.set_out_vec(ov);
    }

    byte_vec *get_out_vec() {
      return output.get_out_vec();
    }

    bldr_options *get_opts() override {
      if (opts->opts_count < 2)
        return opts;
      if (is_processing_cols)
        return opts + 1;
      return opts;
    }

    #define APPEND_REC_NOKEY 0
    #define APPEND_REC_KEY_MIDDLE 1
    #define APPEND_REC_KEY_LAST 2
    size_t append_rec_value(char type, char encoding_type, mdx_val_in value, size_t value_len, byte_vec& rec, int val_type) {
      switch (type) {
        case MST_TEXT:
        case MST_BIN: {
          const uint8_t *txt_bin = value.txt_bin;
          if (value.i64 == 0) {
            txt_bin = null_value;
            value_len = null_value_len;
          }
          if (value_len == 0) {
            txt_bin = empty_value;
            value_len = empty_value_len;
          }
          if (val_type == APPEND_REC_NOKEY)
            gen::append_vint32(rec, value_len);
          for (size_t j = 0; j < value_len; j++)
            rec.push_back(txt_bin[j]);
          if (val_type == APPEND_REC_KEY_MIDDLE)
            rec.push_back(0);
        } break;
        case MST_INT:
        case MST_DECV:
        case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
        case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS: {
          if (val_type == APPEND_REC_NOKEY) {
            if (type == MST_DECV) {
              uint8_t *v64 = (uint8_t *) &value.dbl;
              value_len = 8;
              for (size_t vi = 0; vi < value_len; vi++)
                rec.push_back(v64[vi]);
            } else {
              if (value.i64 == INT64_MIN) { // null
                for (size_t i = 0; i < 9; i++)
                  rec.push_back(0xFF);
                value_len = 9;
              } else {
                int64_t i64 = value.i64;
                if (type >= MST_DEC0 && type <= MST_DEC9) {
                  double dbl = value.dbl;
                  i64 = static_cast<int64_t>(dbl * gen::pow10(type - MST_DEC0));
                }
                i64 = allflic::allflic48::zigzag_encode(i64);
                uint8_t v64[16];
                uint8_t *v_end = allflic::allflic48::simple_encode_single(i64, v64, 0);
                value_len = (v_end - v64);
                for (size_t vi = 0; vi < value_len; vi++)
                  rec.push_back(v64[vi]);
              }
            }
          } else {
            if (value.i64 == INT64_MIN) { // null
              rec.push_back(0);
              value_len = 1;
            } else {
              int64_t i64 = value.i64;
              if (type >= MST_DEC0 && type <= MST_DEC9) {
                double dbl = value.dbl;
                i64 = static_cast<int64_t>(dbl * gen::pow10(type - MST_DEC0));
              }
              gen::append_svint60(rec, i64);
              value_len = gen::get_svint60_len(i64);
            }
          }
        } break;
      }
      if (max_val_len < value_len)
        max_val_len = value_len;
      return value_len;
    }

    size_t get_value_len(size_t i, char type, const mdx_val_in *values, const size_t value_lens[]) {
      size_t value_len = 0;
      if (value_lens != nullptr)
        value_len = value_lens[i];
      if (value_lens == nullptr) {
        if (type == MST_TEXT && values[i].i64 != 0 && values[i].i64 != UINT64_MAX)
          value_len = strlen((const char *) values[i].txt_bin);
      }
      return value_len;
    }
    bool insert_record(const mdx_val_in *values, const size_t value_lens[] = NULL) {
      cur_seq_idx++;
      byte_vec rec;
      byte_vec key_rec;
      size_t i = 0;
      for (; i < column_count; i++) {
        uint8_t type = column_types[i];
        if (type == MST_SEC_2WAY)
          break;
        // printf("col: %lu - ", i);
        size_t value_len = get_value_len(i, type, values, value_lens);
        append_rec_value(type, column_encodings[i], values[i], value_len, rec, APPEND_REC_NOKEY);
        if (i < pk_col_count) {
          append_rec_value(type, column_encodings[i], values[i], value_len, key_rec,
             i < (pk_col_count - 1) ? APPEND_REC_KEY_MIDDLE : APPEND_REC_KEY_LAST);
        }
      }
      size_t sec_idx = 0;
      for (; i < column_count; i++, sec_idx++) {
        const char *sec_cols = names + names_positions[i + 2];
        size_t sec_cols_len = strlen(sec_cols);
        const char *cur_col = sec_cols;
        byte_vec sec_rec;
        for (size_t j = 0; j < sec_cols_len; j++) {
          if (sec_cols[j] == '+') {
            int sec_col_idx = atoi(cur_col) - 1;
            char sec_col_type = column_types[sec_col_idx];
            size_t value_len = get_value_len(sec_col_idx, sec_col_type, values, value_lens);
            append_rec_value(sec_col_type, column_encodings[sec_col_idx], values[sec_col_idx], value_len, sec_rec, APPEND_REC_KEY_MIDDLE);
            cur_col = sec_cols + j + 1;
          }
        }
        int sec_col_idx = atoi(cur_col) - 1;
        char sec_col_type = column_types[sec_col_idx];
        size_t value_len = get_value_len(sec_col_idx, sec_col_type, values, value_lens);
        append_rec_value(sec_col_type, column_encodings[sec_col_idx], values[sec_col_idx], value_len, sec_rec, APPEND_REC_KEY_LAST);
        mdx_val_in sec_val;
        sec_val.txt_bin = sec_rec.data();
        append_rec_value('*', 'T', sec_val, sec_rec.size(), rec, APPEND_REC_NOKEY);
      }
      if (pk_col_count == 0) {
        uintxx_t val_pos = all_vals->push_back_with_vlen(rec.data(), rec.size(), 8);
        rec_pos_vec.push_back(val_pos);
        memtrie::node_set_handler::create_node_set(memtrie.all_node_sets, 1);
        memtrie::node_set_handler nsh(memtrie.all_node_sets, cur_seq_idx);
        memtrie::node n = nsh.first_node();
        n.set_flags(NFLAG_LEAF | NFLAG_TERM);
        nsh.hdr()->node_id = cur_seq_idx;
        memtrie.node_count++;
      } else {
        memtrie::node n;
        memtrie::node_set_vars nsv;
        bool exists = memtrie.lookup(key_rec.data(), key_rec.size(), nsv);
        bool to_append = true;
        uintxx_t val_pos;
        if (exists) {
          memtrie::node_set_handler nsh(memtrie.all_node_sets, nsv.node_set_pos);
          n = nsh[nsv.cur_node_idx];
          val_pos = n.get_col_val();
          size_t vlen;
          uint8_t *val_loc = (*all_vals)[val_pos];
          uintxx_t old_len = gen::read_vint32(val_loc, &vlen);
          if (rec.size() <= old_len) {
            to_append = false;
            gen::copy_vint32(rec.size(), val_loc, vlen);
            memcpy(val_loc + vlen, rec.data(), rec.size());
            rec_pos_vec.push_back(val_pos);
          }
          return true;
        }
        if (to_append) {
          val_pos = all_vals->push_back_with_vlen(rec.data(), rec.size(), 8);
          rec_pos_vec.push_back(val_pos);
          memtrie.insert(key_rec.data(), key_rec.size(), val_pos);
          if (exists)
            n.set_col_val(val_pos);
        }
        // printf("Key: [%.*s]\n", (int) key_len, rec.data() + key_loc_pos);
      }
      return false;
    }

};

}}

// 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx
// 16 - followed by pointer
// 0 2-15/18-31 bytes 16 ptrs bytes 1

// 0 0000 - terminator
// 0 0001 to 0014 - length of affix and terminator
// 0 0015 - If length more than 14

// 1 xxxx 01xxxxxx - dictionary reference 1024 bytes
// 1 xxxx 1xxxxxxx 01xxxxxx - dictionary reference 131kb
// 1 xxxx 1xxxxxxx 1xxxxxxx 01xxxxxx - dictionary reference 16mb
// 1 xxxx 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx - dictionary reference 2gb

// dictionary

// no need to refer data if ptr bits > data size
// RLE within grouped pointers
// ijklmnopqrs - triple bit overhead double
// pPqQrRsS - double bit overhead double
// xXyY - single bit overhead double
// zZ - 0 bit overhead double
// sub-byte width delta coding
// inverted delta coding ??
// 

#endif
