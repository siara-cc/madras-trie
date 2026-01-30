#ifndef __DV1_READER_STATIC_TRIE_HPP
#define __DV1_READER_STATIC_TRIE_HPP

#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/stat.h>

#include "cmn.hpp"
#include "leapfrog.hpp"
#include "interfaces.hpp"
#include "lt_builder.hpp"
#include "trie_caches.hpp"
#include "tail_pointers.hpp"

namespace madras { namespace dv1 {

struct trie_flags {
  uint64_t bm_ptr;
  uint64_t bm_term;
  uint64_t bm_child;
  uint64_t bm_leaf;
};

class inner_trie : public inner_trie_fwd {
  private:
    // __fq1 __fq2 inner_trie(inner_trie const&);
    // __fq1 __fq2 inner_trie& operator=(inner_trie const&);
  protected:
    uintxx_t node_count;

    uint8_t *trie_loc;
    uint8_t lt_not_given;
    uint8_t is_tail_flat_map;
    union {
      tail_ptr_flat_map tail_flat_map;
      tail_ptr_group_map tail_group_map;
    };
    bvlt_select child_lt;
    bvlt_select term_lt;
    GCFC_rev_cache rev_cache;

  public:
    __fq1 __fq2 bool compare_trie_tail(uintxx_t node_id, input_ctx& in_ctx) {
      do {
        if (tail_lt.is_set1(node_id)) {
          uintxx_t ptr_bit_count = UINTXX_MAX;
          bool tail_matches;
          if (is_tail_flat_map)
            tail_matches = tail_flat_map.compare_tail(node_id, in_ctx, ptr_bit_count);
          else
            tail_matches = tail_group_map.compare_tail(node_id, in_ctx, ptr_bit_count);
          if (!tail_matches)
            return false;
        } else {
          if (in_ctx.key_pos >= in_ctx.key_len || trie_loc[node_id] != in_ctx.key[in_ctx.key_pos])
            return false;
          in_ctx.key_pos++;
        }
        if (!rev_cache.try_find(node_id, in_ctx))
          node_id = child_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    __fq1 __fq2 bool copy_trie_tail(uintxx_t node_id, gen::byte_str& tail_str) {
      do {
        if (tail_lt.is_set1(node_id)) {
          if (is_tail_flat_map)
            tail_flat_map.get_tail_str(node_id, tail_str);
          else
            tail_group_map.get_tail_str(node_id, tail_str);
        } else
          tail_str.append(trie_loc[node_id]);
        if (!rev_cache.try_find(node_id, tail_str))
          node_id = child_lt.select1(node_id + 1) - node_id - 2;
      } while (node_id != 0);
      return true;
    }
    __fq1 __fq2 inner_trie() {
    }
    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      inner_trie *it = new inner_trie();
      it->trie_level = mem[TRIE_LEVEL_LOC];
      it->load_inner_trie(mem);
      return it;
    }
    __fq1 __fq2 void load_inner_trie(uint8_t *trie_bytes) {

      trie_loc = nullptr;

      node_count = cmn::read_uint64(trie_bytes + NODE_COUNT_LOC);
      uintxx_t node_set_count = cmn::read_uint64(trie_bytes + NS_COUNT_LOC);
      uintxx_t key_count = cmn::read_uint64(trie_bytes + KEY_COUNT_LOC);
      lt_not_given = 0;
      if (key_count > 0) {
        uintxx_t rev_cache_count = cmn::read_uint64(trie_bytes + RC_COUNT_LOC);
        uintxx_t rev_cache_max_node_id = cmn::read_uint64(trie_bytes + RC_MAX_NID_LOC);
        uint8_t *rev_cache_loc = trie_bytes + cmn::read_uint64(trie_bytes + RCACHE_LOC);
        rev_cache.init(rev_cache_loc, rev_cache_count, rev_cache_max_node_id);

        uint8_t *term_select_lkup_loc = trie_bytes + cmn::read_uint64(trie_bytes + TERM_SEL_LT_LOC);
        uint8_t *term_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + TERM_RANK_LT_LOC);
        uint8_t *child_select_lkup_loc = trie_bytes + cmn::read_uint64(trie_bytes + CHILD_SEL_LT_LOC);
        uint8_t *child_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + CHILD_RANK_LT_LOC);

        uint8_t *tail_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + TAIL_RANK_LT_LOC);
        uint8_t *trie_tail_ptrs_data_loc = trie_bytes + cmn::read_uint64(trie_bytes + TRIE_TAIL_PTRS_DATA_LOC);

        uintxx_t tail_size = cmn::read_uint64(trie_tail_ptrs_data_loc);
        //uintxx_t trie_flags_size = cmn::read_uint32(trie_tail_ptrs_data_loc + 4);
        uint8_t *tails_loc = trie_tail_ptrs_data_loc + 16;
        trie_loc = tails_loc + tail_size;

        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TRIE_FLAGS_LOC));
        uint64_t *tf_ptr_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TAIL_FLAGS_PTR_LOC));

        bldr_options *opts = (bldr_options *) (trie_bytes + MDX_HEADER_SIZE);
        uint8_t multiplier = opts->trie_leaf_count > 0 ? 4 : 3;

        uint8_t encoding_type = tails_loc[TV_ENC_TYPE_LOC];
        uint8_t *tail_data_loc = tails_loc + cmn::read_uint64(tails_loc + TV_GRP_DATA_LOC);
        if (encoding_type == MSE_TRIE || encoding_type == MSE_TRIE_2WAY || *tail_data_loc == 1) {
          tail_flat_map.init(this, &tail_lt, trie_loc, tails_loc);
          is_tail_flat_map = true;
        } else {
          tail_ptr_group_map *tail_grp_map = new tail_ptr_group_map();
          tail_group_map.init_ptr_grp_map(this, trie_loc, tf_loc, trie_level == 0 ? multiplier : 1, trie_level == 0 ? tf_loc : tf_ptr_loc, tails_loc, key_count, node_count, true);
          is_tail_flat_map = false;
        }

        if (term_select_lkup_loc == trie_bytes) term_select_lkup_loc = nullptr;
        if (term_lt_loc == trie_bytes) term_lt_loc = nullptr;
        if (child_select_lkup_loc == trie_bytes) child_select_lkup_loc = nullptr;
        if (child_lt_loc == trie_bytes) child_lt_loc = nullptr;
        if (tail_lt_loc == trie_bytes) tail_lt_loc = nullptr; // TODO: to build if dessicated?
        if (term_lt_loc == nullptr) {
          lt_not_given |= BV_LT_TYPE_TERM;
          term_lt_loc = lt_builder::create_rank_lt_from_trie(BV_LT_TYPE_TERM, node_count, tf_loc);
          term_select_lkup_loc = lt_builder::create_select_lt_from_trie(BV_LT_TYPE_TERM, key_count, node_set_count, node_count, tf_loc);
        }
        if (child_lt_loc == nullptr) {
          lt_not_given |= BV_LT_TYPE_CHILD;
          child_lt_loc = lt_builder::create_rank_lt_from_trie(BV_LT_TYPE_CHILD, node_count, tf_loc);
          child_select_lkup_loc = lt_builder::create_select_lt_from_trie(BV_LT_TYPE_CHILD, key_count, node_set_count, node_count, tf_loc);
        }

        if (trie_level == 0) {
          term_lt.init(term_lt_loc, term_select_lkup_loc, node_count, tf_loc + TF_TERM, multiplier);
          child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, tf_loc + TF_CHILD, multiplier);
        } else {
          child_lt.init(child_lt_loc, child_select_lkup_loc, node_count * 2, tf_loc, 1);
        }
        tail_lt.init(tail_lt_loc, trie_level == 0 ? tf_loc + TF_PTR : tf_ptr_loc, trie_level == 0 ? multiplier : 1); //trie_level == 0 ? 3 : 1);
      }

    }

    __fq1 __fq2 virtual ~inner_trie() {
      if (lt_not_given & BV_LT_TYPE_CHILD) {
        delete [] child_lt.get_rank_loc();
        delete [] child_lt.get_select_loc1();
      }
      if (lt_not_given & BV_LT_TYPE_TERM) {
        delete [] term_lt.get_rank_loc();
        delete [] term_lt.get_select_loc1();
      }
    }

};

class static_trie : public inner_trie {
  protected:
    bvlt_select *leaf_lt;
    GCFC_fwd_cache fwd_cache;
    trie_flags *trie_flags_loc;
    leapfrog_asc *leaper;
    uintxx_t key_count;
    uint8_t *trie_bytes;
    uint16_t max_tail_len;
    uint16_t max_level;

  private:
    // __fq1 __fq2 static_trie(static_trie const&); // todo: restore? can't return beacuse of this
    // __fq1 __fq2 static_trie& operator=(static_trie const&);
    uintxx_t max_key_len;
  protected:
    bldr_options *opts;
  public:
    __fq1 __fq2 bool lookup(input_ctx& in_ctx) {
      in_ctx.key_pos = 0;
      in_ctx.node_id = 1;
      trie_flags *tf;
      uint64_t bm_mask;
      do {
        int ret = fwd_cache.try_find(in_ctx);
        bm_mask = bm_init_mask << (in_ctx.node_id % nodes_per_bv_block_n);
        tf = trie_flags_loc + in_ctx.node_id / nodes_per_bv_block_n;
        if (ret == 0)
          return bm_mask & tf->bm_leaf;
        if (leaper != nullptr) {
          if ((bm_mask & tf->bm_leaf) == 0 && (bm_mask & tf->bm_child) == 0) {
            leaper->find_pos(in_ctx.node_id, trie_loc, in_ctx.key[in_ctx.key_pos]);
            bm_mask = bm_init_mask << (in_ctx.node_id % nodes_per_bv_block_n);
            tf = trie_flags_loc + in_ctx.node_id / nodes_per_bv_block_n;
          }
        }
        uintxx_t ptr_bit_count = UINTXX_MAX;
        do {
          if ((bm_mask & tf->bm_ptr) == 0) {
            if (in_ctx.key[in_ctx.key_pos] == trie_loc[in_ctx.node_id]) {
              in_ctx.key_pos++;
              break;
            }
            #ifdef MDX_IN_ORDER
              if (in_ctx.key[in_ctx.key_pos] < trie_loc[in_ctx.node_id])
                return false;
            #endif
          } else {
            uintxx_t prev_key_pos = in_ctx.key_pos;
            bool tail_matches;
            if (is_tail_flat_map)
              tail_matches = tail_flat_map.compare_tail(in_ctx.node_id, in_ctx, ptr_bit_count);
            else
              tail_matches = tail_group_map.compare_tail(in_ctx.node_id, in_ctx, ptr_bit_count);
            if (tail_matches)
              break;
            if (prev_key_pos != in_ctx.key_pos)
              return false;
            }
          if (bm_mask & tf->bm_term)
            return false;
          in_ctx.node_id++;
          bm_mask <<= 1;
          if (bm_mask == 0) {
            bm_mask = bm_init_mask;
            tf++;
          }
        } while (1);
        if (in_ctx.key_pos == in_ctx.key_len) {
          return 0 != (bm_mask & tf->bm_leaf);
        }
        if ((bm_mask & tf->bm_child) == 0)
          return false;
        in_ctx.node_id = term_lt.select1(child_lt.rank1(in_ctx.node_id) + 1);
      } while (1);
      return false;
    }

    __fq1 __fq2 bool reverse_lookup(uintxx_t leaf_id, size_t *in_size_out_key_len, uint8_t *ret_key) {
      leaf_id++;
      uintxx_t node_id = leaf_lt->select1(leaf_id) - 1;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key);
    }

    __fq1 __fq2 bool reverse_lookup_from_node_id(uintxx_t node_id, size_t *in_size_out_key_len, uint8_t *ret_key) {
      gen::byte_str tail_str(ret_key, max_key_len);
      do {
        if (tail_lt[node_id]) {
          size_t prev_len = tail_str.length();
          if (is_tail_flat_map)
            tail_flat_map.get_tail_str(node_id, tail_str);
          else
            tail_group_map.get_tail_str(node_id, tail_str);
          reverse_byte_str(tail_str.data() + prev_len, tail_str.length() - prev_len);
        } else {
          tail_str.append(trie_loc[node_id]);
        }
        if (!rev_cache.try_find(node_id, tail_str))
          node_id = child_lt.select1(term_lt.rank1(node_id)) - 1;
      } while (node_id != 0);
      reverse_byte_str(tail_str.data(), tail_str.length());
      *in_size_out_key_len = tail_str.length();
      return true;
    }

    __fq1 __fq2 inline void reverse_byte_str(uint8_t *str, size_t len) {
      uint8_t b;
      size_t i = len >> 1;
      size_t im = len - i;
      while (i--) {
        b = str[i];
        str[i] = str[im];
        str[im++] = b;
      }
    }

    __fq1 __fq2 uintxx_t leaf_rank1(uintxx_t node_id) {
      return leaf_lt->rank1(node_id);
    }

    __fq1 __fq2 bool is_leaf(uintxx_t node_id) {
      return (*leaf_lt)[node_id];
    }

    __fq1 __fq2 uintxx_t leaf_select1(uintxx_t leaf_id) {
      return leaf_lt->select1(leaf_id);
    }

    __fq1 __fq2 void push_to_ctx(iter_ctx& ctx, gen::byte_str& tail, uintxx_t node_id) {
      ctx.cur_idx++;
      tail.clear();
      update_ctx(ctx, tail, node_id);
    }

    __fq1 __fq2 void insert_arr(uintxx_t *arr, int arr_len, int pos, uintxx_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void insert_arr(uint16_t *arr, int arr_len, int pos, uint16_t val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    __fq1 __fq2 void update_ctx(iter_ctx& ctx, gen::byte_str& tail, uintxx_t node_id) {
      ctx.node_path[ctx.cur_idx] = node_id;
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = tail.length();
      memcpy(ctx.key + ctx.key_len, tail.data(), tail.length());
      ctx.key_len += tail.length();
    }

    __fq1 __fq2 void clear_last_tail(iter_ctx& ctx) {
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
    }

    __fq1 __fq2 uintxx_t pop_from_ctx(iter_ctx& ctx, gen::byte_str& tail) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      return read_from_ctx(ctx, tail);
    }

    __fq1 __fq2 uintxx_t read_from_ctx(iter_ctx& ctx, gen::byte_str& tail) {
      uintxx_t node_id = ctx.node_path[ctx.cur_idx];
      return node_id;
    }

    __fq1 __fq2 int next(iter_ctx& ctx, uint8_t *key_buf = nullptr) {
      gen::byte_str tail_str;
      uint8_t stack_buf[FAST_STACK_BUF];
      uint8_t* heap_buf = nullptr;
      uint8_t *tail_bytes = get_fast_buffer<uint8_t>(max_tail_len + 1, stack_buf, heap_buf);
      BufferGuard<uint8_t> guard(heap_buf);
      tail_str.set_buf_max_len(tail_bytes, max_tail_len);
      uintxx_t node_id = read_from_ctx(ctx, tail_str);
      while (node_id < node_count) {
        if (leaf_lt != nullptr && !(*leaf_lt)[node_id] && !child_lt[node_id]) {
          node_id++;
          continue;
        }
        if (leaf_lt == nullptr || (*leaf_lt)[node_id]) {
          if (ctx.to_skip_first_leaf) {
            if (!child_lt[node_id]) {
              while (term_lt[node_id]) {
                if (ctx.cur_idx == 0) {
                  return -2;
                }
                node_id = pop_from_ctx(ctx, tail_str);
              }
              node_id++;
              update_ctx(ctx, tail_str, node_id);
              ctx.to_skip_first_leaf = false;
              continue;
            }
          } else {
            tail_str.clear();
            if (tail_lt[node_id]) {
              if (is_tail_flat_map)
                tail_flat_map.get_tail_str(node_id, tail_str);
              else
                tail_group_map.get_tail_str(node_id, tail_str);
            } else
              tail_str.append(trie_loc[node_id]);
            update_ctx(ctx, tail_str, node_id);
            if (key_buf != nullptr)
              memcpy(key_buf, ctx.key, ctx.key_len);
            ctx.to_skip_first_leaf = true;
            return ctx.key_len;
          }
        }
        ctx.to_skip_first_leaf = false;
        if (child_lt[node_id]) {
          tail_str.clear();
          if (tail_lt[node_id]) {
            if (is_tail_flat_map)
              tail_flat_map.get_tail_str(node_id, tail_str);
            else
              tail_group_map.get_tail_str(node_id, tail_str);
          } else
            tail_str.append(trie_loc[node_id]);
          update_ctx(ctx, tail_str, node_id);
          node_id = term_lt.select1(child_lt.rank1(node_id) + 1);
          push_to_ctx(ctx, tail_str, node_id);
        }
      }
      return -2;
    }

    __fq1 __fq2 uintxx_t get_max_level() {
      return max_level;
    }

    __fq1 __fq2 uintxx_t get_key_count() {
      return key_count;
    }

    __fq1 __fq2 uintxx_t get_node_count() {
      return node_count;
    }

    __fq1 __fq2 uintxx_t get_max_key_len() {
      return max_key_len;
    }

    __fq1 __fq2 uintxx_t get_max_tail_len() {
      return max_tail_len;
    }

    __fq1 __fq2 size_t extract_from_composite(const uint8_t *key, uintxx_t key_len, size_t idx, const char data_type, uintxx_t &out_len) {
      size_t out_pos = 0;
      out_len = 0;
      for (int i = 0; i <= idx && out_pos < key_len; i++) {
        if (data_type == MST_TEXT) {
          out_len = 0;
          while (out_pos + out_len < key_len && key[out_pos + out_len] != 0) out_len++;
          out_len++;
        } else if (data_type == MST_BIN || data_type == MST_DECV) {
          out_len = key_len;
          out_pos = out_len;
        } else {
          out_len = gen::read_svint60_len(key + out_pos);
        }
        out_pos += out_len;
      }
      out_pos -= out_len;
      if (data_type == MST_TEXT) out_len--;
      return out_pos;
    }

    __fq1 __fq2 void insert_into_ctx(iter_ctx& ctx, gen::byte_str& tail, uintxx_t node_id) {
      insert_arr(ctx.node_path, ctx.cur_idx, 0, node_id);
      insert_arr(ctx.last_tail_len, ctx.cur_idx, 0, (uint16_t) tail.length());
      memmove(ctx.key + tail.length(), ctx.key, ctx.key_len);
      for (size_t i = 0; i < tail.length(); i++)
        ctx.key[i] = tail[i];
      ctx.key_len += tail.length();
      ctx.cur_idx++;
    }

    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      static_trie *it = new static_trie();
      it->trie_level = mem[TRIE_LEVEL_LOC];
      it->load_static_trie(mem);
      return it;
    }

    __fq1 __fq2 uintxx_t find_first(const uint8_t *prefix, size_t prefix_len, iter_ctx& ctx, bool for_next = false) {
      input_ctx in_ctx;
      in_ctx.key = prefix;
      in_ctx.key_len = prefix_len;
      in_ctx.node_id = 0;
      lookup(in_ctx);
      uint8_t stack_buf[FAST_STACK_BUF];
      uint8_t* heap_buf = nullptr;
      uint8_t *tail_buf = get_fast_buffer<uint8_t>(max_tail_len + 1, stack_buf, heap_buf);
      BufferGuard<uint8_t> guard(heap_buf);
      ctx.cur_idx = 0;
      gen::byte_str tail_str(tail_buf, max_tail_len);
      uintxx_t node_id = in_ctx.node_id;
      do {
        if (tail_lt[node_id]) {
          if (is_tail_flat_map)
            tail_flat_map.get_tail_str(node_id, tail_str);
          else
            tail_group_map.get_tail_str(node_id, tail_str);
        } else
          tail_str.append(trie_loc[node_id]);
        insert_into_ctx(ctx, tail_str, node_id);
        // printf("[%.*s]\n", (int) tail.length(), tail.data());
        // if (!rev_cache.try_find(in_ctx.node_id, tail))
          node_id = child_lt.select1(term_lt.rank1(node_id)) - 1;
        tail_str.clear();
      } while (node_id != 0);
      ctx.cur_idx--;
        // for (int i = 0; i < ctx.key_len; i++)
        //   printf("%c", ctx.key[i]);
        // printf("\nlsat tail len: %d\n", ctx.last_tail_len[ctx.cur_idx]);
      if (for_next) {
        ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
        ctx.last_tail_len[ctx.cur_idx] = 0;
        ctx.to_skip_first_leaf = false;
      }
      return in_ctx.node_id;
    }

    __fq1 __fq2 bvlt_select *get_leaf_lt() {
      return leaf_lt;
    }

    __fq1 __fq2 uint8_t *get_trie_loc() {
      return trie_loc;
    }

    __fq1 __fq2 uint8_t *get_trie_bytes() {
      return trie_bytes;
    }

    __fq1 __fq2 uint8_t *get_null_value(size_t& null_value_len) {
      uint8_t *nv_loc = trie_bytes + cmn::read_uint64(trie_bytes + NULL_VAL_LOC);
      null_value_len = *nv_loc++;
      return nv_loc;
    }

    __fq1 __fq2 uint8_t *get_empty_value(size_t& empty_value_len) {
      uint8_t *ev_loc = trie_bytes + cmn::read_uint64(trie_bytes + EMPTY_VAL_LOC);
      empty_value_len = *ev_loc++;
      return ev_loc;
    }

    __fq1 __fq2 void load_static_trie(uint8_t *_trie_bytes = nullptr) {

      if (_trie_bytes != nullptr)
        trie_bytes = _trie_bytes;

      load_inner_trie(trie_bytes);
      opts = (bldr_options *) (trie_bytes + MDX_HEADER_SIZE);
      key_count = cmn::read_uint64(trie_bytes + KEY_COUNT_LOC);
      if (key_count > 0) {
        max_tail_len = cmn::read_uint16(trie_bytes + MAX_TAIL_LEN_LOC) + 1;

        uintxx_t node_set_count = cmn::read_uint64(trie_bytes + NS_COUNT_LOC);
        uint8_t *leaf_select_lkup_loc = trie_bytes + cmn::read_uint64(trie_bytes + LEAF_SEL_LT_LOC);
        uint8_t *leaf_lt_loc = trie_bytes + cmn::read_uint64(trie_bytes + LEAF_RANK_LT_LOC);
        uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TRIE_FLAGS_LOC));
        trie_flags_loc = (trie_flags *) tf_loc;

        if (leaf_select_lkup_loc == trie_bytes) leaf_select_lkup_loc = nullptr;
        if (leaf_lt_loc == trie_bytes) leaf_lt_loc = nullptr;
        if (opts->trie_leaf_count > 0) {
          if (leaf_lt_loc == nullptr) {
            lt_not_given |= BV_LT_TYPE_LEAF;
            leaf_lt_loc = lt_builder::create_rank_lt_from_trie(BV_LT_TYPE_LEAF, node_count, tf_loc);
            leaf_select_lkup_loc = lt_builder::create_select_lt_from_trie(BV_LT_TYPE_LEAF, key_count, node_set_count, node_count, tf_loc);
          }
          leaf_lt = new bvlt_select();
          leaf_lt->init(leaf_lt_loc, leaf_select_lkup_loc, node_count, tf_loc + 3, 4);
        }
      }

      if (key_count > 0) {
        max_key_len = cmn::read_uint64(trie_bytes + MAX_KEY_LEN_LOC);
        max_level = cmn::read_uint16(trie_bytes + MAX_LVL_LOC);
        uintxx_t fwd_cache_count = cmn::read_uint64(trie_bytes + FC_COUNT_LOC);
        uintxx_t fwd_cache_max_node_id = cmn::read_uint64(trie_bytes + FC_MAX_NID_LOC);
        uint8_t *fwd_cache_loc = trie_bytes + cmn::read_uint64(trie_bytes + FCACHE_LOC);
        fwd_cache.init(fwd_cache_loc, fwd_cache_count, fwd_cache_max_node_id);

        min_pos_stats min_stats;
        memcpy(&min_stats, trie_bytes + MIN_STAT_LOC, 4);
        uint8_t *min_pos_loc = trie_bytes + cmn::read_uint64(trie_bytes + SEC_CACHE_LOC);
        if (min_pos_loc == trie_bytes)
          min_pos_loc = nullptr;
        if (min_pos_loc != nullptr) {
          if (!opts->sort_nodes_on_freq) {
            leaper = new leapfrog_asc();
            leaper->init(min_stats, min_pos_loc);
          }
        }

      }

    }

    __fq1 __fq2 static_trie() {
      init_vars();
      trie_level = 0;
    }

    __fq1 __fq2 void init_vars() {

      trie_bytes = nullptr;

      max_key_len = max_level = 0;
      leaper = nullptr;

      max_tail_len = 0;
      trie_loc = nullptr;
      leaf_lt = nullptr;

    }

    __fq1 __fq2 virtual ~static_trie() {
      if (leaper != nullptr)
        delete leaper;
      if (lt_not_given & BV_LT_TYPE_LEAF) {
        delete [] leaf_lt->get_rank_loc();
        delete [] leaf_lt->get_select_loc1();
      }
      if (leaf_lt != nullptr)
        delete leaf_lt;
    }

};

}}

#endif
