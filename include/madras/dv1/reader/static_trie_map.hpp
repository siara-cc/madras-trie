#ifndef __DV1_READER_STATIC_TRIE_MAP_HPP
#define __DV1_READER_STATIC_TRIE_MAP_HPP

#include "cmn.hpp"
#include "ifaces.hpp"
#include "pointers.hpp"
#include "static_trie.hpp"

namespace madras { namespace dv1 {

template<char param>
constexpr char template_val() {
    return param;
}

class value_retriever_base : public ptr_group_map {
  public:
    __fq1 __fq2 virtual ~value_retriever_base() {}
    __fq1 __fq2 virtual void load_bm(uintxx_t node_id, val_ctx& vctx) = 0;
    __fq1 __fq2 virtual void init(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier,
            uint64_t *_bm_ptr_loc, uint8_t *val_loc, uintxx_t _key_count, uintxx_t _node_count) = 0;
    __fq1 __fq2 virtual bool next_val(val_ctx& vctx) = 0;
    __fq1 __fq2 virtual void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) = 0;
    __fq1 __fq2 virtual const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) = 0;
    __fq1 __fq2 virtual static_trie *get_col_trie() = 0;
};

template<char pri_key>
class value_retriever : public value_retriever_base {
  protected:
    uintxx_t node_count;
    uintxx_t key_count;
    gen::int_bv_reader *int_ptr_bv = nullptr;
    gen::bv_reader<uint64_t> null_bv;
    inner_trie_fwd *col_trie = nullptr;
  public:
    __fq1 __fq2 uintxx_t scan_ptr_bits_val(val_ctx& vctx) {
      uintxx_t node_id_from = vctx.node_id - (vctx.node_id % nodes_per_ptr_block_n);
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      uint64_t bm_leaf = UINT64_MAX;
      if (key_count > 0)
        bm_leaf = ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * multiplier];
      vctx.rpt_left = 0;
      uintxx_t last_pbc = vctx.ptr_bit_count;
      while (node_id_from < vctx.node_id) {
        if (bm_leaf & bm_mask) {
          if (vctx.rpt_left == 0) {
            uint8_t code = ptr_reader.read8(vctx.ptr_bit_count);
            uint8_t grp_no = code_lt_code_len[code] & 0x0F;
            if (grp_no != rpt_grp_no && rpt_grp_no > 0)
              last_pbc = vctx.ptr_bit_count;
            uint8_t code_len = code_lt_code_len[code] >> 4;
            uint8_t bit_len = code_lt_bit_len[code];
            vctx.ptr_bit_count += code_len;
            uintxx_t count = ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
            if (rpt_grp_no > 0 && grp_no == rpt_grp_no) {
              vctx.rpt_left = count - 1; // todo: -1 not needed
            }
          } else
            vctx.rpt_left--;
        }
        node_id_from++;
        bm_mask <<= 1;
      }
      if (vctx.rpt_left > 0) {
        vctx.next_pbc = vctx.ptr_bit_count;
        vctx.ptr_bit_count = last_pbc;
        return vctx.ptr_bit_count;
      }
      uint8_t code = ptr_reader.read8(vctx.ptr_bit_count);
      uint8_t grp_no = code_lt_code_len[code] & 0x0F;
      if ((grp_no == rpt_grp_no && rpt_grp_no > 0)) {
        uint8_t code_len = code_lt_code_len[code] >> 4;
        uint8_t bit_len = code_lt_bit_len[code];
        vctx.ptr_bit_count += code_len;
        vctx.rpt_left = ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
        vctx.next_pbc = vctx.ptr_bit_count;
        vctx.ptr_bit_count = last_pbc;
      }
      return vctx.ptr_bit_count;
    }
    __fq1 __fq2 uintxx_t get_ptr_bit_count_val(val_ctx& vctx) {
      vctx.ptr_bit_count = ptr_reader.get_ptr_block2(vctx.node_id);
      return scan_ptr_bits_val(vctx);
    }

    bool is_repeat(val_ctx& vctx) {
      if (vctx.rpt_left > 0) {
        vctx.rpt_left--;
        if (vctx.rpt_left == 0 && vctx.next_pbc != 0) {
          vctx.ptr_bit_count = vctx.next_pbc;
          vctx.next_pbc = 0;
        }
        return true;
      }
      return false;
    }
    __fq1 __fq2 uint8_t *get_val_loc(val_ctx& vctx) {
      if (group_count == 1) {
        vctx.grp_no = 0;
        uintxx_t ptr_pos = vctx.node_id;
        if (key_count > 0)
          ptr_pos = ((static_trie *) dict_obj)->get_leaf_lt()->rank1(vctx.node_id);
        vctx.ptr = (*int_ptr_bv)[ptr_pos];
        // if (vctx.grp_no < grp_idx_limit)
        //   ptr = read_ptr_from_idx(vctx.grp_no, ptr);
        return grp_data[0] + vctx.ptr + 2;
      }
      if (vctx.ptr_bit_count == UINTXX_MAX)
        get_ptr_bit_count_val(vctx);
      uint8_t code = ptr_reader.read8(vctx.ptr_bit_count);
      uint8_t bit_len = code_lt_bit_len[code];
      uint8_t grp_no = code_lt_code_len[code] & 0x0F;
      uint8_t code_len = code_lt_code_len[code] >> 4;
      vctx.ptr_bit_count += code_len;
      uintxx_t rptct_or_ptr = ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
      if (rpt_grp_no > 0 && grp_no == rpt_grp_no) {
        vctx.rpt_left = rptct_or_ptr - 1; // todo: -1 not needed
        return grp_data[vctx.grp_no] + vctx.ptr;
      }
      vctx.grp_no = grp_no;
      vctx.ptr = rptct_or_ptr;
      if (vctx.grp_no < grp_idx_limit)
        vctx.ptr = read_ptr_from_idx(vctx.grp_no, vctx.ptr);
      return grp_data[vctx.grp_no] + vctx.ptr;
    }

    void load_bm(uintxx_t node_id, val_ctx& vctx) {
      if (template_val<pri_key>() == 'Y') {
        vctx.bm_mask = (bm_init_mask << (node_id % nodes_per_bv_block_n));
        vctx.bm_leaf = UINT64_MAX;
        vctx.bm_leaf = ptr_bm_loc[(node_id / nodes_per_bv_block_n) * multiplier];
      }
    }

    __fq1 __fq2 bool skip_non_leaf_nodes(val_ctx& vctx) {
      if (template_val<pri_key>() == 'Y') {
        while (vctx.bm_mask && (vctx.bm_leaf & vctx.bm_mask) == 0) {
          vctx.node_id++;
          vctx.bm_mask <<= 1;
        }
      }
      if (vctx.node_id >= node_count)
        return false;
      if (template_val<pri_key>() == 'N') {
        return true;
      } else {
        if (vctx.bm_mask != 0)
          return true;
        load_bm(vctx.node_id, vctx);
        return skip_non_leaf_nodes(vctx);
      }
    }
    __fq1 __fq2 bool skip_to_next_leaf(val_ctx& vctx) {
      vctx.node_id++;
      if (template_val<pri_key>() == 'Y') {
        vctx.bm_mask <<= 1;
      }
      return skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 void init(inner_trie_fwd *_dict_obj, uint8_t *_trie_loc, uint64_t *_bm_loc, uint8_t _multiplier,
                uint64_t *_bm_ptr_loc, uint8_t *val_loc, uintxx_t _key_count, uintxx_t _node_count) {
      key_count = _key_count;
      node_count = _node_count;
      init_ptr_grp_map(_dict_obj, _trie_loc, _bm_loc, _multiplier, _bm_ptr_loc, val_loc, _key_count, _node_count, false);
      uint8_t *data_loc = val_loc + cmn::read_uint64(val_loc + TV_GRP_DATA_LOC);
      uint8_t *ptrs_loc = val_loc + cmn::read_uint64(val_loc + TV_GRP_PTRS_LOC);
      uint64_t *null_bv_loc = (uint64_t *) (val_loc + cmn::read_uint64(val_loc + TV_NULL_BV_LOC));
      uintxx_t null_bv_size = cmn::read_uint64(val_loc + TV_NULL_BV_SIZE_LOC);
      null_bv.set_bv(null_bv_loc, null_bv_size);
      if (group_count == 1 || val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY) {
        int_ptr_bv = new gen::int_bv_reader();
        int_ptr_bv->init(ptrs_loc, val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY ? val_loc[0] : data_loc[1]);
      }
      col_trie = nullptr;
      if (val_loc[2] == MSE_TRIE || val_loc[2] == MSE_TRIE_2WAY) {
        col_trie = _dict_obj->new_instance(data_loc);
      }
    }
    __fq1 __fq2 bool is_null(uintxx_t node_id) {
      return null_bv[node_id];
    }
    __fq1 __fq2 static_trie *get_col_trie() {
      return (static_trie *) col_trie;
    }
    __fq1 __fq2 virtual ~value_retriever() {
      if (int_ptr_bv != nullptr)
        delete int_ptr_bv;
      if (col_trie != nullptr)
        delete col_trie;
    }
};

template<char pri_key>
class stored_val_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~stored_val_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      size_t len_len;
      *vctx.val_len = cmn::read_vint32(vctx.byts, &len_len);
      if (vctx.val != nullptr)
        vctx.val->txt_bin = (vctx.byts + len_len);
      vctx.byts += len_len;
      vctx.byts += *vctx.val_len;
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.ptr_bit_count = Parent::ptr_reader.get_ptr_block3(node_id);
      vctx.byts = Parent::grp_data[0] + vctx.ptr_bit_count + 2;
      size_t len_len;
      uintxx_t vint_len;
      uintxx_t node_id_from = vctx.node_id - (vctx.node_id % nodes_per_ptr_block_n);
      Parent::load_bm(node_id_from, vctx);
      while (node_id_from < vctx.node_id) {
        if (vctx.bm_leaf & vctx.bm_mask) {
          vint_len = cmn::read_vint32(vctx.byts, &len_len);
          // printf("nid from: %u, to: %u, len:%u, len_len:%lu\n", node_id_from, node_id, vint_len, len_len);
          vctx.byts += len_len;
          vctx.byts += vint_len;
        }
        node_id_from++;
        vctx.bm_mask <<= 1;
      }
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(0, false, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return vctx.val->txt_bin;
    }
};

template<char pri_key>
class main_trie_retriever : public value_retriever<pri_key> {
  private:
    size_t col_idx;
    uint16_t pk_col_count;
  public:
    main_trie_retriever(size_t _col_idx, uint16_t _pk_col_count)
      : col_idx (_col_idx), pk_col_count (_pk_col_count) {
    }
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~main_trie_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      uint8_t val_loc[64];
      if (Parent::data_type != MST_TEXT && Parent::data_type != MST_BIN)
        vctx.val->txt_bin = val_loc;
      static_trie *stm = (static_trie *) Parent::dict_obj;
      stm->reverse_lookup_from_node_id(vctx.node_id, vctx.val_len, vctx.val->txt_bin);
      if (pk_col_count > 1) {
        size_t val_pos;
        uintxx_t val_len;
        val_pos = stm->extract_from_composite(vctx.val->txt_bin, *vctx.val_len, col_idx, Parent::data_type, val_len);
        if (val_pos > 0) memmove(vctx.val->txt_bin, vctx.val->txt_bin + val_pos, val_len);
        *vctx.val_len = val_len;
      }
      if (Parent::data_type != MST_TEXT && Parent::data_type != MST_BIN) {
        cmn::convert_back(Parent::data_type, vctx.val->txt_bin, *vctx.val, vctx.val_len);
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      Parent::load_bm(node_id, vctx);
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(0, false, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return vctx.val->txt_bin;
    }
};

template<char pri_key>
class col_trie_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~col_trie_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      uintxx_t ptr_pos = vctx.node_id;
      if (Parent::key_count > 0)
        ptr_pos = ((static_trie *) Parent::dict_obj)->get_leaf_lt()->rank1(vctx.node_id);
      uintxx_t col_trie_node_id = (*Parent::int_ptr_bv)[ptr_pos];
      // printf("Col trie node_id: %u\n", col_trie_node_id);
      uint8_t val_loc[16];
      if (Parent::data_type != MST_TEXT && Parent::data_type != MST_BIN)
        vctx.val->txt_bin = val_loc;
      ((static_trie *) Parent::col_trie)->reverse_lookup_from_node_id(col_trie_node_id, vctx.val_len, vctx.val->txt_bin, true);
      if (Parent::data_type != MST_TEXT && Parent::data_type != MST_BIN) {
        cmn::convert_back(Parent::data_type, vctx.val->txt_bin, *vctx.val, vctx.val_len);
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      Parent::load_bm(node_id, vctx);
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(0, false, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return vctx.val->txt_bin;
    }
};

template<char pri_key>
class words_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~words_retriever() {}
    __fq1 __fq2 uintxx_t scan_ptr_bits_words(uintxx_t node_id, uintxx_t ptr_bit_count) {
      uintxx_t node_id_from = node_id - (node_id % nodes_per_ptr_block_n);
      uint64_t bm_mask = (bm_init_mask << (node_id_from % nodes_per_bv_block_n));
      uint64_t bm_leaf = UINT64_MAX;
      if (Parent::key_count > 0)
        bm_leaf = Parent::ptr_bm_loc[(node_id_from / nodes_per_bv_block_n) * Parent::multiplier];
      size_t to_skip = 0;
      uintxx_t last_pbc = ptr_bit_count;
      while (node_id_from < node_id) {
        if (bm_leaf & bm_mask) {
          if (to_skip == 0) {
            uint8_t code = Parent::ptr_reader.read8(ptr_bit_count);
            uint8_t grp_no = Parent::code_lt_code_len[code] & 0x0F;
            if (grp_no != Parent::rpt_grp_no && Parent::rpt_grp_no > 0)
              last_pbc = ptr_bit_count;
            uint8_t code_len = Parent::code_lt_code_len[code] >> 4;
            uint8_t bit_len = Parent::code_lt_bit_len[code];
            ptr_bit_count += code_len;
            uintxx_t count = Parent::ptr_reader.read(ptr_bit_count, bit_len - code_len);
            if (Parent::rpt_grp_no > 0 && grp_no == Parent::rpt_grp_no) {
              to_skip = count - 1; // todo: -1 not needed
              //printf("To skip: %lu\n", to_skip);
            } else {
              while (count--) {
                code = Parent::ptr_reader.read8(ptr_bit_count);
                ptr_bit_count += Parent::code_lt_bit_len[code];
              }
            }
          } else
            to_skip--;
        }
        node_id_from++;
        bm_mask <<= 1;
      }
      uint8_t code = Parent::ptr_reader.read8(ptr_bit_count);
      uint8_t grp_no = Parent::code_lt_code_len[code] & 0x0F;
      if (to_skip > 0 || (grp_no == Parent::rpt_grp_no && Parent::rpt_grp_no > 0))
        ptr_bit_count = last_pbc;
      return ptr_bit_count;
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      uint8_t code = Parent::ptr_reader.read8(vctx.ptr_bit_count);
      uint8_t bit_len = Parent::code_lt_bit_len[code];
      uint8_t grp_no = Parent::code_lt_code_len[code] & 0x0F;
      uint8_t code_len = Parent::code_lt_code_len[code] >> 4;
      if (grp_no != 0) {
        printf("Group no. ought to be 0: %d\n", grp_no);
      }
      vctx.ptr_bit_count += code_len;
      uintxx_t word_count = Parent::ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
      size_t word_len;
      *vctx.val_len = 0;
      for (size_t i = 0; i < word_count; i++) {
        code = Parent::ptr_reader.read8(vctx.ptr_bit_count);
        bit_len = Parent::code_lt_bit_len[code];
        grp_no = Parent::code_lt_code_len[code] & 0x0F;
        code_len = Parent::code_lt_code_len[code] >> 4;
        vctx.ptr_bit_count += code_len;
        uintxx_t it_leaf_id = Parent::ptr_reader.read(vctx.ptr_bit_count, bit_len - code_len);
        ((static_trie *) Parent::inner_tries[grp_no])->reverse_lookup(it_leaf_id, &word_len,
            vctx.val->txt_bin + *vctx.val_len, true);
        *vctx.val_len += word_len;
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      Parent::load_bm(node_id, vctx);
      Parent::skip_non_leaf_nodes(vctx);
      uintxx_t pbc = Parent::ptr_reader.get_ptr_block3(vctx.node_id);
      vctx.ptr_bit_count = scan_ptr_bits_words(vctx.node_id, pbc);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(0, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      // printf("pbc: %u\n", *p_ptr_bit_count);
      return ret_val.txt_bin;
    }
};

class block_retriever_base {
  public:
    __fq1 __fq2 virtual ~block_retriever_base() {}
    __fq1 __fq2 virtual void block_operation(uintxx_t node_id, int len, int64_t *out) = 0;
    __fq1 __fq2 virtual void block_operation32(uintxx_t node_id, int len, int32_t *out) = 0;
};

template <char type_id, typename T>
class fast_vint_block_retriever : public block_retriever_base {
  private:
    value_retriever_base *val_retriever;
  public:
    __fq1 __fq2 fast_vint_block_retriever(value_retriever_base *_val_retriever) {
      val_retriever = _val_retriever;
    }
    __fq1 __fq2 void block_operation(uintxx_t node_id, int len, int64_t *out) {
      int64_t i64;
      int32_t *i32_vals = new int32_t[64];
      int64_t *i64_vals = new int64_t[64];
      uint8_t *byts;
      if (template_val<type_id>() == '.')
        byts = new uint8_t[64];
      uint8_t *data = val_retriever->get_grp_data(0) + val_retriever->get_ptr_reader()->get_ptr_block3(node_id) + 2;
      size_t out_pos = 0;
      size_t offset;
      if (template_val<type_id>() == '.')
        offset = 3;
      else
        offset = 2;
      size_t block_size;
      while (len > 0) {
        if ((*data & 0x80) == 0) {
          block_size = allflic::allflic48::decode(data + offset, data[1], i32_vals);
          for (size_t i = 0; i < data[1]; i++) {
            i64 = allflic::allflic48::zigzag_decode(i32_vals[i]);
            if (template_val<type_id>() == 'i') {
              out[out_pos++] = i64;
            } else {
              i64_vals[i] = i64;
            }
          }
          if (template_val<type_id>() == '.')
            memset(byts, '\0', 64);
        } else {
          if (template_val<type_id>() == '.')
            block_size = allflic::allflic48::decode(data + offset, data[1], i64_vals, byts);
          else
            block_size = allflic::allflic48::decode(data + offset, data[1], i64_vals);
          for (size_t i = 0; i < data[1]; i++) {
            i64 = allflic::allflic48::zigzag_decode(i64_vals[i]);
            if (template_val<type_id>() == 'i')
               out[out_pos++] = i64;
            else {
              i64_vals[i] = i64;
            }
          }
        }
        if (template_val<type_id>() != 'i') {
          double dbl;
          for (size_t i = 0; i < data[1]; i++) {
            if (template_val<type_id>() == '.') {
              if (byts[i] == 7) {
                memcpy(&dbl, i64_vals + i, 8);
              } else {
                dbl = static_cast<double>(i64_vals[i]);
                dbl /= allflic::allflic48::tens()[data[2]];
              }
            } else {
              dbl = static_cast<double>(i64_vals[i]);
              dbl /= allflic::allflic48::tens()[val_retriever->data_type - MST_DEC0];
            }
            memcpy(out + out_pos, &dbl, 8);
            out_pos++;
          }
        }
        len -= data[1];
        data += block_size;
        data += offset;
      }
      delete [] i32_vals;
      delete [] i64_vals;
      if (template_val<type_id>() == '.')
        delete[] byts;
    }
    __fq1 __fq2 void block_operation32(uintxx_t node_id, int len, int32_t *out) {
      uint8_t *data = val_retriever->get_grp_data(0) + val_retriever->get_ptr_reader()->get_ptr_block3(node_id) + 2;
      size_t out_pos = 0;
      size_t offset;
      if (template_val<type_id>() == '.')
        offset = 3;
      else
        offset = 2;
      size_t block_size;
      while (len > 0) {
        block_size = allflic::allflic48::decode(data + offset, data[1], out + out_pos);
        for (size_t i = 0; i < data[1]; i++) {
          int32_t i32 = (int32_t) allflic::allflic48::zigzag_decode(out[out_pos]);
          if (template_val<type_id>() == 'i') {
            out[out_pos] = i32;
          } else {
            float flt;
            if (template_val<type_id>() == '.') {
              flt = static_cast<float>(i32);
              flt /= allflic::allflic48::tens()[data[2]];
            } else {
              flt = static_cast<float>(i32);
              flt /= allflic::allflic48::tens()[val_retriever->data_type - MST_DEC0];
            }
            memcpy(out + out_pos, &flt, 4);
          }
          out_pos++;
        }
        len -= data[1];
        data += block_size;
        data += offset;
      }
    }
};

template<char pri_key>
class fast_vint_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~fast_vint_retriever() {}
    __fq1 __fq2 void retrieve_block(uintxx_t node_id, val_ctx& vctx) {
      Parent::load_bm(node_id, vctx);
      // if ((node_id / nodes_per_bv_block_n) == (vctx.node_id / nodes_per_bv_block_n))
      //   return;
      // if (vctx.node_id != UINTXX_MAX)
      //   return;
      vctx.node_id = node_id;
      vctx.ptr_bit_count = Parent::ptr_reader.get_ptr_block3(node_id);
      uint8_t *data = Parent::grp_data[0] + vctx.ptr_bit_count + 2;
      vctx.count = data[1];
      vctx.dec_count = data[2]; // only for data_type MST_DECV
      size_t offset = (Parent::data_type == MST_DECV ? 3 : 2);
      if ((*data & 0x80) == 0) {
        allflic::allflic48::decode(data + offset, vctx.count, vctx.i32_vals);
        for (size_t i = 0; i < vctx.count; i++)
          vctx.i64_vals[i] = allflic::allflic48::zigzag_decode(vctx.i32_vals[i]);
        memset(vctx.byts, '\0', vctx.count); // not setting lens, just 0s
      } else {
        allflic::allflic48::decode(data + offset, vctx.count, vctx.i64_vals, vctx.byts);
        for (size_t i = 0; i < vctx.count; i++)
          vctx.i64_vals[i] = allflic::allflic48::zigzag_decode(vctx.i64_vals[i]);
      }
    }
    __fq1 __fq2 bool skip_non_leaf_nodes(val_ctx& vctx) {
      if (template_val<pri_key>() == 'Y') {
        while (vctx.bm_mask && (vctx.bm_leaf & vctx.bm_mask) == 0) {
          vctx.node_id++;
          vctx.bm_mask <<= 1;
        }
      }
      if (vctx.node_id >= Parent::node_count)
        return false;
      if (template_val<pri_key>() == 'N') {
        if ((vctx.node_id % nodes_per_bv_block_n) == 0)
          retrieve_block(vctx.node_id, vctx);
        return true;
      } else {
        if (vctx.bm_mask != 0)
          return true;
        retrieve_block(vctx.node_id, vctx);
        return skip_non_leaf_nodes(vctx);
      }
    }
    __fq1 __fq2 bool skip_to_next_leaf(val_ctx& vctx) {
      vctx.node_id++;
      if (template_val<pri_key>() == 'Y') {
        vctx.bm_mask <<= 1;
      }
      return skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      int64_t i64;
      int to_skip;
      if (template_val<pri_key>() == 'N') {
        to_skip = vctx.node_id % nodes_per_bv_block_n;
      } else {
        to_skip = cmn::pop_cnt(vctx.bm_leaf & (vctx.bm_mask - 1));
      }
      i64 = *(vctx.i64_vals + to_skip);
      switch (Parent::data_type) {
        case MST_INT:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISO_MS: case MST_DATETIME_ISOT_MS: {
          *((int64_t *) vctx.val) = i64;
          // printf("%lld\n", i64);
        } break;
        case MST_DECV: {
          if (vctx.byts[to_skip] == 7) {
            *((uint64_t *) vctx.val) = i64;
          } else {
            double dbl = static_cast<double>(i64);
            dbl /= allflic::allflic48::tens()[vctx.dec_count];
            *((double *) vctx.val) = dbl;
            // printf("%.2lf\n", *((double *) vctx.val));
          }
        } break;
        case MST_DEC0: case MST_DEC1: case MST_DEC2:
        case MST_DEC3: case MST_DEC4: case MST_DEC5: case MST_DEC6:
        case MST_DEC7: case MST_DEC8: case MST_DEC9: {
          double dbl = static_cast<double>(i64);
          dbl /= allflic::allflic48::tens()[Parent::data_type - MST_DEC0];
          *((double *) vctx.val) = dbl;
          // printf("%.2lf\n", *((double *) vctx.val));
        } break;
      }
      i64 = *((int64_t *) vctx.val);
      if (i64 == 0 && Parent::is_null(vctx.node_id))
        *((int64_t *) vctx.val) = INT64_MIN;
      return skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      retrieve_block(node_id, vctx);
      skip_non_leaf_nodes(vctx);
      *vctx.val_len = 8;
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(8, false, true);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class uniq_bin_val_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~uniq_bin_val_retriever() {}
    __fq1 __fq2 const uint8_t *get_bin_val(uint8_t *val_loc, uintxx_t& bin_len) {
      tail_ptr_flat_map::read_len_bw(val_loc, bin_len);
      return val_loc + 1;
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if (!Parent::is_repeat(vctx)) {
        uint8_t *val_loc = Parent::get_val_loc(vctx);
        uintxx_t bin_len;
        get_bin_val(val_loc, bin_len);
        *vctx.val_len = bin_len;
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.init_pbc_vars();
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class uniq_text_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~uniq_text_retriever() {}
    __fq1 __fq2 void get_val_str(gen::byte_str& ret, uintxx_t val_ptr, uint8_t grp_no, size_t max_valset_len) {
      uint8_t *val = Parent::grp_data[grp_no];
      ret.clear();
      uint8_t *t = val + val_ptr;
      if (*t > 15 && *t < 32) {
        uintxx_t bin_len;
        tail_ptr_flat_map::read_len_bw(t++, bin_len);
        while (bin_len--)
          ret.append(*t++);
        return;
      }
      while (*t < 15 || *t > 31)
        t--;
      t++;
      while (*t < 15 || *t > 31)
        ret.append(*t++);
      if (val[val_ptr - 1] < 15 || val[val_ptr - 1] > 31)
        ret.set_length(ret.length() - (t - val - val_ptr));
      if (*t == 15)
        return;
      uintxx_t pfx_len = tail_ptr_flat_map::read_len(t);
      memmove(ret.data() + pfx_len, ret.data(), ret.length());
      ret.set_length(ret.length() + pfx_len);
      read_prefix(ret.data() + pfx_len - 1, val + val_ptr - 1, pfx_len);
    }
    __fq1 __fq2 void read_prefix(uint8_t *out_str, uint8_t *t, uintxx_t pfx_len) {
      while (*t < 15 || *t > 31)
        t--;
      while (*t != 15) {
        uintxx_t prev_pfx_len;
        t = tail_ptr_flat_map::read_len_bw(t, prev_pfx_len);
        while (*t < 15 || *t > 31)
          t--;
        while (pfx_len > prev_pfx_len) {
          *out_str-- = *(t + pfx_len - prev_pfx_len);
          pfx_len--;
        }
      }
      t--;
      while (*t < 15 || *t > 31)
        t--;
      t++;
      while (pfx_len-- > 0)
        *out_str-- = *(t + pfx_len);
    }
    __fq1 __fq2 const uint8_t *get_text_val(uint8_t *val_loc, uint8_t grp_no, uint8_t *ret_val, size_t& val_len) {
      uint8_t stack_buf[FAST_STACK_BUF];
      uint8_t* heap_buf = nullptr;
      uint8_t *val_str_buf = get_fast_buffer<uint8_t>(Parent::max_len, stack_buf, heap_buf);
      BufferGuard<uint8_t> guard(heap_buf);
      gen::byte_str val_str(val_str_buf, Parent::max_len);
      uint8_t *val_start = Parent::grp_data[grp_no];
      if (*val_start != 0)
        Parent::inner_tries[grp_no]->copy_trie_tail((uintxx_t) (val_loc - val_start), val_str);
      else
        get_val_str(val_str, (uintxx_t) (val_loc - val_start), grp_no, Parent::max_len);
      val_len = val_str.length();
      memcpy(ret_val, val_str.data(), val_len);
      return ret_val;
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if (!Parent::is_repeat(vctx)) {
        uint8_t *val_loc = Parent::get_val_loc(vctx);
        get_text_val(val_loc, vctx.grp_no, vctx.val->txt_bin, *vctx.val_len);
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.init_pbc_vars();
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class uniq_ifp_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~uniq_ifp_retriever() {}
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if (!Parent::is_repeat(vctx)) {
        uint8_t *val_loc = Parent::get_val_loc(vctx);
        int64_t i64;
        allflic::allflic48::simple_decode(val_loc, 1, &i64);
        i64 = allflic::allflic48::zigzag_decode(i64);
        // printf("i64: %lld\n", i64);
        if (Parent::data_type >= MST_DEC0 && Parent::data_type <= MST_DEC9 && i64 != INT64_MIN) {
          double dbl = static_cast<double>(i64);
          dbl /= allflic::allflic48::tens()[Parent::data_type - MST_DEC0];
          *((double *)vctx.val) = dbl;
        } else
          memcpy(vctx.val, &i64, sizeof(int64_t));
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.node_id = node_id;
      vctx.init_pbc_vars();
      *vctx.val_len = 8;
      Parent::skip_non_leaf_nodes(vctx);
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

template<char pri_key>
class delta_val_retriever : public value_retriever<pri_key> {
  public:
    using Parent = value_retriever<pri_key>;
    __fq1 __fq2 virtual ~delta_val_retriever() {}
    __fq1 __fq2 void add_delta(val_ctx& vctx) {
      if (Parent::is_repeat(vctx)) {
        vctx.i64 += vctx.i64_delta;
        return;
      }
      uint8_t *val_loc = Parent::get_val_loc(vctx);
      int64_t i64;
      if (val_loc != nullptr) {
        if (*val_loc == 0xF8 && val_loc[1] == 1) {
          i64 = INT64_MIN;
        } else {
          allflic::allflic48::simple_decode(val_loc, 1, &i64);
          i64 = allflic::allflic48::zigzag_decode(i64);
        }
        // printf("Delta node id: %u, value: %lld\n", delta_node_id, delta_val);
        vctx.i64_delta = i64;
        vctx.i64 += i64;
      }
    }
    __fq1 __fq2 bool next_val(val_ctx& vctx) {
      if ((vctx.node_id % nodes_per_bv_block_n) == 0)
        vctx.i64 = 0;
      add_delta(vctx);
      if (Parent::data_type == MST_INT || (Parent::data_type >= MST_DATE_US && Parent::data_type <= MST_DATETIME_ISOT_MS)) {
        *((int64_t *) vctx.val) = vctx.i64;
      } else {
        double dbl = static_cast<double>(vctx.i64);
        dbl /= allflic::allflic48::tens()[Parent::data_type - MST_DEC0];
        *((double *) vctx.val) = dbl;
      }
      return Parent::skip_to_next_leaf(vctx);
    }
    __fq1 __fq2 void fill_val_ctx(uintxx_t node_id, val_ctx& vctx) {
      vctx.init_pbc_vars();
      vctx.node_id = node_id / nodes_per_bv_block_n;
      vctx.node_id *= nodes_per_bv_block_n;
      Parent::load_bm(vctx.node_id, vctx);
      vctx.i64 = 0;
      while (vctx.node_id < node_id) {
        // printf("Delta Node-id: %u\n", delta_node_id);
        if (vctx.bm_mask & vctx.bm_leaf)
          add_delta(vctx);
        vctx.bm_mask <<= 1;
        vctx.node_id++;
      }
      *vctx.val_len = 8;
    }
    __fq1 __fq2 const uint8_t *get_val(uintxx_t node_id, size_t *in_size_out_value_len, mdx_val &ret_val) {
      val_ctx vctx;
      vctx.init(Parent::max_len, false);
      vctx.set_ptrs(&ret_val, in_size_out_value_len);
      fill_val_ctx(node_id, vctx);
      next_val(vctx);
      return ret_val.txt_bin;
    }
};

class cleanup_interface {
  public:
    __fq1 __fq2 virtual ~cleanup_interface() {
    }
    __fq1 __fq2 virtual void release() = 0;
};

class cleanup : public cleanup_interface {
  private:
    // __fq1 __fq2 cleanup(cleanup const&);
    // __fq1 __fq2 cleanup& operator=(cleanup const&);
    uint8_t *bytes;
  public:
    __fq1 __fq2 cleanup() {
    }
    __fq1 __fq2 virtual ~cleanup() {
    }
    __fq1 __fq2 void release() {
      delete [] bytes;
    }
    __fq1 __fq2 void init(uint8_t *_bytes) {
      bytes = _bytes;
    }
};

#define MDX_REV_ST_INIT 0
#define MDX_REV_ST_NEXT 1
#define MDX_REV_ST_END 2
struct rev_nodes_ctx : public input_ctx, public iter_ctx {
  const uint8_t *rev_node_list;
  size_t rev_nl_len;
  uintxx_t rev_nl_pos;
  uintxx_t prev_node_id;
  uint8_t rev_state;
  uint8_t trie_no;
  __fq1 __fq2 rev_nodes_ctx() {
    reset();
  }
  __fq1 __fq2 void init() {
    reset();
  }
  __fq1 __fq2 void init(static_trie *trie) {
    if (is_allocated)
      this->close();
    ((iter_ctx *) this)->init((uint16_t) trie->get_max_key_len(), (uint16_t) trie->get_max_level());
  }
  __fq1 __fq2 void reset() {
    rev_node_list = nullptr;
    prev_node_id = 0;
    node_id = 0;
    trie_no = 0;
    rev_state = MDX_REV_ST_INIT;
  }
};

class static_trie_map : public static_trie {
  private:
    // __fq1 __fq2 static_trie_map(static_trie_map const&); // todo: restore? can't return because of this
    // __fq1 __fq2 static_trie_map& operator=(static_trie_map const&);
    value_retriever_base **val_map;
    uint16_t val_count;
    uint16_t pk_col_count;
    uintxx_t max_val_len;
    uint8_t *names_loc;
    char *names_start;
    const char *column_encoding;
    cleanup_interface *cleanup_object;
    bool is_mmapped;
    size_t trie_size;
  public:
    __fq1 __fq2 static_trie_map() {
      val_map = nullptr;
      is_mmapped = false;
      cleanup_object = nullptr;
    }
    __fq1 __fq2 ~static_trie_map() {
#if !defined(ESP32)
      if (is_mmapped)
        map_unmap();
#endif
      if (trie_bytes != nullptr) {
        if (cleanup_object != nullptr) {
          cleanup_object->release();
          delete cleanup_object;
          cleanup_object = nullptr;
        }
      }
      if (val_map != nullptr) {
        for (size_t i = 0; i < val_count; i++) {
          delete val_map[i];
        }
        delete [] val_map;
      }
    }

    __fq1 __fq2 void set_cleanup_object(cleanup_interface *_cleanup_obj) {
      cleanup_object = _cleanup_obj;
    }

    __fq1 __fq2 inner_trie_fwd *new_instance(uint8_t *mem) {
      // Where is this released?
      static_trie_map *it = new static_trie_map();
      it->trie_level = mem[TRIE_LEVEL_LOC];
      it->load_from_mem(mem, 0);
      return it;
    }

    __fq1 __fq2 bool get(input_ctx& in_ctx, size_t *in_size_out_value_len, mdx_val &val) {
      bool is_found = lookup(in_ctx);
      if (is_found) {
        if (val_count > 1)
          val_map[1]->get_val(in_ctx.node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
    }

    __fq1 __fq2 const bool next_col_val(int col_val_idx, val_ctx& vctx) {
      return val_map[col_val_idx]->next_val(vctx);
    }

    __fq1 __fq2 const uint8_t *get_col_val(uintxx_t node_id, size_t col_val_idx,
          size_t *in_size_out_value_len, mdx_val &val) {
      if (col_val_idx < pk_col_count) { // TODO: Extract from composite keys,Convert numbers back
        if (!reverse_lookup_from_node_id(node_id, in_size_out_value_len, val.txt_bin))
          return nullptr;
        if (pk_col_count > 1) {
          size_t val_pos;
          uintxx_t val_len;
          char col_data_type = get_column_type(col_val_idx);
          val_pos = extract_from_composite(val.txt_bin, *in_size_out_value_len, col_val_idx, col_data_type, val_len);
          if (val_pos > 0) memmove(val.txt_bin, val.txt_bin + val_pos, val_len);
          *in_size_out_value_len = val_len;
        }
        cmn::convert_back(get_column_type(col_val_idx), val.txt_bin, val, in_size_out_value_len);
        return (const uint8_t *) val.txt_bin;
      }
      return val_map[col_val_idx]->get_val(node_id, in_size_out_value_len, val);
    }

    __fq1 __fq2 const char *get_table_name() {
      return names_start + cmn::read_uint16(names_loc + 2);
    }

    __fq1 __fq2 size_t get_column_size(size_t i) {
      uint8_t *val_table_loc = trie_bytes + cmn::read_uint64(trie_bytes + VAL_TBL_LOC);
      size_t col_start = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
      if (col_start == 0)
        col_start = cmn::read_uint64(val_table_loc);
      size_t col_end = trie_size;
      if (col_end == 0)
        col_end = col_start;
      while (++i < val_count && col_end == 0)
        col_end = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
      return col_end - col_start;
    }

    __fq1 __fq2 const char *get_column_name(size_t i) {
      return names_start + cmn::read_uint16(names_loc + (i + 2) * 2);
    }

    __fq1 __fq2 const char *get_column_types() {
      return names_start;
    }

    __fq1 __fq2 char get_column_type(size_t i) {
      return names_start[i];
    }

    __fq1 __fq2 const char *get_column_encodings() {
      return column_encoding;
    }

    __fq1 __fq2 char get_column_encoding(size_t i) {
      return column_encoding[i];
    }

    __fq1 __fq2 uintxx_t get_max_val_len() {
      return max_val_len;
    }

    __fq1 __fq2 uintxx_t get_max_val_len(size_t col_val_idx) {
      return val_map[col_val_idx]->max_len;
    }

    __fq1 __fq2 static_trie *get_col_trie(size_t col_val_idx) {
      return val_map[col_val_idx]->get_col_trie();
    }

    __fq1 __fq2 static_trie_map *get_col_trie_map(size_t col_val_idx) {
      return (static_trie_map *) val_map[col_val_idx]->get_col_trie();
    }

    __fq1 __fq2 static_trie_map **get_trie_groups(size_t col_val_idx) {
      inner_trie_fwd **inner_tries = val_map[col_val_idx]->inner_tries;
      return (static_trie_map **) inner_tries;
    }

    __fq1 __fq2 size_t get_trie_start_group(size_t col_val_idx) {
      return val_map[col_val_idx]->inner_trie_start_grp;
    }

    __fq1 __fq2 size_t get_group_count(size_t col_val_idx) {
      return val_map[col_val_idx]->group_count;
    }

    __fq1 __fq2 uint16_t get_column_count() {
      return val_count;
    }

    __fq1 __fq2 uint16_t get_pk_col_count() {
      return pk_col_count;
    }

    __fq1 __fq2 value_retriever_base *get_value_retriever(size_t col_val_idx) {
      return val_map[col_val_idx];
    }

    __fq1 __fq2 block_retriever_base *get_block_retriever(size_t col_val_idx) {
      if (get_column_encoding(col_val_idx) != MSE_VINTGB)
        return nullptr;
      value_retriever_base *val_retriever = val_map[col_val_idx];
      block_retriever_base *inst;
      switch (val_retriever->data_type) {
        case MST_INT:
        case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
        case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
        case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS: {
          inst = new fast_vint_block_retriever<'i', int64_t>(val_retriever);
        } break;
        case MST_DECV: {
          inst = new fast_vint_block_retriever<'.', double>(val_retriever);
        } break;
        case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
        case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9: {
          inst = new fast_vint_block_retriever<'x', double>(val_retriever);
        } break;
      }
      return inst;
    }

    typedef bool (*emit_node_id_func)(void *, uintxx_t);
    static __fq1 __fq2 void emit_rev_nids(static_trie_map *rev_trie_map, uintxx_t ct_node_id,
            emit_node_id_func emit_nid_func, void *cb_ctx = nullptr) {
      mdx_val ct_val;
      size_t out_rev_len;
      const uint8_t *ct_rev_key = rev_trie_map->get_col_val(ct_node_id, 1, &out_rev_len, ct_val); // , &ptr_count[col_val_idx]);
      // printf("Out rev len: %zu\n", out_rev_len);
      uintxx_t prev_nid = 0;
      for (size_t ct_nid_ctr = 0; ct_nid_ctr < out_rev_len;) {
         size_t nid_len;
         uintxx_t nid_start = gen::read_vint32(ct_rev_key + ct_nid_ctr, &nid_len);
         nid_start += (prev_nid << 1);
         prev_nid = (nid_start >> 1) + 1;
        //  printf("main nid: %u, len: %lu, ", nid_start >> 1, nid_len);
         if (emit_nid_func(cb_ctx, nid_start >> 1)) return;
         ct_nid_ctr += nid_len;
         if (nid_start & 1) {
            uintxx_t nid_end = gen::read_vint32(ct_rev_key + ct_nid_ctr, &nid_len);
            nid_end += prev_nid;
            nid_end++;
            prev_nid = nid_end;
            nid_start >>= 1;
            nid_start++;
            // printf("end nid: %u, len: %lu", nid_end, nid_len);
            while (nid_start < nid_end) {
               if (emit_nid_func(cb_ctx, nid_start)) return;
               nid_start++;
            }
            ct_nid_ctr += nid_len;
         }
         // printf("\n");
      }
    }

    __fq1 __fq2 void shortlist_word_records(int col_idx, const char *word, size_t word_len,
            emit_node_id_func emit_nid_func, void *cb_ctx = nullptr) {
      static_trie_map **stm_rev_tries = get_trie_groups(col_idx);
      size_t trie_no = get_trie_start_group(col_idx);
      size_t group_count = get_group_count(col_idx);
      // printf("Group count: %zu\n", group_count);
      // printf("Longest word: %zu, %.*s\n", sr.max_word_len, sr.max_word_len, longest_word);
      static_trie_map *cur_trie = nullptr;
      while (trie_no < group_count) {
        cur_trie = stm_rev_tries[trie_no];
        iter_ctx it_ctx;
        it_ctx.init(cur_trie->get_max_key_len(), cur_trie->get_max_level());
        cur_trie->find_first((const uint8_t *) word, word_len, it_ctx, true);
        int trie_key_len = cur_trie->next(it_ctx);
        while (trie_key_len != -2 && word_len <= it_ctx.key_len && memcmp(word, it_ctx.key, word_len) == 0) {
          // printf("Trie key: %.*s, len: %zu\n", it_ctx.key_len, (const char *) it_ctx.key, it_ctx.key_len);
          uintxx_t node_id = it_ctx.node_path[it_ctx.cur_idx];
          emit_rev_nids(cur_trie, node_id, emit_nid_func, cb_ctx);
          trie_key_len = cur_trie->next(it_ctx);
        }
        trie_no++;
      }
    }

    __fq1 __fq2 bool read_rev_node_list(uintxx_t ct_node_id, rev_nodes_ctx& rev_ctx) {
      mdx_val mv;
      rev_ctx.rev_node_list = get_col_val(ct_node_id, 1, &rev_ctx.rev_nl_len, mv);
      printf("rev_nl_len: %zu\n", rev_ctx.rev_nl_len);
      if (rev_ctx.rev_node_list == nullptr || rev_ctx.rev_nl_len == 0) {
        rev_ctx.rev_state = MDX_REV_ST_END;
        return false;
      }
      size_t nid_len;
      rev_ctx.rev_nl_pos = 0;
      rev_ctx.node_id = cmn::read_vint32(rev_ctx.rev_node_list, &nid_len) >> 1;
      printf("rev_node id: %" PRIuXX "\n", rev_ctx.node_id);
      rev_ctx.prev_node_id = 0;
      return true;
    }

    __fq1 __fq2 bool get_next_rev_node_id(rev_nodes_ctx& rev_ctx) {
      size_t nid_len;
      uintxx_t nid_start = cmn::read_vint32(rev_ctx.rev_node_list + rev_ctx.rev_nl_pos, &nid_len);
      uintxx_t nid_end = 0;
      bool has_end = (nid_start & 1);
      nid_start = (nid_start >> 1) + rev_ctx.prev_node_id;
      if (has_end) {
        size_t nid_end_len;
        nid_end = cmn::read_vint32(rev_ctx.rev_node_list + rev_ctx.rev_nl_pos + nid_len, &nid_end_len);
        nid_end += nid_start;
        nid_end++;
        nid_len += nid_end_len;
      } else
        nid_end = nid_start;
      printf("nid_start: %" PRIuXX ", nid_end: %" PRIuXX ", prev_nid: %" PRIuXX ", nid: %" PRIuXX "\n", nid_start, nid_end, rev_ctx.prev_node_id, rev_ctx.node_id);
      rev_ctx.node_id++;
      if (rev_ctx.node_id > nid_end) {
        rev_ctx.rev_nl_pos += nid_len;
        rev_ctx.prev_node_id = nid_start + 1;
        if (has_end)
          rev_ctx.prev_node_id = nid_end + 1;
        printf("rev_nl_len: %zu, pos: %" PRIuXX "\n", rev_ctx.rev_nl_len, rev_ctx.rev_nl_pos);
        if (rev_ctx.rev_nl_pos >= rev_ctx.rev_nl_len) {
          rev_ctx.node_id = UINTXX_MAX;
          return false;
        }
        nid_start = cmn::read_vint32(rev_ctx.rev_node_list + rev_ctx.rev_nl_pos, &nid_len);
        rev_ctx.node_id = (nid_start >> 1) + rev_ctx.prev_node_id;
      }
      return true;
    }

    __fq1 __fq2 bool rev_trie_next(rev_nodes_ctx& rev_ctx) {
      if (rev_ctx.rev_state == MDX_REV_ST_END)
        return false;
      if (rev_ctx.rev_state == MDX_REV_ST_INIT) {
        rev_ctx.init(this);
        rev_ctx.rev_state = MDX_REV_ST_NEXT;
        input_ctx *in_ctx = &rev_ctx;
        printf("key: %" PRIuXX ", [%.*s]\n", in_ctx->key_len, (int) in_ctx->key_len, in_ctx->key);
        uintxx_t ct_node_id = find_first(in_ctx->key, in_ctx->key_len, rev_ctx, true);
        printf("ct node id: %" PRIuXX "\n", ct_node_id);
        if (ct_node_id == 0) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        int res = next(rev_ctx);
        printf("res: %d\n", res);
        if (res == -2) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        iter_ctx *it_ctx = &rev_ctx;
        printf("found first: %d, [%.*s]\n", it_ctx->key_len, (int) it_ctx->key_len, it_ctx->key);
        if (it_ctx->key_len < in_ctx->key_len ||
            cmn::memcmp(in_ctx->key, it_ctx->key, cmn::min(it_ctx->key_len, in_ctx->key_len)) != 0) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        ct_node_id = rev_ctx.node_path[rev_ctx.cur_idx];
        printf("ct node id: %" PRIuXX "\n", ct_node_id);
        if (get_column_count() == 1)
          return true;
        if (!read_rev_node_list(ct_node_id, rev_ctx))
          return false;
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        return true;
      } else {
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        if (get_column_count() == 2) {
          if (get_next_rev_node_id(rev_ctx))
            return true;
        }
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        int res = next(rev_ctx);
        if (res == -2) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        input_ctx *in_ctx = &rev_ctx;
        iter_ctx *it_ctx = &rev_ctx;
        printf("found next: %d, [%.*s]\n", it_ctx->key_len, (int) it_ctx->key_len, it_ctx->key);
        if (it_ctx->key_len < in_ctx->key_len ||
            cmn::memcmp(in_ctx->key, it_ctx->key, cmn::min(it_ctx->key_len, in_ctx->key_len)) != 0) {
          rev_ctx.rev_state = MDX_REV_ST_END;
          return false;
        }
        uintxx_t ct_node_id = rev_ctx.node_path[rev_ctx.cur_idx];
        if (!read_rev_node_list(ct_node_id, rev_ctx))
          return false;
        printf("rev_ctx node_id: %" PRIuXX "\n", rev_ctx.node_id);
        return true;
      }
      return false;
    }

    __fq1 __fq2 void map_from_memory(uint8_t *mem) {
      load_from_mem(mem, 0);
    }

    __fq1 __fq2 void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

#if !defined(ESP32)
    __fq1 __fq2 uint8_t* map_file(const char* filename, off_t& sz) {
    #ifdef _WIN32
      HANDLE hFile = CreateFileA(filename, GENERIC_READ, FILE_SHARE_READ,
                      NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
      if (hFile == INVALID_HANDLE_VALUE) {
        fprintf(stderr, "CreateFileA failed: %lu\n", GetLastError());
        return NULL;
      }
      LARGE_INTEGER filesize;
      if (!GetFileSizeEx(hFile, &filesize)) {
        fprintf(stderr, "GetFileSizeEx failed: %lu\n", GetLastError());
        CloseHandle(hFile);
        return NULL;
      }
      sz = static_cast<off_t>(filesize.QuadPart);
      HANDLE hMap = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
      if (!hMap) {
        fprintf(stderr, "CreateFileMappingA failed: %lu\n", GetLastError());
        CloseHandle(hFile);
        return NULL;
      }
      uint8_t* map_buf = (uint8_t*)MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);
      if (!map_buf) {
        fprintf(stderr, "MapViewOfFile failed: %lu\n", GetLastError());
        CloseHandle(hMap);
        CloseHandle(hFile);
        return NULL;
      }
      CloseHandle(hMap);
      CloseHandle(hFile);
      sz = filesize;
      trie_size = sz;
      return map_buf;
    #else
      FILE* fp = fopen(filename, "rb");
      if (!fp) {
        perror("fopen");
        return NULL;
      }
      int fd = fileno(fp);
      struct stat buf;
      if (fstat(fd, &buf) < 0) {
        perror("fstat");
        fclose(fp);
        return NULL;
      }
      sz = buf.st_size;
      uint8_t* map_buf = (uint8_t*)mmap(0, sz, PROT_READ, MAP_PRIVATE, fd, 0);
      if (map_buf == MAP_FAILED) {
        perror("mmap");
        fclose(fp);
        return NULL;
      }
      fclose(fp);
      trie_size = sz;
      return map_buf;
    #endif
    }

    __fq1 __fq2 bool map_file_to_mem(const char *filename) {
      off_t dict_size;
      trie_bytes = map_file(filename, dict_size);
      if (trie_bytes == nullptr) return false;
      //       int len_will_need = (dict_size >> 2);
      //       //madvise(trie_bytes, len_will_need, MADV_WILLNEED);
      // #ifndef _WIN32
      //       mlock(trie_bytes, len_will_need);
      // #endif
      load_into_vars();
      is_mmapped = true;
      return true;
    }

    __fq1 __fq2 void map_unmap() {
    #ifdef _WIN32
      if (trie_bytes) {
        if (!UnmapViewOfFile(trie_bytes)) {
          fprintf(stderr, "UnmapViewOfFile failed: %lu\n", GetLastError());
        }
      }
    #else
      if (trie_bytes) {
        // munlock(trie_bytes, dict_size >> 2);  // optional; ignore errors
        int err = munmap(trie_bytes, trie_size);
        if (err != 0) {
          perror("munmap");
          printf("UnMapping trie_bytes Failed\n");
        }
      }
    #endif
      trie_bytes = nullptr;
      is_mmapped = false;
    }
#endif

    __fq1 __fq2 void load_from_mem(uint8_t *mem, size_t sz) {
      trie_bytes = mem;
      trie_size = sz;
      cleanup_object = nullptr;
      load_into_vars();
    }

    size_t get_size() {
      return trie_size;
    }

    __fq1 __fq2 void load(const char* filename) {

      init_vars();
      struct stat file_stat;
      memset(&file_stat, 0, sizeof(file_stat));
      stat(filename, &file_stat);
      trie_bytes = new uint8_t[file_stat.st_size];
      cleanup_object = new cleanup();
      ((cleanup *)cleanup_object)->init(trie_bytes);

      FILE *fp = fopen(filename, "rb");
      if (fp == nullptr) {
        printf("fopen failed: %s (errno: %d), %ld\n", strerror(errno), errno, (long) file_stat.st_size);
        #ifdef __CUDA_ARCH__
        return;
        #else
        throw errno;
        #endif
      }
      size_t bytes_read = fread(trie_bytes, 1, file_stat.st_size, fp);
      if (bytes_read != file_stat.st_size) {
        printf("Read error: [%s], %zu, %zu\n", filename, (size_t) file_stat.st_size, bytes_read);
        #ifdef __CUDA_ARCH__
        return;
        #else
        throw errno;
        #endif
      }
      fclose(fp);
      trie_size = bytes_read;

      // int len_will_need = (dict_size >> 1);
      // #ifndef _WIN32
      //       mlock(trie_bytes, len_will_need);
      // #endif
      //madvise(trie_bytes, len_will_need, MADV_WILLNEED);
      //madvise(trie_bytes + len_will_need, dict_size - len_will_need, MADV_RANDOM);

      load_into_vars();

    }

    value_retriever_base *new_value_retriever(size_t col_idx, char data_type, char encoding_type) {
      value_retriever_base *val_retriever = nullptr;
      if (col_idx < pk_col_count) {
        encoding_type = MSE_TRIE;
        val_retriever = new main_trie_retriever<'Y'>(col_idx, pk_col_count);
        return val_retriever;
      }
      switch (encoding_type) {
        case MSE_TRIE: case MSE_TRIE_2WAY:
          if (pk_col_count > 0)
            val_retriever = new col_trie_retriever<'Y'>();
          else
            val_retriever = new col_trie_retriever<'N'>();
          break;
        case MSE_WORDS: case MSE_WORDS_2WAY:
          if (pk_col_count > 0)
            val_retriever = new words_retriever<'Y'>();
          else
            val_retriever = new words_retriever<'N'>();
          break;
        case MSE_VINTGB:
          if (pk_col_count > 0)
            val_retriever = new fast_vint_retriever<'Y'>();
          else
            val_retriever = new fast_vint_retriever<'N'>();
          break;
        case MSE_STORE:
          if (pk_col_count > 0)
            val_retriever = new stored_val_retriever<'Y'>();
          else
            val_retriever = new stored_val_retriever<'N'>();
          break;
        default:
          switch (data_type) {
            case MST_BIN:
              if (pk_col_count > 0)
                val_retriever = new uniq_bin_val_retriever<'Y'>();
              else
                val_retriever = new uniq_bin_val_retriever<'N'>();
              break;
            case MST_TEXT:
              if (pk_col_count > 0)
                val_retriever = new uniq_text_retriever<'Y'>();
              else
                val_retriever = new uniq_text_retriever<'N'>();
              break;
            case MST_INT:
            case MST_DECV:
            case MST_DEC0: case MST_DEC1: case MST_DEC2: case MST_DEC3: case MST_DEC4:
            case MST_DEC5: case MST_DEC6: case MST_DEC7: case MST_DEC8: case MST_DEC9:
            case MST_DATE_US: case MST_DATE_EUR: case MST_DATE_ISO:
            case MST_DATETIME_US: case MST_DATETIME_EUR: case MST_DATETIME_ISO:
            case MST_DATETIME_ISOT: case MST_DATETIME_ISOT_MS:
              if (encoding_type == MSE_DICT_DELTA) {
                if (pk_col_count > 0)
                  val_retriever = new delta_val_retriever<'Y'>();
                else
                  val_retriever = new delta_val_retriever<'N'>();
              } else {
                if (pk_col_count > 0)
                  val_retriever = new uniq_ifp_retriever<'Y'>();
                else
                  val_retriever = new uniq_ifp_retriever<'N'>();
              }
              break;
        }
      }
      return val_retriever;
    }
    __fq1 __fq2 void load_into_vars() {
      load_static_trie();
      pk_col_count = trie_bytes[PK_COL_COUNT_LOC];
      val_count = cmn::read_uint64(trie_bytes + VAL_COUNT_LOC);
      max_val_len = cmn::read_uint64(trie_bytes + MAX_VAL_LEN_LOC);

      uint64_t *tf_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TRIE_FLAGS_LOC));

      names_loc = trie_bytes + cmn::read_uint64(trie_bytes + NAMES_LOC);
      uint8_t *val_table_loc = trie_bytes + cmn::read_uint64(trie_bytes + VAL_TBL_LOC);
      // printf("Val table loc: %lu\n", val_table_loc - trie_bytes);
      names_start = (char *) names_loc + (val_count + 2) * sizeof(uint16_t);
      column_encoding = names_start + cmn::read_uint16(names_loc);

      val_map = new value_retriever_base*[val_count]();
      for (size_t i = 0; i < val_count; i++) {
        val_map[i] = new_value_retriever(i, get_column_type(i), get_column_encoding(i));
        if (i < pk_col_count) {
          uint8_t multiplier = opts->trie_leaf_count > 0 ? 4 : 3;
          uint64_t *tf_ptr_loc = (uint64_t *) (trie_bytes + cmn::read_uint64(trie_bytes + TAIL_FLAGS_PTR_LOC));
          uint8_t *trie_tail_ptrs_data_loc = trie_bytes + cmn::read_uint64(trie_bytes + TRIE_TAIL_PTRS_DATA_LOC);
          uint8_t *tails_loc = trie_tail_ptrs_data_loc + 16;
          val_map[i]->init(this, trie_loc, tf_loc + TF_LEAF, trie_level == 0 ? multiplier : 1, tf_ptr_loc, tails_loc, key_count, node_count);
          val_map[i]->data_type = get_column_type(i);
          val_map[i]->encoding_type = get_column_encoding(i);
        } else {
          uint64_t vl64 = cmn::read_uint64(val_table_loc + i * sizeof(uint64_t));
          // printf("Val idx: %lu, %llu\n", i, vl64);
          uint8_t *val_loc = trie_bytes + vl64;
          if (val_loc == trie_bytes)
            continue;
          val_map[i]->init(this, trie_loc, tf_loc + TF_LEAF, opts->trie_leaf_count > 0 ? 4 : 3, nullptr, val_loc, key_count, node_count);
        }
      }
    }
};

}}

#endif
