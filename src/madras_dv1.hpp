#ifndef STATIC_DICT_H
#define STATIC_DICT_H

#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/stat.h> 
#include <sys/types.h>
//#include <immintrin.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif
#include <stdint.h>

namespace madras_dv1 {

#define DCT_INSERT_AFTER -2
#define DCT_INSERT_BEFORE -3
#define DCT_INSERT_LEAF -4
#define DCT_INSERT_EMPTY -5
#define DCT_INSERT_THREAD -6
#define DCT_INSERT_CONVERT -7
#define DCT_INSERT_CHILD_LEAF -8

#define DCT_BIN '*'
#define DCT_TEXT 't'
#define DCT_FLOAT 'f'
#define DCT_DOUBLE 'd'
#define DCT_S64_INT '0'
#define DCT_S64_DEC1 '1'
#define DCT_S64_DEC2 '2'
#define DCT_S64_DEC3 '3'
#define DCT_S64_DEC4 '4'
#define DCT_S64_DEC5 '5'
#define DCT_S64_DEC6 '6'
#define DCT_S64_DEC7 '7'
#define DCT_S64_DEC8 '8'
#define DCT_S64_DEC9 '9'
#define DCT_U64_INT 'i'
#define DCT_U64_DEC1 'j'
#define DCT_U64_DEC2 'k'
#define DCT_U64_DEC3 'l'
#define DCT_U64_DEC4 'm'
#define DCT_U64_DEC5 'n'
#define DCT_U64_DEC6 'o'
#define DCT_U64_DEC7 'p'
#define DCT_U64_DEC8 'q'
#define DCT_U64_DEC9 'r'
#define DCT_U15_DEC1 'X'
#define DCT_U15_DEC2 'Z'

#define bm_init_mask 0x0000000000000001UL
#define sel_divisor 512
#define nodes_per_bv_block 256
#define bytes_per_bv_block 384
#define nodes_per_bv_block3 64
#define bytes_per_bv_block3 96

struct cache {
  uint8_t parent_node_id1;
  uint8_t parent_node_id2;
  uint8_t parent_node_id3;
  uint8_t node_offset;
  uint8_t child_node_id1;
  uint8_t child_node_id2;
  uint8_t child_node_id3;
  uint8_t node_byte;
};

struct min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
};

class byte_str {
  int max_len;
  int len;
  uint8_t *buf;
  public:
    byte_str() {
    }
    byte_str(uint8_t *_buf, int _max_len) {
      set_buf_max_len(_buf, _max_len);
    }
    void set_buf_max_len(uint8_t *_buf, int _max_len) {
      len = 0;
      buf = _buf;
      max_len = _max_len;
    }
    void append(uint8_t b) {
      if (len >= max_len)
        return;
      buf[len++] = b;
    }
    void append(uint8_t *b, size_t blen) {
      size_t start = 0;
      while (len < max_len && start < blen) {
        buf[len++] = *b++;
        start++;
      }
    }
    uint8_t *data() {
      return buf;
    }
    uint8_t operator[](uint32_t idx) const {
      return buf[idx];
    }
    size_t length() {
      return len;
    }
    void clear() {
      len = 0;
    }
};

static bool is_dict_print_enabled = false;
static void dict_printf(const char* format, ...) {
  if (!is_dict_print_enabled)
    return;
  va_list args;
  va_start(args, format);
  vprintf(format, args);
  va_end(args);
}

constexpr double dbl_div[] = {1.0000000000001, 10.0000000000001, 100.0000000000001, 1000.0000000000001, 10000.0000000000001, 100000.0000000000001, 1000000.0000000000001, 10000000.0000000000001, 100000000.0000000000001, 1000000000.0000000000001};
class cmn {
  public:
    static int compare(const uint8_t *v1, int len1, const uint8_t *v2,
            int len2, int k = 0) {
        int lim = (len2 < len1 ? len2 : len1);
        do {
          if (v1[k] != v2[k])
            return ++k;
        } while (++k < lim);
        if (len1 == len2)
          return 0;
        return ++k;
    }
    static uint32_t read_uintx(uint8_t *ptr, uint32_t mask) {
      uint32_t ret = *((uint32_t *) ptr);
      return ret & mask; // faster endian dependent
    }
    static uint32_t read_uint32(uint8_t *ptr) {
      return *((uint32_t *) ptr); // faster endian dependent
      // uint32_t ret = 0;
      // int i = 4;
      // while (i--) {
      //   ret <<= 8;
      //   ret += *pos++;
      // }
      // return ret;
    }
    static uint32_t read_uint24(uint8_t *ptr) {
      return *((uint32_t *) ptr) & 0x00FFFFFF; // faster endian dependent
      // uint32_t ret = *ptr++;
      // ret |= (*ptr++ << 8);
      // ret |= (*ptr << 16);
      // return ret;
    }
    static uint32_t read_uint16(uint8_t *ptr) {
      return *((uint16_t *) ptr); // faster endian dependent
      // uint32_t ret = *ptr++;
      // ret |= (*ptr << 8);
      // return ret;
    }
    static uint8_t *read_uint64(uint8_t *t, uint64_t& u64) {
      u64 = *((uint64_t *) t); // faster endian dependent
      return t + 8;
      // u64 = 0;
      // for (int v = 0; v < 8; v++) {
      //   u64 <<= 8;
      //   u64 |= *t++;
      // }
      // return t;
    }
    static uint32_t read_vint32(const uint8_t *ptr, int8_t *vlen) {
      uint32_t ret = 0;
      int8_t len = 5; // read max 5 bytes
      do {
        ret <<= 7;
        ret += *ptr & 0x7F;
        len--;
      } while ((*ptr++ >> 7) && len);
      *vlen = 5 - len;
      return ret;
    }
    static int read_svint60_len(uint8_t *ptr) {
      return 1 + ((*ptr >> 4) & 0x07);
    }
    static int64_t read_svint60(uint8_t *ptr) {
      int64_t ret = *ptr & 0x0F;
      bool is_neg = true;
      if (*ptr & 0x80)
        is_neg = false;
      int len = (*ptr >> 4) & 0x07;
      while (len--) {
        ret <<= 8;
        ptr++;
        ret |= *ptr;
      }
      return is_neg ? -ret : ret;
    }
    static int read_svint61_len(uint8_t *ptr) {
      return 1 + (*ptr >> 5);
    }
    static uint64_t read_svint61(uint8_t *ptr) {
      uint64_t ret = *ptr & 0x1F;
      int len = (*ptr >> 5);
      while (len--) {
        ret <<= 8;
        ptr++;
        ret |= *ptr;
      }
      return ret;
    }
    static int read_svint15_len(uint8_t *ptr) {
      return 1 + (*ptr >> 7);
    }
    static uint64_t read_svint15(uint8_t *ptr) {
      uint64_t ret = *ptr & 0x7F;
      int len = (*ptr >> 7);
      while (len--) {
        ret <<= 8;
        ptr++;
        ret |= *ptr;
      }
      return ret;
    }
    static size_t pow10(int p) {
      return dbl_div[p];
    }
    static uint32_t min(uint32_t v1, uint32_t v2) {
      return v1 < v2 ? v1 : v2;
    }
};

struct ctx_vars {
  uint8_t *t;
  uint32_t node_id, child_count;
  uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
  byte_str tail;
  uint32_t tail_ptr;
  uint32_t ptr_bit_count;
  uint8_t grp_no;
  ctx_vars() {
    memset(this, '\0', sizeof(*this));
    ptr_bit_count = UINT32_MAX;
  }
  static uint8_t *read_flags(uint8_t *t, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr) {
    t = cmn::read_uint64(t, bm_leaf);
    t = cmn::read_uint64(t, bm_term);
    t = cmn::read_uint64(t, bm_child);
    return cmn::read_uint64(t, bm_ptr);
  }
  void read_flags() {
    t = cmn::read_uint64(t, bm_leaf);
    t = cmn::read_uint64(t, bm_term);
    t = cmn::read_uint64(t, bm_child);
    t = cmn::read_uint64(t, bm_ptr);
  }
  void read_flags_block_begin() {
    if ((node_id % nodes_per_bv_block3) == 0) {
      bm_mask = bm_init_mask;
      read_flags();
    }
  }
  void init_cv_nid0(uint8_t *trie_loc) {
    read_flags();
    bm_mask = bm_init_mask;
    ptr_bit_count = 0;
  }
  void init_cv_from_node_id(uint8_t *trie_loc) {
    t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
    if (node_id % nodes_per_bv_block3) {
      read_flags();
      t += node_id % nodes_per_bv_block3;
    }
    bm_mask = bm_init_mask << (node_id % nodes_per_bv_block3);
  }
};

class grp_ptr_data_map {
  private:
    uint8_t ptr_lkup_tbl_ptr_width;
    uint8_t *ptr_lookup_tbl_loc;
    uint32_t ptr_lkup_tbl_mask;
    uint8_t *ptrs_loc;
    uint8_t start_bits;
    uint8_t idx_step_bits;
    int8_t grp_idx_limit;
    uint8_t group_count;
    uint8_t *grp_data_loc;
    uint32_t two_byte_data_count;
    uint32_t idx2_ptr_count;
    uint8_t *two_byte_data_loc;
    uint8_t *code_lookup_tbl;
    uint8_t **grp_data;

    uint8_t *dict_buf;
    uint8_t *trie_loc;
    uint32_t node_count;

    int idx_map_arr[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint8_t *idx2_ptrs_map_loc;
    uint8_t idx2_ptr_size;
    uint32_t idx_ptr_mask;
    uint32_t read_ptr_from_idx(uint32_t grp_no, uint32_t ptr) {
      int idx_map_start = idx_map_arr[grp_no];
      ptr = cmn::read_uintx(idx2_ptrs_map_loc + idx_map_start + ptr * idx2_ptr_size, idx_ptr_mask);
      return ptr;
    }

    uint32_t scan_ptr_bits_tail(uint32_t node_id, uint8_t *t, uint32_t ptr_bit_count) {
      uint64_t bm_mask = bm_init_mask;
      uint64_t bm_ptr;
      t = cmn::read_uint64(t + 24, bm_ptr);
      uint8_t *t_upto = t + (node_id % 64);
      while (t < t_upto) {
        if (bm_ptr & bm_mask)
          ptr_bit_count += code_lookup_tbl[*t * 2];
        bm_mask <<= 1;
        t++;
      }
      return ptr_bit_count;
    }

    uint32_t scan_ptr_bits_val(uint32_t node_id, uint8_t *t, uint32_t ptr_bit_count) {
      uint64_t bm_mask = bm_init_mask;
      uint64_t bm_leaf;
      cmn::read_uint64(t, bm_leaf);
      t += 32;
      uint8_t *t_upto = t + (node_id % 64);
      while (t < t_upto) {
        if (bm_leaf & bm_mask) {
          uint8_t code = read_ptr_bits8(node_id, ptr_bit_count);
          ptr_bit_count += code_lookup_tbl[code * 2];
        }
        bm_mask <<= 1;
        t++;
      }
      return ptr_bit_count;
    }

    #define nodes_per_ptr_block 256
    #define nodes_per_ptr_block3 64
    #define bytes_per_ptr_block3 96
    uint8_t *get_ptr_block_t(uint32_t node_id, uint32_t& ptr_bit_count) {
      uint8_t *block_ptr = ptr_lookup_tbl_loc + (node_id / nodes_per_ptr_block) * ptr_lkup_tbl_ptr_width;
      ptr_bit_count = cmn::read_uintx(block_ptr, ptr_lkup_tbl_mask);
      int pos = (node_id / nodes_per_ptr_block3) % 4;
      if (pos) {
        pos--;
        uint8_t *ptr3 = block_ptr + (ptr_lkup_tbl_ptr_width - 6) + pos * 2;
        ptr_bit_count += cmn::read_uint16(ptr3);
      }
      return trie_loc + (node_id / nodes_per_ptr_block3) * bytes_per_ptr_block3;
    }

    uint32_t get_ptr_bit_count_tail(uint32_t node_id) {
      uint32_t ptr_bit_count;
      uint8_t *t = get_ptr_block_t(node_id, ptr_bit_count);
      return scan_ptr_bits_tail(node_id, t, ptr_bit_count);
    }

    uint32_t get_ptr_bit_count_val(uint32_t node_id) {
      uint32_t ptr_bit_count;
      uint8_t *t = get_ptr_block_t(node_id, ptr_bit_count);
      return scan_ptr_bits_val(node_id, t, ptr_bit_count);
    }

    uint8_t read_ptr_bits8(uint32_t node_id, uint32_t& ptr_bit_count) {
      uint8_t *ptr_loc = ptrs_loc + ptr_bit_count / 8;
      uint8_t bits_filled = (ptr_bit_count % 8);
      uint8_t ret = (*ptr_loc++ << bits_filled);
      ret |= (*ptr_loc >> (8 - bits_filled));
      return ret;
    }

    uint32_t read_extra_ptr(uint32_t node_id, uint32_t& ptr_bit_count, int bits_left) {
      uint8_t *ptr_loc = ptrs_loc + ptr_bit_count / 8;
      uint8_t bits_occu = (ptr_bit_count % 8);
      ptr_bit_count += bits_left;
      bits_left += (bits_occu - 8);
      uint32_t ret = *ptr_loc++ & (0xFF >> bits_occu);
      while (bits_left > 0) {
        ret = (ret << 8) | *ptr_loc++;
        bits_left -= 8;
      }
      ret >>= (bits_left * -1);
      return ret;
    }

    uint32_t read_len(uint8_t *t, uint8_t& len_len) {
      len_len = 1;
      if (*t < 15)
        return *t;
      t++;
      uint32_t ret = 0;
      while (*t & 0x80) {
        ret <<= 7;
        ret |= (*t++ & 0x7F);
        len_len++;
      }
      len_len++;
      ret <<= 4;
      ret |= (*t & 0x0F);
      return ret + 15;
    }

  public:
    char data_type;
    uint32_t max_len;

    grp_ptr_data_map() {
      dict_buf = trie_loc = NULL;
      grp_data = NULL;
    }

    ~grp_ptr_data_map() {
      if (grp_data != NULL)
        delete grp_data;
      grp_data = NULL;
    }

    bool exists() {
      return dict_buf != NULL;
    }

    void init(uint8_t *_dict_buf, uint8_t *_trie_loc, uint8_t *data_loc, uint32_t _node_count) {

      dict_buf = _dict_buf;
      trie_loc = _trie_loc;
      node_count = _node_count;

      ptr_lkup_tbl_ptr_width = *data_loc;
      if (ptr_lkup_tbl_ptr_width == 10)
        ptr_lkup_tbl_mask = 0xFFFFFFFF;
      else
        ptr_lkup_tbl_mask = 0x00FFFFFF;
      data_type = data_loc[1];
      max_len = cmn::read_uint32(data_loc + 2);
      ptr_lookup_tbl_loc = data_loc + cmn::read_uint32(data_loc + 6);
      grp_data_loc = data_loc + cmn::read_uint32(data_loc + 10);
      two_byte_data_count = cmn::read_uint32(data_loc + 14);
      idx2_ptr_count = cmn::read_uint32(data_loc + 18);
      idx2_ptr_size = idx2_ptr_count & 0x80000000 ? 3 : 2;
      idx_ptr_mask = idx2_ptr_size == 3 ? 0x00FFFFFF : 0x0000FFFF;
      start_bits = (idx2_ptr_count >> 20) & 0x0F;
      grp_idx_limit = (idx2_ptr_count >> 24) & 0x1F;
      idx_step_bits = (idx2_ptr_count >> 29) & 0x03;
      idx2_ptr_count &= 0x000FFFFF;
      ptrs_loc = data_loc + cmn::read_uint32(data_loc + 22);
      two_byte_data_loc = data_loc + cmn::read_uint32(data_loc + 26);
      idx2_ptrs_map_loc = data_loc + cmn::read_uint32(data_loc + 30);

      group_count = *grp_data_loc;
      code_lookup_tbl = grp_data_loc + 2;
      uint8_t *grp_data_idx_start = code_lookup_tbl + 512;
      grp_data = new uint8_t*[group_count];
      for (int i = 0; i < group_count; i++)
        grp_data[i] = data_loc + cmn::read_uint32(grp_data_idx_start + i * 4);
      int _start_bits = start_bits;
      for (int i = 1; i <= grp_idx_limit; i++) {
        idx_map_arr[i] = idx_map_arr[i - 1] + (1 << _start_bits) * idx2_ptr_size;
        _start_bits += idx_step_bits;
      }
    }

    bool next_val(ctx_vars& cv, int *in_size_out_value_len, uint8_t *ret_val) {
      if (cv.node_id >= node_count)
        return false;
      cv.init_cv_from_node_id(trie_loc);
      do {
        cv.read_flags_block_begin();
        if (cv.bm_mask & cv.bm_leaf) {
          get_val(cv.node_id, in_size_out_value_len, ret_val, &cv.ptr_bit_count);
          return true;
        }
        cv.node_id++;
        cv.t++;
        cv.bm_mask <<= 1;
      } while (cv.node_id < node_count);
      return false;
    }

    void get_val(uint32_t node_id, int *in_size_out_value_len, uint8_t *ret_val, uint32_t *p_ptr_bit_count = NULL) {
      uint32_t ptr_bit_count = UINT32_MAX;
      if (p_ptr_bit_count == NULL)
        p_ptr_bit_count = &ptr_bit_count;
      if (*p_ptr_bit_count == UINT32_MAX)
        *p_ptr_bit_count = get_ptr_bit_count_val(node_id);
      uint8_t code = read_ptr_bits8(node_id, *p_ptr_bit_count);
      uint8_t *lookup_tbl_ptr = code_lookup_tbl + code * 2;
      uint8_t bit_len = *lookup_tbl_ptr++;
      uint8_t grp_no = *lookup_tbl_ptr & 0x0F;
      uint8_t code_len = *lookup_tbl_ptr >> 5;
      *p_ptr_bit_count += code_len;
      uint32_t ptr = read_extra_ptr(node_id, *p_ptr_bit_count, bit_len - code_len);
      if (grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      uint8_t *val_loc = grp_data[grp_no] + ptr;
      int8_t len_of_len = 0;
      int val_len = 0;
      if (data_type == DCT_TEXT || data_type == DCT_BIN)
        val_len = cmn::read_vint32(val_loc, &len_of_len);
      else if (data_type == DCT_S64_INT || (data_type >= DCT_S64_DEC1 && data_type <= DCT_S64_DEC9))
        val_len = cmn::read_svint60_len(val_loc);
      else if (data_type == DCT_U64_INT || (data_type >= DCT_U64_DEC1 && data_type <= DCT_U64_DEC9))
        val_len = cmn::read_svint61_len(val_loc);
      else if (data_type >= DCT_U15_DEC1 && data_type <= DCT_U15_DEC2)
        val_len = cmn::read_svint15_len(val_loc);
      //val_len = cmn::min(*in_size_out_value_len, val_len);
      *in_size_out_value_len = val_len;
      memcpy(ret_val, val_loc + len_of_len, val_len);
    }

    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint8_t& grp_no) {
      uint8_t *lookup_tbl_ptr = code_lookup_tbl + node_byte * 2;
      uint8_t bit_len = *lookup_tbl_ptr++;
      grp_no = *lookup_tbl_ptr & 0x0F;
      uint8_t code_len = *lookup_tbl_ptr >> 5;
      uint8_t node_val_bits = 8 - code_len;
      uint32_t ptr = node_byte & ((1 << node_val_bits) - 1);
      if (bit_len > 0) {
        if (ptr_bit_count == UINT32_MAX)
          ptr_bit_count = get_ptr_bit_count_tail(node_id);
        ptr |= (read_extra_ptr(node_id, ptr_bit_count, bit_len) << node_val_bits);
      }
      if (grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      return ptr;
    }

    void get_tail_str(byte_str& ret, uint32_t node_id, uint8_t node_byte, uint32_t max_tail_len, uint32_t& tail_ptr, uint32_t& ptr_bit_count, uint8_t& grp_no, bool is_ptr) {
      ret.clear();
      if (!is_ptr) {
        ret.append(node_byte);
        return;
      }
      //ptr_bit_count = UINT32_MAX;
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      get_tail_str(ret, tail_ptr, grp_no, max_tail_len);
    }

    uint8_t get_first_byte(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint32_t& tail_ptr, uint8_t& grp_no) {
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_data[grp_no];
      tail += tail_ptr;
      uint8_t first_byte = *tail;
      if (first_byte >= 32)
        return first_byte;
      uint8_t len_len;
      uint32_t bin_len = read_len(tail, len_len);
      return tail[len_len];
    }

    uint8_t get_first_byte(uint32_t tail_ptr, uint8_t grp_no) {
      uint8_t *tail = grp_data[grp_no];
      return tail[tail_ptr];
    }

    //const int max_tailset_len = 129;
    void get_tail_str(byte_str& ret, uint32_t tail_ptr, uint8_t grp_no, int max_tailset_len) {
      uint8_t *tail = grp_data[grp_no];
      ret.clear();
      uint8_t *t = tail + tail_ptr;
      if (*t < 32) {
        uint8_t len_len;
        uint32_t bin_len = read_len(t, len_len);
        t += len_len;
        while (bin_len--)
          ret.append(*t++);
        return;
      }
      uint8_t byt = *t++;
      while (byt > 31) {
        ret.append(byt);
        byt = *t++;
      }
      if (byt == 0)
        return;
      uint8_t len_len = 0;
      uint32_t sfx_len = read_len(t - 1, len_len);
      uint32_t ptr_end = tail_ptr;
      uint32_t ptr = tail_ptr;
      do {
        byt = tail[ptr--];
      } while (byt != 0 && ptr);
      do {
        byt = tail[ptr--];
      } while (byt > 31 && ptr);
      ptr++;
      uint8_t prev_str_buf[max_tailset_len];
      byte_str prev_str(prev_str_buf, max_tailset_len);
      byt = tail[++ptr];
      while (byt != 0) {
        prev_str.append(byt);
        byt = tail[++ptr];
      }
      uint8_t last_str_buf[max_tailset_len];
      byte_str last_str(last_str_buf, max_tailset_len);
      while (ptr < ptr_end) {
        byt = tail[++ptr];
        while (byt > 31) {
          last_str.append(byt);
          byt = tail[++ptr];
        }
        uint32_t prev_sfx_len = read_len(tail + ptr, len_len);
        last_str.append(prev_str.data() + prev_str.length()-prev_sfx_len, prev_sfx_len);
        ptr += len_len;
        ptr--;
        prev_str.clear();
        prev_str.append(last_str.data(), last_str.length());
        last_str.clear();
      }
      ret.append(prev_str.data() + prev_str.length()-sfx_len, sfx_len);
    }
};

class bv_lookup_tbl {
  private:
    uint8_t *lt_sel_loc;
    uint8_t *lt_rank_loc;
    uint32_t node_count;
    uint8_t *trie_loc;
    int bm_pos;
  public:
    bv_lookup_tbl() {
    }
    void init(uint8_t *_lt_rank_loc, uint8_t *_lt_sel_loc, uint32_t _node_count, uint8_t *_trie_loc, int _bm_pos) {
      lt_rank_loc = _lt_rank_loc;
      lt_sel_loc = _lt_sel_loc;
      node_count = _node_count;
      trie_loc = _trie_loc;
      bm_pos = _bm_pos;
    }
    int bin_srch_lkup_tbl(uint32_t first, uint32_t last, uint32_t given_count) {
      while (first < last) {
        const uint32_t middle = (first + last) >> 1;
        if (cmn::read_uint32(lt_rank_loc + middle * 7) < given_count)
          first = middle + 1;
        else
          last = middle;
      }
      return last;
    }
    uint32_t block_select(uint32_t target_count, uint32_t& node_id) {
      uint32_t block;
      uint8_t *select_loc = lt_sel_loc + target_count / sel_divisor * 3;
      if ((target_count % sel_divisor) == 0) {
        block = cmn::read_uint24(select_loc);
      } else {
        uint32_t start_block = cmn::read_uint24(select_loc);
        uint32_t end_block = cmn::read_uint24(select_loc + 3);
        if (start_block + 10 >= end_block) {
          do {
            start_block++;
          } while (cmn::read_uint32(lt_rank_loc + start_block * 7) < target_count && start_block <= end_block);
          block = start_block - 1;
        } else {
          block = bin_srch_lkup_tbl(start_block, end_block, target_count);
        }
      }
      block++;
      uint32_t cur_count;
      do {
        block--;
        cur_count = cmn::read_uint32(lt_rank_loc + block * 7);
      } while (cur_count >= target_count);
      node_id = block * nodes_per_bv_block;
      uint8_t *bv3 = lt_rank_loc + block * 7 + 4;
      int pos3;
      for (pos3 = 0; pos3 < 3 && node_id + nodes_per_bv_block3 < node_count; pos3++) {
        uint8_t count3 = bv3[pos3];
        if (cur_count + count3 < target_count) {
          node_id += nodes_per_bv_block3;
        } else
          break;
      }
      if (pos3) {
        pos3--;
        cur_count += bv3[pos3];
      }
      return cur_count;
    }
    uint32_t block_rank(uint32_t node_id) {
      uint8_t *rank_ptr = lt_rank_loc + node_id / nodes_per_bv_block * 7;
      uint32_t rank = cmn::read_uint32(rank_ptr);
      int pos = (node_id / nodes_per_bv_block3) % 4;
      if (pos > 0) {
        uint8_t *bv3 = rank_ptr + 4;
        rank += bv3[--pos];
      }
      return rank;
    }
    uint32_t rank(uint32_t node_id) {
      uint32_t rank = block_rank(node_id);
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
      uint64_t bm;
      cmn::read_uint64(t + bm_pos, bm);
      uint64_t mask = bm_init_mask << (node_id % nodes_per_bv_block3);
      return rank + __builtin_popcountll(bm & (mask - 1));
    }
    const uint8_t bit_count[256] = {
      0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
      3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
    const uint8_t select_lookup_tbl[8][256] = {{
      8, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      7, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      8, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      7, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 
      6, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1, 5, 1, 2, 1, 3, 1, 2, 1, 4, 1, 2, 1, 3, 1, 2, 1
    }, {
      8, 8, 8, 2, 8, 3, 3, 2, 8, 4, 4, 2, 4, 3, 3, 2, 8, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      8, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      8, 7, 7, 2, 7, 3, 3, 2, 7, 4, 4, 2, 4, 3, 3, 2, 7, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      7, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      8, 8, 8, 2, 8, 3, 3, 2, 8, 4, 4, 2, 4, 3, 3, 2, 8, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      8, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      8, 7, 7, 2, 7, 3, 3, 2, 7, 4, 4, 2, 4, 3, 3, 2, 7, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2, 
      7, 6, 6, 2, 6, 3, 3, 2, 6, 4, 4, 2, 4, 3, 3, 2, 6, 5, 5, 2, 5, 3, 3, 2, 5, 4, 4, 2, 4, 3, 3, 2
    }, {
      8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 4, 8, 4, 4, 3, 8, 8, 8, 5, 8, 5, 5, 3, 8, 5, 5, 4, 5, 4, 4, 3, 
      8, 8, 8, 6, 8, 6, 6, 3, 8, 6, 6, 4, 6, 4, 4, 3, 8, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3, 
      8, 8, 8, 7, 8, 7, 7, 3, 8, 7, 7, 4, 7, 4, 4, 3, 8, 7, 7, 5, 7, 5, 5, 3, 7, 5, 5, 4, 5, 4, 4, 3, 
      8, 7, 7, 6, 7, 6, 6, 3, 7, 6, 6, 4, 6, 4, 4, 3, 7, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3, 
      8, 8, 8, 8, 8, 8, 8, 3, 8, 8, 8, 4, 8, 4, 4, 3, 8, 8, 8, 5, 8, 5, 5, 3, 8, 5, 5, 4, 5, 4, 4, 3, 
      8, 8, 8, 6, 8, 6, 6, 3, 8, 6, 6, 4, 6, 4, 4, 3, 8, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3, 
      8, 8, 8, 7, 8, 7, 7, 3, 8, 7, 7, 4, 7, 4, 4, 3, 8, 7, 7, 5, 7, 5, 5, 3, 7, 5, 5, 4, 5, 4, 4, 3, 
      8, 7, 7, 6, 7, 6, 6, 3, 7, 6, 6, 4, 6, 4, 4, 3, 7, 6, 6, 5, 6, 5, 5, 3, 6, 5, 5, 4, 5, 4, 4, 3
    }, {
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 
      8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5, 5, 4, 
      8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 
      8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 8, 8, 8, 5, 8, 8, 8, 5, 8, 5, 5, 4, 
      8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 4, 8, 8, 8, 6, 8, 6, 6, 5, 8, 6, 6, 5, 6, 5, 5, 4, 
      8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 4, 8, 8, 8, 7, 8, 7, 7, 5, 8, 7, 7, 5, 7, 5, 5, 4, 
      8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 4, 8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4
    }, {
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 
      8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 5, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 8, 8, 8, 8, 6, 8, 8, 8, 6, 8, 6, 6, 5, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 5, 
      8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 8, 8, 8, 7, 8, 7, 7, 6, 8, 7, 7, 6, 7, 6, 6, 5
    }, {
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 7, 6
    }, {
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7
    }, {
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 
      8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8
    }};
    void select(uint32_t& node_id, uint32_t target_count) {
      if (target_count == 0) {
        node_id = 0;
        return;
      }
      uint32_t block_count = block_select(target_count, node_id);
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
      uint64_t bm;
      cmn::read_uint64(t + bm_pos, bm);
      // int remaining = target_count - block_count - 1;
      // uint64_t isolated_bit = _pdep_u64(1ULL << remaining, bm);
      // size_t bit_loc = _tzcnt_u64(isolated_bit) + 1;
      // // size_t bit_loc = find_nth_set_bit(bm, i) + 1;
      // if (bit_loc == 65) {
      //   std::cout << "WARNING: UNEXPECTED bit_loc=65, node_id: " << node_id << " nc: " << node_count <<
      //     " tc: " << block_count << " ttc: " << target_count << std::endl;
      //   return;
      // }
      // node_id += bit_loc;

      // The performance of this is not too different from using bmi2. Keeping this for now
      size_t bit_loc = 0;
      while (bit_loc < 64) {
        uint8_t next_count = bit_count[(bm >> bit_loc) & 0xFF];
        if (block_count + next_count >= target_count)
          break;
        bit_loc += 8;
        block_count += next_count;
      }
      if (block_count < target_count)
        bit_loc += select_lookup_tbl[target_count - block_count - 1][(bm >> bit_loc) & 0xFF];
      node_id += bit_loc;

      // uint64_t bm_mask = bm_init_mask << bit_loc;
      // while (block_count < target_count) {
      //   if (bm & bm_mask)
      //     block_count++;
      //   bit_loc++;
      //   bm_mask <<= 1;
    }

};

class dict_iter_ctx {
  public:
    int32_t cur_idx;
    uint16_t key_len;
    uint8_t *key;
    uint32_t *node_path;
    uint32_t *child_count;
    uint16_t *last_tail_len;
    bool to_skip_first_leaf;
    bool is_allocated = false;
    dict_iter_ctx() {
      is_allocated = false;
    }
    ~dict_iter_ctx() {
      close();
    }
    void close() {
      if (is_allocated) {
        delete key;
        delete node_path;
        delete child_count;
        delete last_tail_len;
      }
      is_allocated = false;
    }
    void init(uint16_t max_key_len, uint16_t max_level) {
      if (!is_allocated) {
        key = new uint8_t[max_key_len];
        node_path = new uint32_t[max_level];
        child_count = new uint32_t[max_level];
        last_tail_len = new uint16_t[max_level];
      }
      memset(node_path, '\0', max_level * sizeof(uint32_t));
      memset(child_count, '\0', max_level * sizeof(uint32_t));
      memset(last_tail_len, '\0', max_level * sizeof(uint16_t));
      cur_idx = key_len = 0;
      to_skip_first_leaf = false;
      is_allocated = true;
    }
};

class static_dict {

  private:
    uint8_t *val_buf;
    size_t dict_size;
    size_t val_size;
    bool is_mmapped;

    uint32_t node_count;
    uint32_t common_node_count;
    uint32_t key_count;
    uint32_t max_val_len;
    uint32_t cache_count;
    uint32_t bv_block_count;
    uint16_t max_tail_len;
    uint16_t max_level;
    uint8_t *common_nodes_loc;
    uint8_t *cache_loc;
    uint8_t *term_lt_loc;
    uint8_t *child_lt_loc;
    uint8_t *leaf_lt_loc;
    uint8_t *term_select_lkup_loc;
    uint8_t *child_select_lkup_loc;
    uint8_t *leaf_select_lkup_loc;
    uint8_t *trie_loc;
    uint8_t *val_table_loc;

    bv_lookup_tbl term_lt;
    bv_lookup_tbl child_lt;
    bv_lookup_tbl leaf_lt;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

  public:
    uint8_t *dict_buf;
    uint32_t val_count;
    uint8_t *names_pos;
    char *names_loc;
    uint32_t last_exit_loc;
    min_pos_stats min_stats;
    uint8_t *sec_cache_loc;
    grp_ptr_data_map tail_map;
    grp_ptr_data_map *val_map;
    uint32_t max_key_len;
    static_dict() {
      init_vars();
    }

    void init_vars() {
      dict_buf = NULL;
      val_buf = NULL;
      val_map = NULL;
      is_mmapped = false;
      last_exit_loc = 0;
    }

    ~static_dict() {
      if (is_mmapped)
        map_unmap();
      if (dict_buf != NULL) {
#ifndef _WIN32
        madvise(dict_buf, dict_size, MADV_NORMAL);
#endif
        free(dict_buf);
      }
      if (val_map != NULL) {
        delete [] val_map;
      }
    }

    void set_print_enabled(bool to_print_messages = true) {
      is_dict_print_enabled = to_print_messages;
    }

    void load_into_vars() {

      val_count = cmn::read_uint16(dict_buf + 4);
      names_pos = dict_buf + cmn::read_uint32(dict_buf + 6);
      names_loc = (char *) names_pos + (val_count + 2) * sizeof(uint16_t);
      val_table_loc = dict_buf + cmn::read_uint32(dict_buf + 10);
      node_count = cmn::read_uint32(dict_buf + 14);
      bv_block_count = node_count / nodes_per_bv_block;
      common_node_count = cmn::read_uint32(dict_buf + 22);
      key_count = cmn::read_uint32(dict_buf + 26);
      max_key_len = cmn::read_uint32(dict_buf + 30);
      max_val_len = cmn::read_uint32(dict_buf + 34);
      max_tail_len = cmn::read_uint16(dict_buf + 38) + 1;
      max_level = cmn::read_uint16(dict_buf + 40);
      cache_count = cmn::read_uint32(dict_buf + 42);
      memcpy(&min_stats, dict_buf + 46, 4);
      cache_loc = dict_buf + cmn::read_uint32(dict_buf + 50);
      sec_cache_loc = dict_buf + cmn::read_uint32(dict_buf + 54);

      term_select_lkup_loc = dict_buf + cmn::read_uint32(dict_buf + 58);
      term_lt_loc = dict_buf + cmn::read_uint32(dict_buf + 62);
      child_select_lkup_loc = dict_buf + cmn::read_uint32(dict_buf + 66);
      child_lt_loc = dict_buf + cmn::read_uint32(dict_buf + 70);
      leaf_select_lkup_loc = dict_buf + cmn::read_uint32(dict_buf + 74);
      leaf_lt_loc = dict_buf + cmn::read_uint32(dict_buf + 78);

      uint8_t *trie_tail_ptrs_data_loc = dict_buf + cmn::read_uint32(dict_buf + 82);
      uint32_t tail_size = cmn::read_uint32(trie_tail_ptrs_data_loc);
      uint8_t *tails_loc = trie_tail_ptrs_data_loc + 4;
      trie_loc = tails_loc + tail_size;
      tail_map.init(dict_buf, trie_loc, tails_loc, node_count);

      term_lt.init(term_lt_loc, term_select_lkup_loc, node_count, trie_loc, 8);
      child_lt.init(child_lt_loc, child_select_lkup_loc, node_count, trie_loc, 16);
      leaf_lt.init(leaf_lt_loc, leaf_select_lkup_loc, node_count, trie_loc, 0);

      if (val_count > 0) {
        val_map = new grp_ptr_data_map[val_count];
        for (int i = 0; i < val_count; i++) {
          val_buf = dict_buf + cmn::read_uint32(val_table_loc + i * sizeof(uint32_t));
          val_map[i].init(val_buf, trie_loc, val_buf, node_count);
        }
      }

    }

    uint8_t *map_file(const char *filename, size_t& sz) {
#ifdef _WIN32
      load(filename);
      return dict_buf;
#else
      struct stat buf;
      int fd = open(filename, O_RDONLY);
      if (fd < 0) {
        perror("open: ");
        return NULL;
      }
      fstat(fd, &buf);
      sz = buf.st_size;
      uint8_t *map_buf = (uint8_t *) mmap((caddr_t) 0, sz, PROT_READ, MAP_PRIVATE, fd, 0);
      if (map_buf == MAP_FAILED) {
        perror("mmap: ");
        close(fd);
        return NULL;
      }
      close(fd);
      return map_buf;
#endif
    }

    void map_file_to_mem(const char *filename) {
      dict_buf = map_file(filename, dict_size);
      int len_will_need = (dict_size >> 2);
      //madvise(dict_buf, len_will_need, MADV_WILLNEED);
#ifndef _WIN32
      mlock(dict_buf, len_will_need);
#endif
      load_into_vars();
      is_mmapped = true;
    }

    void map_unmap() {
#ifndef _WIN32
      munlock(dict_buf, dict_size >> 2);
      int err = munmap(dict_buf, dict_size);
      if(err != 0){
        printf("UnMapping dict_buf Failed\n");
        return;
      }
#endif
      dict_buf = NULL;
      is_mmapped = false;
    }

    void load(const char* filename) {

      init_vars();
      struct stat file_stat;
      memset(&file_stat, '\0', sizeof(file_stat));
      stat(filename, &file_stat);
      dict_size = file_stat.st_size;
      dict_buf = (uint8_t *) malloc(dict_size);

      FILE *fp = fopen(filename, "rb");
      fread(dict_buf, dict_size, 1, fp);
      fclose(fp);

      int len_will_need = (dict_size >> 1);
#ifndef _WIN32
      mlock(dict_buf, len_will_need);
#endif
      //madvise(dict_buf, len_will_need, MADV_WILLNEED);
      //madvise(dict_buf + len_will_need, dict_size - len_will_need, MADV_RANDOM);

      load_into_vars();

    }

    void find_child(uint32_t& node_id, uint32_t child_count) {
      term_lt.select(node_id, child_count);
      //child_count = child_lt.rank(node_id);
    }

    int find_in_cache(const uint8_t *key, int key_len, int& key_pos, uint32_t& node_id) {
      uint8_t key_byte = key[key_pos];
      uint32_t cache_mask = cache_count - 1;
      cache *cche0 = (cache *) cache_loc;
      do {
        uint32_t cache_idx = (node_id ^ (node_id << 5) ^ key_byte) & cache_mask;
        cache *cche = cche0 + cache_idx;
        uint32_t cache_node_id = cmn::read_uint24(&cche->parent_node_id1);
        if (node_id == cache_node_id) {
          if (cche->node_byte == key_byte) {
            key_pos++;
            if (key_pos < key_len) {
              node_id = cmn::read_uint24(&cche->child_node_id1);
              key_byte = key[key_pos];
              continue;
            }
            node_id += cche->node_offset;
            uint8_t *t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
            uint64_t bm_leaf;
            cmn::read_uint64(t, bm_leaf);
            uint64_t bm_mask = (bm_init_mask << (node_id % 64));
            if (bm_leaf & bm_mask) {
              last_exit_loc = 0;
              return 0;
            } else {
              last_exit_loc = t - dict_buf;
              return -1;
            }
          }
        }
        break;
      } while (1);
      return -1;
    }

    uint8_t *get_t(uint32_t node_id, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr, uint64_t& bm_mask) {
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
      if (node_id % nodes_per_bv_block3) {
        t = ctx_vars::read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
        t += (node_id % nodes_per_bv_block3);
      }
      bm_mask = (bm_init_mask << (node_id % nodes_per_bv_block3));
      return t;
    }

    bool lookup(const uint8_t *key, int key_len, uint32_t& node_id, int *pcmp = NULL) {
      int cmp = 0;
      if (pcmp == NULL)
        pcmp = &cmp;
      int key_pos = 0;
      node_id = 0;
      uint8_t tail_str_buf[max_tail_len];
      byte_str tail_str(tail_str_buf, max_tail_len);
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      uint8_t trie_byte;
      uint8_t grp_no;
      uint32_t tail_ptr = 0;
      uint32_t ptr_bit_count = UINT32_MAX;
      uint8_t *t = trie_loc;
      uint8_t key_byte = key[key_pos];
      do {
        int ret = find_in_cache(key, key_len, key_pos, node_id);
        if (ret == 0)
          return true;
        key_byte = key[key_pos];
        t = get_t(node_id, bm_leaf, bm_term, bm_child, bm_ptr, bm_mask);
        ptr_bit_count = UINT32_MAX;
        do {
          if ((node_id % nodes_per_bv_block3) == 0) {
            bm_mask = bm_init_mask;
            t = ctx_vars::read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
          }
          if ((bm_mask & bm_child) == 0 && (bm_mask & bm_leaf) == 0) {
            uint8_t min_offset = sec_cache_loc[((*t - min_stats.min_len) * 256) + key_byte];
            uint32_t old_node_id = node_id;
            node_id += min_offset;
            if ((old_node_id / nodes_per_bv_block3) == (node_id / nodes_per_bv_block3)) {
              t += min_offset;
              bm_mask <<= min_offset;
            } else {
              t = get_t(node_id, bm_leaf, bm_term, bm_child, bm_ptr, bm_mask);
            }
            continue;
          }
          uint8_t node_byte = trie_byte = *t++;
          if (bm_mask & bm_ptr) {
            trie_byte = tail_map.get_first_byte(node_byte, node_id, ptr_bit_count, tail_ptr, grp_no);
          }
          if (key_byte > trie_byte) {
            if (bm_mask & bm_term) {
              last_exit_loc = t - dict_buf;
              return false;
            }
            bm_mask <<= 1;
            node_id++;
          } else
            break;
        } while (1);
        if (key_byte == trie_byte) {
          *pcmp = 0;
          uint32_t tail_len = 1;
          if (bm_mask & bm_ptr) {
            tail_map.get_tail_str(tail_str, tail_ptr, grp_no, max_tail_len);
            tail_len = tail_str.length();
            *pcmp = cmn::compare(tail_str.data(), tail_len, key + key_pos, key_len - key_pos);
          }
          key_pos += tail_len;
          if (key_pos < key_len && (*pcmp == 0 || *pcmp - 1 == tail_len)) {
            if ((bm_mask & bm_child) == 0) {
              last_exit_loc = t - dict_buf;
              return false;
            }
            find_child(node_id, child_lt.rank(node_id) + 1);
            continue;
          }
          if (*pcmp == 0 && key_pos == key_len && (bm_leaf & bm_mask)) {
            last_exit_loc = 0;
            return true;
          }
        }
        last_exit_loc = t - dict_buf;
        return false;
      } while (node_id < node_count);
      last_exit_loc = t - dict_buf;
      return false;
    }

    bool get_col_val(uint32_t node_id, int col_val_idx, int *in_size_out_value_len, uint8_t *val) {
      val_map[col_val_idx].get_val(node_id, in_size_out_value_len, val);
      return true;
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, uint8_t *val) {
      uint32_t node_id;
      return get(key, key_len, in_size_out_value_len, val, node_id);
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, uint8_t *val, uint32_t& node_id) {
      int key_pos, cmp;
      bool is_found = lookup(key, key_len, node_id);
      if (node_id >= 0 && val != NULL) {
        val_map[0].get_val(node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
    }

    void push_to_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      ctx.cur_idx++;
      cv.tail.clear();
      update_ctx(ctx, cv);
    }

    template<typename INS_ARR_T>
    void insert_arr(INS_ARR_T *arr, int arr_len, int pos, INS_ARR_T val) {
      for (int i = arr_len - 1; i >= pos; i--)
        arr[i + 1] = arr[i];
      arr[pos] = val;
    }

    void insert_into_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      insert_arr(ctx.child_count, ctx.cur_idx, 0, cv.child_count);
      insert_arr(ctx.node_path, ctx.cur_idx, 0, cv.node_id);
      insert_arr(ctx.last_tail_len, ctx.cur_idx, 0, (uint16_t) cv.tail.length());
      ctx.cur_idx++;
    }

    void update_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      ctx.child_count[ctx.cur_idx] = cv.child_count;
      ctx.node_path[ctx.cur_idx] = cv.node_id;
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = cv.tail.length();
      memcpy(ctx.key + ctx.key_len, cv.tail.data(), cv.tail.length());
      ctx.key_len += cv.tail.length();
    }

    void clear_last_tail(dict_iter_ctx& ctx) {
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
    }

    void pop_from_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      clear_last_tail(ctx);
      ctx.cur_idx--;
      read_from_ctx(ctx, cv);
    }

    void read_from_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      cv.child_count = ctx.child_count[ctx.cur_idx];
      cv.node_id = ctx.node_path[ctx.cur_idx];
      cv.init_cv_from_node_id(trie_loc);
    }

    int next(dict_iter_ctx& ctx, uint8_t *key_buf, uint8_t *val_buf = NULL, int *val_buf_len = NULL) {
      ctx_vars cv;
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      read_from_ctx(ctx, cv);
      do {
        cv.read_flags_block_begin();
        if ((cv.bm_mask & cv.bm_child) == 0 && (cv.bm_mask & cv.bm_leaf) == 0) {
          cv.t++;
          cv.bm_mask <<= 1;
          cv.node_id++;
          continue;
        }
        if (cv.bm_mask & cv.bm_leaf) {
          if (ctx.to_skip_first_leaf) {
            if ((cv.bm_mask & cv.bm_child) == 0) {
              while (cv.bm_mask & cv.bm_term) {
                if (ctx.cur_idx == 0)
                  return 0;
                pop_from_ctx(ctx, cv);
                cv.read_flags_block_begin();
              }
              cv.bm_mask <<= 1;
              cv.node_id++;
              cv.t++;
              update_ctx(ctx, cv);
              ctx.to_skip_first_leaf = false;
              continue;
            }
          } else {
            tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, max_tail_len, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no, cv.bm_mask & cv.bm_ptr);
            update_ctx(ctx, cv);
            memcpy(key_buf, ctx.key, ctx.key_len);
            val_map[0].get_val(cv.node_id, val_buf_len, val_buf);
            ctx.to_skip_first_leaf = true;
            return ctx.key_len;
          }
        }
        ctx.to_skip_first_leaf = false;
        if (cv.bm_mask & cv.bm_child) {
          cv.child_count++;
          tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, max_tail_len, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no, cv.bm_mask & cv.bm_ptr);
          update_ctx(ctx, cv);
          find_child(cv.node_id, cv.child_count);
          cv.child_count = child_lt.rank(cv.node_id);
          cv.init_cv_from_node_id(trie_loc);
          cv.ptr_bit_count = UINT32_MAX;
          push_to_ctx(ctx, cv);
        }
      } while (cv.node_id < node_count);
      return 0;
    }

    uint32_t get_max_level() {
      return max_level;
    }

    uint32_t get_max_key_len() {
      return max_key_len;
    }

    uint32_t get_max_tail_len() {
      return max_tail_len;
    }

    uint32_t get_max_val_len() {
      return max_val_len;
    }

    uint32_t get_leaf_rank(uint32_t node_id) {
      return leaf_lt.rank(node_id);
    }

    bool reverse_lookup(uint32_t leaf_id, int *in_size_out_key_len, uint8_t *ret_key, int *in_size_out_value_len = NULL, uint8_t *ret_val = NULL) {
      leaf_id++;
      uint32_t node_id;
      leaf_lt.select(node_id, leaf_id);
      node_id--;
      return reverse_lookup_from_node_id(node_id, in_size_out_key_len, ret_key, in_size_out_value_len, ret_val);
    }

    bool reverse_lookup_from_node_id(uint32_t node_id, int *in_size_out_key_len, uint8_t *ret_key, int *in_size_out_value_len = NULL, uint8_t *ret_val = NULL, dict_iter_ctx *ctx = NULL) {
      ctx_vars cv;
      uint8_t key_str_buf[max_key_len];
      byte_str key_str(key_str_buf, max_key_len);
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      cv.node_id = node_id;
      if (ret_val != NULL && val_map[0].exists())
        val_map[0].get_val(cv.node_id, in_size_out_value_len, ret_val);
      cv.node_id++;
      do {
        cv.node_id--;
        cv.t = trie_loc + cv.node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
        cv.t = ctx_vars::read_flags(cv.t, cv.bm_leaf, cv.bm_term, cv.bm_child, cv.bm_ptr);
        cv.t += cv.node_id % 64;
        cv.bm_mask = bm_init_mask << (cv.node_id % nodes_per_bv_block3);
        cv.ptr_bit_count = UINT32_MAX;
        tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, max_tail_len, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no, cv.bm_mask & cv.bm_ptr);
        for (int i = cv.tail.length() - 1; i >= 0; i--)
          key_str.append(cv.tail[i]);
        if (ctx != NULL)
          insert_into_ctx(*ctx, cv);
        uint32_t term_count = term_lt.rank(cv.node_id);
        child_lt.select(cv.node_id, term_count);
      } while (cv.node_id > 0);
      int key_pos = 0;
      for (int i = key_str.length() - 1; i >= 0; i--) {
        ret_key[key_pos++] = key_str[i];
        if (ctx != NULL)
          ctx->key[ctx->key_len++] = key_str[i];
      }
      *in_size_out_key_len = key_str.length();
      return true;
    }

    bool find_first(const uint8_t *prefix, int prefix_len, dict_iter_ctx& ctx) {
      int cmp;
      uint32_t lkup_node_id;
      lookup(prefix, prefix_len, lkup_node_id, &cmp);
      uint8_t key_buf[max_key_len];
      int key_len;
      // TODO: set last_key_len
      ctx.cur_idx = 0;
      reverse_lookup_from_node_id(lkup_node_id, &key_len, key_buf, NULL, NULL, &ctx);
      ctx.cur_idx--;
      // for (int i = 0; i < ctx.key_len; i++)
      //   printf("%c", ctx.key[i]);
      // printf("\nlsat tail len: %d\n", ctx.last_tail_len[ctx.cur_idx]);
      ctx.key_len -= ctx.last_tail_len[ctx.cur_idx];
      ctx.last_tail_len[ctx.cur_idx] = 0;
      ctx.to_skip_first_leaf = false;
      return true;
    }

    uint8_t *get_trie_loc() {
      return trie_loc;
    }

    int64_t get_val_int60(uint8_t *val) {
      return cmn::read_svint60(val);
    }

    double get_val_int60_dbl(uint8_t *val, char type) {
      int64_t i64 = cmn::read_svint60(val);
      double ret = static_cast<double>(i64);
      ret /= cmn::pow10(type - DCT_S64_DEC1 + 1);
      return ret;
    }

    uint64_t get_val_int61(uint8_t *val) {
      return cmn::read_svint61(val);
    }

    double get_val_int61_dbl(uint8_t *val, char type) {
      uint64_t i64 = cmn::read_svint61(val);
      double ret = static_cast<double>(i64);
      ret /= cmn::pow10(type - DCT_U64_DEC1 + 1);
      return ret;
    }

    uint64_t get_val_int15(uint8_t *val) {
      return cmn::read_svint15(val);
    }

    double get_val_int15_dbl(uint8_t *val, char type) {
      uint64_t i64 = cmn::read_svint15(val);
      double ret = static_cast<double>(i64);
      ret /= cmn::pow10(type - DCT_U15_DEC1 + 1);
      return ret;
    }

};

}
#endif

// find_longest_match
// binary keys
