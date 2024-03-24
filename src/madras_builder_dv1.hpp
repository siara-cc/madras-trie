#ifndef builder_H
#define builder_H

#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <cstring>
#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>
#include <time.h>

#include "../../leopard-trie/src/leopard.hpp"

enum {SRCH_ST_UNKNOWN, SRCH_ST_NEXT_SIBLING, SRCH_ST_NOT_FOUND, SRCH_ST_NEXT_CHAR};
#define DCT_INSERT_AFTER -2
#define DCT_INSERT_BEFORE -3
#define DCT_INSERT_LEAF -4
#define DCT_INSERT_EMPTY -5
#define DCT_INSERT_THREAD -6
#define DCT_INSERT_CONVERT -7
#define DCT_INSERT_CHILD_LEAF -8

namespace madras_dv1 {

#define nodes_per_bv_block3 64
#define nodes_per_bv_block 256
#define sel_divisor 512

typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uint8_t *> byte_ptr_vec;

struct bldr_cache {
  uint8_t parent_node_id1;
  uint8_t parent_node_id2;
  uint8_t parent_node_id3;
  uint8_t node_offset;
  uint8_t child_node_id1;
  uint8_t child_node_id2;
  uint8_t child_node_id3;
  uint8_t node_byte;
};

struct tail_token {
  uint32_t token_pos;
  uint32_t token_len;
  uint32_t fwd_pos;
  uint32_t cmp_max;
};

static bool is_bldr_print_enabled = false;
static void bldr_printf(const char* format, ...) {
  if (!is_bldr_print_enabled)
    return;
  va_list args;
  va_start(args, format);
  vprintf(format, args);
  va_end(args);
}

class gen {
  public:
    static uint32_t log4(uint32_t val) {
      return (val < 4 ? 1 : (val < 16 ? 2 : (val < 64 ? 3 : (val < 256 ? 4 : (val < 1024 ? 5 : (val < 4096 ? 6 : (val < 16384 ? 7 : (val < 65536 ? 8 : 9))))))));
    }
    static uint32_t log5(uint32_t val) {
      return (val < 5 ? 1 : (val < 25 ? 2 : (val < 125 ? 3 : (val < 625 ? 4 : (val < 3125 ? 5 : (val < 15625 ? 6 : (val < 78125 ? 7 : (val < 390625 ? 8 : 9))))))));
    }
    static uint32_t log8(uint32_t val) {
      return (val < 8 ? 1 : (val < 64 ? 2 : (val < 512 ? 3 : (val < 4096 ? 4 : (val < 32768 ? 5 : (val < 262144 ? 6 : (val < 2097152 ? 7 : 8)))))));
    }
    static uint32_t lg10(uint32_t val) {
      return (val < 10 ? 1 : (val < 100 ? 2 : (val < 1000 ? 3 : (val < 10000 ? 4 : (val < 100000 ? 5 : (val < 1000000 ? 6 : (val < 10000000 ? 7 : 8)))))));
    }
    static uint32_t log12(uint32_t val) {
      return (val < 12 ? 1 : (val < 96 ? 2 : (val < 768 ? 3 : (val < 6144 ? 4 : (val < 49152 ? 5 : (val < 393216 ? 6 : (val < 3145728 ? 7 : 8)))))));
    }
    static uint32_t log16(uint32_t val) {
      return (val < 16 ? 1 : (val < 256 ? 2 : (val < 4096 ? 3 : (val < 65536 ? 4 : (val < 1048576 ? 5 : (val < 16777216 ? 6 : (val < 268435456 ? 7 : 8)))))));
    }
    static uint32_t log256(uint32_t val) {
      return (val < 256 ? 1 : (val < 65536 ? 2 : (val < 16777216 ? 3 : 4)));
    }
    static size_t max(size_t v1, size_t v2) {
      return v1 > v2 ? v1 : v2;
    }
    static size_t min(size_t v1, size_t v2) {
      return v1 < v2 ? v1 : v2;
    }
    static int8_t get_vlen_of_uint32(uint32_t vint) {
      return vint < (1 << 7) ? 1 : (vint < (1 << 14) ? 2 : (vint < (1 << 21) ? 3 :
              (vint < (1 << 28) ? 4 : 5)));
    }
    static int append_vint32(byte_vec& vec, uint32_t vint) {
      int len = get_vlen_of_uint32(vint);
      for (int i = len - 1; i > 0; i--)
        vec.push_back(0x80 + ((vint >> (7 * i)) & 0x7F));
      vec.push_back(vint & 0x7F);
      return len;
    }
    static uint32_t read_vint32(const uint8_t *ptr, int8_t *vlen = NULL) {
      uint32_t ret = 0;
      int8_t len = 5; // read max 5 bytes
      do {
        ret <<= 7;
        ret += *ptr & 0x7F;
        len--;
      } while ((*ptr++ >> 7) && len);
      if (vlen != NULL)
        *vlen = 5 - len;
      return ret;
    }
    static int8_t get_v2len_of_uint32(uint32_t vint) {
      return vint < (1 << 6) ? 1 : (vint < (1 << 14) ? 2 : (vint < (1 << 22) ? 3 : 4));
    }
    static int append_v2uint32(byte_vec& vec, uint32_t vint) {
      int len = get_v2len_of_uint32(vint);
      if (len == 1) {
        vec.push_back(vint);
        return 1;
      }
      len--;
      vec.push_back((len << 6) + vint & 0x1F);
      vint >>= 6;
      while (len--) {
        vec.push_back(vint & 0xFF);
        vint >>= 8;
      }
      return len;
    }
    static clock_t print_time_taken(clock_t t, const char *msg) {
      t = clock() - t;
      double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
      bldr_printf("%s%.5f\n", msg, time_taken);
      return clock();
    }
    static int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2) {
        int lim = (len2 < len1 ? len2 : len1);
        int k = 0;
        do {
            uint8_t c1 = v1[k];
            uint8_t c2 = v2[k];
            k++;
            if (c1 < c2)
                return -k;
            else if (c1 > c2)
                return k;
        } while (k < lim);
        if (len1 == len2)
            return 0;
        k++;
        return (len1 < len2 ? -k : k);
    }
    static int compare_rev(const uint8_t *v1, int len1, const uint8_t *v2,
            int len2) {
        int lim = (len2 < len1 ? len2 : len1);
        int k = 0;
         while (k++ < lim) {
            uint8_t c1 = v1[len1 - k];
            uint8_t c2 = v2[len2 - k];
            if (c1 < c2)
                return -k;
            else if (c1 > c2)
                return k;
        }
        if (len1 == len2)
            return 0;
        return (len1 < len2 ? -k : k);
    }
    static void copy_uint16(uint16_t input, uint8_t *out) {
      *out++ = input & 0xFF;
      *out = (input >> 8);
    }
    static void copy_uint24(uint32_t input, uint8_t *out) {
      *out++ = input & 0xFF;
      *out++ = (input >> 8) & 0xFF;
      *out = (input >> 16);
    }
    static void copy_uint32(uint32_t input, uint8_t *out) {
      *out++ = input & 0xFF;
      *out++ = (input >> 8) & 0xFF;
      *out++ = (input >> 16) & 0xFF;
      *out = (input >> 24);
    }
    static void copy_vint32(uint32_t input, uint8_t *out, int8_t vlen) {
      for (int i = vlen - 1; i > 0; i--)
        *out++ = 0x80 + ((input >> (7 * i)) & 0x7F);
      *out = input & 0x7F;
    }
    static void write_uint16(uint32_t input, FILE *fp) {
      fputc(input & 0xFF, fp);
      fputc(input >> 8, fp);
    }
    static void write_uint24(uint32_t input, FILE *fp) {
      fputc(input & 0xFF, fp);
      fputc((input >> 8) & 0xFF, fp);
      fputc(input >> 16, fp);
    }
    static void write_uint32(uint32_t input, FILE *fp) {
      fputc(input & 0xFF, fp);
      fputc((input >> 8) & 0xFF, fp);
      fputc((input >> 16) & 0xFF, fp);
      fputc(input >> 24, fp);
    }
    static void append_uint16(uint16_t u16, byte_vec& v) {
      v.push_back(u16 & 0xFF);
      v.push_back(u16 >> 8);
    }
    static void append_uint24(uint32_t u24, byte_vec& v) {
      v.push_back(u24 & 0xFF);
      v.push_back((u24 >> 8) & 0xFF);
      v.push_back(u24 >> 16);
    }
    static uint32_t read_uint16(byte_vec& v, int pos) {
      uint32_t ret = v[pos++];
      ret |= (v[pos] << 8);
      return ret;
    }
    static uint32_t read_uint16(uint8_t *ptr) {
      uint32_t ret = *ptr++;
      ret |= (*ptr << 8);
      return ret;
    }
    static uint32_t read_uint24(byte_vec& v, int pos) {
      uint32_t ret = v[pos++];
      ret |= (v[pos++] << 8);
      ret |= (v[pos] << 16);
      return ret;
    }
    static uint32_t read_uint24(uint8_t *ptr) {
      uint32_t ret = *ptr++;
      ret |= (*ptr++ << 8);
      ret |= (*ptr << 16);
      return ret;
    }
    static uint32_t read_uint32(uint8_t *ptr) {
      return *((uint32_t *) ptr);
    }
    static uint32_t get_lkup_tbl_size(uint32_t count, int block_size, int entry_size) {
      double d = count - 1;
      d /= block_size;
      uint32_t ret = floor(d) + 1;
      ret *= entry_size;
      return ret;
    }
    static uint32_t get_lkup_tbl_size2(uint32_t count, int block_size, int entry_size) {
      return get_lkup_tbl_size(count, block_size, entry_size) + entry_size;
    }
};

typedef int (*cmp_fn) (const uint8_t *v1, int len1, const uint8_t *v2, int len2);

template<class T>
class huffman {
  std::vector<uint32_t> _freqs;
  struct entry {
    T freq;
    entry *parent;
    bool is_zero;
    entry(T _freq, entry *_parent, bool _is_zero) {
      freq = _freq;
      parent = _parent;
      is_zero = _is_zero;
    }
  };
  std::vector<entry *> _all_entries;
  entry *root;
  static bool greater(const entry *e1, const entry *e2) {
    return e1->freq > e2->freq;
  }
  void process_input() {
    for (int i = 0; i < _freqs.size(); i++) {
      _all_entries.push_back(new entry(_freqs[i], NULL, true));
    }
    std::vector<entry *> _entries = _all_entries;
    while (true) {
      std::sort(_entries.begin(), _entries.end(), greater);
      size_t last_idx = _entries.size() - 1;
      if (last_idx == 0)
        return;
      entry *e1 = _entries[last_idx--];
      entry *e2 = _entries[last_idx];
      _entries.resize(last_idx);
      entry *parent = new entry(e1->freq + e2->freq, NULL, true);
      e1->parent = parent;
      e1->is_zero = false;
      e2->parent = parent;
      e2->is_zero = true;
      _all_entries.push_back(parent);
      if (_entries.size() == 0) {
        root = parent;
        return;
      }
      _entries.push_back(parent);
    }
  }
  public:
    huffman(std::vector<uint32_t> freqs) {
      _freqs = freqs;
      process_input();
    }
    ~huffman() {
      for (int i = 0; i < _all_entries.size(); i++)
        delete _all_entries[i];
    }
    uint32_t get_code(int i, int& len) {
      uint32_t ret = 0;
      len = 0;
      entry *start = _all_entries[i];
      do {
        ret >>= 1;
        ret |= start->is_zero ? 0 : 0x80000000;
        len++;
        start = start->parent;
      } while (start != NULL && start->parent != NULL);
      ret >>= (32 - len);
      return ret;
    }
};

struct freq_grp {
  uint32_t grp_no;
  uint32_t grp_log2;
  uint32_t grp_limit;
  uint32_t count;
  uint32_t freq_count;
  uint32_t grp_size;
  uint8_t code;
  uint8_t code_len;
};

#define step_bits_idx 3
#define step_bits_rest 3
class freq_grp_ptrs_data {
  private:
    std::vector<freq_grp> freq_grp_vec;
    std::vector<byte_vec> grp_data;
    byte_vec ptrs;
    byte_vec idx2_ptrs_map;
    int last_byte_bits;
    int idx_limit;
    int start_bits;
    uint8_t idx_ptr_size;
    uint8_t ptr_lkup_tbl_ptr_width;
    uint32_t next_idx;
    uint32_t ptr_lookup_tbl;
    uint32_t ptr_lookup_tbl_loc;
    uint32_t grp_data_loc;
    uint32_t grp_data_size;
    uint32_t grp_ptrs_loc;
    uint32_t two_byte_data_loc;
    uint32_t idx2_ptrs_map_loc;
    uint32_t two_byte_count;
    uint32_t idx2_ptr_count;
    uint32_t tot_ptr_bit_count;
    uint32_t max_len;
  public:
    freq_grp_ptrs_data() {
      reset();
    }
    void reset() {
      freq_grp_vec.resize(0);
      grp_data.resize(0);
      ptrs.resize(0);
      idx2_ptrs_map.resize(0);
      next_idx = 0;
      idx_limit = 0;
      idx_ptr_size = 3;
      last_byte_bits = 8;
      max_len = 0;
      ptrs.push_back(0);
    }
    int get_idx_limit() {
      return idx_limit;
    }
    int idx_map_arr[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    void set_idx_info(int _start_bits, int new_idx_limit, uint8_t _idx_ptr_size) {
      start_bits = _start_bits;
      idx_limit = new_idx_limit;
      idx_ptr_size = _idx_ptr_size;
      for (int i = 1; i <= idx_limit; i++) {
        idx_map_arr[i] = idx_map_arr[i - 1] + pow(2, _start_bits) * idx_ptr_size;
        _start_bits += step_bits_idx;
        //bldr_printf("idx_map_arr[%d] = %d\n", i, idx_map_arr[i]);
      }
    }
    int get_idx_ptr_size() {
      return idx_ptr_size;
    }
    int *get_idx_map_arr() {
      return idx_map_arr;
    }
    uint32_t get_idx2_ptrs_count() {
      uint32_t idx2_ptr_count = idx2_ptrs_map.size() / get_idx_ptr_size() + (start_bits << 20) + (get_idx_limit() << 24) + (step_bits_idx << 29);
      if (get_idx_ptr_size() == 3)
        idx2_ptr_count |= 0x80000000;
      return idx2_ptr_count;
    }
    void add_freq_grp(freq_grp freq_grp) {
      freq_grp_vec.push_back(freq_grp);
    }
    uint32_t check_next_grp(uint8_t& grp_no, uint32_t cur_limit, uint32_t len) {
      bool next_grp = false;
      if (grp_no <= idx_limit) {
        if (next_idx == cur_limit) {
          next_grp = true;
          next_idx = 0;
        }
      } else {
        if ((freq_grp_vec[grp_no].grp_size + len) >= (cur_limit - 1))
          next_grp = true;
      }
      if (next_grp) {
        if (grp_no < idx_limit)
          cur_limit = pow(2, log2(cur_limit) + step_bits_idx);
        else
          cur_limit = pow(2, log2(cur_limit) + step_bits_rest);
        grp_no++;
        freq_grp_vec.push_back((freq_grp) {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0, 0, 0});
      }
      return cur_limit;
    }
    void update_current_grp(uint32_t grp_no, int32_t len, int32_t freq) {
      freq_grp_vec[grp_no].grp_size += len;
      freq_grp_vec[grp_no].freq_count += freq;
      freq_grp_vec[grp_no].count += (len < 0 ? -1 : 1);
      next_idx++;
    }
    uint32_t append_ptr2_idx_map(uint32_t grp_no, uint32_t _ptr) {
      if (idx2_ptrs_map.size() == idx_map_arr[grp_no - 1])
        next_idx = 0;
      if (idx_ptr_size == 2)
        gen::append_uint16(_ptr, idx2_ptrs_map);
      else
        gen::append_uint24(_ptr, idx2_ptrs_map);
      return next_idx++;
    }
    byte_vec& get_data(int grp_no) {
      grp_no--;
      while (grp_no >= grp_data.size()) {
        byte_vec data;
        data.push_back(0);
        grp_data.push_back(data);
      }
      return grp_data[grp_no];
    }
    uint32_t get_var_len(uint32_t len, byte_vec *vec = NULL) {
      uint32_t var_len = (len < 16 ? 2 : (len < 2048 ? 3 : (len < 262144 ? 4 : 5)));
      if (vec != NULL) {
        vec->push_back(15);
        int bit7s = var_len - 2;
        for (int i = bit7s - 1; i >= 0; i--)
          vec->push_back(0x80 + ((len >> (i * 7 + 4)) & 0x7F));
        vec->push_back(0x10 + (len & 0x0F));
      }
      return var_len;
    }
    uint32_t get_set_len_len(uint32_t len, byte_vec *vec = NULL) {
      if (len < 15) {
        if (vec != NULL)
          vec->push_back(len);
        return 1;
      }
      len -= 15;
      return get_var_len(len, vec);
    }
    uint32_t append_text_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len, bool append0 = false) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      for (int k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (append0)
        grp_data_vec.push_back(0);
      if (len > max_len)
        max_len = len;
      return ptr;
    }
    uint32_t append_bin_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len, char data_type = LPDT_BIN) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      if (data_type == LPDT_TEXT || data_type == LPDT_BIN)
        gen::append_vint32(grp_data_vec, len);
      for (int k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (len > max_len)
        max_len = len;
      return ptr;
    }
    uint32_t append_bin15_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      get_set_len_len(len, &grp_data_vec);
      for (int k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      // grp_data_vec.push_back(0);
      if (len > max_len)
        max_len = len;
      return ptr;
    }
    int append_ptr_bits(uint32_t ptr, int bits_to_append) {
      int last_idx = ptrs.size() - 1;
      while (bits_to_append > 0) {
        if (bits_to_append < last_byte_bits) {
          last_byte_bits -= bits_to_append;
          ptrs[last_idx] |= (ptr << last_byte_bits);
          bits_to_append = 0;
        } else {
          bits_to_append -= last_byte_bits;
          ptrs[last_idx] |= (ptr >> bits_to_append);
          last_byte_bits = 8;
          ptrs.push_back(0);
          last_idx++;
        }
      }
      return last_byte_bits;
    }
    uint32_t get_hdr_size() {
      return 514 + grp_data.size() * 4;
    }
    uint32_t get_data_size() {
      uint32_t data_size = 0;
      for (int i = 0; i < grp_data.size(); i++)
        data_size += grp_data[i].size();
      return data_size;
    }
    uint32_t get_ptrs_size() {
      return ptrs.size();
    }
    uint32_t get_total_size() {
      return get_hdr_size() + get_data_size() + get_ptrs_size() + idx2_ptrs_map.size() + ptr_lookup_tbl + 8 * 4 + 3;
    }
    #define nodes_per_ptr_block 256
    #define nodes_per_ptr_block3 64
    void build(uint32_t node_count) {
      ptr_lkup_tbl_ptr_width = 9;
      if (tot_ptr_bit_count >= (1 << 24))
        ptr_lkup_tbl_ptr_width = 10;
      ptr_lookup_tbl_loc = 8 * 4 + 3;
      ptr_lookup_tbl = gen::get_lkup_tbl_size2(node_count, nodes_per_ptr_block, ptr_lkup_tbl_ptr_width);
      two_byte_count = 0; // todo: fix two_byte_tails.size() / 2;
      two_byte_data_loc = ptr_lookup_tbl_loc + ptr_lookup_tbl;
      idx2_ptr_count = get_idx2_ptrs_count();
      idx2_ptrs_map_loc = two_byte_data_loc + 0; // todo: fix two_byte_tails.size();
      grp_data_loc = idx2_ptrs_map_loc + idx2_ptrs_map.size();
      grp_data_size = get_hdr_size() + get_data_size();
      grp_ptrs_loc = grp_data_loc + grp_data_size;
    }
    void write_code_lookup_tbl(bool is_tail, FILE* fp) {
      for (int i = 0; i < 256; i++) {
        uint8_t code_i = i;
        bool code_found = false;
        for (int j = 1; j < freq_grp_vec.size(); j++) {
          uint8_t code_len = freq_grp_vec[j].code_len;
          uint8_t code = freq_grp_vec[j].code;
          if ((code_i >> (8 - code_len)) == code) {
            int bit_len = freq_grp_vec[j].grp_log2;
            fputc(bit_len, fp);
            fputc((j - 1) | (code_len << 5), fp);
            code_found = true;
            break;
          }
        }
        if (!code_found) {
          //printf("Code not found: %d", i);
          fputc(0, fp);
          fputc(0, fp);
        }
      }
    }
    void write_grp_data(uint32_t offset, bool is_tail, FILE* fp) {
      int grp_count = grp_data.size();
      fputc(grp_count, fp);
      if (grp_count > 0)
        fputc(freq_grp_vec[grp_count].grp_log2, fp);
      else
        fputc(0, fp);
      write_code_lookup_tbl(is_tail, fp);
      uint32_t total_data_size = 0;
      for (int i = 0; i < grp_count; i++) {
        gen::write_uint32(offset + grp_count * 4 + total_data_size, fp);
        total_data_size += grp_data[i].size();
      }
      for (int i = 0; i < grp_count; i++) {
        fwrite(grp_data[i].data(), grp_data[i].size(), 1, fp);
      }
    }
    void write_ptrs(FILE *fp) {
      fwrite(ptrs.data(), ptrs.size(), 1, fp);
    }
    typedef leopard::uniq_info *(*get_info_fn) (leopard::node *cur_node, std::vector<leopard::uniq_info *>& info_vec);
    static leopard::uniq_info *get_tails_info_fn(leopard::node *cur_node, std::vector<leopard::uniq_info *>& info_vec) {
      return (leopard::uniq_info *) info_vec[cur_node->get_tail()];
    }
    static leopard::uniq_info *get_vals_info_fn(leopard::node *cur_node, std::vector<leopard::uniq_info *>& info_vec) {
      return (leopard::uniq_info *) info_vec[cur_node->get_col_val()];
    }
    void write_ptr_lookup_tbl(byte_ptr_vec& all_node_sets, get_info_fn get_info_func, bool is_tail,
          std::vector<leopard::uniq_info *>& info_vec, FILE* fp) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      uint32_t bit_count3 = 0;
      int pos3 = 0;
      uint16_t bit_counts[3];
      memset(bit_counts, '\0', 6);
      uint32_t ftell1 = ftell(fp);
      if (ptr_lkup_tbl_ptr_width == 10)
        gen::write_uint32(bit_count, fp);
      else
        gen::write_uint24(bit_count, fp);
      for (int i = 1; i < all_node_sets.size(); i++) {
       leopard::node_set_handler cur_ns(all_node_sets, i);
       bool len_flag = false;
       if ((cur_ns.get_ns_hdr()->flags & NODE_SET_LEAP) == 0)
         len_flag = true;
       leopard::node cur_node = cur_ns.first_node();
       for (int k = 0; k <= cur_ns.last_node_idx(); k++) {
        uint8_t cur_node_flags = 0;
        if (len_flag)
          cur_node_flags = cur_node.get_flags();
        if (node_id && (node_id % nodes_per_ptr_block) == 0) {
          for (int j = 0; j < 3; j++)
            gen::write_uint16(bit_counts[j], fp);
          if (ptr_lkup_tbl_ptr_width == 10)
            gen::write_uint32(bit_count, fp);
          else
            gen::write_uint24(bit_count, fp);
          bit_count3 = 0;
          pos3 = 0;
          memset(bit_counts, '\0', 6);
        } else {
          if (node_id && (node_id % nodes_per_ptr_block3) == 0) {
            if (bit_count3 > 65535)
              std::cout << "UNEXPECTED: PTR_LOOKUP_TBL bit_count3 > 65k" << std::endl;
            bit_counts[pos3] = bit_count3;
            pos3++;
          }
        }
        if (is_tail) {
          if (cur_node_flags & NFLAG_TAIL) {
            leopard::uniq_info *vi = get_info_func(&cur_node, info_vec);
            freq_grp& fg = freq_grp_vec[vi->grp_no];
            bit_count += fg.grp_log2;
            bit_count3 += fg.grp_log2;
          }
        } else {
          if (cur_node_flags & NFLAG_LEAF) {
            leopard::uniq_info *vi = get_info_func(&cur_node, info_vec);
            freq_grp& fg = freq_grp_vec[vi->grp_no];
            bit_count += fg.grp_log2;
            bit_count3 += fg.grp_log2;
          }
        }
        node_id++;
        if (!len_flag) {
          len_flag = true;
          k--;
          continue;
        }
        cur_node.next();
       }
      }
      for (int j = 0; j < 3; j++)
        gen::write_uint16(bit_counts[j], fp);
      if (ptr_lkup_tbl_ptr_width == 10)
        gen::write_uint32(bit_count, fp);
      else
        gen::write_uint24(bit_count, fp);
      for (int j = 0; j < 3; j++)
        gen::write_uint16(bit_counts[j], fp);
      if (ftell(fp) - ftell1 != ptr_lookup_tbl) {
        std::cout << "WARNING PTR_LOOKUP_TBL size not maching: " << ftell(fp) - ftell1 << ":" << ptr_lookup_tbl << std::endl;
      }
    }
    void write_ptrs_data(byte_ptr_vec& all_node_sets, get_info_fn get_info_func, bool is_tail,
          std::vector<leopard::uniq_info *>& info_vec, char data_type, char encoding_type, FILE *fp) {
      fputc(ptr_lkup_tbl_ptr_width, fp);
      fputc(data_type, fp);
      fputc(encoding_type, fp);
      gen::write_uint32(max_len, fp);
      gen::write_uint32(ptr_lookup_tbl_loc, fp);
      gen::write_uint32(grp_data_loc, fp);
      gen::write_uint32(two_byte_count, fp);
      gen::write_uint32(idx2_ptr_count, fp);
      gen::write_uint32(grp_ptrs_loc, fp);
      gen::write_uint32(two_byte_data_loc, fp);
      gen::write_uint32(idx2_ptrs_map_loc, fp);
      write_ptr_lookup_tbl(all_node_sets, get_info_func, is_tail, info_vec, fp);
      fwrite(all_node_sets.data(), 0, 1, fp); // todo: fix two_byte_tails.size(), 1, fp);
      byte_vec *idx2_ptrs_map = get_idx2_ptrs_map();
      fwrite(idx2_ptrs_map->data(), idx2_ptrs_map->size(), 1, fp);
      write_grp_data(grp_data_loc + 514, is_tail, fp); // group count, 512 lookup tbl, tail locs, tails
      write_ptrs(fp);
      bldr_printf("Data size: %u, Ptrs size: %u, LkupTbl size: %u\nIdxMap size: %u, Uniq count: %u, Total size: %u\n\n",
        get_data_size(), get_ptrs_size(), ptr_lookup_tbl, idx2_ptrs_map->size(), info_vec.size(), get_total_size());
    }
    void reset_freq_counts() {
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        fg->grp_size = fg->freq_count = fg->count = 0;
      }
    }
    void set_freq_grps(std::vector<freq_grp> freq_grps) {
      freq_grp_vec = freq_grps;
    }
    std::vector<freq_grp>& get_freq_grps() {
      return freq_grp_vec;
    }
    freq_grp *get_freq_grp(int grp_no) {
      return &freq_grp_vec[grp_no];
    }
    size_t get_grp_count() {
      return freq_grp_vec.size();
    }
    byte_vec *get_idx2_ptrs_map() {
      return &idx2_ptrs_map;
    }
    byte_vec *get_ptrs() {
      return &ptrs;
    }
    void show_freq_codes() {
      bldr_printf("bits\tcd\tct_t\tfct_t\tlen_t\tcdln\tmxsz\tbyts\n");
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        bldr_printf("%u\t%2x\t%u\t%u\t%u\t%u\t%u\t%u\n", fg->grp_log2, fg->code,
              fg->count, fg->freq_count, fg->grp_size, fg->code_len, fg->grp_limit, (fg->grp_log2 * fg->freq_count) >> 3);
      }
      bldr_printf("Idx limit; %d\n", idx_limit);
    }
    void build_freq_codes(bool is_val = false) {
      if (freq_grp_vec.size() == 1) {
        for (int i = 1; i < freq_grp_vec.size(); i++) {
          freq_grp *fg = &freq_grp_vec[i];
          fg->code = fg->code_len = 0;
          fg->grp_log2 = ceil(log2(fg->grp_size));
          if (!is_val) {
            if (fg->grp_log2 > 8)
              fg->grp_log2 -= 8;
            else
              fg->grp_log2 = 0;
          }
        }
        return;
      }
      std::vector<uint32_t> freqs;
      for (int i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      huffman<uint32_t> _huffman(freqs);
      tot_ptr_bit_count = 0;
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        int len;
        fg->code = (uint8_t) _huffman.get_code(i - 1, len);
        fg->code_len = len;
        if (i <= idx_limit) {
          fg->grp_log2 = ceil(log2(fg->grp_limit));
        } else {
          fg->grp_log2 = ceil(log2(fg->grp_size));
        }
        if (is_val)
          fg->grp_log2 += fg->code_len;
        else {
          if (fg->grp_log2 > (8 - len))
            fg->grp_log2 -= (8 - len);
          else
            fg->grp_log2 = 0;
        }
        tot_ptr_bit_count += (fg->grp_log2 * fg->freq_count);
      }
    }
};

class tail_val_maps {
  private:
    byte_vec& uniq_tails;
    leopard::uniq_info_vec& uniq_tails_rev;
    //leopard::uniq_info_vec uniq_tails_fwd;
    freq_grp_ptrs_data tail_ptrs;
    freq_grp_ptrs_data val_ptrs;
    byte_vec two_byte_tails;
    byte_vec& uniq_vals;
    leopard::uniq_info_vec& uniq_vals_fwd;
    int start_nid, end_nid;
  public:
    tail_val_maps(byte_vec& _uniq_tails, leopard::uniq_info_vec& _uniq_tails_rev, byte_vec& _uniq_vals, leopard::uniq_info_vec& _uniq_vals_fwd)
        : uniq_tails (_uniq_tails), uniq_tails_rev (_uniq_tails_rev), uniq_vals (_uniq_vals), uniq_vals_fwd (_uniq_vals_fwd) {
    }
    ~tail_val_maps() {
      for (int i = 0; i < uniq_tails_rev.size(); i++)
        delete uniq_tails_rev[i];
      for (int i = 0; i < uniq_vals_fwd.size(); i++)
        delete uniq_vals_fwd[i];
    }
    struct sort_data {
      uint8_t *data;
      uint32_t len;
      uint32_t freq;
      uint32_t ns_id;
      uint8_t node_idx;
    };

    uint8_t *get_tail(leopard::byte_blocks& all_tails, leopard::node n, uint32_t& len) {
      uint8_t *v = all_tails[n.get_tail()];
      int8_t vlen;
      len = gen::read_vint32(v, &vlen);
      v += vlen;
      return v;
    }

    const double idx_cost_frac_cutoff = 0.1;
    uint32_t make_uniq_freq(leopard::uniq_info_vec& uniq_arr_vec, leopard::uniq_info_vec& uniq_freq_vec, uint32_t tot_freq_count, uint32_t& last_data_len, uint8_t& start_bits, uint8_t& grp_no) {
      clock_t t = clock();
      uniq_freq_vec = uniq_arr_vec;
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.end(), [this](const struct leopard::uniq_info *lhs, const struct leopard::uniq_info *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });

      uint32_t sum_freq = 0;
      if (start_bits < 7) {
        for (int i = 0; i < uniq_freq_vec.size(); i++) {
          leopard::uniq_info *vi = uniq_freq_vec[i];
          if (i >= pow(2, start_bits)) {
            double bit_width = log2(tot_freq_count / sum_freq);
            if (bit_width < 1.1) {
              bldr_printf("i: %d, freq: %u, Bit width: %.9f, start_bits: %d\n", i, sum_freq, bit_width, (int) start_bits);
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

      sum_freq = 0;
      int freq_idx = 0;
      last_data_len = 0;
      uint32_t cutoff_bits = start_bits;
      uint32_t nxt_idx_limit = pow(2, cutoff_bits);
      for (int i = 0; i < uniq_freq_vec.size(); i++) {
        leopard::uniq_info *vi = uniq_freq_vec[i];
        if (last_data_len >= nxt_idx_limit) {
          double cost_frac = last_data_len + nxt_idx_limit * 3;
          cost_frac /= (sum_freq * cutoff_bits / 8);
          if (cost_frac > idx_cost_frac_cutoff)
            break;
          sum_freq = 0;
          freq_idx = 0;
          last_data_len = 0;
          cutoff_bits += step_bits_idx;
          nxt_idx_limit = pow(2, cutoff_bits);
        }
        last_data_len += vi->len;
        last_data_len++;
        sum_freq += vi->freq_count;
        freq_idx++;
      }

      grp_no = 1;
      freq_idx = 0;
      last_data_len = 0;
      uint32_t cumu_freq_idx;
      uint32_t next_bits = start_bits;
      nxt_idx_limit = pow(2, next_bits);
      for (cumu_freq_idx = 0; cumu_freq_idx < uniq_freq_vec.size(); cumu_freq_idx++) {
        leopard::uniq_info *vi = uniq_freq_vec[cumu_freq_idx];
        if (freq_idx == nxt_idx_limit) {
          next_bits += step_bits_idx;
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

      // grp_no = 0;
      // uint32_t cumu_freq_idx = 0;
      //printf("%.1f\t%d\t%u\t%u\n", ceil(log2(freq_idx)), freq_idx, ftot, tail_len_tot);
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.begin() + cumu_freq_idx, [this](const struct leopard::uniq_info *lhs, const struct leopard::uniq_info *rhs) -> bool {
        return (lhs->grp_no == rhs->grp_no) ? (lhs->arr_idx > rhs->arr_idx) : (lhs->grp_no < rhs->grp_no);
      });
      std::sort(uniq_freq_vec.begin() + cumu_freq_idx, uniq_freq_vec.end(), [this](const struct leopard::uniq_info *lhs, const struct leopard::uniq_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / (lhs->len == 0 ? 1 : lhs->len);
        uint32_t rhs_freq = rhs->freq_count / (rhs->len == 0 ? 1 : rhs->len);
        lhs_freq = ceil(log10(lhs_freq));
        rhs_freq = ceil(log10(rhs_freq));
        return (lhs_freq == rhs_freq) ? (lhs->arr_idx > rhs->arr_idx) : (lhs_freq > rhs_freq);
      });
      t = gen::print_time_taken(t, "Time taken for uniq_freq: ");
      return cumu_freq_idx;

    }

    constexpr static uint32_t idx_ovrhds[] = {384, 3072, 24576, 196608, 1572864, 10782081};
    #define sfx_set_max_dflt 64
    #define MT_DEM_SUFFIX 1
    #define MT_DEM_UNIQ_ONLY 2
    #define MT_DEM_PREFIX 3
    void build_tail_maps(uint32_t tot_freq_count) {

      FILE *fp;

      clock_t t = clock();

      leopard::uniq_info_vec uniq_tails_freq;
      uint8_t grp_no;
      uint32_t last_data_len;
      uint8_t start_bits = 7;
      uint32_t cumu_freq_idx = make_uniq_freq((leopard::uniq_info_vec&) uniq_tails_rev, (leopard::uniq_info_vec&) uniq_tails_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      tail_ptrs.set_idx_info(start_bits, grp_no, last_data_len > 65535 ? 3 : 2);

      uint32_t freq_pos = 0;
      bool is_bin = false;
      if (uniq_tails_rev[0]->flags & LPDU_BIN)
        is_bin = true;
      if (!is_bin) {
        leopard::uniq_info *prev_ti = uniq_tails_freq[freq_pos];
        while (freq_pos < uniq_tails_freq.size()) {
          leopard::uniq_info *ti = uniq_tails_freq[freq_pos];
          freq_pos++;
          int cmp = gen::compare_rev(uniq_tails.data() + prev_ti->pos, prev_ti->len, uniq_tails.data() + ti->pos, ti->len);
          cmp--;
          if (cmp == ti->len || (freq_pos >= cumu_freq_idx && cmp > 1)) {
            ti->flags |= (cmp == ti->len ? LPDU_SUFFIX_FULL : LPDU_SUFFIX_PARTIAL);
            ti->cmp_rev = cmp;
            if (ti->cmp_rev_max < cmp)
              ti->cmp_rev_max = cmp;
            if (prev_ti->cmp_rev_min > cmp) {
              prev_ti->cmp_rev_min = cmp;
              if (cmp == ti->len && prev_ti->cmp_rev >= cmp)
                prev_ti->cmp_rev = cmp - 1;
            }
            if (prev_ti->cmp_rev_max < cmp)
              prev_ti->cmp_rev_max = cmp;
            prev_ti->flags |= LPDU_HAS_SUFFIX;
            ti->link_arr_idx = prev_ti->arr_idx;
          }
          if (cmp != ti->len)
            prev_ti = ti;
        }
      }

      uint32_t savings_prefix = 0;
      uint32_t savings_count_prefix = 0;
      // uint32_t pfx_set_len = sfx_set_max;
      // FILE *fp = fopen("prefixes.txt", "wb+");
      // FILE *fp1 = fopen("prefixes1.txt", "wb+");
      // leopard::uniq_info *prev_fwd_ti = uniq_tails_fwd[0];
      // uint32_t fwd_pos = 1;
      // while (fwd_pos < uniq_tails_fwd.size()) {
      //   leopard::uniq_info *ti = uniq_tails_fwd[fwd_pos];
      //   fwd_pos++;
      //   if (ti->flags & LPDU_SUFFIX_FULL)
      //     continue;
      //   int cmp = gen::compare(uniq_tails.data() + ti->tail_pos, ti->tail_len,
      //               uniq_tails.data() + prev_fwd_ti->tail_pos, prev_fwd_ti->tail_len);
      //   cmp--;
      //   if (cmp > 2) {
      //     if (cmp >= ti->tail_len - ti->cmp_rev_max)
      //       cmp = ti->tail_len - ti->cmp_rev_max;
      //     if (cmp > 5) {
      //       savings_prefix += (cmp - 1);
      //       savings_count_prefix++;
      //       ti->cmp_fwd = cmp - 1;
      //       int pfx_cmp = gen::compare(uniq_tails.data() + ti->tail_pos, cmp,
      //                       uniq_tails.data() + prev_fwd_ti->tail_pos, prev_fwd_ti->cmp_fwd + 1);
      //       fprintf(fp1, "%u\t%u\t%.*s\n", pfx_cmp, cmp, ti->tail_len, uniq_tails.data() + ti->tail_pos);
      //       if (pfx_cmp == 0) {
      //         //printf("[%.*s][%.*s]\n", cmp, uniq_tails.data() + ti->tail_pos, prev_fwd_ti->cmp_fwd + 1, uniq_tails.data() + prev_fwd_ti->tail_pos);
      //         //fprintf(fp, "\n%.*s%c", cmp - 1, uniq_tails.data() + ti->tail_pos + 1, 0);
      //       } else {
      //         pfx_cmp--;
      //         if (pfx_set_len + (cmp - pfx_cmp) < sfx_set_max) {
      //           fprintf(fp, "%.*s%c", cmp - pfx_cmp - 1, uniq_tails.data() + ti->tail_pos + 1 + pfx_cmp, '0' + (cmp - pfx_cmp - 1));
      //           pfx_set_len += (cmp - pfx_cmp);
      //         } else {
      //           pfx_set_len = cmp - 1;
      //           fprintf(fp, "\n%.*s%c", cmp - 1, uniq_tails.data() + ti->tail_pos + 1, '0');
      //         }
      //       }
      //       prev_fwd_ti = ti;
      //     }
      //   }
      //   prev_fwd_ti = ti;
      // }
      // fclose(fp);
      // fclose(fp1);

      freq_pos = 0;
      grp_no = 1;
      uint32_t cur_limit = pow(2, start_bits);
      tail_ptrs.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      tail_ptrs.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      uint32_t savings_full = 0;
      uint32_t savings_count_full = 0;
      uint32_t savings_partial = 0;
      uint32_t savings_count_partial = 0;
      uint32_t sfx_set_len = 0;
      uint32_t sfx_set_max = sfx_set_max_dflt;
      uint32_t sfx_set_count = 0;
      uint32_t sfx_set_freq = 0;
      uint32_t sfx_set_tot_cnt = 0;
      uint32_t sfx_set_tot_len = 0;
      while (freq_pos < uniq_tails_freq.size()) {
        leopard::uniq_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (is_bin) {
          uint32_t bin_len = ti->len;
          uint32_t len_len = tail_ptrs.get_set_len_len(bin_len);
          bin_len += len_len;
          uint32_t new_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, bin_len);
          ti->ptr = tail_ptrs.append_bin15_to_grp_data(grp_no, uniq_tails.data() + ti->pos, ti->len);
          ti->grp_no = grp_no;
          tail_ptrs.update_current_grp(grp_no, bin_len, ti->freq_count);
          cur_limit = new_limit;
          continue;
        } else if (ti->flags & LPDU_SUFFIX_FULL) {
          savings_full += ti->len;
          savings_full++;
          savings_count_full++;
          leopard::uniq_info *link_ti = uniq_tails_rev[ti->link_arr_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, link_ti->len + 1);
            link_ti->grp_no = grp_no;
            tail_ptrs.update_current_grp(link_ti->grp_no, link_ti->len + 1, link_ti->freq_count);
            link_ti->ptr = tail_ptrs.append_text_to_grp_data(grp_no, uniq_tails.data() + link_ti->pos, link_ti->len, true);
          }
          //cur_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, 0);
          tail_ptrs.update_current_grp(link_ti->grp_no, 0, ti->freq_count);
          ti->ptr = link_ti->ptr + link_ti->len - ti->len;
          ti->grp_no = link_ti->grp_no;
        } else {
          // uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, ti->grp_no, savings_prefix, savings_count_prefix, ti->cmp_rev_max);
          // if (prefix_len > 0) {
          //   leopard::uniq_info *link_ti = uniq_tails_rev[ti->link_rev_idx];
          //   uint32_t pfx_diff = grp_tails[grp_no - 1].size() - link_ti->tail_ptr;
          //   uint32_t pfx_len_len = get_pfx_len(pfx_diff);
          //   if (pfx_len_len < prefix_len) {
          //     savings_prefix += prefix_len;
          //     savings_prefix -= pfx_len_len;
          //     savings_count_prefix++;
          //     // bldr_printf("%u\t%u\t%u\t%u\n", prefix_len, grp_no, pfx_diff, pfx_len_len);
          //   }
          // }
          if (ti->flags & LPDU_SUFFIX_PARTIAL) {
            uint32_t cmp = ti->cmp_rev;
            uint32_t remain_len = ti->len - cmp;
            uint32_t len_len = tail_ptrs.get_set_len_len(cmp);
            remain_len += len_len;
            uint32_t new_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, remain_len);
            if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
              ti->cmp_rev_min = 0;
              if (sfx_set_len == 1)
                sfx_set_len += cmp;
              sfx_set_len += remain_len;
              sfx_set_count++;
              sfx_set_freq += ti->freq_count;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              tail_ptrs.update_current_grp(grp_no, remain_len, ti->freq_count);
              remain_len -= len_len;
              ti->ptr = tail_ptrs.append_text_to_grp_data(grp_no, uniq_tails.data() + ti->pos, remain_len);
              byte_vec& tail_data = tail_ptrs.get_data(grp_no);
              tail_ptrs.get_set_len_len(cmp, &tail_data);
            } else {
              // bldr_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
              sfx_set_len = 1;
              sfx_set_tot_len += ti->len;
              sfx_set_count = 1;
              sfx_set_freq = ti->freq_count;
              sfx_set_tot_cnt++;
              sfx_set_max = sfx_set_max_dflt;
              if (ti->len > sfx_set_max)
               sfx_set_max = ti->len * 2;
              tail_ptrs.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
              ti->ptr = tail_ptrs.append_text_to_grp_data(grp_no, uniq_tails.data() + ti->pos, ti->len, true);
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
            cur_limit = new_limit;
          } else {
            // bldr_printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
            sfx_set_len = 1;
            sfx_set_tot_len += ti->len;
            sfx_set_count = 1;
            sfx_set_freq = ti->freq_count;
            sfx_set_tot_cnt++;
            sfx_set_max = sfx_set_max_dflt;
            if (ti->len > sfx_set_max)
             sfx_set_max = ti->len * 2;
            cur_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, ti->len + 1);
            tail_ptrs.update_current_grp(grp_no, ti->len + 1, ti->freq_count);
            ti->ptr = tail_ptrs.append_text_to_grp_data(grp_no, uniq_tails.data() + ti->pos, ti->len, true);
          }
          ti->grp_no = grp_no;
        }
      }
      bldr_printf("Savings full: %u, %u\nSavings Partial: %u, %u / Sfx set: %u, %u\n", savings_full, savings_count_full, savings_partial, savings_count_partial, sfx_set_tot_len, sfx_set_tot_cnt);

      for (freq_pos = 0; freq_pos < cumu_freq_idx; freq_pos++) {
        leopard::uniq_info *ti = uniq_tails_freq[freq_pos];
        ti->ptr = tail_ptrs.append_ptr2_idx_map(ti->grp_no, ti->ptr);
      }

      // uint32_t remain_tot = 0;
      // uint32_t remain_cnt = 0;
      // uint32_t cmp_rev_min_tot = 0;
      // uint32_t cmp_rev_min_cnt = 0;
      // uint32_t free_tot = 0;
      // uint32_t free_cnt = 0;
      // fp = fopen("remain.txt", "w+");
      // freq_pos = 0;
      // while (freq_pos < uniq_tails_freq.size()) {
      //   leopard::uniq_info *ti = uniq_tails_freq[freq_pos];
      //   freq_pos++;
      //   if (ti->flags & LPDU_SUFFIX_FULL)
      //    continue;
      //   if ((ti->flags & 0x07) == 0) {
      //     // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
      //     //printf("%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
      //     free_tot += ti->len;
      //     free_cnt++;
      //   }
      //   int remain_len = ti->len - ti->cmp_rev_max - ti->cmp_fwd;
      //   if (remain_len > 3) {
      //     // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
      //     //fprintf(fp, "%.*s\n", remain_len - 1, uniq_tails.data() + ti->tail_pos + 1);
      //     remain_tot += remain_len;
      //     remain_tot--;
      //     remain_cnt++;
      //   }
      //   fprintf(fp, "%u\t%u\t%u\t%u\n", ti->freq_count, ti->len, ti->arr_idx, ti->cmp_rev_max);
      //   if (ti->cmp_rev_min != 0xFFFFFFFF && ti->cmp_rev_min > 4) {
      //     cmp_rev_min_tot += (ti->cmp_rev_min - 1);
      //     cmp_rev_min_cnt++;
      //   }
      //   //uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, ti->grp_no, savings_prefix, savings_count_prefix, ti->cmp_rev_max, fp);
      // }
      // fclose(fp);
      // bldr_printf("Free entries: %u, %u\n", free_tot, free_cnt);
      // bldr_printf("Savings prefix: %u, %u\n", savings_prefix, savings_count_prefix);
      // bldr_printf("Remaining: %u, %u\n", remain_tot, remain_cnt);
      // bldr_printf("Cmp_rev_min_tot: %u, %u\n", cmp_rev_min_tot, cmp_rev_min_cnt);

      tail_ptrs.build_freq_codes();
      tail_ptrs.show_freq_codes();

    }

    void build_val_maps(uint32_t tot_freq_count, char data_type) {
      uint32_t last_data_len;
      uint8_t start_bits = 1;
      uint8_t grp_no;
      leopard::uniq_info_vec uniq_vals_freq;
      uint32_t cumu_freq_idx = make_uniq_freq(uniq_vals_fwd, uniq_vals_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      val_ptrs.reset();
      val_ptrs.set_idx_info(start_bits, grp_no, last_data_len > 65535 ? 3 : 2);
      uint32_t freq_pos = 0;
      uint32_t cur_limit = pow(2, start_bits);
      grp_no = 1;
      val_ptrs.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
      val_ptrs.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
      while (freq_pos < uniq_vals_freq.size()) {
        leopard::uniq_info *vi = uniq_vals_freq[freq_pos];
        freq_pos++;
        uint8_t len_of_len = 0;
        if (data_type == LPDT_TEXT || data_type == LPDT_BIN)
          len_of_len = gen::get_vlen_of_uint32(vi->len);
        uint32_t len_plus_len = vi->len + len_of_len;
        cur_limit = val_ptrs.check_next_grp(grp_no, cur_limit, len_plus_len);
        vi->grp_no = grp_no;
        val_ptrs.update_current_grp(grp_no, len_plus_len, vi->freq_count);
        vi->ptr = val_ptrs.append_bin_to_grp_data(grp_no, uniq_vals.data() + vi->pos, vi->len, data_type);
      }
      for (freq_pos = 0; freq_pos < cumu_freq_idx; freq_pos++) {
        leopard::uniq_info *vi = uniq_vals_freq[freq_pos];
        vi->ptr = val_ptrs.append_ptr2_idx_map(vi->grp_no, vi->ptr);
      }
      val_ptrs.build_freq_codes(true);
      val_ptrs.show_freq_codes();
    }

    // uint32_t get_pfx_len(uint32_t sz) {
    //   return (sz < 1024 ? 2 : (sz < 131072 ? 3 : (sz < 16777216 ? 4 : 5)));
    // }

    // uint32_t find_prefix(byte_vec& uniq_tails, leopard::uniq_info_vec& uniq_tails_fwd, leopard::uniq_info *ti, uint32_t grp_no, uint32_t& savings_prefix, uint32_t& savings_count_prefix, int cmp_sfx) {
    //   uint32_t limit = ti->fwd_pos + 300;
    //   if (limit > uniq_tails_fwd.size())
    //     limit = uniq_tails_fwd.size();
    //   for (uint32_t i = ti->fwd_pos + 1; i < limit; i++) {
    //     leopard::uniq_info *ti_fwd = uniq_tails_fwd[i];
    //     int cmp = gen::compare(uniq_tails.data() + ti_fwd->pos, ti_fwd->len, uniq_tails.data() + ti->pos, ti->len);
    //     cmp = abs(cmp) - 1;
    //     if (cmp > ti->len - cmp_sfx - 1)
    //       cmp = ti->len - cmp_sfx - 1;
    //     if (cmp > 4 && grp_no == ti_fwd->grp_no && ti_fwd->cmp_fwd == 0) {
    //       if (cmp > ti_fwd->len - ti_fwd->cmp_rev_max - 1)
    //         cmp = ti_fwd->len - ti_fwd->cmp_rev_max - 1;
    //       if (cmp > 4) {
    //         ti->cmp_fwd = cmp;
    //         ti->link_arr_idx = ti_fwd->arr_idx;
    //         ti_fwd->cmp_fwd = cmp;
    //         return cmp;
    //       }
    //     }
    //   }
    //   return 0;
    // }

    void write_tail_ptrs_data(byte_ptr_vec& all_node_sets, FILE *fp) {
      tail_ptrs.write_ptrs_data(all_node_sets, tail_ptrs.get_tails_info_fn, true,
            (std::vector<leopard::uniq_info *>&) uniq_tails_rev, LPDT_BIN, 'u', fp);
    }

    void write_val_ptrs_data(byte_ptr_vec& all_node_sets, char data_type, char encoding_type, FILE *fp) {
      val_ptrs.write_ptrs_data(all_node_sets, tail_ptrs.get_vals_info_fn, false,
            (std::vector<leopard::uniq_info *>&) uniq_vals_fwd, data_type, encoding_type, fp);
    }

    uint32_t get_tail_ptr(uint32_t grp_no, leopard::uniq_info *ti) {
      uint32_t ptr = ti->ptr;
      if (grp_no <= tail_ptrs.get_idx_limit()) {
        byte_vec& idx2_ptr_map = *(tail_ptrs.get_idx2_ptrs_map());
        uint32_t pos = tail_ptrs.idx_map_arr[grp_no - 1] + ptr * tail_ptrs.get_idx_ptr_size();
        ptr = tail_ptrs.get_idx_ptr_size() == 2 ? gen::read_uint16(idx2_ptr_map, pos) : gen::read_uint24(idx2_ptr_map, pos);
      }
      return ptr;
    }

    uint32_t read_len(byte_vec& tail, uint32_t ptr, uint8_t& len_len) {
      len_len = 1;
      if (tail[ptr] < 15)
        return tail[ptr];
      uint32_t ret = 0;
      while (tail[++ptr] & 0x80) {
        ret <<= 7;
        ret |= (tail[ptr] & 0x7F);
        len_len++;
      }
      len_len++;
      ret <<= 4;
      ret |= (tail[ptr] & 0x0F);
      return ret + 15;
    }

    uint8_t get_first_byte(leopard::byte_blocks& all_tails, leopard::node *n) {
      if (uniq_tails_rev.size() == 0) {
        uint32_t tail_len;
        uint8_t *tail = get_tail(all_tails, *n, tail_len);
        return *tail;
      }
      if ((n->get_flags() & NFLAG_TAIL) == 0)
        return n->get_byte();
      leopard::uniq_info *ti = uniq_tails_rev[n->get_tail()];
      uint32_t grp_no = ti->grp_no;
      uint32_t ptr = get_tail_ptr(grp_no, ti);
      byte_vec& tail = tail_ptrs.get_data(grp_no);
      return *(tail.data() + ptr);
    }

    std::string get_tail_str(leopard::byte_blocks& all_tails, leopard::node *n) {
      if (uniq_tails_rev.size() == 0) {
        uint32_t tail_len;
        uint8_t *v = get_tail(all_tails, *n, tail_len);
        return std::string((const char *) v, tail_len);
      }
      leopard::uniq_info *ti = uniq_tails_rev[n->get_tail()];
      uint32_t grp_no = ti->grp_no;
      uint32_t tail_ptr = get_tail_ptr(grp_no, ti);
      uint32_t ptr = tail_ptr;
      byte_vec& tail = tail_ptrs.get_data(grp_no);
      std::string ret;
      int byt = tail[ptr++];
      while (byt > 31) {
        ret.append(1, byt);
        byt = tail[ptr++];
      }
      if (tail[--ptr] == 0)
        return ret;
      uint8_t len_len = 0;
      uint32_t sfx_len = read_len(tail, ptr, len_len);
      uint32_t ptr_end = tail_ptr;
      ptr = tail_ptr;
      do {
        byt = tail[ptr--];
      } while (byt != 0);
      do {
        byt = tail[ptr--];
      } while (byt > 31);
      ptr++;
      std::string prev_str;
      byt = tail[++ptr];
      while (byt != 0) {
        prev_str.append(1, byt);
        byt = tail[++ptr];
      }
      std::string last_str;
      while (ptr < ptr_end) {
        byt = tail[++ptr];
        while (byt > 31) {
          last_str.append(1, byt);
          byt = tail[++ptr];
        }
        uint32_t prev_sfx_len = read_len(tail, ptr, len_len);
        last_str.append(prev_str.substr(prev_str.length()-prev_sfx_len));
        ptr += len_len;
        ptr--;
        prev_str = last_str;
        last_str.clear();
      }
      ret.append(prev_str.substr(prev_str.length()-sfx_len));
      return ret;
    }

    leopard::uniq_info_vec *get_uniq_tails_rev() {
      return &uniq_tails_rev;
    }
    byte_vec *get_uniq_tails() {
      return &uniq_tails;
    }
    byte_vec *get_uniq_vals() {
      return &uniq_vals;
    }
    leopard::uniq_info_vec *get_uniq_vals_fwd() {
      return &uniq_vals_fwd;
    }
    freq_grp_ptrs_data *get_tail_grp_ptrs() {
      return &tail_ptrs;
    }
    freq_grp_ptrs_data *get_val_grp_ptrs() {
      return &val_ptrs;
    }

};

struct node_cache {
  uint32_t parent_node_id;
  uint32_t child_node_id;
  uint32_t freq_count;
  uint8_t node_offset;
  uint8_t node_byte;
};

class builder {

  private:
    uint32_t common_node_count;
    std::vector<node_cache> node_cache_vec;
    //dfox uniq_basix_map;
    //basix uniq_basix_map;
    //art_tree at;

    //builder(builder const&);
    //builder& operator=(builder const&);

    void append64_t(byte_vec& byv, uint64_t b64) {
      // byv.push_back(b64 >> 56);
      // byv.push_back((b64 >> 48) & 0xFF);
      // byv.push_back((b64 >> 40) & 0xFF);
      // byv.push_back((b64 >> 32) & 0xFF);
      // byv.push_back((b64 >> 24) & 0xFF);
      // byv.push_back((b64 >> 16) & 0xFF);
      // byv.push_back((b64 >> 8) & 0xFF);
      // byv.push_back(b64 & 0xFF);
      byv.push_back(b64 & 0xFF);
      byv.push_back((b64 >> 8) & 0xFF);
      byv.push_back((b64 >> 16) & 0xFF);
      byv.push_back((b64 >> 24) & 0xFF);
      byv.push_back((b64 >> 32) & 0xFF);
      byv.push_back((b64 >> 40) & 0xFF);
      byv.push_back((b64 >> 48) & 0xFF);
      byv.push_back(b64 >> 56);
    }
    void append_flags(byte_vec& byv, uint64_t bm_leaf, uint64_t bm_term, uint64_t bm_child, uint64_t bm_ptr) {
      append64_t(byv, bm_leaf);
      append64_t(byv, bm_term);
      append64_t(byv, bm_child);
      append64_t(byv, bm_ptr);
    }
    void append_byte_vec(byte_vec& byv1, byte_vec& byv2) {
      for (int k = 0; k < byv2.size(); k++)
        byv1.push_back(byv2[k]);
    }

  public:
    leopard::trie memtrie;
    char *out_filename;
    byte_vec trie;
    uint32_t end_loc;
    tail_val_maps tail_vals;
    uint32_t val_count;
    char *names;
    char *value_encoding;
    char *value_types;
    uint16_t *names_positions;
    uint16_t names_len;
    uint32_t *val_table;
    uint32_t col_val_table_loc;
    builder *col_trie_builder;
    FILE *fp;
    // other config options: sfx_set_max, step_bits_idx, dict_comp, prefix_comp
    builder(const char *out_file = NULL, const char *_names = "kv_tbl,key,value", const int _value_count = 1,
        const char *_value_encoding = "uu", const char *_value_types = "tt")
        : memtrie(_value_count, _value_encoding, _value_types),
          tail_vals (memtrie.uniq_tails, memtrie.uniq_tails_rev,
                      memtrie.uniq_vals, memtrie.uniq_vals_fwd) {
      col_trie_builder = NULL;
      val_count = _value_count;
      val_table = new uint32_t[_value_count];
      value_encoding = new char[_value_count + 1];
      memset(value_encoding, 'u', _value_count + 1);
      *value_encoding = 't'; // first letter is for key
      memcpy(value_encoding, _value_encoding, gen::min(strlen(_value_encoding), _value_count + 1));
      value_types = new char[_value_count + 1];
      memset(value_types, '*', _value_count + 1);
      memcpy(value_types, _value_encoding, gen::min(strlen(_value_encoding), _value_count + 1));
      set_names(_names, _value_types, value_encoding);
      common_node_count = 0;
      //art_tree_init(&at);
      memtrie.set_print_enabled(is_bldr_print_enabled);
      fp = NULL;
      out_filename = NULL;
      if (out_file != NULL)
        set_out_file(out_file);
    }

    ~builder() {
      delete names;
      delete val_table;
      delete names_positions;
      delete value_encoding;
      if (out_filename != NULL)
        delete out_filename;
      if (col_trie_builder != NULL)
        delete col_trie_builder;
      close_file();
    }

    void close_file() {
      if (fp != NULL)
        fclose(fp);
      fp = NULL;
    }

    void set_names(const char *_names, const char *_value_types, const char *_value_encoding) {
      int col_count = val_count + 1;
      names_len = strlen(_names) + col_count * 2 + 3;
      names = new char[names_len];
      memset(names, '*', col_count);
      memcpy(names, _value_types, gen::min(strlen(_value_types), col_count));
      names[col_count] = ',';
      memcpy(names + col_count + 1, _value_encoding, col_count);
      names[col_count * 2 + 1] = ',';
      memcpy(names + col_count * 2 + 2, _names, strlen(_names));
      // std::cout << names << std::endl;
      names_positions = new uint16_t[col_count + 2];
      int idx = 0;
      int name_str_pos = col_count;
      while (name_str_pos < names_len) {
        if (names[name_str_pos] == ',') {
          names[name_str_pos] = '\0';
          names_positions[idx++] = name_str_pos + 1;
        }
        name_str_pos++;
      }
      name_str_pos--;
      names[col_count] = '\0';
      names[col_count * 2 + 1] = '\0';
      names[name_str_pos] = '\0';
    }

    void set_print_enabled(bool to_print_messages = true) {
      is_bldr_print_enabled = to_print_messages;
      memtrie.set_print_enabled(is_bldr_print_enabled);
    }

    void set_out_file(const char *out_file) {
      if (out_file == NULL)
        return;
      int len = strlen(out_file);
      if (out_filename != NULL)
        delete out_filename;
      out_filename = new char[len + 1];
      strcpy(out_filename, out_file);
    }

    size_t size() {
      return memtrie.key_count;
    }

    uint8_t append_tail_ptr(leopard::node *cur_node) {
      if ((cur_node->get_flags() & NFLAG_TAIL) == 0)
        return cur_node->get_byte();
      uint8_t node_val;
      uint32_t ptr = 0;
      freq_grp_ptrs_data *tail_ptrs = tail_vals.get_tail_grp_ptrs();
        leopard::uniq_info *ti = (*tail_vals.get_uniq_tails_rev())[cur_node->get_tail()];
        uint8_t grp_no = ti->grp_no;
        // if (grp_no == 0 || ti->tail_ptr == 0)
        //   bldr_printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
        ptr = ti->ptr;
        freq_grp *fg = tail_ptrs->get_freq_grp(grp_no);
        int node_val_bits = 8 - fg->code_len;
        node_val = (fg->code << node_val_bits) | (ptr & ((1 << node_val_bits) - 1));
        ptr >>= node_val_bits;
        tail_ptrs->append_ptr_bits(ptr, fg->grp_log2);
      return node_val;
    }

    void make_delta_values() {
      leopard::byte_blocks *delta_vals = new leopard::byte_blocks();
      int64_t prev_val = 0;
      uint32_t node_id = 0;
      for (uint32_t cur_ns_idx = 1; cur_ns_idx < memtrie.all_node_sets.size(); cur_ns_idx++) {
        leopard::node_set_handler cur_ns(memtrie.all_node_sets, cur_ns_idx);
        leopard::node cur_node = cur_ns.first_node();
        leopard::node_set_header *ns_hdr = cur_ns.get_ns_hdr();
        node_id = ns_hdr->node_id;
        if (ns_hdr->flags & NODE_SET_LEAP)
          node_id++;
        if ((node_id % nodes_per_bv_block3) == 0)
          prev_val = 0;
        for (int k = 0; k <= cur_ns.last_node_idx(); k++) {
          if (cur_node.get_flags() & NFLAG_LEAF) {
            uint32_t val_pos = cur_node.get_col_val();
            int64_t col_val = leopard::gen::read_svint60((*memtrie.all_vals)[val_pos]);
            int64_t delta_val = col_val;
            // std::cout << node_id << ", " << val_pos << ", ";
            if (node_id % nodes_per_bv_block3)
              delta_val -= prev_val;
            prev_val = col_val;
            // std::cout << col_val << ": " << delta_val << std::endl;
            val_pos = delta_vals->append_svint60(delta_val);
            cur_node.set_col_val(val_pos);
          }
          node_id++;
          if ((node_id % nodes_per_bv_block3) == 0)
            prev_val = 0;
          cur_node.next();
        }
      }
      std::cout << "All vals len: " << delta_vals->size() << std::endl;
      delete memtrie.all_vals;
      memtrie.all_vals = delta_vals;
    }

    void build_col_trie() {
      col_trie_builder->build("col_trie.mdx");
      std::vector<leopard::val_sequence>& col_trie_val_seq = col_trie_builder->memtrie.val_seq;
      for (int seq_idx = 0; seq_idx < col_trie_val_seq.size(); seq_idx++) {
        leopard::val_sequence& val_seq_obj = col_trie_val_seq[seq_idx];
        if (val_seq_obj.val_pos == UINT_MAX)
          continue;
        leopard::node_set_handler col_trie_ns(col_trie_builder->memtrie.all_node_sets, val_seq_obj.node_set_id);
        leopard::node_set_header *col_trie_ns_hdr = col_trie_ns.get_ns_hdr();
        int64_t col_trie_node_id = col_trie_ns_hdr->node_id + val_seq_obj.node_idx;
        if (col_trie_ns_hdr->flags & NODE_SET_LEAP)
          col_trie_node_id++;
        uint32_t node_id_val_pos = memtrie.append_val(&col_trie_node_id, 8, '0');
        leopard::val_sequence& this_val_seq = memtrie.val_seq[seq_idx];
        leopard::node_set_handler cur_ns(memtrie.all_node_sets, this_val_seq.node_set_id);
        leopard::node cur_node = cur_ns[this_val_seq.node_idx];
        cur_node.set_col_val(node_id_val_pos);
      }
    }

    uint32_t build_and_write_col_val() {
        char encoding_type = value_encoding[memtrie.cur_val_idx + 1];
      if (memtrie.all_vals->size() > 0 || encoding_type == 't') {
        bldr_printf("Col: %s, ", names + names_positions[memtrie.cur_val_idx + 3]);
        char data_type = memtrie.value_types[memtrie.cur_val_idx + 1];
        bldr_printf("Type: %c, Enc: %c. ", data_type, encoding_type);
        if (encoding_type == 't') {
          build_col_trie();
        }
        if (encoding_type == 'd' || encoding_type == 't')
          make_delta_values();
        if (encoding_type == 't')
          data_type = '0';
        leopard::val_sort_callbacks val_sort_cb(memtrie.all_node_sets, *memtrie.all_vals, memtrie.uniq_vals);
        uint32_t tot_freq_count = leopard::uniq_maker::make_uniq(memtrie.all_node_sets, *memtrie.all_vals,
          memtrie.uniq_vals, memtrie.uniq_vals_fwd, val_sort_cb, memtrie.max_val_len, data_type);
        tail_vals.build_val_maps(tot_freq_count, data_type);
        for (uint32_t cur_ns_idx = 1; cur_ns_idx < memtrie.all_node_sets.size(); cur_ns_idx++) {
          leopard::node_set_handler cur_ns(memtrie.all_node_sets, cur_ns_idx);
          leopard::node cur_node = cur_ns.first_node();
          for (int k = 0; k <= cur_ns.last_node_idx(); k++) {
            append_val_ptr(&cur_node);
            cur_node.next();
          }
        }
        tail_vals.get_val_grp_ptrs()->append_ptr_bits(0x00, 8); // read beyond protection
        freq_grp_ptrs_data *val_ptrs = tail_vals.get_val_grp_ptrs();
        val_ptrs->build(memtrie.node_count);
      }
      return write_col_val();
    }

    void reset_for_next_col() {
      char encoding_type = value_encoding[memtrie.cur_val_idx + 2];
      if (encoding_type == 't') {
        if (col_trie_builder != NULL)
          delete col_trie_builder;
        col_trie_builder = new builder(NULL, "col_trie,key,val", 1, "*u", "*0");
        return memtrie.reset_for_next_col(&col_trie_builder->memtrie);
      }
      return memtrie.reset_for_next_col();
    }

    bool insert_col_val(const void *val, const int val_len) {
      memtrie.insert_col_val(val, val_len);
      return true;
    }

    void append_val_ptr(leopard::node *cur_node) {
      if ((cur_node->get_flags() & NFLAG_LEAF) == 0)
        return;
      freq_grp_ptrs_data *val_ptrs = tail_vals.get_val_grp_ptrs();
      leopard::uniq_info *vi = (*tail_vals.get_uniq_vals_fwd())[cur_node->get_col_val()];
      freq_grp *fg = val_ptrs->get_freq_grp(vi->grp_no);
      // if (cur_node->node_id < 500)
      //   std::cout << "node_id: " << cur_node->node_id << "grp no: " << (int) vi->grp_no << ", bitlen: " << fg->grp_log2 << ", ptr: " << vi->ptr << std::endl;
      uint32_t to_append = vi->ptr;
      to_append |= (fg->code << (fg->grp_log2 - fg->code_len));
      val_ptrs->append_ptr_bits(to_append, fg->grp_log2);
    }

    uint32_t build_trie() {
      leopard::tail_sort_callbacks tail_sort_cb(memtrie.all_node_sets, *memtrie.all_tails, memtrie.uniq_tails);
      uint32_t tot_freq_count = leopard::uniq_maker::make_uniq(memtrie.all_node_sets, *memtrie.all_tails,
          memtrie.uniq_tails, memtrie.uniq_tails_rev, tail_sort_cb, memtrie.max_tail_len, LPDT_BIN);
      if (memtrie.uniq_tails_rev.size() > 0)
        tail_vals.build_tail_maps(tot_freq_count);
      uint32_t flag_counts[8];
      uint32_t char_counts[8];
      memset(flag_counts, '\0', sizeof(uint32_t) * 8);
      memset(char_counts, '\0', sizeof(uint32_t) * 8);
      uint32_t sfx_full_count = 0;
      uint32_t sfx_partial_count = 0;
      uint64_t bm_leaf = 0;
      uint64_t bm_term = 0;
      uint64_t bm_child = 0;
      uint64_t bm_ptr = 0;
      uint64_t bm_mask = 1UL;
      byte_vec byte_vec64;
      //trie.reserve(node_count + (node_count >> 1));
      uint32_t ptr_count = 0;
      uint32_t node_count = 0;
      uint32_t cur_ns_idx = 1;
      for (; cur_ns_idx < memtrie.all_node_sets.size(); cur_ns_idx++) {
       leopard::node_set_handler cur_ns(memtrie.all_node_sets, cur_ns_idx);
       bool len_written = false;
       if ((cur_ns.get_ns_hdr()->flags & NODE_SET_LEAP) == 0)
         len_written = true;
       leopard::node cur_node = cur_ns.first_node();
       for (int k = 0; k <= cur_ns.last_node_idx(); k++) {
        uint8_t node_byte, cur_node_flags;
        if (!len_written) {
          node_byte = cur_ns.last_node_idx();
          cur_node_flags = 0;
        } else {
          node_byte = append_tail_ptr(&cur_node);
          cur_node_flags = cur_node.get_flags();
        }
        if (node_count && (node_count % 64) == 0) {
          append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
          append_byte_vec(trie, byte_vec64);
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
        bm_mask <<= 1;
        byte_vec64.push_back(node_byte);
        node_count++;
        if (!len_written) {
          len_written = true;
          k--;
          continue;
        }
        if (cur_node.get_flags() & NFLAG_TAIL) {
          leopard::uniq_info_vec *uniq_tails_rev = tail_vals.get_uniq_tails_rev();
          leopard::uniq_info *ti = (*uniq_tails_rev)[cur_node.get_tail()];
          if (ti->flags & LPDU_SUFFIX_FULL)
            sfx_full_count++;
          if (ti->flags & LPDU_SUFFIX_PARTIAL)
            sfx_partial_count++;
          if (ti->len > 1)
            char_counts[(ti->len > 8) ? 7 : (ti->len - 2)]++;
          ptr_count++;
        }
        uint8_t flags = (cur_node.get_flags() & NFLAG_LEAF ? 1 : 0) +
          (cur_node.get_child() > 0 ? 2 : 0) + (cur_node.get_flags() & NFLAG_TAIL ? 4 : 0) +
          (cur_node.get_flags() & NFLAG_TERM ? 8 : 0);
        flag_counts[flags & 0x07]++;
        cur_node.next();
       }
      }
      // TODO: write on all cases?
      append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
      append_byte_vec(trie, byte_vec64);
      for (int i = 0; i < 8; i++) {
        bldr_printf("Flag %d: %d\tChar: %d: %d\n", i, flag_counts[i], i + 2, char_counts[i]);
      }
      bldr_printf("Tot ptr count: %u, Full sfx count: %u, Partial sfx count: %u\n", ptr_count, sfx_full_count, sfx_partial_count);
      tail_vals.get_tail_grp_ptrs()->build(node_count);
      uint32_t tail_size = tail_vals.get_tail_grp_ptrs()->get_total_size();
      end_loc = (4 + tail_size + trie.size());
      return end_loc;
    }
    uint32_t write_trie_tail_ptrs_data(FILE *fp) {
      bldr_printf("\nTrie size: %u\n", trie.size());
      uint32_t tail_size = tail_vals.get_tail_grp_ptrs()->get_total_size();
      gen::write_uint32(tail_size, fp);
      bldr_printf("Tail stats - ");
      tail_vals.write_tail_ptrs_data(memtrie.all_node_sets, fp);
      fwrite(trie.data(), trie.size(), 1, fp);
      return 0;
    }
    uint32_t write_val_ptrs_data(char data_type, char encoding_type, FILE *fp_val) {
      uint32_t val_fp_offset = 0;
      if (get_uniq_val_count() > 0) {
        bldr_printf("Stats - ");
        tail_vals.write_val_ptrs_data(memtrie.all_node_sets, data_type, encoding_type, fp_val);
        val_fp_offset += tail_vals.get_val_grp_ptrs()->get_total_size();
      }
      return val_fp_offset;
    }
    size_t trie_data_ptr_size() {
      size_t ret = 8 + trie.size() + tail_vals.get_tail_grp_ptrs()->get_total_size();
      //if (get_uniq_val_count() > 0)
      //  ret += tail_vals.get_val_grp_ptrs()->get_total_size();
      return ret;
    }
    size_t get_uniq_val_count() {
      return tail_vals.get_uniq_vals_fwd()->size();
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, uint8_t *val) {
      return memtrie.get(key, key_len, in_size_out_value_len, val);
    }

    bool put(const uint8_t *key, int key_len, const void *value, int value_len) {
      return memtrie.insert(key, key_len, value, value_len);
    }

    bool insert(const uint8_t *key, int key_len) {
      return memtrie.insert(key, key_len, NULL, 0);
    }

    bool insert(const uint8_t *key, int key_len, const void *val, int val_len) {
      return memtrie.insert(key, key_len, val, val_len);
    }

    void set_node_id() {
      uint32_t node_id = 0;
      for (int i = 1; i < memtrie.all_node_sets.size(); i++) {
        leopard::node_set_header *ns_hdr = (leopard::node_set_header *) memtrie.all_node_sets[i];
        ns_hdr->node_id = node_id;
        if (ns_hdr->last_node_idx > 4) {
          node_id++;
          memtrie.node_count++;
          ns_hdr->flags |= NODE_SET_LEAP;
        }
        node_id += ns_hdr->last_node_idx;
        node_id++;
      }
    }

    uint32_t build_cache() {
      set_node_id();
      uint32_t cache_count = 256;
      while (cache_count < memtrie.key_count / 512)
        cache_count *= 2;
      //cache_count *= 2;
      node_cache_vec.resize(cache_count);
      memset(node_cache_vec.data(), '\0', cache_count * sizeof(node_cache));
      build_cache(1, cache_count, 1);
      return cache_count;
    }

    uint32_t build_cache(uint32_t ns_id, uint32_t cache_count, int level) {
      if (ns_id == 0)
        return 1;
      if (memtrie.max_level < level)
        memtrie.max_level = level;
      leopard::node_set_handler ns(memtrie.all_node_sets, ns_id);
      leopard::node n = ns.first_node();
      leopard::node_set_header *ns_hdr = ns.get_ns_hdr();
      uint32_t freq_count = 0;
      for (int i = 0; i <= ns_hdr->last_node_idx; i++) {
        uint32_t node_freq = build_cache(n.get_child(), cache_count, level + 1);
        freq_count += node_freq;
        if ((n.get_flags() & NFLAG_TAIL) == 0) {
          uint8_t node_byte = n.get_byte();
          uint32_t cache_loc = (ns_hdr->node_id ^ (ns_hdr->node_id << 5) ^ node_byte) & (cache_count - 1);
          node_cache& nc = node_cache_vec[cache_loc];
          leopard::node_set_handler child_nsh(memtrie.all_node_sets, n.get_child());
          uint32_t child_node_id = child_nsh.get_ns_hdr()->node_id;
          int node_offset = i + (ns_hdr->flags & NODE_SET_LEAP ? 1 : 0);
          if (nc.freq_count <= node_freq && ns_hdr->node_id < (1 << 24) && child_node_id < (1 << 24) && node_offset < 256) {
            nc.freq_count = node_freq;
            nc.parent_node_id = ns_hdr->node_id;
            nc.child_node_id = child_node_id;
            nc.node_offset = node_offset;
            nc.node_byte = node_byte;
          }
        }
        n.next();
      }
      return freq_count;
    }

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

    uint8_t min_pos[256][256];
    bldr_min_pos_stats make_min_positions() {
      bldr_min_pos_stats stats;
      memset(min_pos, 0xFF, 65536);
      for (int i = 1; i < memtrie.all_node_sets.size(); i++) {
        leopard::node_set_handler cur_ns(memtrie.all_node_sets, i);
        uint8_t len = cur_ns.last_node_idx();
        if (stats.min_len > len)
          stats.min_len = len;
        if (stats.max_len < len)
          stats.max_len = len;
        leopard::node cur_node = cur_ns.first_node();
        for (int k = 0; k <= len; k++) {
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
      return stats;
    }

    uint32_t decide_min_stat_to_use(bldr_min_pos_stats& stats) {
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
      return 0; //todo
    }

    void write_sec_cache(bldr_min_pos_stats& stats, uint32_t sec_cache_size, FILE *fp) {
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        for (int j = 0; j <= 255; j++) {
          uint8_t min_len = min_pos[i][j];
          if (min_len == 0xFF)
            min_len = 0;
          min_len++;
          fputc(min_len, fp);
        }
      }
    }

    #define BV_LEAF 1
    #define BV_CHILD 2
    #define BV_TERM 3
    void build(const char *filename = NULL) {

      set_out_file(filename);

      clock_t t = clock();

      printf("Key count: %u\n", memtrie.key_count);

      memtrie.sort_node_sets();
      bldr_min_pos_stats min_stats = make_min_positions();

      if (memtrie.all_vals->size() == 0) {
        val_count = 0;
        memtrie.value_count = 0;
      }

      uint32_t cache_count = build_cache();
      uint32_t cache_size = cache_count * sizeof(bldr_cache);

      uint32_t sec_cache_count = decide_min_stat_to_use(min_stats);
      uint32_t sec_cache_size = (min_stats.max_len - min_stats.min_len + 1) * 256;

      uint32_t term_bvlt_sz = gen::get_lkup_tbl_size2(memtrie.node_count, nodes_per_bv_block, 7);
      uint32_t child_bvlt_sz = term_bvlt_sz;
      uint32_t leaf_bvlt_sz = term_bvlt_sz;
      uint32_t term_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count, sel_divisor, 3);
      uint32_t child_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.node_set_count, sel_divisor, 3);
      uint32_t leaf_select_lt_sz = gen::get_lkup_tbl_size2(memtrie.key_count, sel_divisor, 3);

      uint32_t dummy_loc = 4 + 10 + 18 * 4; // 86
      uint32_t common_node_loc = dummy_loc + 0;
      uint32_t cache_loc = common_node_loc + ceil(common_node_count * 1.5);
      uint32_t sec_cache_loc = cache_loc + cache_size;
      uint32_t term_select_lkup_loc = sec_cache_loc + sec_cache_size;
      uint32_t term_bv_loc = term_select_lkup_loc + term_select_lt_sz;
      uint32_t child_bv_loc = term_bv_loc + term_bvlt_sz;
      uint32_t trie_tail_ptrs_data_loc = child_bv_loc + child_bvlt_sz;
      uint32_t trie_tail_ptrs_data_sz = build_trie();
      uint32_t leaf_select_lkup_loc = trie_tail_ptrs_data_loc + trie_tail_ptrs_data_sz;
      uint32_t leaf_bv_loc = leaf_select_lkup_loc + leaf_select_lt_sz;
      uint32_t child_select_lkup_loc = leaf_bv_loc + leaf_bvlt_sz;
      uint32_t names_loc = child_select_lkup_loc + child_select_lt_sz;
      col_val_table_loc = names_loc + (val_count + 3) * sizeof(uint16_t) + names_len;
      uint32_t col_val_loc0 = col_val_table_loc + val_count * sizeof(uint32_t);

      fp = fopen(out_filename, "wb+");
      fclose(fp);
      fp = fopen(out_filename, "rb+");
      if (fp == NULL)
        throw errno;

      fputc(0xA5, fp); // magic byte
      fputc(0x01, fp); // version 1.0
      fputc(0, fp); // reserved
      fputc(0, fp);

      gen::write_uint16(memtrie.value_count, fp);
      gen::write_uint32(names_loc, fp);
      gen::write_uint32(col_val_table_loc, fp);

      gen::write_uint32(memtrie.node_count, fp);
      gen::write_uint32(dummy_loc, fp);
      gen::write_uint32(common_node_count, fp);
      gen::write_uint32(memtrie.key_count, fp);
      gen::write_uint32(memtrie.max_key_len, fp);
      gen::write_uint32(memtrie.max_val_len, fp);
      gen::write_uint16(memtrie.max_tail_len, fp);
      gen::write_uint16(memtrie.max_level, fp);
      gen::write_uint32(cache_count, fp);
      fwrite(&min_stats, 4, 1, fp);
      gen::write_uint32(cache_loc, fp);
      gen::write_uint32(sec_cache_loc, fp);

      gen::write_uint32(term_select_lkup_loc, fp);
      gen::write_uint32(term_bv_loc, fp);
      gen::write_uint32(child_select_lkup_loc, fp);
      gen::write_uint32(child_bv_loc, fp);
      gen::write_uint32(leaf_select_lkup_loc, fp);
      gen::write_uint32(leaf_bv_loc, fp);
      gen::write_uint32(trie_tail_ptrs_data_loc, fp);

      write_pri_cache(node_cache_vec, fp);
      write_sec_cache(min_stats, sec_cache_size, fp);
      write_bv_select_lt(BV_TERM, fp);
      write_bv_rank_lt(BV_TERM, fp);
      write_bv_rank_lt(BV_CHILD, fp);

      uint32_t total_idx_size = 4 + 18 * 4 + cache_size + sec_cache_size +
                term_select_lt_sz + term_bvlt_sz + child_bvlt_sz +
                leaf_select_lt_sz + child_select_lt_sz + leaf_bvlt_sz;
      write_trie_tail_ptrs_data(fp);

      total_idx_size += trie_data_ptr_size();
      write_bv_select_lt(BV_LEAF, fp);
      write_bv_rank_lt(BV_LEAF, fp);
      write_bv_select_lt(BV_CHILD, fp);

      uint32_t val_size = 0;
      if (memtrie.all_vals->size() > 0) {
        val_table[0] = col_val_loc0;
        write_names(fp);
        write_col_val_table(fp);
        val_size = build_and_write_col_val();
      }
      
      uint32_t total_size = total_idx_size + val_size;

      bldr_printf("\nNode count: %u, Trie bit vectors: %u, Leaf bit vectors: %u\nSelect lookup tables - Term: %u, Child: %u, Leaf: %u\n"
        "Pri cache: %u, struct size: %u, Sec cache: %u\nNode struct size: %u, Max tail len: %u\n",
            memtrie.node_count, term_bvlt_sz * 2, leaf_bvlt_sz, term_select_lt_sz, child_select_lt_sz, leaf_select_lt_sz,
            cache_size, sizeof(bldr_cache), sec_cache_size, sizeof(leopard::node), memtrie.max_tail_len);

      // fp = fopen("nodes.txt", "wb+");
      // // dump_nodes(first_node, fp);
      // find_rpt_nodes(fp);
      // fclose(fp);

      gen::print_time_taken(t, "Time taken for build(): ");
      bldr_printf("Total idx size: %u, Val size: %u, Total size: %u\n", total_idx_size, val_size, total_size);

    }

    void write_names(FILE *fp) {
      int name_count = val_count + 3;
      for (int i = 0; i < name_count; i++)
        gen::write_uint16(names_positions[i], fp);
      fwrite(names, names_len, 1, fp);
    }

    void write_col_val_table(FILE *fp) {
      for (int i = 0; i < val_count; i++)
        gen::write_uint32(val_table[i], fp);
    }

    uint32_t write_col_val() {
      uint32_t val_size = write_val_ptrs_data(memtrie.value_types[memtrie.cur_val_idx + 1],
          value_encoding[memtrie.cur_val_idx + 1], fp);
      if (memtrie.cur_val_idx > 0) {
        uint32_t prev_val_loc = val_table[memtrie.cur_val_idx - 1];
        val_table[memtrie.cur_val_idx] = prev_val_loc + memtrie.prev_val_size;
      }
      memtrie.prev_val_size = val_size;
      return val_size;
    }

    void write_final_val_table() {
      fseek(fp, col_val_table_loc, SEEK_SET);
      fwrite(val_table, val_count * sizeof(uint32_t), 1, fp);
      bldr_printf("Total size: %u\n", val_table[memtrie.cur_val_idx] + memtrie.prev_val_size);
      close_file();
    }

    // struct nodes_ptr_grp {
    //   uint32_t node_id;
    //   uint32_t ptr;
    // };

    void write_bv3(uint32_t node_id, uint32_t& count, uint32_t& count3, uint8_t *buf3, uint8_t& pos3, FILE *fp) {
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        fwrite(buf3, 3, 1, fp);
        gen::write_uint32(count, fp);
        count3 = 0;
        memset(buf3, 0, 3);
        pos3 = 0;
      } else if (node_id && (node_id % nodes_per_bv_block3) == 0) {
        buf3[pos3] = count3;
        //count3 = 0;
        pos3++;
      }
    }

    void write_bv_rank_lt(int which, FILE *fp) {
      uint32_t node_id = 0;
      uint32_t count = 0;
      uint32_t count3 = 0;
      uint8_t buf3[3];
      uint8_t pos3 = 0;
      memset(buf3, 0, 3);
      gen::write_uint32(0, fp);
      for (int i = 1; i < memtrie.all_node_sets.size(); i++) {
       leopard::node_set_handler cur_ns(memtrie.all_node_sets, i);
       bool len_flag = false;
       if ((cur_ns.get_ns_hdr()->flags & NODE_SET_LEAP) == 0)
         len_flag = true;
       leopard::node cur_node = cur_ns.first_node();
       for (int k = 0; k <= cur_ns.last_node_idx(); k++) {
        uint8_t cur_node_flags = 0;
        if (len_flag)
          cur_node_flags = cur_node.get_flags();
        write_bv3(node_id, count, count3, buf3, pos3, fp);
        uint32_t ct;
        switch (which) {
          case BV_TERM:
            ct = (cur_node_flags & NFLAG_TERM ? 1 : 0);
            break;
          case BV_CHILD:
            ct = (cur_node_flags & NFLAG_CHILD ? 1 : 0);
            break;
          case BV_LEAF:
            ct = (cur_node_flags & NFLAG_LEAF ? 1 : 0);
            break;
        }
        count += ct;
        count3 += ct;
        node_id++;
        if (!len_flag) {
          len_flag = true;
          k--;
          continue;
        }
        cur_node.next();
       }
      }
      fwrite(buf3, 3, 1, fp);
      // extra (guard)
      gen::write_uint32(count, fp);
      fwrite(buf3, 3, 1, fp);
    }

    void write_pri_cache(std::vector<node_cache>& node_cache_vec, FILE *fp) {
      for (int i = 0; i < node_cache_vec.size(); i++) {
        node_cache& nc = node_cache_vec[i];
        gen::write_uint24(nc.parent_node_id, fp);
        fputc(nc.node_offset, fp);
        gen::write_uint24(nc.child_node_id, fp);
        fputc(nc.node_byte, fp);
      }
    }

    uint32_t write_bv_select_val(uint32_t node_id, FILE *fp) {
      uint32_t val_to_write = node_id / nodes_per_bv_block;
      // if (val_to_write - prev_val > 4)
      //   bldr_printf("Nid:\t%u\tblk:\t%u\tdiff:\t%u\n", node_id, val_to_write, val_to_write-prev_val);
      gen::write_uint24(val_to_write, fp);
      return val_to_write;
    }

    bool node_qualifies_for_select(leopard::node *cur_node, uint8_t cur_node_flags, int which) {
      switch (which) {
        case BV_TERM:
          return (cur_node_flags & NFLAG_TERM) > 0;
        case BV_LEAF:
          return (cur_node_flags & NFLAG_LEAF) > 0;
        case BV_CHILD:
          return (cur_node_flags & NFLAG_CHILD) > 0;
      }
      return false;
    }

    void write_bv_select_lt(int which, FILE *fp) {
      uint32_t node_id = 0;
      uint32_t sel_count = 0;
      uint32_t prev_val = 0;
      gen::write_uint24(0, fp);
      for (int i = 1; i < memtrie.all_node_sets.size(); i++) {
       leopard::node_set_handler cur_ns(memtrie.all_node_sets, i);
       bool len_flag = false;
       if ((cur_ns.get_ns_hdr()->flags & NODE_SET_LEAP) == 0)
         len_flag = true;
       leopard::node cur_node = cur_ns.first_node();
       for (int k = 0; k <= cur_ns.last_node_idx(); k++) {
        uint8_t cur_node_flags = 0;
        if (len_flag)
          cur_node_flags = cur_node.get_flags();
        if (node_qualifies_for_select(&cur_node, cur_node_flags, which)) {
          if (sel_count && (sel_count % sel_divisor) == 0) {
            uint32_t val_to_write = write_bv_select_val(node_id, fp);
            if (val_to_write > (1 << 24))
              bldr_printf("WARNING: %u\t%u\n", sel_count, val_to_write);
            prev_val = val_to_write;
          }
          sel_count++;
        }
        node_id++;
        if (!len_flag) {
          len_flag = true;
          k--;
          continue;
        }
        cur_node.next();
       }
      }
      gen::write_uint24(memtrie.node_count/nodes_per_bv_block, fp);
    }

    leopard::uniq_info *get_ti(leopard::node *n) {
      tail_val_maps *tm = &tail_vals;
      leopard::uniq_info_vec *uniq_tails_rev = tm->get_uniq_tails_rev();
      return (*uniq_tails_rev)[n->get_tail()];
    }

    uint32_t get_tail_ptr(leopard::node *cur_node) {
      leopard::uniq_info *ti = get_ti(cur_node);
      return ti->ptr;
    }

    leopard::uniq_info *get_vi(leopard::node *n) {
      tail_val_maps *tm = &tail_vals;
      leopard::uniq_info_vec *uniq_vals_fwd = tm->get_uniq_vals_fwd();
      return (*uniq_vals_fwd)[n->get_col_val()];
    }

    uint32_t get_node_id_from_sequence(uint32_t ins_seq_id) {
      return memtrie.get_node_id_from_sequence(ins_seq_id);
    }

};

}

// 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx
// 16 - followed by pointer
// 0 2-15/18-31 bytes 16 ptrs bytes 1

// 0 0000 - terminator
// 0 0001 to 0014 - length of suffix and terminator
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
