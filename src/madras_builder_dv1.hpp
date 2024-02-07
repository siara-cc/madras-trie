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

#define NFLAG_LEAF 1
#define NFLAG_TERM 2
#define NFLAG_NODEID_SET 4
#define NFLAG_PTR 8
struct node {
  uint32_t first_child;
  union {
    uint32_t next_sibling;
    uint32_t node_id;
  };
  //node *parent;
  uint32_t tail_pos;
  union {
    uint32_t swap_pos;
    uint32_t freq_count;
  };
  uint32_t val_pos;
  uint8_t flags;
  uint8_t level;
  uint8_t grp_no;
  uint8_t b0;
  node() {
    memset(this, '\0', sizeof(*this));
    freq_count = 1;
  }
};

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

class bit_vector {
  private:
    std::vector<uint8_t> bv;
    bool all_ones;
  public:
    bit_vector(bool _all_ones = false) {
      all_ones = _all_ones;
    }
    // bit_no starts from 0
    void set(size_t bit_no, bool val) {
      size_t byte_pos = bit_no / 8;
      if ((bit_no % 8) == 0)
        byte_pos++;
      while (bv.size() < byte_pos + 1)
        bv.push_back(all_ones ? 0xFF : 0x00);
      uint8_t mask = 0x80 >> (bit_no % 8);
      if (val)
        bv[byte_pos] |= mask;
      else
        bv[byte_pos] &= ~mask;
    }
    bool operator[](size_t bit_no) {
      size_t byte_pos = bit_no / 8;
      if ((bit_no % 8) == 0)
        byte_pos++;
      uint8_t mask = 0x80 >> (bit_no % 8);
      return (bv[byte_pos] & mask) > 0;
    }
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

#define byte_block_size 4096
class byte_block {
  private:
    std::vector<uint8_t *> blocks;
    bit_vector is_allocated;
    size_t block_remaining;
    bool is_released;
    size_t count;
    uint8_t *reserve(size_t val_len, size_t& pos) {
      if (val_len > block_remaining) {
        size_t needed_bytes = val_len;
        int needed_blocks = needed_bytes / byte_block_size;
        if (needed_bytes % byte_block_size)
          needed_blocks++;
        pos = blocks.size() * byte_block_size;
        block_remaining = needed_blocks * byte_block_size;
        uint8_t *new_block = new uint8_t[block_remaining];
        for (int i = 0; i < needed_blocks; i++) {
          is_allocated.set(blocks.size(), i == 0);
          blocks.push_back(new_block + i * byte_block_size);
        }
        block_remaining -= val_len;
        return new_block;
      } else {
        pos = blocks.size();
        pos--;
        uint8_t *ret = blocks[pos];
        pos *= byte_block_size;
        size_t block_pos = (byte_block_size - block_remaining);
        pos += block_pos;
        block_remaining -= val_len;
        return ret + block_pos;
      }
      return NULL;
    }
  public:
    byte_block() {
      count = 0;
      block_remaining = 0;
      is_released = false;
    }
    ~byte_block() {
      release_blocks();
    }
    void release_blocks() {
      for (size_t i = 0; i < blocks.size(); i++) {
        if (is_allocated[i])
          delete blocks[i];
      }
      blocks.resize(0);
      is_released = true;
      count = 0;
    }
    size_t push_back(const uint8_t *val, int val_len) {
      size_t pos;
      uint8_t *buf = reserve(val_len, pos);
      memcpy(buf, val, val_len);
      count++;
      return pos;
    }
    size_t push_back_with_vlen(const uint8_t *val, int val_len) {
      size_t pos;
      int8_t vlen = gen::get_vlen_of_uint32(val_len);
      uint8_t *buf = reserve(val_len + vlen, pos);
      gen::copy_vint32(val_len, buf, vlen);
      memcpy(buf + vlen, val, val_len);
      count++;
      return pos;
    }
    uint8_t *operator[](size_t pos) {
      return blocks[pos / byte_block_size] + (pos % byte_block_size);
    }
    size_t size() {
      return count;
    }
};

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

#define ns_alloc_unit 8

class node_set_handler {
  private:
    std::vector<uint8_t *>& node_set_vec;
  public:
    node_set_handler(std::vector<uint8_t *>& _node_set_vec) : node_set_vec (_node_set_vec) {
    }
    int read_actual_len(uint8_t *node_set) {
      return gen::read_uint16(node_set);
    }
    void set_actual_len(uint8_t *node_set, int actual_len) {
      gen::copy_uint16(actual_len, node_set);
    }
    int read_remain_len(uint8_t *node_set) {
      return node_set[2];
    }
    void set_remain_len(uint8_t *node_set, int remain_len) {
      node_set[2] = remain_len;
    }
    int read_bm_len(uint8_t *node_set) {
      return node_set[3];
    }
    void set_bm_len(uint8_t *node_set, int bm_len) {
      node_set[3] = bm_len;
    }
    uint8_t *ensure_capacity(int arr_pos, int addl_len, int& node_set_len) {
      uint8_t *node_set = node_set_vec[arr_pos];
      int remain_len = read_remain_len(node_set);
      if (remain_len >= addl_len)
        return node_set;
      node_set_len = read_actual_len(node_set);
      int new_len = node_set_len + addl_len;
      if (new_len % ns_alloc_unit)
        new_len += (ns_alloc_unit - (new_len % ns_alloc_unit));
      uint8_t *new_bytes = new uint8_t[new_len];
      memcpy(new_bytes, node_set, node_set_len);
      delete node_set;
      node_set_vec[arr_pos] = node_set = new_bytes;
      set_actual_len(node_set, node_set_len + addl_len);
      set_remain_len(node_set, new_len - (node_set_len + addl_len));
      return new_bytes;
    }
    void append(int arr_pos, uint8_t *b, int len) {
      int node_set_len;
      uint8_t *node_set = ensure_capacity(arr_pos, len, node_set_len);
      memcpy(node_set + node_set_len, b, len);
    }
    void append(int arr_pos, uint8_t b) {
      int node_set_len;
      uint8_t *node_set = ensure_capacity(arr_pos, 1, node_set_len);
      node_set[node_set_len] = b;
    }
    void insert(int arr_pos, int pos, uint8_t *b, int len) {
      int node_set_len;
      uint8_t *node_set = ensure_capacity(arr_pos, len, node_set_len);
      memmove(node_set + pos + len, node_set + pos, node_set_len - pos);
      memcpy(node_set + pos, b, len);
    }
    void insert(int arr_pos, int pos, uint8_t b) {
      int node_set_len;
      uint8_t *node_set = ensure_capacity(arr_pos, 1, node_set_len);
      memmove(node_set + pos + 1, node_set + pos, node_set_len - pos);
      node_set[pos] = b;
    }
    int create_node_set() {
      uint8_t *node_set = (uint8_t *) malloc(ns_alloc_unit);
      node_set_vec.push_back(node_set);
      set_actual_len(node_set, 0);
      set_remain_len(node_set, ns_alloc_unit);
      return node_set_vec.size() - 1;
    }
};

class node_sets {
  private:
    std::vector<uint8_t *> node_set_vec;
    node_set_handler node_set;
  public:
    node_sets() : node_set (node_set_vec) {
      node_set.create_node_set();
    }
    uint8_t *operator[](int arr_pos) {
      return node_set_vec[arr_pos];
    }
};

struct ptr_vals_info {
  uint32_t pos;
  uint32_t len;
  uint32_t arr_idx;
  uint32_t freq_count;
  uint32_t link_arr_idx;
  uint32_t ptr;
  uint8_t grp_no;
  uint8_t flags;
  ptr_vals_info() { }
  ptr_vals_info(uint32_t _pos, uint32_t _len, uint32_t _arr_pos, uint32_t _freq_count) {
    memset(this, '\0', sizeof(ptr_vals_info));
    pos = _pos; len = _len;
    arr_idx = _arr_pos;
    freq_count = _freq_count;
  }
};
typedef std::vector<ptr_vals_info *> ptr_vals_info_vec;

#define UTI_FLAG_SUFFIX_FULL 0x01
#define UTI_FLAG_SUFFIX_PARTIAL 0x02
#define UTI_FLAG_SUFFIXES 0x03
#define UTI_FLAG_HAS_SUFFIX 0x04
#define UTI_FLAG_PREFIX_PARTIAL 0x08
#define UTI_FLAG_HAS_CHILD 0x10
struct uniq_tails_info : ptr_vals_info {
  //uint32_t fwd_pos;
  uint32_t cmp_fwd;
  uint32_t cmp_rev;
  uint32_t cmp_rev_min;
  uint32_t cmp_rev_max;
  uint32_t avg_nid;
  uint32_t token_arr_pos;
  uniq_tails_info(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _rev_pos, uint32_t _freq_count) {
    memset(this, '\0', sizeof(uniq_tails_info));
    pos = _tail_pos; len = _tail_len;
    arr_idx = _rev_pos;
    cmp_rev_min = 0xFFFFFFFF;
    freq_count = _freq_count;
    link_arr_idx = 0xFFFFFFFF;
    token_arr_pos = 0xFFFFFFFF;
  }
};
typedef std::vector<uniq_tails_info *> uniq_tails_info_vec;

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
  public:
    freq_grp_ptrs_data() {
      next_idx = 0;
      idx_limit = 0;
      idx_ptr_size = 3;
      last_byte_bits = 8;
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
    uint32_t append_text_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len, bool append0 = false) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      for (int k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
      if (append0)
        grp_data_vec.push_back(0);
      return ptr;
    }
    uint32_t append_bin_to_grp_data(uint32_t grp_no, uint8_t *val, uint32_t len) {
      byte_vec& grp_data_vec = get_data(grp_no);
      uint32_t ptr = grp_data_vec.size();
      gen::append_vint32(grp_data_vec, len);
      for (int k = 0; k < len; k++)
        grp_data_vec.push_back(val[k]);
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
      return get_hdr_size() + get_data_size() + get_ptrs_size() + idx2_ptrs_map.size() + ptr_lookup_tbl + 7 * 4 + 1;
    }
    #define nodes_per_ptr_block 256
    #define nodes_per_ptr_block3 64
    void build(uint32_t node_count) {
      ptr_lkup_tbl_ptr_width = 9;
      if (tot_ptr_bit_count >= (1 << 24))
        ptr_lkup_tbl_ptr_width = 10;
      ptr_lookup_tbl_loc = 7 * 4 + 1;
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
      fputc(grp_count - 1, fp);
      fputc(freq_grp_vec[grp_count].grp_log2, fp);
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
    typedef ptr_vals_info *(*get_info_fn) (node *cur_node, std::vector<ptr_vals_info *>& info_vec);
    static ptr_vals_info *get_tails_info_fn(node *cur_node, std::vector<ptr_vals_info *>& info_vec) {
      return (ptr_vals_info *) info_vec[cur_node->tail_pos];
    }
    static ptr_vals_info *get_vals_info_fn(node *cur_node, std::vector<ptr_vals_info *>& info_vec) {
      return (ptr_vals_info *) info_vec[cur_node->val_pos];
    }
    void write_ptr_lookup_tbl(std::vector<node>& all_nodes, get_info_fn get_info_func, bool is_tail,
          std::vector<ptr_vals_info *>& info_vec, FILE* fp) {
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
      for (int i = 1; i < all_nodes.size(); i++) {
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
        node *cur_node = &all_nodes[i];
        if (is_tail) {
          if (cur_node->flags & NFLAG_PTR) {
            ptr_vals_info *vi = get_info_func(cur_node, info_vec);
            freq_grp& fg = freq_grp_vec[vi->grp_no];
            bit_count += fg.grp_log2;
            bit_count3 += fg.grp_log2;
          }
        } else {
          if (cur_node->flags & NFLAG_LEAF) {
            ptr_vals_info *vi = get_info_func(cur_node, info_vec);
            freq_grp& fg = freq_grp_vec[vi->grp_no];
            bit_count += fg.grp_log2;
            bit_count3 += fg.grp_log2;
          }
        }
        node_id++;
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
    void write_ptrs_data(std::vector<node>& all_nodes, get_info_fn get_info_func, bool is_tail,
          std::vector<ptr_vals_info *>& info_vec, FILE *fp) {
      fputc(ptr_lkup_tbl_ptr_width, fp);
      gen::write_uint32(ptr_lookup_tbl_loc, fp);
      gen::write_uint32(grp_data_loc, fp);
      gen::write_uint32(two_byte_count, fp);
      gen::write_uint32(idx2_ptr_count, fp);
      gen::write_uint32(grp_ptrs_loc, fp);
      gen::write_uint32(two_byte_data_loc, fp);
      gen::write_uint32(idx2_ptrs_map_loc, fp);
      write_ptr_lookup_tbl(all_nodes, get_info_func, is_tail, info_vec, fp);
      fwrite(all_nodes.data(), 0, 1, fp); // todo: fix two_byte_tails.size(), 1, fp);
      byte_vec *idx2_ptrs_map = get_idx2_ptrs_map();
      fwrite(idx2_ptrs_map->data(), idx2_ptrs_map->size(), 1, fp);
      write_grp_data(grp_data_loc + 514, is_tail, fp); // group count, 512 lookup tbl, tail locs, tails
      write_ptrs(fp);
      bldr_printf("Data size: %u, Ptrs size: %u, LkupTbl size: %u\nIdxMap size: %u, Uniq count: %u\n",
        get_data_size(), get_ptrs_size(), ptr_lookup_tbl, idx2_ptrs_map->size(), info_vec.size());
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
      bldr_printf("bits\tcd\tct_t\tfct_t\tlen_t\tcdln\tmxsz\n");
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        bldr_printf("%u\t%2x\t%u\t%u\t%u\t%u\t%u\n", fg->grp_log2, fg->code,
              fg->count, fg->freq_count, fg->grp_size, fg->code_len, fg->grp_limit);
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
    byte_vec uniq_tails;
    uniq_tails_info_vec uniq_tails_rev;
    //uniq_tails_info_vec uniq_tails_fwd;
    freq_grp_ptrs_data tail_ptrs;
    freq_grp_ptrs_data val_ptrs;
    byte_vec two_byte_tails;
    byte_vec uniq_vals;
    ptr_vals_info_vec uniq_vals_fwd;
    int start_nid, end_nid;
  public:
    tail_val_maps() {
    }
    ~tail_val_maps() {
      for (int i = 0; i < uniq_tails_rev.size(); i++)
        delete uniq_tails_rev[i];
      for (int i = 0; i < uniq_vals_fwd.size(); i++)
        delete uniq_vals_fwd[i];
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

    struct sort_data {
      uint8_t *data;
      uint32_t len;
      uint32_t n;
      uint32_t freq;
    };

    uint8_t *get_tail(byte_block& all_tails, node *n, uint32_t& len) {
      uint8_t *v = all_tails[n->tail_pos];
      int8_t vlen;
      len = gen::read_vint32(v, &vlen);
      v += vlen;
      return v;
    }

    uint32_t make_uniq_tails(std::vector<node>& all_nodes, byte_block& all_tails) {
      clock_t t = clock();
      std::vector<sort_data> nodes_for_sort;
      for (uint32_t i = 1; i < all_nodes.size(); i++) {
        node *n = &all_nodes[i];
        uint32_t tail_len;
        uint8_t *tail = get_tail(all_tails, n, tail_len);
        n->b0 = tail[0];
        if (n->flags & NFLAG_PTR) {
          nodes_for_sort.push_back((struct sort_data) { tail, tail_len, i, 1}); // n->freq_count} );
        }
      }
      uint32_t tot_freq = make_uniq(all_nodes, nodes_for_sort, uniq_tails,
          (ptr_vals_info_vec *) &uniq_tails_rev, gen::compare_rev, set_tail_pos_fn, new_tails_info_fn);
      t = gen::print_time_taken(t, "Time taken for make_uniq_tails: ");
      //all_tails.release_blocks();

      return tot_freq;

    }

    typedef void (*set_pos_fn) (std::vector<node>& all_nodes, uint32_t node_id, uint32_t pos);
    static void set_tail_pos_fn(std::vector<node>& all_nodes, uint32_t node_id, uint32_t pos) {
      all_nodes[node_id].tail_pos = pos;
    }
    static void set_val_pos_fn(std::vector<node>& all_nodes, uint32_t node_id, uint32_t pos) {
      all_nodes[node_id].val_pos = pos;
    }

    typedef ptr_vals_info *(*new_info_fn) (uint32_t _tail_pos, uint32_t _tail_len, uint32_t _arr_pos, uint32_t _freq_count);
    static ptr_vals_info *new_tails_info_fn(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _arr_pos, uint32_t _freq_count) {
      return new uniq_tails_info(_tail_pos, _tail_len, _arr_pos, _freq_count);
    }
    static ptr_vals_info *new_vals_info_fn(uint32_t _val_pos, uint32_t _val_len, uint32_t _arr_pos, uint32_t _freq_count) {
      return new ptr_vals_info(_val_pos, _val_len, _arr_pos, _freq_count);
    }

    uint32_t make_uniq(std::vector<node>& all_nodes, std::vector<sort_data>& nodes_for_sort, byte_vec& uniqs, ptr_vals_info_vec *uniq_vec,
             cmp_fn cmp_func, set_pos_fn set_pos_func, new_info_fn new_info_func) {
      bldr_printf("Nodes for sort size: %lu\n", nodes_for_sort.size());
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this, cmp_func](const struct sort_data& lhs, const struct sort_data& rhs) -> bool {
        return (*cmp_func)(lhs.data, lhs.len, rhs.data, rhs.len) < 0;
      });
      uint32_t freq_count = 0;
      uint32_t tot_freq = 0;
      std::vector<sort_data>::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->data;
      uint32_t prev_val_len = it->len;
      while (it != nodes_for_sort.end()) {
        int cmp = cmp_func(it->data, it->len, prev_val, prev_val_len);
        if (cmp != 0) {
          ptr_vals_info *vi_ptr = new_info_func(uniqs.size(), prev_val_len, uniq_vec->size(), 0);
          vi_ptr->freq_count = freq_count;
          uniq_vec->push_back(vi_ptr);
          tot_freq += freq_count;
          for (int i = 0; i < prev_val_len; i++)
            uniqs.push_back(prev_val[i]);
          freq_count = 0;
          prev_val = it->data;
          prev_val_len = it->len;
        }
        freq_count += it->freq;
        set_pos_func(all_nodes, it->n, uniq_vec->size());
        it++;
      }
      ptr_vals_info *vi_ptr = new_info_func(uniqs.size(), prev_val_len, uniq_vec->size(), 0);
      vi_ptr->freq_count = freq_count;
      tot_freq += freq_count;
      uniq_vec->push_back(vi_ptr);
      for (int i = 0; i < prev_val_len; i++)
        uniqs.push_back(prev_val[i]);
      bldr_printf("\n");
      return tot_freq;
    }

    uint32_t make_uniq_vals(std::vector<node>& all_nodes, byte_block& all_vals) {
      clock_t t = clock();
      std::vector<sort_data> nodes_for_sort;
      for (uint32_t i = 1; i < all_nodes.size(); i++) {
        node *n = &all_nodes[i];
        uint8_t *v = all_vals[n->val_pos];
        int8_t vlen;
        uint32_t val_len = gen::read_vint32(v, &vlen);
        v += vlen;
        if (n->flags & NFLAG_LEAF) {
          nodes_for_sort.push_back((struct sort_data) { v, val_len, i, 1 } );
        }
      }
      uint32_t tot_freq = make_uniq(all_nodes, nodes_for_sort, uniq_vals,
                  &uniq_vals_fwd, gen::compare, set_val_pos_fn, new_vals_info_fn);
      t = gen::print_time_taken(t, "Time taken for make_uniq_vals: ");
      //all_vals.release_blocks();

      return tot_freq;

    }

    const double idx_cost_frac_cutoff = 0.1;
    uint32_t make_uniq_freq(ptr_vals_info_vec& uniq_arr_vec, ptr_vals_info_vec& uniq_freq_vec, uint32_t tot_freq_count, uint32_t& last_data_len, uint8_t& start_bits, uint8_t& grp_no) {
      clock_t t = clock();
      uniq_freq_vec = uniq_arr_vec;
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.end(), [this](const struct ptr_vals_info *lhs, const struct ptr_vals_info *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });

      uint32_t sum_freq = 0;
      if (start_bits < 7) {
        for (int i = 0; i < uniq_freq_vec.size(); i++) {
          ptr_vals_info *vi = uniq_freq_vec[i];
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
        ptr_vals_info *vi = uniq_freq_vec[i];
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
        ptr_vals_info *vi = uniq_freq_vec[cumu_freq_idx];
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
      std::sort(uniq_freq_vec.begin(), uniq_freq_vec.begin() + cumu_freq_idx, [this](const struct ptr_vals_info *lhs, const struct ptr_vals_info *rhs) -> bool {
        return (lhs->grp_no == rhs->grp_no) ? (lhs->arr_idx > rhs->arr_idx) : (lhs->grp_no < rhs->grp_no);
      });
      std::sort(uniq_freq_vec.begin() + cumu_freq_idx, uniq_freq_vec.end(), [this](const struct ptr_vals_info *lhs, const struct ptr_vals_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / lhs->len;
        uint32_t rhs_freq = rhs->freq_count / rhs->len;
        lhs_freq = ceil(log10(lhs_freq));
        rhs_freq = ceil(log10(rhs_freq));
        return (lhs_freq == rhs_freq) ? (lhs->arr_idx > rhs->arr_idx) : (lhs_freq > rhs_freq);
      });
      t = gen::print_time_taken(t, "Time taken for uniq_freq: ");
      return cumu_freq_idx;

    }

    constexpr static uint32_t idx_ovrhds[] = {384, 3072, 24576, 196608, 1572864, 10782081};
    #define sfx_set_max_dflt 64
    void build_tail_val_maps(std::vector<node>& all_nodes, byte_block& all_tails, byte_block& all_vals) {

      FILE *fp;

      uint32_t tot_freq_count = make_uniq_tails(all_nodes, all_tails);

      clock_t t = clock();
      // uniq_tails_fwd = uniq_tails_rev;
      // std::sort(uniq_tails_fwd.begin(), uniq_tails_fwd.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
      //   return gen::compare(uniq_tails.data() + lhs->pos, lhs->len, uniq_tails.data() + rhs->pos, rhs->len) < 0;
      // });
      // for (int i = 0; i < uniq_tails_fwd.size(); i++)
      //   uniq_tails_fwd[i]->fwd_pos = i;
      // t = gen::print_time_taken(t, "Time taken for uniq_tails fwd sort: ");

      uniq_tails_info_vec uniq_tails_freq;
      uint8_t grp_no;
      uint32_t last_data_len;
      uint8_t start_bits = 7;
      uint32_t cumu_freq_idx = make_uniq_freq((ptr_vals_info_vec&) uniq_tails_rev, (ptr_vals_info_vec&) uniq_tails_freq, tot_freq_count, last_data_len, start_bits, grp_no);
      tail_ptrs.set_idx_info(start_bits, grp_no, last_data_len > 65535 ? 3 : 2);

      uint32_t freq_pos = 0;
      uniq_tails_info *prev_ti = uniq_tails_freq[freq_pos];
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        int cmp = gen::compare_rev(uniq_tails.data() + prev_ti->pos, prev_ti->len, uniq_tails.data() + ti->pos, ti->len);
        cmp--;
        if (cmp == ti->len || (freq_pos >= cumu_freq_idx && cmp > 1)) {
          ti->flags |= (cmp == ti->len ? UTI_FLAG_SUFFIX_FULL : UTI_FLAG_SUFFIX_PARTIAL);
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
          prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
          ti->link_arr_idx = prev_ti->arr_idx;
        }
        if (cmp != ti->len)
          prev_ti = ti;
      }

      uint32_t savings_prefix = 0;
      uint32_t savings_count_prefix = 0;
      // uint32_t pfx_set_len = sfx_set_max;
      // FILE *fp = fopen("prefixes.txt", "wb+");
      // FILE *fp1 = fopen("prefixes1.txt", "wb+");
      // uniq_tails_info *prev_fwd_ti = uniq_tails_fwd[0];
      // uint32_t fwd_pos = 1;
      // while (fwd_pos < uniq_tails_fwd.size()) {
      //   uniq_tails_info *ti = uniq_tails_fwd[fwd_pos];
      //   fwd_pos++;
      //   if (ti->flags & UTI_FLAG_SUFFIX_FULL)
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
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->flags & UTI_FLAG_SUFFIX_FULL) {
          savings_full += ti->len;
          savings_full++;
          savings_count_full++;
          uniq_tails_info *link_ti = uniq_tails_rev[ti->link_arr_idx];
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
          //   uniq_tails_info *link_ti = uniq_tails_rev[ti->link_rev_idx];
          //   uint32_t pfx_diff = grp_tails[grp_no - 1].size() - link_ti->tail_ptr;
          //   uint32_t pfx_len_len = get_pfx_len(pfx_diff);
          //   if (pfx_len_len < prefix_len) {
          //     savings_prefix += prefix_len;
          //     savings_prefix -= pfx_len_len;
          //     savings_count_prefix++;
          //     // bldr_printf("%u\t%u\t%u\t%u\n", prefix_len, grp_no, pfx_diff, pfx_len_len);
          //   }
          // }
          if (ti->flags & UTI_FLAG_SUFFIX_PARTIAL) {
            uint32_t cmp = ti->cmp_rev;
            uint32_t remain_len = ti->len - cmp;
            uint32_t len_len = get_set_len_len(cmp);
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
              get_set_len_len(cmp, &tail_data);
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
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
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
      //   uniq_tails_info *ti = uniq_tails_freq[freq_pos];
      //   freq_pos++;
      //   if (ti->flags & UTI_FLAG_SUFFIX_FULL)
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

      if (all_vals.size() > 0) {
        tot_freq_count = make_uniq_vals(all_nodes, all_vals);
        if (uniq_vals_fwd.size() > 0) {
          ptr_vals_info_vec uniq_vals_freq;
          start_bits = 1;
          cumu_freq_idx = make_uniq_freq(uniq_vals_fwd, uniq_vals_freq, tot_freq_count, last_data_len, start_bits, grp_no);
          val_ptrs.set_idx_info(start_bits, grp_no, last_data_len > 65535 ? 3 : 2);
          freq_pos = 0;
          grp_no = 1;
          cur_limit = pow(2, start_bits);
          val_ptrs.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0, 0, 0});
          val_ptrs.add_freq_grp((freq_grp) {grp_no, start_bits, cur_limit, 0, 0, 0, 0, 0});
          while (freq_pos < uniq_vals_freq.size()) {
            ptr_vals_info *vi = uniq_vals_freq[freq_pos];
            freq_pos++;
            uint8_t len_of_len = gen::get_vlen_of_uint32(vi->len);
            uint32_t len_plus_len = vi->len + len_of_len;
            cur_limit = val_ptrs.check_next_grp(grp_no, cur_limit, len_plus_len);
            vi->grp_no = grp_no;
            val_ptrs.update_current_grp(grp_no, len_plus_len, vi->freq_count);
            vi->ptr = val_ptrs.append_bin_to_grp_data(grp_no, uniq_vals.data() + vi->pos, vi->len);
          }
          for (freq_pos = 0; freq_pos < cumu_freq_idx; freq_pos++) {
            ptr_vals_info *vi = uniq_vals_freq[freq_pos];
            vi->ptr = val_ptrs.append_ptr2_idx_map(vi->grp_no, vi->ptr);
          }
          val_ptrs.build_freq_codes(true);
          val_ptrs.show_freq_codes();
        }
      }

    }

    // uint32_t get_pfx_len(uint32_t sz) {
    //   return (sz < 1024 ? 2 : (sz < 131072 ? 3 : (sz < 16777216 ? 4 : 5)));
    // }

    // uint32_t find_prefix(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd, uniq_tails_info *ti, uint32_t grp_no, uint32_t& savings_prefix, uint32_t& savings_count_prefix, int cmp_sfx) {
    //   uint32_t limit = ti->fwd_pos + 300;
    //   if (limit > uniq_tails_fwd.size())
    //     limit = uniq_tails_fwd.size();
    //   for (uint32_t i = ti->fwd_pos + 1; i < limit; i++) {
    //     uniq_tails_info *ti_fwd = uniq_tails_fwd[i];
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

    void write_tail_ptrs_data(std::vector<node>& all_nodes, FILE *fp) {
      tail_ptrs.write_ptrs_data(all_nodes, tail_ptrs.get_tails_info_fn, true,
            (std::vector<ptr_vals_info *>&) uniq_tails_rev, fp);
    }

    void write_val_ptrs_data(std::vector<node>& all_nodes, FILE *fp) {
      val_ptrs.write_ptrs_data(all_nodes, tail_ptrs.get_vals_info_fn, false,
            (std::vector<ptr_vals_info *>&) uniq_vals_fwd, fp);
    }

    uint32_t get_tail_ptr(uint32_t grp_no, uniq_tails_info *ti) {
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

    uint8_t get_first_byte(byte_block& all_tails, node *n) {
      if (uniq_tails_rev.size() == 0) {
        uint32_t tail_len;
        uint8_t *tail = get_tail(all_tails, n, tail_len);
        return *tail;
      }
      if ((n->flags & NFLAG_PTR) == 0)
        return n->b0;
      uniq_tails_info *ti = uniq_tails_rev[n->tail_pos];
      uint32_t grp_no = ti->grp_no;
      uint32_t ptr = get_tail_ptr(grp_no, ti);
      byte_vec& tail = tail_ptrs.get_data(grp_no);
      return *(tail.data() + ptr);
    }

    std::string get_tail_str(byte_block& all_tails, node *n) {
      if (uniq_tails_rev.size() == 0) {
        uint32_t tail_len;
        uint8_t *v = get_tail(all_tails, n, tail_len);
        return std::string((const char *) v, tail_len);
      }
      uniq_tails_info *ti = uniq_tails_rev[n->tail_pos];
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

    uniq_tails_info_vec *get_uniq_tails_rev() {
      return &uniq_tails_rev;
    }
    byte_vec *get_uniq_tails() {
      return &uniq_tails;
    }
    byte_vec *get_uniq_vals() {
      return &uniq_vals;
    }
    ptr_vals_info_vec *get_uniq_vals_fwd() {
      return &uniq_vals_fwd;
    }
    freq_grp_ptrs_data *get_tail_grp_ptrs() {
      return &tail_ptrs;
    }
    freq_grp_ptrs_data *get_val_grp_ptrs() {
      return &val_ptrs;
    }

};

class trie_ptrs_data_builder {
  private:
    byte_block& all_tails;
    byte_block& all_vals;
    std::vector<node>& all_nodes;
    uint32_t& total_node_count;
    uint32_t& max_tail_len;
    uint32_t trie_loc;
    uint32_t node_count;
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
    byte_vec trie;
    uint32_t end_loc;
    tail_val_maps tail_vals;
    trie_ptrs_data_builder(std::vector<node>& _all_nodes, byte_block& _all_tails, byte_block& _all_vals,
        uint32_t& _node_count, uint32_t& _max_tail_len)
            : all_tails (_all_tails), all_vals (_all_vals), all_nodes (_all_nodes),
              max_tail_len (_max_tail_len), total_node_count (_node_count) {
    }
    uint8_t append_tail_ptr(node *cur_node) {
      if ((cur_node->flags & NFLAG_PTR) == 0)
        return cur_node->b0;
      uint8_t node_val;
      uint32_t ptr = 0;
      freq_grp_ptrs_data *tail_ptrs = tail_vals.get_tail_grp_ptrs();
      if (cur_node->flags & NFLAG_PTR) {
        uniq_tails_info *ti = (*tail_vals.get_uniq_tails_rev())[cur_node->tail_pos];
        uint8_t grp_no = ti->grp_no;
        // if (grp_no == 0 || ti->tail_ptr == 0)
        //   bldr_printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
        ptr = ti->ptr;
        freq_grp *fg = tail_ptrs->get_freq_grp(grp_no);
        int node_val_bits = 8 - fg->code_len;
        node_val = (fg->code << node_val_bits) | (ptr & ((1 << node_val_bits) - 1));
        ptr >>= node_val_bits;
        tail_ptrs->append_ptr_bits(ptr, fg->grp_log2);
      }
      return node_val;
    }

    void append_val_ptr(node *cur_node) {
      if ((cur_node->flags & NFLAG_LEAF) == 0)
        return;
      freq_grp_ptrs_data *val_ptrs = tail_vals.get_val_grp_ptrs();
      ptr_vals_info *vi = (*tail_vals.get_uniq_vals_fwd())[cur_node->val_pos];
      freq_grp *fg = val_ptrs->get_freq_grp(vi->grp_no);
      // if (cur_node->node_id < 500)
      //   std::cout << "node_id: " << cur_node->node_id << "grp no: " << (int) vi->grp_no << ", bitlen: " << fg->grp_log2 << ", ptr: " << vi->ptr << std::endl;
      uint32_t to_append = vi->ptr;
      to_append |= (fg->code << (fg->grp_log2 - fg->code_len));
      val_ptrs->append_ptr_bits(to_append, fg->grp_log2);
    }

    uint32_t build() {
      tail_vals.build_tail_val_maps(all_nodes, all_tails, all_vals);
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
      node_count = 0;
      max_tail_len = 1;
      uint32_t cur_node_idx = 1;
      for (; cur_node_idx < all_nodes.size(); cur_node_idx++) {
        node *cur_node = &all_nodes[cur_node_idx];
        uint8_t flags = (cur_node->flags & NFLAG_LEAF ? 1 : 0) +
          (cur_node->first_child != 0 ? 2 : 0) + (cur_node->flags & NFLAG_PTR ? 4 : 0) +
          (cur_node->flags & NFLAG_TERM ? 8 : 0);
        if (cur_node->flags & NFLAG_PTR) {
          uniq_tails_info_vec *uniq_tails_rev = tail_vals.get_uniq_tails_rev();
          uniq_tails_info *ti = (*uniq_tails_rev)[cur_node->tail_pos];
          if (ti->flags & UTI_FLAG_SUFFIX_FULL)
            sfx_full_count++;
          if (ti->flags & UTI_FLAG_SUFFIX_PARTIAL)
            sfx_partial_count++;
          char_counts[(ti->len > 8) ? 7 : (ti->len - 2)]++;
          if (ti->len > max_tail_len)
            max_tail_len = ti->len;
          ptr_count++;
        }
        flag_counts[flags & 0x07]++;
        uint8_t node_val = append_tail_ptr(cur_node);
        if (get_uniq_val_count() > 0)
          append_val_ptr(cur_node);
        if (node_count && (node_count % 64) == 0) {
          append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
          append_byte_vec(trie, byte_vec64);
          bm_term = 0; bm_child = 0; bm_leaf = 0; bm_ptr = 0;
          bm_mask = 1UL;
          byte_vec64.clear();
        }
        if (cur_node->flags & NFLAG_LEAF)
          bm_leaf |= bm_mask;
        if (cur_node->flags & NFLAG_TERM)
          bm_term |= bm_mask;
        if (cur_node->first_child != 0)
          bm_child |= bm_mask;
        if (cur_node->flags & NFLAG_PTR)
          bm_ptr |= bm_mask;
        bm_mask <<= 1;
        byte_vec64.push_back(node_val);
        node_count++;
      }
      if (get_uniq_val_count() > 0) // read beyond protection
        tail_vals.get_val_grp_ptrs()->append_ptr_bits(0x00, 8);
      // TODO: write on all cases?
      append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
      append_byte_vec(trie, byte_vec64);
      for (int i = 0; i < 8; i++) {
        bldr_printf("Flag %d: %d\tChar: %d: %d\n", i, flag_counts[i], i + 2, char_counts[i]);
      }
      bldr_printf("Tot ptr count: %u, Full sfx count: %u, Partial sfx count: %u\n", ptr_count, sfx_full_count, sfx_partial_count);
      tail_vals.get_tail_grp_ptrs()->build(node_count);
      uint32_t tail_size = tail_vals.get_tail_grp_ptrs()->get_total_size();
      end_loc = 0;
      if (get_uniq_val_count() > 0) {
        freq_grp_ptrs_data *val_ptrs = tail_vals.get_val_grp_ptrs();
        val_ptrs->build(node_count);
        //fragment_end_loc += val_ptrs->get_total_size();
      }
      trie_loc = 8 + tail_size;
      end_loc += (8 + tail_size + trie.size());
      return end_loc;
    }
    uint32_t write(FILE *fp, FILE *fp_val, uint32_t val_fp_offset) {
      bldr_printf("\nTrie size: %u\n", trie.size());
      uint32_t tail_size = tail_vals.get_tail_grp_ptrs()->get_total_size();
      gen::write_uint32(tail_size, fp);
      gen::write_uint32(get_uniq_val_count() > 0 ? val_fp_offset + 1 : 0, fp);
      bldr_printf("Tail stats - ");
      tail_vals.write_tail_ptrs_data(all_nodes, fp);
      fwrite(trie.data(), trie.size(), 1, fp);
      if (get_uniq_val_count() > 0) {
        bldr_printf("Val stats - ");
        tail_vals.write_val_ptrs_data(all_nodes, fp_val);
        val_fp_offset += tail_vals.get_val_grp_ptrs()->get_total_size();
      }
      return val_fp_offset;
    }
    size_t size() {
      size_t ret = 8 + trie.size() + tail_vals.get_tail_grp_ptrs()->get_total_size();
      //if (get_uniq_val_count() > 0)
      //  ret += tail_vals.get_val_grp_ptrs()->get_total_size();
      return ret;
    }
    size_t get_uniq_val_count() {
      return tail_vals.get_uniq_vals_fwd()->size();
    }
};

class builder {

  private:
    node root;
    uint32_t node_count;
    uint32_t key_count;
    uint32_t max_key_len;
    uint32_t max_val_len;
    uint32_t max_level;
    uint32_t max_tail_len;
    uint32_t common_node_count;
    uint32_t term_count;
    bool nodes_sorted;
    std::vector<node> all_nodes;
    std::vector<uint32_t> last_children;
    std::vector<uint8_t> prev_key;
    //dfox uniq_basix_map;
    //basix uniq_basix_map;
    //art_tree at;

    //builder(builder const&);
    //builder& operator=(builder const&);

    node *swap_nodes(uint32_t pos_from, uint32_t pos_to) {
      while (all_nodes[pos_from].flags & NFLAG_NODEID_SET)
        pos_from = all_nodes[pos_from].swap_pos;
      node n = all_nodes[pos_to];
      all_nodes[pos_to] = all_nodes[pos_from];
      all_nodes[pos_from] = n;
      all_nodes[pos_to].swap_pos = pos_from;
      all_nodes[pos_to].flags |= NFLAG_NODEID_SET;
      return &all_nodes[pos_to];
    }

    void append_tail_vec(const uint8_t *tail, int tail_len, uint32_t node_pos) {
      node *n = &all_nodes[node_pos];
      n->tail_pos = all_tails.push_back_with_vlen(tail, tail_len);
      n->b0 = *tail;
      if (tail_len > 1)
        n->flags |= NFLAG_PTR;
      else
        n->flags &= ~NFLAG_PTR;
    }

    void append_val_vec(const uint8_t *val, int val_len, uint32_t node_pos) {
      if (val == NULL)
        return;
      all_nodes[node_pos].val_pos = all_vals.push_back_with_vlen(val, val_len);
    }

    void replace_or_append_val(const uint8_t *val, int val_len, uint32_t& node_pos) {
      if (val == NULL)
        return;
      node *n = &all_nodes[node_pos];
      int8_t vlen;
      int node_val_len = gen::read_vint32(all_vals[n->val_pos], &vlen);
      if (node_val_len >= val_len) {
        vlen = gen::get_vlen_of_uint32(val_len);
        gen::copy_vint32(val_len, all_vals[n->val_pos], vlen);
        memcpy(all_vals[n->val_pos] + vlen, val, val_len);
        return;
      }
      append_val_vec(val, val_len, node_pos);
    }

  public:
    byte_block all_tails;
    byte_block all_vals;
    trie_ptrs_data_builder trie_builder;
    std::string out_filename;
    // other config options: sfx_set_max, step_bits_idx, dict_comp, prefix_comp
    builder(const char *out_file = NULL) : trie_builder(all_nodes, all_tails, all_vals, node_count, max_tail_len) {
      node_count = 0;
      common_node_count = 0;
      key_count = 0;
      max_key_len = 0;
      max_val_len = 0;
      max_level = 1;
      nodes_sorted = false;
      //root->parent = NULL;
      root.flags = NFLAG_TERM;
      root.node_id = 0;
      root.first_child = 1;
      root.next_sibling = 0;
      //first_node->parent = root;
      node first_node;
      first_node.next_sibling = 0;
      all_nodes.push_back(root);
      all_nodes.push_back(first_node);
      //art_tree_init(&at);
      if (out_file != NULL)
        set_out_file(out_file);
      last_children.push_back(1);
      max_tail_len = 0;
      term_count = 0;
    }

    ~builder() {
    }

    void set_print_enabled(bool to_print_messages = true) {
      is_bldr_print_enabled = to_print_messages;
    }

    void set_out_file(const char *out_file) {
      out_filename = out_file;
    }

    size_t size() {
      return key_count;
    }

    void sort_nodes(bool mark_sorted = true) {
      const bool to_sort_nodes_on_freq = false;
      clock_t t = clock();
      uint32_t nxt = 0;
      uint32_t node_id = 1;
      // FILE *fp = fopen("sort_nodex.txt", "wb+");
      while (node_id < all_nodes.size() && nxt < all_nodes.size()) {
        if (all_nodes[nxt].first_child == 0) {
          nxt++;
          continue;
        }
        uint32_t nxt_n = all_nodes[nxt].first_child;
        all_nodes[nxt].first_child = node_id;
        node *n;
        std::vector<uint32_t> swap_pos_vec;
        do {
          n = swap_nodes(nxt_n, node_id);
          swap_pos_vec.push_back(n->swap_pos);
          nxt_n = n->next_sibling;
          n->flags &= ~NFLAG_TERM;
          n->flags |= (nxt_n == 0 ? NFLAG_TERM : 0);
          n->node_id = node_id++;
          // uint32_t tail_len;
          // uint8_t *tail = this->trie_builder.tail_vals.get_tail(this->all_tails, n, tail_len);
          // fprintf(fp, "[%.*s] ", tail_len, tail);
        } while (nxt_n != 0);
        // fprintf(fp, "\n");
        if (to_sort_nodes_on_freq) {
          n->flags &= ~NFLAG_TERM;
          uint32_t start_nid = all_nodes[nxt].first_child;
          std::sort(all_nodes.begin() + start_nid, all_nodes.begin() + node_id, [this](struct node& lhs, struct node& rhs) -> bool {
            uint32_t dummy;
            uint8_t *lhs_tail = this->trie_builder.tail_vals.get_tail(this->all_tails, &lhs, dummy);
            uint8_t *rhs_tail = this->trie_builder.tail_vals.get_tail(this->all_tails, &rhs, dummy);
            return (lhs.freq_count == rhs.freq_count) ? *lhs_tail < *rhs_tail : (lhs.freq_count > rhs.freq_count);
          });
          std::vector<uint32_t>::iterator it = swap_pos_vec.begin();
          while (start_nid < node_id) {
            node *n = &all_nodes[start_nid];
            n->node_id = start_nid;
            n->swap_pos = *it++;
            start_nid++;
          }
          all_nodes[start_nid - 1].flags |= NFLAG_TERM;
        }
        swap_pos_vec.clear();
        nxt++;
      }
      // fclose(fp);
      nodes_sorted = mark_sorted;
      gen::print_time_taken(t, "Time taken for sort_nodes(): ");
    }

    void set_first_node(const uint8_t *key, int key_len, const uint8_t *val, int val_len) {
      append_tail_vec(key, key_len, 1);
      append_val_vec(val, val_len, 1);
      all_nodes[1].flags |= NFLAG_LEAF;
      all_nodes[1].flags |= NFLAG_TERM;
      node_count++;
      term_count++;
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, uint8_t *val) {
      if (key_count == 0)
        return false;
      int result, key_pos, cmp;
      uint32_t node_id;
      node *n = lookup(key, key_len, result, key_pos, cmp, node_id);
      if (result == 0 && val != NULL) {
        int8_t vlen;
        ptr_vals_info_vec *vi_vec = trie_builder.tail_vals.get_uniq_vals_fwd();
        uint8_t *v;
        int node_val_len;
        if (vi_vec->size() > 0) {
          ptr_vals_info *vi = vi_vec->at(n->val_pos);
          v = trie_builder.tail_vals.get_uniq_vals()->data() + vi->pos;
          node_val_len = vi->len;
        } else {
          v = all_vals[n->val_pos];
          node_val_len = gen::read_vint32(v, &vlen);
          v += vlen;
        }
        *in_size_out_value_len = node_val_len;
        memcpy(val, v, node_val_len);
        return true;
      }
      return false;
    }

    bool put(const uint8_t *key, int key_len, const uint8_t *value, int value_len) {
      return insert(key, key_len, value, value_len);
    }

    node *lookup(const uint8_t *key, int key_len, int& result, int& key_pos, int& cmp, uint32_t& node_id) {
      key_pos = 0;
      node_id = 1;
      uint8_t key_byte = key[key_pos];
      node *cur_node = all_nodes.data() + node_id;
      do {
        while (key_byte > cur_node->b0) {
           if (cur_node->flags & NFLAG_TERM) { // nodes_sorted ? (cur_node->flags & NFLAG_TERM) : (cur_node->next_sibling == 0
            result = DCT_INSERT_AFTER;
            return cur_node;
          }
          if (nodes_sorted)
            node_id++;
          else
            node_id = cur_node->next_sibling;
          cur_node = all_nodes.data() + node_id; // &all_nodes[node_id];
        }
        if (key_byte == cur_node->b0) {
          uint32_t tail_len = 1;
          if (cur_node->flags & NFLAG_PTR) {
            std::string tail_str = trie_builder.tail_vals.get_tail_str(all_tails, cur_node);
            tail_len = tail_str.length();
            cmp = gen::compare((const uint8_t *) tail_str.c_str(), tail_len,
                    (const uint8_t *) key + key_pos, key_len - key_pos);
            // bldr_printf("%d\t%d\t%.*s =========== ", cmp, tail_len, tail_len, tail_data);
            // bldr_printf("%d\t%.*s\n", (int) key.size() - key_pos, (int) key.size() - key_pos, key.data() + key_pos);
          } else
            cmp = 0;
          if (cmp == 0 && key_pos + tail_len == key_len && (cur_node->flags & NFLAG_LEAF)) {
            result = 0;
            return cur_node;
          }
          if (cmp == 0 || abs(cmp) - 1 == tail_len) {
            key_pos += tail_len;
            if (key_pos >= key_len) {
              result = DCT_INSERT_LEAF;
              return cur_node;
            }
            node_id = cur_node->first_child;
            if (node_id == 0) {
              result = DCT_INSERT_CHILD_LEAF;
              return cur_node;
            }
            cur_node = all_nodes.data() + node_id;
            key_byte = key[key_pos];
            continue;
          }
          if (abs(cmp) - 1 == key_len - key_pos) {
            result = DCT_INSERT_CHILD_LEAF;
            return cur_node;
          }
          result = DCT_INSERT_THREAD;
          return cur_node;
        }
        result = DCT_INSERT_BEFORE;
        return cur_node;
      } while (1);
      return 0;
    }

    bool insert(const uint8_t *key, int key_len) {
      return insert(key, key_len, NULL, 0);
    }

    bool insert(const uint8_t *key, int key_len, const uint8_t *val, int val_len) {
      if (node_count == 0) {
        key_count++;
        max_key_len = key_len;
        max_val_len = val_len;
        set_first_node(key, key_len, val, val_len);
        return false;
      }
      int result, key_pos, cmp;
      uint32_t ins_node_pos;
      node *ins_node = lookup(key, key_len, result, key_pos, cmp, ins_node_pos);
      if (result == 0) {
        replace_or_append_val(val, val_len, ins_node_pos);
        return true;
      }
      key_count++;
      if (max_key_len < key_len)
        max_key_len = key_len;
      if (max_val_len < val_len)
        max_val_len = val_len;
      switch (result) {
        case DCT_INSERT_AFTER:
          add_sibling(ins_node, key, key_len, key_pos, val, val_len);
          break;
        case DCT_INSERT_BEFORE:
          insert_sibling(ins_node, key, key_len, key_pos, val, val_len, ins_node_pos);
          break;
        case DCT_INSERT_LEAF:
          ins_node->flags |= NFLAG_LEAF;
          append_val_vec(val, val_len, ins_node_pos);
          break;
        case DCT_INSERT_CHILD_LEAF:
          add_child_leaf(ins_node, key, key_len, key_pos, val, val_len, cmp);
          break;
        case DCT_INSERT_THREAD: {
          bool swap = (cmp > 0);
          if (cmp != 0) {
            cmp = abs(cmp) - 1;
            key_pos += cmp;
          }
          add_children(ins_node, key, key_len, key_pos, val, val_len, cmp, swap);
        } break;
      }
      return false;
    }

    uint32_t add_child_leaf(node *last_child, const uint8_t *key, int key_len, int key_pos, const uint8_t *val, int val_len, int cmp = 0) {
      bool swap = false;
      if (cmp > 0)
        swap = true;
      cmp = abs(cmp) - 1;
      node child1;
      uint32_t child1_pos = all_nodes.size();
      child1.flags |= NFLAG_LEAF;
      child1.flags |= NFLAG_TERM;
      //child1->parent = last_child;
      child1.next_sibling = 0;
      int8_t vlen;
      uint32_t last_child_len = gen::read_vint32(all_tails[last_child->tail_pos], &vlen);
      if (swap) {
        gen::copy_vint32(cmp, all_tails[last_child->tail_pos], vlen);
        //last_child->tail_len = cmp;
        last_child->flags |= NFLAG_LEAF;
        if (cmp == 1)
          last_child->flags &= ~NFLAG_PTR;
        child1.first_child = last_child->first_child;
      }
      last_child->first_child = child1_pos;
      all_nodes.push_back(child1);
      if (swap) {
        append_tail_vec(all_tails[last_child->tail_pos] + cmp, last_child_len - cmp, child1_pos);
      } else {
        append_tail_vec(key + key_pos, key_len - key_pos, child1_pos);
      }
      append_val_vec(val, val_len, child1_pos);
      node_count++;
      term_count++;
      return child1_pos;
    }

    uint32_t add_sibling(node *last_child, const uint8_t *key, int key_len, int key_pos, const uint8_t *val, int val_len) {
      node new_node;
      uint32_t new_node_pos = all_nodes.size();
      new_node.flags |= NFLAG_LEAF;
      new_node.flags |= NFLAG_TERM;
      //new_node->parent = last_child->parent;
      new_node.next_sibling = 0;
      last_child->next_sibling = new_node_pos;
      last_child->flags &= ~NFLAG_TERM;
      all_nodes.push_back(new_node);
      append_tail_vec(key + key_pos, key_len - key_pos, new_node_pos);
      append_val_vec(val, val_len, new_node_pos);
      node_count++;
      return new_node_pos;
    }

    uint32_t insert_sibling(node *last_child, const uint8_t *key, int key_len, int key_pos, const uint8_t *val, int val_len, uint32_t ins_node_pos) {
      uint32_t swap_node_pos = all_nodes.size();
      node new_node;
      new_node.flags |= NFLAG_LEAF;
      //new_node->parent = last_child->parent;
      new_node.next_sibling = swap_node_pos;
      node swap_node = *last_child;
      all_nodes[ins_node_pos] = new_node;
      all_nodes.push_back(swap_node);
      append_tail_vec(key + key_pos, key_len - key_pos, ins_node_pos);
      append_val_vec(val, val_len, ins_node_pos);
      node_count++;
      return ins_node_pos;
    }

    uint32_t add_children(node *last_child, const uint8_t *key, int key_len, int key_pos, const uint8_t *val, int val_len, uint32_t child_at, bool swap = false) {
      node child1;
      uint32_t child1_pos = all_nodes.size();
      uint32_t child2_pos = child1_pos + 1;
      child1.flags |= (last_child->flags & NFLAG_LEAF);
      //child1.parent = last_child;
      child1.next_sibling = child2_pos;
      //child1.tail_pos = last_child->tail_pos + child_at;
      int8_t vlen;
      uint32_t last_child_tail_len = gen::read_vint32(all_tails[last_child->tail_pos], &vlen);
      //child1.tail_len = last_child->tail_len - child_at;
      if (last_child_tail_len - child_at == 1)
        child1.flags &= ~NFLAG_PTR;
      else
        child1.flags |= NFLAG_PTR;
      if (last_child->first_child != 0)
        child1.first_child = last_child->first_child;
      node child2;
      child2.flags |= NFLAG_LEAF;
      //child2->parent = last_child;
      child2.next_sibling = 0;
      //if (child2->val == string(" discuss"))
      //  cout << "Child2 node: " << key << endl;
      // do these before push_back all_nodes
      // as push_back could invalidate last_child pointer
      last_child->first_child = child1_pos;
      if (last_child->flags & NFLAG_LEAF) {
        child1.val_pos = last_child->val_pos;
        last_child->val_pos = 0;
        last_child->flags &= ~NFLAG_LEAF;
      }
      gen::copy_vint32(child_at, all_tails[last_child->tail_pos], vlen);
      //last_child->tail_len = child_at;
      if (child_at == 1)
        last_child->flags &= ~NFLAG_PTR;
      uint32_t last_child_tail_pos = last_child->tail_pos;
      if (swap) {
        child2_pos = all_nodes.size();
        child1_pos = child2_pos + 1;
        last_child->first_child = child2_pos;
        child1.next_sibling = 0;
        child1.flags |= NFLAG_TERM;
        child2.next_sibling = child1_pos;
        all_nodes.push_back(child2);
        all_nodes.push_back(child1);
      } else {
        child2.flags |= NFLAG_TERM;
        all_nodes.push_back(child1);
        all_nodes.push_back(child2);
      }
      append_tail_vec(all_tails[last_child_tail_pos + vlen + child_at], last_child_tail_len - child_at, child1_pos);
      append_tail_vec(key + key_pos, key_len - key_pos, child2_pos);
      append_val_vec(val, val_len, child2_pos);
      node_count += 2;
      term_count++;
      return child2_pos;
    }

    void append(const uint8_t *key, int key_len, const uint8_t *val = NULL, int val_len = 0) {
      if (key_len == prev_key.size() && memcmp(key, prev_key.data(), key_len) == 0)
         return;
      prev_key.clear();
      for (int i = 0; i < key_len; i++)
        prev_key.push_back(key[i]);
      if (val != NULL) {
        all_vals.push_back_with_vlen(val, val_len);
      }
      if (max_key_len < key_len)
        max_key_len = key_len;
      if (max_val_len < val_len)
        max_val_len = val_len;
      key_count++;
      if (node_count == 0) {
        set_first_node(key, key_len, val, val_len);
        return;
      }
      int key_pos = 0;
      int level = 0;
      node *last_child;
      do {
        last_child = &all_nodes[last_children[level]];
        uint32_t last_child_tail_len;
        uint8_t *tail = trie_builder.tail_vals.get_tail(all_tails, last_child, last_child_tail_len);
        int i = 0;
        for (; i < last_child_tail_len; i++) {
          if (key[key_pos] != tail[i]) {
            last_children.resize(level + 1);
            if (i == 0) {
              uint32_t new_node_pos = add_sibling(last_child, key, key_len, key_pos, val, val_len);
              last_children[level] = new_node_pos;
            } else {
              //last_child->freq_count++;
              uint32_t child2_pos = add_children(last_child, key, key_len, key_pos, val, val_len, i);
              last_children.push_back(child2_pos);
            }
            return;
          }
          key_pos++;
        }
        if (key_pos < key_len && (last_child->flags & NFLAG_LEAF) && last_child->first_child == 0) {
          //last_child->freq_count++;
          uint32_t child1_pos = add_child_leaf(last_child, key, key_len, key_pos, val, val_len);
          last_children.resize(level + 1);
          last_children.push_back(child1_pos);
          return;
        }
        //last_child->freq_count++;
        level++;
      } while (last_child != 0);
    }

    uint32_t get_level_node_count(int level) {
      uint32_t ret = 0;
      uint32_t next_child = 1;
      level++;
      while (level-- && next_child > 0) {
        ret = next_child;
        next_child = all_nodes[next_child].first_child;
      }
      return ret;
    }

    uint8_t node_set_min_b;
    uint8_t node_set_max_b;
    uint8_t node_set_min_len;
    uint8_t node_set_max_len;
    uint32_t node_set_len_count[256];
    uint32_t node_set_lvl_sum[256];
    uint32_t node_set_lvl_count[256];
    uint8_t node_set_byte_pos_min[256][256];
    uint8_t node_set_byte_pos_max[256][256];
    uint32_t node_set_byte_pos_sum[256][256];
    uint32_t node_set_byte_pos_count[256][256];
    uint32_t set_level_and_freq_count(uint32_t node_id, int level) {
      if (node_id == 0)
        return 0;
      if (level > max_level)
        max_level = level;
      node *n;
      uint32_t orig_node_id = node_id;
      uint32_t freq_count = 1;
      uint32_t node_set_len = 0;
      do {
        n = &all_nodes[node_id];
        n->level = level;
        n->freq_count = set_level_and_freq_count(n->first_child, level + 1);
        freq_count += n->freq_count;
        if (n->flags & NFLAG_LEAF) {
          n->freq_count++;
          freq_count++;
        }
        // node_id = n->next_sibling;
        node_id++;
        node_set_len++;
      } while ((n->flags & NFLAG_TERM) == 0);
      node_set_len--;
      // } while (node_id != 0);
      node_set_len_count[node_set_len]++;
      node_set_lvl_sum[node_set_len] += level;
      node_set_lvl_sum[node_set_len]++;
      node_set_lvl_count[node_set_len]++;
      node_id = orig_node_id;
      int node_pos = 0;
      do {
        n = &all_nodes[node_id];
        if (node_pos < node_set_byte_pos_min[node_set_len][n->b0])
          node_set_byte_pos_min[node_set_len][n->b0] = node_pos;
        if (node_pos > node_set_byte_pos_max[node_set_len][n->b0])
          node_set_byte_pos_max[node_set_len][n->b0] = node_pos;
        node_set_byte_pos_sum[node_set_len][n->b0] += node_pos;
        node_set_byte_pos_count[node_set_len][n->b0]++;
        if (node_set_min_b > n->b0)
          node_set_min_b = n->b0;
        if (node_set_max_b < n->b0)
          node_set_max_b = n->b0;
        node_id++;
        node_pos++;
      } while ((n->flags & NFLAG_TERM) == 0);
      if (node_set_min_len > node_set_len)
        node_set_min_len = node_set_len;
      if (node_set_max_len < node_set_len)
        node_set_max_len = node_set_len;
      return freq_count;
    }

    #define BV_LEAF 1
    #define BV_CHILD 2
    #define BV_TERM 3
    std::string build(std::string filename) {

      out_filename = filename;

      clock_t t = clock();

      memset(node_set_len_count, '\0', 256*sizeof(uint32_t));
      memset(node_set_lvl_sum, '\0', 256*sizeof(uint32_t));
      memset(node_set_lvl_count, '\0', 256*sizeof(uint32_t));
      memset(node_set_byte_pos_min, 0xFF, 256*256);
      memset(node_set_byte_pos_max, '\0', 256*256);
      memset(node_set_byte_pos_sum, '\0', 256*256*sizeof(uint32_t));
      memset(node_set_byte_pos_count, '\0', 256*256*sizeof(uint32_t));
      node_set_min_b = 0xFF;
      node_set_max_b = 0;
      node_set_min_len = 0xFF;
      node_set_max_len = 0;

      sort_nodes();
      set_level_and_freq_count(1, 0);
/*
      for (int i = node_set_min_len; i <= node_set_max_len; i++) {
        printf("Len:\t%d\tcount:\t%u\tavg:\t%lu\t", i, node_set_len_count[i], node_set_lvl_sum[i] / gen::max(node_set_lvl_count[i],1));
        for (int j = node_set_min_b; j <= node_set_max_b; j++) {
          if (node_set_byte_pos_min[i][j] == 255)
            printf("\t_");
          else
            printf("\t%d/%c: %d/%lu", j, j, node_set_byte_pos_min[i][j], node_set_byte_pos_sum[i][j] / gen::max(node_set_byte_pos_count[i][j],1));
        }
        printf("\n");
      }
*/
      printf("N#: %u, NS#: %u, K#: %u, mk#: %u, mt#: %u, mv#: %u\n", node_count, term_count, key_count, max_key_len, max_tail_len, max_val_len);

      byte_vec cache_bytes;
      uint32_t cache_count = 256;
      while (cache_count < key_count / 512) {
        cache_count *= 2;
      }
      //cache_count *= 2;
      uint32_t cache_size = cache_count * sizeof(bldr_cache);

      byte_vec sec_cache_bytes;
      uint32_t sec_cache_size = 0;
      uint32_t sec_cache_count = 0;

      uint32_t term_bvlt_sz = gen::get_lkup_tbl_size2(node_count, nodes_per_bv_block, 7);
      uint32_t child_bvlt_sz = term_bvlt_sz;
      uint32_t leaf_bvlt_sz = term_bvlt_sz;
      uint32_t term_select_lt_sz = gen::get_lkup_tbl_size2(term_count, sel_divisor, 3);
      uint32_t child_select_lt_sz = gen::get_lkup_tbl_size2(term_count, sel_divisor, 3);
      uint32_t leaf_select_lt_sz = gen::get_lkup_tbl_size2(key_count, sel_divisor, 3);

      uint32_t dummy_loc = 4 + 18 * 4; // 76
      uint32_t common_node_loc = dummy_loc + 0;
      uint32_t cache_loc = common_node_loc + ceil(common_node_count * 1.5);
      uint32_t sec_cache_loc = cache_loc + cache_size;
      uint32_t term_select_lkup_loc = sec_cache_loc + sec_cache_size;
      uint32_t term_bv_loc = term_select_lkup_loc + term_select_lt_sz;
      uint32_t child_bv_loc = term_bv_loc + term_bvlt_sz;
      uint32_t trie_ptrs_data_loc = child_bv_loc + child_bvlt_sz;
      uint32_t trie_ptrs_data_sz = trie_builder.build();
      uint32_t leaf_select_lkup_loc = trie_ptrs_data_loc + trie_ptrs_data_sz;
      uint32_t leaf_bv_loc = leaf_select_lkup_loc + leaf_select_lt_sz;
      uint32_t child_select_lkup_loc = leaf_bv_loc + leaf_bvlt_sz;

      make_cache(cache_count, cache_bytes, cache_size);
      //make_sec_cache(sec_cache_count, sec_cache_bytes, sec_cache_size);

      FILE *fp = fopen(out_filename.c_str(), "wb+");
      fputc(0xA5, fp); // magic byte
      fputc(0x01, fp); // version 1.0
      fputc(0, fp); // reserved
      fputc(0, fp);

      gen::write_uint32(node_count, fp);
      gen::write_uint32(dummy_loc, fp);
      gen::write_uint32(common_node_count, fp);
      gen::write_uint32(key_count, fp);
      gen::write_uint32(max_key_len, fp);
      gen::write_uint32(max_val_len, fp);
      gen::write_uint32(max_tail_len, fp);
      gen::write_uint32(cache_count, fp);
      gen::write_uint32(sec_cache_count, fp);
      gen::write_uint32(cache_loc, fp);
      gen::write_uint32(sec_cache_loc, fp);

      gen::write_uint32(term_select_lkup_loc, fp);
      gen::write_uint32(term_bv_loc, fp);
      gen::write_uint32(child_select_lkup_loc, fp);
      gen::write_uint32(child_bv_loc, fp);
      gen::write_uint32(leaf_select_lkup_loc, fp);
      gen::write_uint32(leaf_bv_loc, fp);
      gen::write_uint32(trie_ptrs_data_loc, fp);

      fwrite(cache_bytes.data(), cache_bytes.size(), 1, fp);
      //write_sec_cache(sec_cache_count, fp);
      write_bv_select_lt(BV_TERM, fp);
      write_bv_rank_lt(BV_TERM, fp);
      write_bv_rank_lt(BV_CHILD, fp);

      FILE *fp_val = fopen((out_filename + ".val").c_str(), "wb+");

      uint32_t total_idx_size = 4 + 18 * 4 + cache_size + sec_cache_size +
                term_select_lt_sz + term_bvlt_sz + child_bvlt_sz +
                leaf_select_lt_sz + child_select_lt_sz + leaf_bvlt_sz;
      uint32_t val_fp_offset = 0;
      val_fp_offset = trie_builder.write(fp, fp_val, val_fp_offset);
      total_idx_size += trie_builder.size();
      write_bv_select_lt(BV_LEAF, fp);
      write_bv_rank_lt(BV_LEAF, fp);
      write_bv_select_lt(BV_CHILD, fp);

      uint32_t total_val_size = val_fp_offset;
      uint32_t total_size = total_idx_size + total_val_size;

      fclose(fp);
      fclose(fp_val);

      bldr_printf("\nNode count: %u, Trie bit vectors: %u, Leaf bit vectors: %u\nSelect lookup tables - Term: %u, Child: %u, Leaf: %u\n"
        "Pri cache: %u, struct size: %u, Sec cache: %u\nNode struct size: %u, Max tail len: %u\n",
            node_count, term_bvlt_sz * 2, leaf_bvlt_sz, term_select_lt_sz, child_select_lt_sz, leaf_select_lt_sz,
            cache_size, sizeof(bldr_cache), sec_cache_size, sizeof(node), max_tail_len);

      // fp = fopen("nodes.txt", "wb+");
      // // dump_nodes(first_node, fp);
      // find_rpt_nodes(fp);
      // fclose(fp);

      gen::print_time_taken(t, "Time taken for build(): ");
      bldr_printf("Total idx size: %u, Val size: %u, Total size: %u\n", total_idx_size, total_val_size, total_size);

      return out_filename;

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
      for (int i = 1; i < all_nodes.size(); i++) {
        node *cur_node = &all_nodes[i];
        write_bv3(node_id, count, count3, buf3, pos3, fp);
        uint32_t ct;
        switch (which) {
          case BV_TERM:
            ct = (cur_node->flags & NFLAG_TERM ? 1 : 0);
            break;
          case BV_CHILD:
            ct = (cur_node->first_child > 0 ? 1 : 0);
            break;
          case BV_LEAF:
            ct = (cur_node->flags & NFLAG_LEAF ? 1 : 0);
            break;
        }
        count += ct;
        count3 += ct;
        node_id++;
      }
      fwrite(buf3, 3, 1, fp);
      // extra (guard)
      gen::write_uint32(count, fp);
      fwrite(buf3, 3, 1, fp);
    }

    void make_cache(uint32_t cache_count, byte_vec& cache_bytes, uint32_t& cache_size) {
      uint32_t node_id = 0;
      uint32_t node_set_id = 0;
      uint32_t cache_mask = cache_count - 1;
      cache_size = cache_count * sizeof(bldr_cache);
      cache_bytes.resize(cache_size);
      memset(cache_bytes.data(), 0xFF, cache_size);
      bldr_cache *cche0 = (bldr_cache *) cache_bytes.data();
      do {
        node *cur_node = &all_nodes[node_id + 1];
        uint8_t node_byte = cur_node->b0;
        uint32_t cache_loc = (node_set_id ^ (node_set_id << 5) ^ node_byte) & cache_mask;
        bldr_cache *cche = cche0 + cache_loc;
        uint32_t cche_node_id = gen::read_uint24(&cche->parent_node_id1);
        uint32_t cche_freq = 0;
        if (cche_node_id != 0xFFFFFF)
          cche_freq = all_nodes[cche_node_id + cche->node_offset + 1].freq_count;
        if (cche_freq < cur_node->freq_count && cur_node->first_child > 0 && (cur_node->flags & NFLAG_PTR) == 0) {
          uint32_t child_node_id = cur_node->first_child - 1;
          if (node_set_id < (1 << 24) && child_node_id < (1 << 24)) {
            cche->node_offset = node_id - node_set_id;
            gen::copy_uint24(node_set_id, &cche->parent_node_id1);
            gen::copy_uint24(child_node_id, &cche->child_node_id1);
            cche->node_byte = cur_node->b0;
          }
        }
        node_id++;
        if (cur_node->flags & NFLAG_TERM)
          node_set_id = node_id;
      } while (node_id < node_count);
    }

    void write_sec_cache(uint32_t cache_count, FILE *fp) {
      for (int i = 1; i <= cache_count; i++) {
        fputc(all_nodes[i].b0, fp);
      }
    }

    uint32_t write_bv_select_val(uint32_t node_id, FILE *fp) {
      uint32_t val_to_write = node_id / nodes_per_bv_block;
      // if (val_to_write - prev_val > 4)
      //   bldr_printf("Nid:\t%u\tblk:\t%u\tdiff:\t%u\n", node_id, val_to_write, val_to_write-prev_val);
      gen::write_uint24(val_to_write, fp);
      return val_to_write;
    }

    bool node_qualifies_for_select(node *cur_node, int which) {
      switch (which) {
        case BV_TERM:
          return (cur_node->flags & NFLAG_TERM) > 0;
        case BV_LEAF:
          return (cur_node->flags & NFLAG_LEAF) > 0;
        case BV_CHILD:
          return cur_node->first_child > 0;
      }
      return false;
    }

    void write_bv_select_lt(int which, FILE *fp) {
      uint32_t node_id = 0;
      uint32_t sel_count = 0;
      uint32_t prev_val = 0;
      gen::write_uint24(0, fp);
      for (int i = 1; i < all_nodes.size(); i++) {
        node *cur_node = &all_nodes[i];
        if (node_qualifies_for_select(cur_node, which)) {
          if (sel_count && (sel_count % sel_divisor) == 0) {
            uint32_t val_to_write = write_bv_select_val(node_id, fp);
            if (val_to_write > (1 << 24))
              bldr_printf("WARNING: %u\t%u\n", sel_count, val_to_write);
            prev_val = val_to_write;
          }
          sel_count++;
        }
        node_id++;
      }
      gen::write_uint24(node_count/nodes_per_bv_block, fp);
    }

    uniq_tails_info *get_ti(node *n) {
      tail_val_maps *tm = &trie_builder.tail_vals;
      uniq_tails_info_vec *uniq_tails_rev = tm->get_uniq_tails_rev();
      return (*uniq_tails_rev)[n->tail_pos];
    }

    uint32_t get_tail_ptr(node *cur_node) {
      uniq_tails_info *ti = get_ti(cur_node);
      return ti->ptr;
    }

    ptr_vals_info *get_vi(node *n) {
      tail_val_maps *tm = &trie_builder.tail_vals;
      ptr_vals_info_vec *uniq_vals_fwd = tm->get_uniq_vals_fwd();
      return (*uniq_vals_fwd)[n->val_pos];
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

#endif
