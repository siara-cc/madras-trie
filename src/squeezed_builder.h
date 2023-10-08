#ifndef builder_H
#define builder_H

#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>
#include <time.h>

#include "../../index_research/src/art.h"
#include "../../index_research/src/basix.h"
#include "../../index_research/src/univix_util.h"

enum {SRCH_ST_UNKNOWN, SRCH_ST_NEXT_SIBLING, SRCH_ST_NOT_FOUND, SRCH_ST_NEXT_CHAR};
#define INSERT_AFTER 1
#define INSERT_BEFORE 2
#define INSERT_LEAF 3
#define INSERT_EMPTY 4
#define INSERT_THREAD 5
#define INSERT_CONVERT 6
#define INSERT_CHILD_LEAF 7

namespace squeezed {

typedef std::vector<uint8_t> byte_vec;

static const int NFLAG_LEAF = 1;
static const int NFLAG_TERM = 2;
static const int NFLAG_NODEID_SET = 4;
static const int NFLAG_SORTED = 8;
struct node {
  uint32_t first_child;
  union {
    uint32_t next_sibling;
    uint32_t node_id;
  };
  //node *parent;
  uint32_t tail_pos;
  uint32_t rev_node_info_pos;
  uint32_t tail_len;
  uint8_t flags;
  uint8_t level;
  union {
    uint32_t swap_pos;
    uint32_t v0;
  };
  uint32_t freq_count;
  node() {
    memset(this, '\0', sizeof(node));
    freq_count = 1;
  }
};

const uint8_t UTI_FLAG_SUFFIX_FULL = 0x01;
const uint8_t UTI_FLAG_SUFFIX_PARTIAL = 0x02;
const uint8_t UTI_FLAG_SUFFIXES = 0x03;
const uint8_t UTI_FLAG_HAS_SUFFIX = 0x04;
const uint8_t UTI_FLAG_PREFIX_PARTIAL = 0x08;
const uint8_t UTI_FLAG_HAS_CHILD = 0x10;
struct uniq_tails_info {
    uint32_t tail_pos;
    uint32_t tail_len;
    uint32_t fwd_pos;
    uint32_t cmp_fwd;
    uint32_t rev_pos;
    uint32_t cmp_rev;
    uint32_t cmp_rev_min;
    uint32_t cmp_rev_max;
    uint32_t freq_count;
    uint32_t sum_node_freq;
    uint32_t sum_levels;
    uint32_t tail_ptr;
    uint32_t avg_nid;
      uint32_t link_rev_idx;
      uint32_t node_id;
    uint32_t token_arr_pos;
    uint8_t grp_no;
    uint8_t flags;
    uniq_tails_info(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _rev_pos, uint32_t _freq_count) {
      memset(this, '\0', sizeof(uniq_tails_info));
      tail_pos = _tail_pos; tail_len = _tail_len;
      rev_pos = _rev_pos;
      cmp_rev_min = 0xFFFFFFFF;
      freq_count = _freq_count;
      link_rev_idx = 0xFFFFFFFF;
      token_arr_pos = 0xFFFFFFFF;
    }
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
    static clock_t print_time_taken(clock_t t, const char *msg) {
      t = clock() - t;
      double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
      std::cout << msg << time_taken << std::endl;
      return clock();
    }
    static int compare(const uint8_t *v1, int len1, const uint8_t *v2,
            int len2, int k = 0) {
        int lim = (len2 < len1 ? len2 : len1);
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
    static void write_uint16(uint32_t input, FILE *fp) {
      fputc(input >> 8, fp);
      fputc(input & 0xFF, fp);
    }
    static void write_uint32(uint32_t input, FILE *fp) {
      // int i = 4;
      // while (i--)
      //   fputc((input >> (8 * i)) & 0xFF, fp);
      fputc(input & 0xFF, fp);
      fputc((input >> 8) & 0xFF, fp);
      fputc((input >> 16) & 0xFF, fp);
      fputc(input >> 24, fp);
    }
    static void append_uint16(uint16_t u16, byte_vec& v) {
      v.push_back(u16 >> 8);
      v.push_back(u16 & 0xFF);
    }
    static void append_uint24(uint32_t u24, byte_vec& v) {
      v.push_back(u24 >> 16);
      v.push_back((u24 >> 8) & 0xFF);
      v.push_back(u24 & 0xFF);
    }
    static uint32_t read_uint16(byte_vec& v, int pos) {
      uint32_t ret = v[pos++];
      ret <<= 8;
      ret |= v[pos];
      return ret;
    }
    static uint32_t read_uint24(byte_vec& v, int pos) {
      uint32_t ret = v[pos++];
      ret <<= 8;
      ret |= v[pos++];
      ret <<= 8;
      ret |= v[pos];
      return ret;
    }
};

class byte_block {
  private:
    std::vector<uint8_t *> blocks;
    bit_vector is_allocated;
    size_t block_remaining;
    bool is_released;
    uint8_t *reserve(size_t val_len, size_t& pos) {
      if (val_len > block_remaining) {
        size_t needed_bytes = val_len;
        int needed_blocks = needed_bytes / block_size;
        if (needed_bytes % block_size)
          needed_blocks++;
        pos = blocks.size() * block_size;
        block_remaining = needed_blocks * block_size;
        uint8_t *new_block = new uint8_t[block_remaining];
        for (int i = 0; i < needed_blocks; i++) {
          is_allocated.set(blocks.size(), i == 0);
          blocks.push_back(new_block + i * block_size);
        }
        block_remaining -= val_len;
        return new_block;
      } else {
        pos = blocks.size();
        pos--;
        uint8_t *ret = blocks[pos];
        pos *= block_size;
        size_t block_pos = (block_size - block_remaining);
        pos += block_pos;
        block_remaining -= val_len;
        return ret + block_pos;
      }
      return NULL;
    }
  public:
    const static size_t block_size = 4096;
    byte_block() {
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
    }
    size_t push_back(uint8_t *val, int val_len) {
      size_t pos;
      uint8_t *buf = reserve(val_len, pos);
      memcpy(buf, val, val_len);
      return pos;
    }
    uint8_t *operator[](size_t pos) {
      return blocks[pos / block_size] + (pos % block_size);
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
      } while (start->parent != NULL);
      ret >>= (32 - len);
      return ret;
    }
};

typedef std::vector<uniq_tails_info *> uniq_tails_info_vec;

struct node_cache {
  uint32_t child;
  uint32_t child_ptr_bits;
  uint32_t grp_no;
  uint32_t tail_ptr;
};

struct freq_grp {
  uint32_t grp_no;
  uint8_t grp_log2;
  uint32_t grp_limit;
  uint32_t count;
  uint32_t freq_count;
  uint32_t grp_size;
  uint8_t code;
  uint8_t code_len;
};

class grp_ptrs {
  private:
    std::vector<freq_grp> freq_grp_vec;
    std::vector<byte_vec> grp_data;
    byte_vec ptrs;
    byte_vec idx2_ptrs_map;
    int last_byte_bits;
    int idx_limit;
    uint8_t idx_ptr_size;
    uint32_t next_idx;
  public:
    grp_ptrs() {
      next_idx = 0;
      idx_limit = 0;
      last_byte_bits = 8;
      ptrs.push_back(0);
    }
    int get_idx_limit() {
      return idx_limit;
    }
    void set_idx_limit(int new_idx_limit) {
      idx_limit = new_idx_limit;
    }
    int get_idx_ptr_size() {
      return idx_ptr_size;
    }
    uint32_t get_idx2_ptrs_count() {
      uint32_t idx2_ptr_count = idx2_ptrs_map.size() / get_idx_ptr_size() + ((get_idx_limit() - 1) << 23);
      if (get_idx_ptr_size() == 3)
        idx2_ptr_count |= 0x80000000;
      return idx2_ptr_count;
    }
    int idx_map_arr[6] = {0, 384, 3456, 28032, 224640, 1797504};
    void set_idx_ptr_size(uint8_t _idx_ptr_size) {
      idx_ptr_size = _idx_ptr_size;
      if (idx_ptr_size == 2) {
        idx_map_arr[1] = 256;
        idx_map_arr[2] = 256 + 2048;
        idx_map_arr[3] = 256 + 2048 + 16384;
      }
    }
    void add_freq_grp(freq_grp freq_grp) {
      freq_grp_vec.push_back(freq_grp);
    }
    uint32_t check_next_grp(uint32_t& grp_no, uint32_t cur_limit, uint32_t len) {
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
        cur_limit = pow(2, log2(cur_limit) + 3);
        grp_no++;
        freq_grp_vec.push_back((freq_grp) {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0});
      }
      return cur_limit;
    }
    void update_current_grp(uint32_t grp_no, int32_t len, int32_t freq) {
      freq_grp_vec[grp_no].grp_size += len;
      freq_grp_vec[grp_no].freq_count += freq;
      freq_grp_vec[grp_no].count += (len < 0 ? -1 : 1);
      next_idx++;
    }
    uint32_t append_ptr(uint32_t grp_no, uint32_t _ptr) {
      if (idx2_ptrs_map.size() == idx_map_arr[grp_no - 1])
        next_idx = 0;
      if (idx_ptr_size == 2)
        gen::append_uint16(_ptr, idx2_ptrs_map);
      else
        gen::append_uint24(_ptr, idx2_ptrs_map);
      return next_idx++;
    }
    uint32_t append_to_grp(uint32_t grp_no, uint8_t *val, uint32_t len, bool append0 = false) {
      grp_no--;
      while (grp_no >= grp_data.size()) {
        byte_vec tail;
        tail.push_back(0);
        grp_data.push_back(tail);
      }
      uint32_t ptr = grp_data[grp_no].size();
      for (int k = 0; k < len; k++)
        grp_data[grp_no].push_back(val[k]);
      if (append0)
        grp_data[grp_no].push_back(0);
      return ptr;
    }
    int append_bits(int grp_no, uint32_t ptr) {
      int bits_to_append = freq_grp_vec[grp_no].grp_log2;
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
      return 513 + grp_data.size() * 4;
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
      return get_hdr_size() + get_data_size() + get_ptrs_size() + idx2_ptrs_map.size();
    }
    byte_vec *get_data(int grp_no) {
      return &grp_data[grp_no - 1];
    }
    void write_code_lookup_tbl(FILE* fp) {
      for (int i = 0; i < 256; i++) {
        uint8_t code_i = i;
        bool code_found = false;
        for (int j = 1; j < freq_grp_vec.size(); j++) {
          uint8_t code_len = freq_grp_vec[j].code_len;
          uint8_t code = freq_grp_vec[j].code;
          if ((code_i >> (8 - code_len)) == code) {
            fputc((j - 1) | (code_len << 5), fp);
            fputc(freq_grp_vec[j].grp_log2, fp);
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
    void write_grp_data(uint32_t offset, FILE* fp) {
      uint8_t grp_count = grp_data.size();
      fputc(grp_count, fp);
      write_code_lookup_tbl(fp);
      uint32_t total_tail_size = 0;
      for (int i = 0; i < grp_count; i++) {
        gen::write_uint32(offset + grp_count * 4 + total_tail_size, fp);
        total_tail_size += grp_data[i].size();
      }
      for (int i = 0; i < grp_count; i++) {
        fwrite(grp_data[i].data(), grp_data[i].size(), 1, fp);
      }
    }
    void write_ptrs(FILE *fp) {
      fwrite(ptrs.data(), ptrs.size(), 1, fp);
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
    void build_freq_codes() {
      std::vector<uint32_t> freqs;
      for (int i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      huffman<uint32_t> _huffman(freqs);
      printf("bits\tcd\tct\tfct\tlen\tcdln\tmxsz\n");
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp *fg = &freq_grp_vec[i];
        int len;
        fg->code = (uint8_t) _huffman.get_code(i - 1, len);
        fg->code_len = len;
        if (i <= idx_limit)
          fg->grp_log2 = ceil(log2(fg->grp_limit));
        else
          fg->grp_log2 = ceil(log2(fg->grp_size));
        fg->grp_log2 -= (8 - len);
        printf("%d\t%2x\t%u\t%u\t%u\t%d\t%u\n", (int) fg->grp_log2, fg->code, fg->count, fg->freq_count, fg->grp_size, fg->code_len, fg->grp_limit);
      }
      printf("Idx limit; %d\n", idx_limit);
    }
};

class tail_maps {
  private:
    byte_vec uniq_tails;
    uniq_tails_info_vec uniq_tails_rev;
    uniq_tails_info_vec uniq_tails_fwd;
    grp_ptrs tail_ptrs;
    byte_vec two_byte_tails;
  public:
    tail_maps() {
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

    uint32_t get_len_len(uint32_t len, byte_vec *vec = NULL) {
      if (len < 15) {
        if (vec != NULL)
          vec->push_back(len);
        return 1;
      }
      len -= 15;
      return get_var_len(len, vec);
    }

    bool is_code_near(uint32_t freq, uint32_t tot_freq, int code_len) {
      double code_len_dbl = log2(tot_freq/freq);
      int code_len_int = (int) code_len_dbl;
      if (code_len_dbl - code_len_int < 0.2)
        return true;
      return false;
    }

    void make_two_byte_tails(uniq_tails_info_vec& uniq_tails_freq, uint32_t tot_freq_count) {
      uint32_t freq_pos = 0;
      uint32_t freq_count32 = 0;
      uint32_t freq_count64 = 0;
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->tail_len == 2) {
          two_byte_tails.push_back(uniq_tails[ti->tail_pos]);
          two_byte_tails.push_back(uniq_tails[ti->tail_pos + 1]);
          if (two_byte_tails.size() < 64)
            freq_count32 += ti->freq_count;
          if (two_byte_tails.size() < 128)
            freq_count64 += ti->freq_count;
          else {
            if (is_code_near(freq_count64, tot_freq_count, 2)) {
              return;
            } else if (is_code_near(freq_count32, tot_freq_count, 2)) {
              two_byte_tails.resize(64);
            } else {
              two_byte_tails.clear();
            }
            break;
          }
        }
      }
    }

    struct tails_sort_data {
      uint8_t *tail_data;
      uint32_t tail_len;
      uint32_t n;
    };

    uint32_t make_uniq_tails(std::vector<node>& all_nodes, byte_block& sort_tails, int end_level, int start_level = 0) {
      clock_t t = clock();
      std::vector<tails_sort_data> nodes_for_sort;
      for (uint32_t i = 1; i < all_nodes.size(); i++) {
        node *n = &all_nodes[i];
        uint8_t *v = sort_tails[n->tail_pos];
        n->v0 = v[0];
        if (n->tail_len > 1 && n->level >= start_level && n->level <= end_level) {
          nodes_for_sort.push_back((struct tails_sort_data) { v, n->tail_len, i } );
        }
      }
      printf("Nodes for sort size: %lu\n", nodes_for_sort.size());
      t = gen::print_time_taken(t, "Time taken for adding to nodes_for_sort: ");
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this](const struct tails_sort_data& lhs, const struct tails_sort_data& rhs) -> bool {
        return gen::compare_rev(lhs.tail_data, lhs.tail_len, rhs.tail_data, rhs.tail_len) < 0;
      });
      t = gen::print_time_taken(t, "Time taken to sort: ");
      uint32_t freq_count = 0;
      uint32_t sum_node_freq = 0;
      uint32_t sum_levels = 0;
      uint32_t tot_freq = 0;
      std::vector<tails_sort_data>::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->tail_data;
      uint32_t prev_val_len = it->tail_len;
      while (it != nodes_for_sort.end()) {
        int cmp = gen::compare_rev(it->tail_data, it->tail_len, prev_val, prev_val_len);
        if (cmp != 0) {
          uniq_tails_info *ti_ptr = new uniq_tails_info(uniq_tails.size(), prev_val_len, uniq_tails_rev.size(), 0);
          ti_ptr->freq_count = freq_count;
          ti_ptr->sum_node_freq = sum_node_freq;
          ti_ptr->sum_levels = sum_levels;
          if (ti_ptr->freq_count == 1)
            ti_ptr->node_id = (it - 1)->n;
          uniq_tails_rev.push_back(ti_ptr);
          tot_freq += freq_count;
          for (int i = 0; i < prev_val_len; i++)
            uniq_tails.push_back(prev_val[i]);
          freq_count = 0;
          sum_node_freq = 0;
          sum_levels = 0;
          prev_val = it->tail_data;
          prev_val_len = it->tail_len;
        }
        sum_node_freq += all_nodes[it->n].freq_count;
        sum_levels += all_nodes[it->n].level;
        freq_count++;
        all_nodes[it->n].rev_node_info_pos = uniq_tails_rev.size();
        it++;
      }
      uniq_tails_info *ti_ptr = new uniq_tails_info(uniq_tails.size(), prev_val_len, uniq_tails_rev.size(), 0);
      ti_ptr->freq_count = freq_count;
      ti_ptr->sum_node_freq = sum_node_freq;
      ti_ptr->sum_levels = sum_levels;
      if (ti_ptr->freq_count == 1)
        ti_ptr->node_id = (it - 1)->n;
      tot_freq += freq_count;
      uniq_tails_rev.push_back(ti_ptr);
      for (int i = 0; i < prev_val_len; i++)
        uniq_tails.push_back(prev_val[i]);
      std::cout << std::endl;
      t = gen::print_time_taken(t, "Time taken for uniq_tails_rev: ");
      //sort_tails.release_blocks();

      return tot_freq;

    }

    constexpr static uint32_t idx_ovrhds[] = {384, 3072, 24576, 196608, 1572864, 10782081};
    const uint32_t sfx_set_max = 64;
    void build_tail_maps(std::vector<node>& all_nodes, byte_block& sort_tails, uint8_t end_level, uint8_t start_level = 0) {

      FILE *fp;

      uint32_t tot_freq_count = make_uniq_tails(all_nodes, sort_tails, end_level, start_level);

      clock_t t = clock();
      uniq_tails_fwd = uniq_tails_rev;
      std::sort(uniq_tails_fwd.begin(), uniq_tails_fwd.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return gen::compare(uniq_tails.data() + lhs->tail_pos, lhs->tail_len, uniq_tails.data() + rhs->tail_pos, rhs->tail_len) < 0;
      });
      for (int i = 0; i < uniq_tails_fwd.size(); i++)
        uniq_tails_fwd[i]->fwd_pos = i;
      t = gen::print_time_taken(t, "Time taken for uniq_tails fwd sort: ");

      uniq_tails_info_vec uniq_tails_freq = uniq_tails_rev;
      std::sort(uniq_tails_freq.begin(), uniq_tails_freq.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });
      uint32_t ftot = 0;
      uint32_t tail_len_tot = 0;
      uint32_t start_bits = 7;
      uint32_t grp_no = 1;
      uint32_t nxt_idx_limit = pow(2, start_bits);
      uint32_t remain_freq = tot_freq_count;
      int freq_idx = 0;
      int cumu_freq_idx = 0;
      for (cumu_freq_idx = 0; cumu_freq_idx < uniq_tails_freq.size(); cumu_freq_idx++) {
        uniq_tails_info *ti = uniq_tails_freq[cumu_freq_idx];
        if (cumu_freq_idx == nxt_idx_limit) {
          start_bits += 3;
          nxt_idx_limit += pow(2, start_bits);
          if (remain_freq < pow(2, start_bits) * 3 || start_bits == 19)
            break;
          // printf("%.1f\t%d\t%u\t%u\n", ceil(log2(freq_idx)), freq_idx, ftot, tail_len_tot);
          grp_no++;
          freq_idx = 0;
          ftot = 0;
          tail_len_tot = 0;
        }
        ti->grp_no = grp_no;
        ti->tail_ptr = freq_idx;
        ftot += ti->freq_count;
        tail_len_tot += ti->tail_len;
        tail_len_tot++;
        freq_idx++;
        remain_freq -= ti->freq_count;
      }
      tail_ptrs.set_idx_limit(grp_no);
      tail_ptrs.set_idx_ptr_size(tail_len_tot > 65535 ? 3 : 2);
      // cumu_freq_idx = 0;
      //printf("%.1f\t%d\t%u\t%u\n", ceil(log2(freq_idx)), freq_idx, ftot, tail_len_tot);
      std::sort(uniq_tails_freq.begin(), uniq_tails_freq.begin() + cumu_freq_idx, [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return (lhs->grp_no == rhs->grp_no) ? (lhs->rev_pos > rhs->rev_pos) : (lhs->grp_no < rhs->grp_no);
      });
      std::sort(uniq_tails_freq.begin() + cumu_freq_idx, uniq_tails_freq.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / lhs->tail_len;
        uint32_t rhs_freq = rhs->freq_count / rhs->tail_len;
        lhs_freq = ceil(log10(lhs_freq));
        rhs_freq = ceil(log10(rhs_freq));
        return (lhs_freq == rhs_freq) ? (lhs->rev_pos > rhs->rev_pos) : (lhs_freq > rhs_freq);
      });
      t = gen::print_time_taken(t, "Time taken for uniq_tails freq: ");

      //make_two_byte_tails(uniq_tails_freq, tot_freq_count);

      uint32_t freq_pos = 1;
      uniq_tails_info *prev_ti = uniq_tails_freq[0];
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        int cmp = gen::compare_rev(uniq_tails.data() + prev_ti->tail_pos, prev_ti->tail_len, uniq_tails.data() + ti->tail_pos, ti->tail_len);
        cmp--;
        if (cmp == ti->tail_len || cmp > 1) {
          ti->flags |= (cmp == ti->tail_len ? UTI_FLAG_SUFFIX_FULL : UTI_FLAG_SUFFIX_PARTIAL);
          ti->cmp_rev = cmp;
          if (ti->cmp_rev_max < cmp)
            ti->cmp_rev_max = cmp;
          if (prev_ti->cmp_rev_min > cmp) {
            prev_ti->cmp_rev_min = cmp;
            if (cmp == ti->tail_len && prev_ti->cmp_rev >= cmp)
              prev_ti->cmp_rev = cmp - 1;
          }
          if (prev_ti->cmp_rev_max < cmp)
            prev_ti->cmp_rev_max = cmp;
          prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
          ti->link_rev_idx = prev_ti->rev_pos;
        }
        if (cmp != ti->tail_len)
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
      uint32_t cur_limit = 128;
      tail_ptrs.add_freq_grp((freq_grp) {0, 0, 0, 0, 0, 0});
      tail_ptrs.add_freq_grp((freq_grp) {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0});
      uint32_t savings_full = 0;
      uint32_t savings_count_full = 0;
      uint32_t savings_partial = 0;
      uint32_t savings_count_partial = 0;
      uint32_t sfx_set_len = 0;
      uint32_t sfx_set_count = 0;
      uint32_t sfx_set_freq = 0;
      uint32_t sfx_set_tot_cnt = 0;
      uint32_t sfx_set_tot_len = 0;
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->flags & UTI_FLAG_SUFFIX_FULL) {
          savings_full += ti->tail_len;
          savings_full++;
          savings_count_full++;
          uniq_tails_info *link_ti = uniq_tails_rev[ti->link_rev_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, link_ti->tail_len + 1);
            link_ti->grp_no = grp_no;
            tail_ptrs.update_current_grp(link_ti->grp_no, link_ti->tail_len + 1, link_ti->freq_count);
            link_ti->tail_ptr = tail_ptrs.append_to_grp(grp_no, uniq_tails.data() + link_ti->tail_pos, link_ti->tail_len, true);
          }
          cur_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, 0);
          tail_ptrs.update_current_grp(link_ti->grp_no, 0, ti->freq_count);
          ti->tail_ptr = link_ti->tail_ptr + link_ti->tail_len - ti->tail_len;
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
          //     // printf("%u\t%u\t%u\t%u\n", prefix_len, grp_no, pfx_diff, pfx_len_len);
          //   }
          // }
          if (ti->flags & UTI_FLAG_SUFFIX_PARTIAL) {
            uint32_t cmp = ti->cmp_rev;
            uint32_t remain_len = ti->tail_len - cmp;
            uint32_t len_len = get_len_len(cmp);
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
              ti->tail_ptr = tail_ptrs.append_to_grp(grp_no, uniq_tails.data() + ti->tail_pos, remain_len);
              byte_vec *tail_data = tail_ptrs.get_data(grp_no);
              get_len_len(cmp, tail_data);
            } else {
              // printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
              sfx_set_len = 1;
              sfx_set_tot_len += ti->tail_len;
              sfx_set_count = 1;
              sfx_set_freq = ti->freq_count;
              sfx_set_tot_cnt++;
              tail_ptrs.update_current_grp(grp_no, ti->tail_len + 1, ti->freq_count);
              ti->tail_ptr = tail_ptrs.append_to_grp(grp_no, uniq_tails.data() + ti->tail_pos, ti->tail_len, true);
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
            cur_limit = new_limit;
          } else {
            // printf("%02u\t%03u\t%03u\t%u\n", grp_no, sfx_set_count, sfx_set_freq, sfx_set_len);
            sfx_set_len = 1;
            sfx_set_tot_len += ti->tail_len;
            sfx_set_count = 1;
            sfx_set_freq = ti->freq_count;
            sfx_set_tot_cnt++;
            cur_limit = tail_ptrs.check_next_grp(grp_no, cur_limit, ti->tail_len + 1);
            tail_ptrs.update_current_grp(grp_no, ti->tail_len + 1, ti->freq_count);
            ti->tail_ptr = tail_ptrs.append_to_grp(grp_no, uniq_tails.data() + ti->tail_pos, ti->tail_len, true);
          }
          ti->grp_no = grp_no;
        }
      }
      printf("Savings full: %u, %u\n", savings_full, savings_count_full);
      printf("Savings partial: %u, %u\n", savings_partial, savings_count_partial);
      printf("Suffix set: %u, %u\n", sfx_set_tot_len, sfx_set_tot_cnt);

      std::sort(uniq_tails_freq.begin(), uniq_tails_freq.begin() + cumu_freq_idx, [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return (lhs->grp_no == rhs->grp_no) ? (lhs->sum_levels > rhs->sum_levels) : (lhs->grp_no < rhs->grp_no);
      });
      for (freq_pos = 0; freq_pos < cumu_freq_idx; freq_pos++) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        ti->tail_ptr = tail_ptrs.append_ptr(ti->grp_no, ti->tail_ptr);
      }

      uint32_t remain_tot = 0;
      uint32_t remain_cnt = 0;
      uint32_t cmp_rev_min_tot = 0;
      uint32_t cmp_rev_min_cnt = 0;
      uint32_t free_tot = 0;
      uint32_t free_cnt = 0;
      fp = fopen("remain.txt", "w+");
      freq_pos = 0;
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->flags & UTI_FLAG_SUFFIX_FULL)
         continue;
        if ((ti->flags & 0x07) == 0) {
          // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
          //printf("%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
          free_tot += ti->tail_len;
          free_cnt++;
        }
        int remain_len = ti->tail_len - ti->cmp_rev_max - ti->cmp_fwd;
        if (remain_len > 3) {
          // fprintf(fp, "%u\t%u\t%u\t%u\t[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, ti->grp_no, remain_len, remain_len, uniq_tails.data() + ti->tail_pos);
          //fprintf(fp, "%.*s\n", remain_len - 1, uniq_tails.data() + ti->tail_pos + 1);
          remain_tot += remain_len;
          remain_tot--;
          remain_cnt++;
        }
        fprintf(fp, "%u\t%u\t%u\t%u\t%u\n", ti->freq_count, ti->tail_len, ti->node_id, ti->rev_pos, ti->cmp_rev_max);
        if (ti->cmp_rev_min != 0xFFFFFFFF && ti->cmp_rev_min > 4) {
          cmp_rev_min_tot += (ti->cmp_rev_min - 1);
          cmp_rev_min_cnt++;
        }
        //uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, ti->grp_no, savings_prefix, savings_count_prefix, ti->cmp_rev_max, fp);
      }
      fclose(fp);
      printf("Free entries: %u, %u\n", free_tot, free_cnt);
      printf("Savings prefix: %u, %u\n", savings_prefix, savings_count_prefix);
      printf("Remaining: %u, %u\n", remain_tot, remain_cnt);
      printf("Cmp_rev_min_tot: %u, %u\n", cmp_rev_min_tot, cmp_rev_min_cnt);

      tail_ptrs.build_freq_codes();
    }

    uint32_t get_pfx_len(uint32_t sz) {
      return (sz < 1024 ? 2 : (sz < 131072 ? 3 : (sz < 16777216 ? 4 : 5)));
    }

    const int nodes_per_ptr_block = 64;
    void write_ptr_lookup_tbl(std::vector<node>& all_nodes, FILE* fp) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if ((node_id % nodes_per_ptr_block) == 0)
            gen::write_uint32(bit_count, fp);
          if (cur_node->tail_len > 1) {
            uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
            freq_grp *fg = tail_ptrs.get_freq_grp(ti->grp_no);
            bit_count += fg->grp_log2;
          }
          node_id++;
        }
    }

    uint32_t find_prefix(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd, uniq_tails_info *ti, uint32_t grp_no, uint32_t& savings_prefix, uint32_t& savings_count_prefix, int cmp_sfx) {
      uint32_t limit = ti->fwd_pos + 300;
      if (limit > uniq_tails_fwd.size())
        limit = uniq_tails_fwd.size();
      for (uint32_t i = ti->fwd_pos + 1; i < limit; i++) {
        uniq_tails_info *ti_fwd = uniq_tails_fwd[i];
        int cmp = gen::compare(uniq_tails.data() + ti_fwd->tail_pos, ti_fwd->tail_len, uniq_tails.data() + ti->tail_pos, ti->tail_len);
        cmp = abs(cmp) - 1;
        if (cmp > ti->tail_len - cmp_sfx - 1)
          cmp = ti->tail_len - cmp_sfx - 1;
        if (cmp > 4 && grp_no == ti_fwd->grp_no && ti_fwd->cmp_fwd == 0) {
          if (cmp > ti_fwd->tail_len - ti_fwd->cmp_rev_max - 1)
            cmp = ti_fwd->tail_len - ti_fwd->cmp_rev_max - 1;
          if (cmp > 4) {
            ti->cmp_fwd = cmp;
            ti->link_rev_idx = ti_fwd->rev_pos;
            ti_fwd->cmp_fwd = cmp;
            return cmp;
          }
        }
      }
      return 0;
    }

    uint32_t get_tail_ptr(uint32_t grp_no, uniq_tails_info *ti) {
      uint32_t ptr = ti->tail_ptr;
      if (grp_no <= tail_ptrs.get_idx_limit()) {
        byte_vec& idx2_ptr_map = *(tail_ptrs.get_idx2_ptrs_map());
        uint32_t pos = tail_ptrs.idx_map_arr[grp_no - 1] + ptr * tail_ptrs.get_idx_ptr_size();
        ptr = tail_ptrs.get_idx_ptr_size() == 2 ? gen::read_uint16(idx2_ptr_map, pos) : gen::read_uint24(idx2_ptr_map, pos);
      }
      return ptr;
    }

    uint8_t get_first_byte(byte_block& sort_tails, node *n) {
      if (n->tail_len == 1)
        return n->v0;
      // return *(sort_tails[n->tail_pos]);
      uniq_tails_info *ti = uniq_tails_rev[n->rev_node_info_pos];
      uint32_t grp_no = ti->grp_no;
      uint32_t ptr = get_tail_ptr(grp_no, ti);
      byte_vec *tail = tail_ptrs.get_data(grp_no);
      return *(tail->data() + ptr);
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

    std::string get_tail_str1(byte_block& sort_tails, node *n) {
      return std::string((const char *) sort_tails[n->tail_pos], n->tail_len);
    }

    std::string get_tail_str(byte_block& sort_tails, node *n) {
      uniq_tails_info *ti = uniq_tails_rev[n->rev_node_info_pos];
      uint32_t grp_no = ti->grp_no;
      uint32_t tail_ptr = get_tail_ptr(grp_no, ti);
      uint32_t ptr = tail_ptr;
      byte_vec *tail = tail_ptrs.get_data(grp_no);
      std::string ret;
      int byt = (*tail)[ptr++];
      while (byt > 31) {
        ret.append(1, byt);
        byt = (*tail)[ptr++];
      }
      if ((*tail)[--ptr] == 0)
        return ret;
      uint8_t len_len = 0;
      uint32_t sfx_len = read_len(*tail, ptr, len_len);
      uint32_t ptr_end = tail_ptr;
      ptr = tail_ptr;
      do {
        byt = (*tail)[ptr--];
      } while (byt != 0);
      do {
        byt = (*tail)[ptr--];
      } while (byt > 31);
      ptr++;
      std::string prev_str;
      byt = (*tail)[++ptr];
      while (byt != 0) {
        prev_str.append(1, byt);
        byt = (*tail)[++ptr];
      }
      std::string last_str;
      while (ptr < ptr_end) {
        byt = (*tail)[++ptr];
        while (byt > 31) {
          last_str.append(1, byt);
          byt = (*tail)[++ptr];
        }
        uint32_t prev_sfx_len = read_len(*tail, ptr, len_len);
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
    grp_ptrs *get_grp_ptrs() {
      return &tail_ptrs;
    }

};

struct vals_for_sort {
  uint8_t *val;
  uint32_t val_len;
  uint32_t val_idx;
};

class builder {

  private:
    node root;
    node first_node;
    uint32_t node_count;
    uint32_t key_count;
    uint32_t common_node_count;
    uint32_t two_byte_count;
    uint32_t idx2_ptr_count;
    std::vector<node> all_nodes;
    std::vector<uint32_t> last_children;
    std::string prev_key;
    byte_vec uniq_tails;
    uniq_tails_info_vec uniq_tails_rev_child_leaf;
    uniq_tails_info_vec uniq_tails_rev_only_leaf;
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

    const bool to_sort_nodes_on_freq = true;
    void sort_nodes() {
      clock_t t = clock();
      uint32_t nxt = 0;
      uint32_t node_id = 1;
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
          n->flags |= (nxt_n == 0 ? NFLAG_TERM : 0);
          n->node_id = node_id++;
        } while (nxt_n != 0);
        if (to_sort_nodes_on_freq) {
          n->flags &= ~NFLAG_TERM;
          uint32_t start_nid = all_nodes[nxt].first_child;
          std::sort(all_nodes.begin() + start_nid, all_nodes.begin() + node_id, [this](struct node& lhs, struct node& rhs) -> bool {
            return (lhs.freq_count == rhs.freq_count) ? *(this->sort_tails[lhs.tail_pos]) < *(this->sort_tails[rhs.tail_pos]) : (lhs.freq_count > rhs.freq_count);
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
      gen::print_time_taken(t, "Time taken for sort_nodes(): ");
    }

    void append_tail_vec(std::string val, uint32_t node_pos) {
      all_nodes[node_pos].tail_pos = sort_tails.push_back((uint8_t *) val.c_str(), val.length());
      all_nodes[node_pos].tail_len = val.length();
    }

  public:
    byte_block sort_tails;
    byte_block all_vals;
    tail_maps tails_lvl_set1;
    tail_maps tails_lvl_set2;
    tail_maps tails_lvl_set3;
    grp_ptrs val_ptrs;
    std::string out_filename;
    builder(const char *out_file) {
      node_count = 0;
      common_node_count = 0;
      key_count = 0;
      //root->parent = NULL;
      root.flags = NFLAG_TERM;
      root.node_id = 0;
      root.first_child = 1;
      root.next_sibling = 0;
      //first_node->parent = root;
      first_node.next_sibling = 0;
      all_nodes.push_back(root);
      all_nodes.push_back(first_node);
      util::generate_bit_counts();
      //art_tree_init(&at);
      out_filename = out_file;
      out_filename += ".rst";
      last_children.push_back(1);
    }

    ~builder() {
      // for (int i = 0; i < all_nodes.size(); i++) {
      //   delete all_nodes[i];
      // }
      // for (int i = 0; i < uniq_tails_fwd.size(); i++) {
      //   delete uniq_tails_fwd[i];
      // }
      //delete root; // now part of all_nodes
    }

    std::string dflt_val = "default";
    void insert(std::string key) {
      append(key, dflt_val);
    }

    void insert(std::string key, std::string& val) {
    }

    void append(std::string key) {
      append(key, dflt_val);
    }

    void append(std::string key, std::string& val) {
      if (key == prev_key)
         return;
      if (val != dflt_val) {
        all_vals.push_back((uint8_t *) val.c_str(), val.length());
      }
      key_count++;
      if (node_count == 0) {
        append_tail_vec(key, 1);
        all_nodes[1].flags |= NFLAG_LEAF;
        node_count++;
        return;
      }
      prev_key = key;
      int key_pos = 0;
      int level = 0;
      node *last_child = &all_nodes[last_children[level]];
      do {
        std::string val((const char *) sort_tails[last_child->tail_pos], last_child->tail_len);
        int i = 0;
        for (; i < val.length(); i++) {
          if (key[key_pos] != val[i]) {
            if (i == 0) {
              node new_node;
              uint32_t new_node_pos = all_nodes.size();
              new_node.flags |= NFLAG_LEAF;
              //new_node->parent = last_child->parent;
              new_node.next_sibling = 0;
              new_node.level = level;
              //if (new_node->val == string(" discuss"))
              //  cout << "New node: " << key << endl;
              last_child->next_sibling = new_node_pos;
              all_nodes.push_back(new_node);
              append_tail_vec(key.substr(key_pos), new_node_pos);
              last_children[level] = new_node_pos;
              last_children.resize(level + 1);
              node_count++;
            } else {
              node child1;
              node child2;
              uint32_t child1_pos = all_nodes.size();
              uint32_t child2_pos = child1_pos + 1;
              child1.flags |= (last_child->flags & NFLAG_LEAF);
              //child1.parent = last_child;
              child1.next_sibling = child2_pos;
              child1.tail_pos = last_child->tail_pos + i;
              child1.tail_len = last_child->tail_len - i;
              child1.level = level + 1;
              //if (child1->val == string(" discuss"))
              //  cout << "Child1 node: " << key << endl;
              if (last_child->first_child != 0) {
                child1.first_child = last_child->first_child;
                uint32_t c = child1.first_child;
                do {
                  node *n = &all_nodes[c];
                  n->level++;
                  c = n->first_child;
                } while (c != 0);
              }
              last_children.resize(level + 1);
              last_children.push_back(child2_pos);
              child2.flags |= NFLAG_LEAF;
              //child2->parent = last_child;
              child2.next_sibling = 0;
              child2.level = child1.level;
              //if (child2->val == string(" discuss"))
              //  cout << "Child2 node: " << key << endl;
              last_child->first_child = child1_pos;
              last_child->flags &= ~NFLAG_LEAF;
              last_child->tail_len = i; // do these before push_back all_nodes
              last_child->freq_count++;
              all_nodes.push_back(child1);
              all_nodes.push_back(child2);
              append_tail_vec(key.substr(key_pos), child2_pos);
              node_count += 2;
              //last_child->tail_pos += i;
            }
            return;
          }
          key_pos++;
        }
        if (i == val.length() && key_pos < key.length()
            && (last_child->flags & NFLAG_LEAF) && last_child->first_child == 0) {
          node child1;
          uint32_t child1_pos = all_nodes.size();
          child1.flags |= NFLAG_LEAF;
          //child1->parent = last_child;
          child1.next_sibling = 0;
          child1.level = level + 1;
          //    if (child1->val == string(" discuss"))
          //      cout << "Ext node: " << key << endl;
          last_child->first_child = child1_pos;
          last_child->freq_count++;
          all_nodes.push_back(child1);
          append_tail_vec(key.substr(key_pos), child1_pos);
          last_children.resize(level + 1);
          last_children.push_back(child1_pos);
          node_count++;
          return;
        }
        last_child->freq_count++;
        level++;
        last_child = &all_nodes[last_children[level]];
      } while (last_child != 0);
    }

    void build_val_maps() {
      // clock_t t = clock();
      // std::vector<sort_data> vals_for_sort;
      // for (uint32_t i = 0; i < all_vals.size(); i++) {
      //   uint8_t *v = all_vals[i];
      //   vals_for_sort.push_back((struct sort_data) { v, , i } );
      // }
      // t = gen::print_time_taken(t, "Time taken for adding to nodes_for_sort: ");
      // std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this](const struct sort_data& lhs, const struct sort_data& rhs) -> bool {
      //   return gen::compare_rev(lhs.tail_data, lhs.tail_len, rhs.tail_data, rhs.tail_len) < 0;
      // });
      // t = gen::print_time_taken(t, "Time taken to sort: ");
    }

    uint8_t get_tail_ptr(node *cur_node) {
      uint8_t node_val;
      uniq_tails_info *ti = get_ti(cur_node);
      uint32_t ptr = 0;
      uint8_t grp_no = ti->grp_no;
      // if (grp_no == 0 || ti->tail_ptr == 0)
      //   printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
      ptr = ti->tail_ptr;
      grp_ptrs *tail_ptrs = get_tail_maps(cur_node)->get_grp_ptrs();
      freq_grp *fg = tail_ptrs->get_freq_grp(grp_no);
      uint8_t huf_code = fg->code;
      uint8_t huf_code_len = fg->code_len;
      int node_val_bits = 8 - huf_code_len;
      node_val = (huf_code << node_val_bits) | (ptr & ((1 << node_val_bits) - 1));
      ptr >>= node_val_bits;
      tail_ptrs->append_bits(grp_no, ptr);
      return node_val;
    }

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

    std::string build() {
      std::cout << std::endl;
      byte_vec trie;
      sort_nodes();
      tails_lvl_set1.build_tail_maps(all_nodes, sort_tails, 255, 0);
      // tails_lvl_set2.build_tail_maps(all_nodes, sort_tails, 8, 6);
      // tails_lvl_set3.build_tail_maps(all_nodes, sort_tails, 255, 9);
      build_val_maps();
      uint32_t flag_counts[8];
      uint32_t char_counts[8];
      memset(flag_counts, '\0', sizeof(uint32_t) * 8);
      memset(char_counts, '\0', sizeof(uint32_t) * 8);
      uint32_t node_count = 0;
      uint32_t term_count = 0;
      uint32_t sfx_full_count = 0;
      uint32_t sfx_partial_count = 0;
      uint32_t max_tail_len = 0;
      uint64_t bm_leaf = 0;
      uint64_t bm_term = 0;
      uint64_t bm_child = 0;
      uint64_t bm_ptr = 0;
      uint64_t bm_mask = 1UL;
      byte_vec byte_vec64;
      //trie.reserve(node_count + (node_count >> 1));
      uint32_t ptr_count = 0;
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if (cur_node->tail_len > 1) {
            uniq_tails_info *ti = get_ti(cur_node);
            if (ti->flags & UTI_FLAG_SUFFIX_FULL)
              sfx_full_count++;
            if (ti->flags & UTI_FLAG_SUFFIX_PARTIAL)
              sfx_partial_count++;
          }
          if (cur_node->flags & NFLAG_TERM)
            term_count++;
          uint8_t flags = (cur_node->flags & NFLAG_LEAF ? 1 : 0) +
            (cur_node->first_child != 0 ? 2 : 0) + (cur_node->tail_len > 1 ? 4 : 0) +
            (cur_node->flags & NFLAG_TERM ? 8 : 0);
          flag_counts[flags & 0x07]++;
          uint8_t node_val;
          if (cur_node->tail_len == 1) {
            node_val = cur_node->v0;
          } else {
            char_counts[(cur_node->tail_len > 8) ? 7 : (cur_node->tail_len-2)]++;
            node_val = get_tail_ptr(cur_node);
            ptr_count++;
          }
          if (node_count && (node_count % 64) == 0) {
            append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
            bm_term = 0; bm_child = 0; bm_leaf = 0; bm_ptr = 0;
            bm_mask = 1UL;
            append_byte_vec(trie, byte_vec64);
            byte_vec64.clear();
          }
          if (cur_node->flags & NFLAG_LEAF)
            bm_leaf |= bm_mask;
          if (cur_node->flags & NFLAG_TERM)
            bm_term |= bm_mask;
          if (cur_node->first_child != 0)
            bm_child |= bm_mask;
          if (cur_node->tail_len > 1)
            bm_ptr |= bm_mask;
          if (cur_node->tail_len > max_tail_len)
            max_tail_len = cur_node->tail_len;
          bm_mask <<= 1;
          byte_vec64.push_back(node_val);
          node_count++;
          //fwrite(&flags, 1, 1, fout);
          //fwrite(&node_val, 1, 1, fout);
      }
      if (node_count % 64) {
        append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
        append_byte_vec(trie, byte_vec64);
      }
      std::cout << std::endl;
      for (int i = 0; i < 8; i++) {
        std::cout << "Flag " << i << ": " << flag_counts[i] << "\t";
        std::cout << "Char " << i + 2 << ": " << char_counts[i] << std::endl;
      }
      // for (int i = 0; i < 8; i++)
      //   std::cout << "Val lens " << i << ": " << val_len_counts[i] << std::endl;
      byte_vec *idx2_ptrs_map = tails_lvl_set1.get_grp_ptrs()->get_idx2_ptrs_map();
      uint32_t total_tails = tails_lvl_set1.get_grp_ptrs()->get_data_size();
      uint32_t total_vals = val_ptrs.get_data_size();
      std::cout << "Total pointer count: " << ptr_count << std::endl;
      std::cout << "Full suffix count: " << sfx_full_count << std::endl;
      std::cout << "Partial suffix count: " << sfx_partial_count << std::endl;
      std::cout << "Node count: " << node_count << std::endl;
      std::cout << "Trie size: " << trie.size() << std::endl;
      std::cout << "Term count: " << term_count << std::endl;
      std::cout << "Max tail len: " << max_tail_len << std::endl;
      std::cout << "Total tail size: " << total_tails << std::endl;
      std::cout << "Uniq tail count: " << tails_lvl_set1.get_uniq_tails_rev()->size() << std::endl;
      std::cout << "Tail ptr size: " << tails_lvl_set1.get_grp_ptrs()->get_ptrs_size() << std::endl;
      std::cout << "Idx2Ptrs map size: " << idx2_ptrs_map->size() << std::endl;
      uint32_t ptr_lookup_tbl = (ceil(node_count/64) + 1) * 4;
      uint32_t trie_bv = (ceil(node_count/nodes_per_bv_block) + 1) * 11 * 2;
      uint32_t leaf_bv = (ceil(node_count/nodes_per_bv_block) + 1) * 11;
      uint32_t select_lookup = (ceil(term_count/term_divisor) + 2) * 2;
      std::cout << "Pointer lookup table: " << ptr_lookup_tbl << std::endl;
      std::cout << "Trie bit vectors: " << trie_bv << std::endl;
      std::cout << "Leaf bit vectors: " << leaf_bv << std::endl;
      std::cout << "Select lookup table: " << select_lookup << std::endl;
      uint32_t cache_size = key_count / 512 * sizeof(node_cache);
      std::cout << "Cache size: " << cache_size << std::endl;
      uint32_t total_size = 2 + 15 * 4 + trie.size()
          + ptr_lookup_tbl + trie_bv + leaf_bv + select_lookup
          + cache_size
          + tails_lvl_set1.get_grp_ptrs()->get_total_size();
      std::cout << "Total size: " << total_size << std::endl;
      std::cout << "Node struct size: " << sizeof(node) << std::endl;

      FILE *fp = fopen(out_filename.c_str(), "wb+");
      fputc(0xA5, fp); // magic byte
      fputc(0x01, fp); // version 1.0
      uint32_t grp_tails_loc = 2 + 15 * 4; // 62
      uint32_t grp_tails_size = tails_lvl_set1.get_grp_ptrs()->get_hdr_size() + tails_lvl_set1.get_grp_ptrs()->get_data_size();
      uint32_t grp_vals_loc = grp_tails_loc + grp_tails_size;
      uint32_t grp_vals_size = 0; //val_ptrs.get_size();
      uint32_t cache_loc = grp_vals_loc + grp_vals_size;
      uint32_t ptr_lookup_tbl_loc = cache_loc + cache_size;
      uint32_t trie_bv_loc = ptr_lookup_tbl_loc + ptr_lookup_tbl;
      uint32_t leaf_bv_loc = trie_bv_loc + trie_bv;
      uint32_t select_lkup_loc = leaf_bv_loc + leaf_bv;
      uint32_t tail_ptrs_loc = select_lkup_loc + select_lookup;
      uint32_t two_byte_tails_loc = tail_ptrs_loc + tails_lvl_set1.get_grp_ptrs()->get_ptrs_size();
      uint32_t idx2_ptrs_map_loc = two_byte_tails_loc + 0; // todo: fix two_byte_tails.size();
      uint32_t common_nodes_loc = idx2_ptrs_map_loc + idx2_ptrs_map->size();
      uint32_t trie_loc = common_nodes_loc + ceil(common_node_count * 1.5);
      two_byte_count = 0; // todo: fix two_byte_tails.size() / 2;
      idx2_ptr_count = tails_lvl_set1.get_grp_ptrs()->get_idx2_ptrs_count();
      printf("%u,%u,%u,%u,%u,%u,%u,%u\n", node_count, cache_loc, ptr_lookup_tbl_loc, trie_bv_loc, leaf_bv_loc, select_lkup_loc, tail_ptrs_loc, trie_loc);
      gen::write_uint32(node_count, fp);
      gen::write_uint32(common_node_count, fp);
      gen::write_uint32(two_byte_count, fp);
      gen::write_uint32(idx2_ptr_count, fp);
      gen::write_uint32(max_tail_len, fp);
      gen::write_uint32(grp_vals_loc, fp);
      gen::write_uint32(cache_loc, fp);
      gen::write_uint32(ptr_lookup_tbl_loc, fp);
      gen::write_uint32(trie_bv_loc, fp);
      gen::write_uint32(leaf_bv_loc, fp);
      gen::write_uint32(select_lkup_loc, fp);
      gen::write_uint32(tail_ptrs_loc, fp);
      gen::write_uint32(two_byte_tails_loc, fp);
      gen::write_uint32(idx2_ptrs_map_loc, fp);
      gen::write_uint32(trie_loc, fp);
      tails_lvl_set1.get_grp_ptrs()->write_grp_data(grp_tails_loc + 513, fp); // group count, 512 lookup tbl, tail locs, tails
      //val_ptrs.write_grp_data(grp_vals_loc + 513, fp); // group count, 512 lookup tbl, tail locs, tails
      //write_cache(fp, cache_size);
      fwrite(trie.data(), cache_size, 1, fp);
      tails_lvl_set1.write_ptr_lookup_tbl(all_nodes, fp);
      write_trie_bv(fp);
      write_leaf_bv(fp);
      write_select_lkup(fp);
      tails_lvl_set1.get_grp_ptrs()->write_ptrs(fp);
      fwrite(trie.data(), 0, 1, fp); // todo: fix two_byte_tails.size(), 1, fp);
      fwrite(idx2_ptrs_map->data(), idx2_ptrs_map->size(), 1, fp);
      fwrite(trie.data(), trie.size(), 1, fp);
      fclose(fp);

      // fp = fopen("nodes.txt", "wb+");
      // // dump_nodes(first_node, fp);
      // find_rpt_nodes(fp);
      // fclose(fp);

      return out_filename;

    }

    // struct nodes_ptr_grp {
    //   uint32_t node_id;
    //   uint32_t ptr;
    // };

    const int nodes_per_bv_block7 = 64;
    const int nodes_per_bv_block = 512;
    const int term_divisor = 512;
    void write_bv7(uint32_t node_id, uint32_t& term1_count, uint32_t& child_count,
                    uint32_t& term1_count7, uint32_t& child_count7,
                    uint8_t *term1_buf7, uint8_t *child_buf7, uint8_t& pos7, FILE *fp) {
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        fwrite(term1_buf7, 7, 1, fp);
        fwrite(child_buf7, 7, 1, fp);
        gen::write_uint32(term1_count, fp);
        gen::write_uint32(child_count, fp);
        term1_count7 = 0;
        child_count7 = 0;
        memset(term1_buf7, 0, 7);
        memset(child_buf7, 0, 7);
        pos7 = 0;
      } else if (node_id && (node_id % nodes_per_bv_block7) == 0) {
        term1_buf7[pos7] = term1_count7;
        child_buf7[pos7] = child_count7;
        term1_count7 = 0;
        child_count7 = 0;
        pos7++;
      }
    }

    void write_trie_bv(FILE *fp) {
      uint32_t node_id = 0;
      uint32_t term1_count = 0;
      uint32_t child_count = 0;
      uint32_t term1_count7 = 0;
      uint32_t child_count7 = 0;
      uint8_t term1_buf7[7];
      uint8_t child_buf7[7];
      uint8_t pos7 = 0;
      memset(term1_buf7, 0, 7);
      memset(child_buf7, 0, 7);
      gen::write_uint32(0, fp);
      gen::write_uint32(0, fp);
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          write_bv7(node_id, term1_count, child_count, term1_count7, child_count7, term1_buf7, child_buf7, pos7, fp);
          term1_count += (cur_node->flags & NFLAG_TERM ? 1 : 0);
          child_count += (cur_node->first_child == 0 ? 0 : 1);
          term1_count7 += (cur_node->flags & NFLAG_TERM ? 1 : 0);
          child_count7 += (cur_node->first_child == 0 ? 0 : 1);
          node_id++;
      }
      fwrite(term1_buf7, 7, 1, fp);
      fwrite(child_buf7, 7, 1, fp);
      printf("Term1_count: %u, Child count: %u\n", term1_count, child_count);
    }

    void write_leaf_bv7(uint32_t node_id, uint32_t& leaf_count, uint32_t& leaf_count7, uint8_t *leaf_buf7, uint8_t& pos7, FILE *fp) {
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        fwrite(leaf_buf7, 7, 1, fp);
        gen::write_uint32(leaf_count, fp);
        leaf_count7 = 0;
        memset(leaf_buf7, 0, 7);
        pos7 = 0;
      } else if (node_id && (node_id % nodes_per_bv_block7) == 0) {
        leaf_buf7[pos7] = leaf_count7;
        leaf_count7 = 0;
        pos7++;
      }
    }

    void write_leaf_bv(FILE *fp) {
      uint32_t node_id = 0;
      uint32_t leaf_count = 0;
      uint32_t leaf_count7 = 0;
      uint8_t leaf_buf7[7];
      uint8_t pos7 = 0;
      memset(leaf_buf7, 0, 7);
      gen::write_uint32(0, fp);
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          write_leaf_bv7(node_id, leaf_count, leaf_count7, leaf_buf7, pos7, fp);
          leaf_count += (cur_node->flags & NFLAG_TERM ? 1 : 0);
          leaf_count7 += (cur_node->flags & NFLAG_TERM ? 1 : 0);
          node_id++;
      }
      fwrite(leaf_buf7, 7, 1, fp);
    }

    void write_cache(FILE *fp, uint32_t cache_size) {
      uint32_t cache_count = cache_size / sizeof(node_cache);
      uint32_t node_id = 0;
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          node_id++;
          cache_count--;
      }
    }

    void write_select_lkup(FILE *fp) {
      uint32_t node_id = 0;
      uint32_t term_count = 0;
      gen::write_uint16(0, fp);
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if (cur_node->flags & NFLAG_TERM) {
            if (term_count && (term_count % term_divisor) == 0) {
              gen::write_uint16(node_id / nodes_per_bv_block, fp);
              if (node_id / nodes_per_bv_block > 65535)
                printf("WARNING: %u\t%u\n", term_count, node_id / nodes_per_bv_block);
            }
            term_count++;
          }
          node_id++;
      }
      gen::write_uint16(node_count/nodes_per_bv_block, fp);
    }

    int lookup(std::string key) {
      int key_pos = 0;
      uint8_t key_byte = key[key_pos];
      uint32_t node_id = 1;
      node *cur_node = &all_nodes[node_id];
      do {
        uint8_t trie_byte = tails_lvl_set1.get_first_byte(sort_tails, cur_node);
        while (key_byte != trie_byte) {
          if (cur_node->flags & NFLAG_TERM)
            return ~INSERT_AFTER;
          node_id++;
          cur_node = &all_nodes[node_id];
          trie_byte = tails_lvl_set1.get_first_byte(sort_tails, cur_node);
        }
        if (key_byte == trie_byte) {
          int cmp;
          if (cur_node->tail_len > 1) {
            std::string tail_str = tails_lvl_set1.get_tail_str(sort_tails, cur_node);
            cmp = gen::compare((const uint8_t *) tail_str.c_str(), tail_str.length(),
                    (const uint8_t *) key.c_str() + key_pos, key.size() - key_pos);
            // printf("%d\t%d\t%.*s =========== ", cmp, tail_len, tail_len, tail_data);
            // printf("%d\t%.*s\n", (int) key.size() - key_pos, (int) key.size() - key_pos, key.data() + key_pos);
          } else
            cmp = 0;
          if (cmp == 0 && key_pos + cur_node->tail_len == key.size() && (cur_node->flags & NFLAG_LEAF))
            return 0;
          if (cmp == 0 || abs(cmp) - 1 == cur_node->tail_len) {
            key_pos += cur_node->tail_len;
            node_id = cur_node->first_child;
            cur_node = &all_nodes[node_id];
            if (key_pos >= key.size())
              return ~INSERT_THREAD;
            key_byte = key[key_pos];
            continue;
          }
          return ~INSERT_THREAD;
        }
        return ~INSERT_BEFORE;
      } while (1);
      return 0;
    }

    tail_maps *get_tail_maps(node *n) {
      return &tails_lvl_set1;
      //return n->level < 6 ? &tails_lvl_set1 : &tails_lvl_set2;
    }

    uniq_tails_info *get_ti(node *n) {
      tail_maps *tm = get_tail_maps(n);
      uniq_tails_info_vec *uniq_tails_rev = tm->get_uniq_tails_rev();
      return (*uniq_tails_rev)[n->rev_node_info_pos];
    }

    uint32_t get_ptr(node *cur_node) {
      uniq_tails_info *ti = get_ti(cur_node);
      return ti->tail_ptr;
    }

    int compare_nodes_rev(uint32_t nid_1, int len1, uint32_t nid_2, int len2) {
      int lim = (len2 < len1 ? len2 : len1);
      int k = 0;
      while (k++ < lim) {
        uint32_t v1 = all_nodes[nid_1 + len1 - k].v0;
        uint32_t v2 = all_nodes[nid_2 + len2 - k].v0;
        if (v1 < v2)
          return -k;
        else if (v1 > v2)
          return k;
      }
      if (len1 == len2)
        return 0;
      return (len1 < len2 ? -k : k);
    }

    struct nodes_for_grp {
      uint32_t orig_from_nid;
      uint32_t new_from_nid;
      uint32_t other_nid;
      uint32_t grp_no;
      uint8_t len;
    };

    void find_rpt_nodes(FILE *fp) {
      clock_t t = clock();
      std::vector<nodes_for_grp> for_node_grp;
      uint32_t start_node_id = 1;
      for (uint32_t i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          uniq_tails_info *ti = get_ti(cur_node);
          if (cur_node->tail_len > 1)
            cur_node->v0 = (ti->grp_no << 28) + ti->tail_ptr;
          if (cur_node->flags & NFLAG_TERM)
            continue;
          node *next_node = &all_nodes[i + 1];
          if (cur_node->first_child == 0 && next_node->first_child == 0) {
            nodes_for_grp new_nfg;
            new_nfg.orig_from_nid = cur_node->node_id;
            new_nfg.new_from_nid = cur_node->node_id;
            new_nfg.len = 2;
            new_nfg.other_nid = 0;
            new_nfg.grp_no = 0;
            for_node_grp.push_back(new_nfg);
          }
      }
      t = gen::print_time_taken(t, "Time taken to push to for_node_grp: ");
      uint32_t no_of_nodes = node_count;
      std::sort(for_node_grp.begin(), for_node_grp.end(), [this, no_of_nodes](struct nodes_for_grp& lhs, struct nodes_for_grp& rhs) -> bool {
        return compare_nodes_rev(lhs.new_from_nid, lhs.len, rhs.new_from_nid, rhs.len) < 0;
      });
      t = gen::print_time_taken(t, "Time taken to sort for_node_grp: ");
      uint32_t grp_no = 0;
      nodes_for_grp *prev_node_grp = &for_node_grp[0];
      for (int i = 1; i < for_node_grp.size(); i++) {
        nodes_for_grp *cur_node_grp = &for_node_grp[i];
        int cmp = compare_nodes_rev(cur_node_grp->new_from_nid, 2, prev_node_grp->new_from_nid, 2);
        if (cmp != 0) {
          grp_no++;
          prev_node_grp = cur_node_grp;
        }
        cur_node_grp->grp_no = grp_no;
        cur_node_grp->len = 0;
      }
      t = gen::print_time_taken(t, "Time taken to count for_node_grp: ");
      for (int i = 0; i < for_node_grp.size(); i++) {
        nodes_for_grp *ng1 = &for_node_grp[i];
        int j = i + 1;
        nodes_for_grp *ng2 = &for_node_grp[j];
        while (ng2->grp_no == ng1->grp_no) {
          uint32_t ng1_nid = ng1->orig_from_nid + 1;
          uint32_t ng2_nid = ng2->orig_from_nid + 1;
          uint8_t new_len = 0;
          while (all_nodes[ng1_nid].v0 == all_nodes[ng2_nid].v0 && ng1_nid < node_count && ng2_nid < node_count) {
            if ((all_nodes[ng1_nid].first_child != 0) || (all_nodes[ng2_nid].first_child != 0))
              break;
            new_len++;
            if ((all_nodes[ng1_nid].flags & NFLAG_TERM) || (all_nodes[ng2_nid].flags & NFLAG_TERM))
              break;
            ng1_nid++;
            ng2_nid++;
          }
          ng1_nid = ng1->orig_from_nid;
          ng2_nid = ng2->orig_from_nid;
          bool is_next = false;
          while (all_nodes[ng1_nid].v0 == all_nodes[ng2_nid].v0 && ng1_nid > 0 && ng2_nid > 0) {
            if ((all_nodes[ng1_nid].first_child != 0) || (all_nodes[ng2_nid].first_child != 0))
              break;
            if ((all_nodes[ng1_nid].flags & NFLAG_TERM) || (all_nodes[ng2_nid].flags & NFLAG_TERM))
              break;
            is_next = true;
            ng1_nid--;
            ng2_nid--;
            new_len++;
          }
          if (is_next) {
            ng1_nid++;
            ng2_nid++;
          }
          if (new_len > ng1->len) {
            ng1->len = new_len;
            ng1->new_from_nid = ng1_nid;
            ng1->other_nid = ng2_nid;
          }
          if (new_len > ng2->len) {
            ng2->len = new_len;
            ng2->new_from_nid = ng2_nid;
            ng2->other_nid = ng1_nid;
          }
          j++;
          ng2 = &for_node_grp[j];
        }
        // if ((i % 1000) == 0) {
        //   std::cout << ".";
        //   std::cout.flush();
        // }
      }
      std::cout << std::endl;
      t = gen::print_time_taken(t, "Time taken to cmp for_node_grp: ");
      std::sort(for_node_grp.begin(), for_node_grp.end(), [this](struct nodes_for_grp& lhs, struct nodes_for_grp& rhs) -> bool {
        return (lhs.len == rhs.len) ? (compare_nodes_rev(lhs.new_from_nid, lhs.len, rhs.new_from_nid, rhs.len) < 0) : (lhs.len > rhs.len);
      });
      t = gen::print_time_taken(t, "Time taken to sort for_node_grp: ");
      for (int i = 0; i < for_node_grp.size(); i++) {
        nodes_for_grp *cur_node_grp = &for_node_grp[i];
        fprintf(fp, "%05u\t%02u\t", cur_node_grp->grp_no, cur_node_grp->len);
        for (int j = 0; j < cur_node_grp->len; j++) {
          node *node1 = &all_nodes[cur_node_grp->new_from_nid + j];
          if (node1->tail_len == 1)
            fprintf(fp, "[%u] ", node1->v0);
          else {
            uniq_tails_info *ti1 = get_ti(node1);
            fprintf(fp, "[%d][%u] ", ti1->grp_no, ti1->tail_ptr);
          }
        }
        fprintf(fp, "\n");
      }
      t = gen::print_time_taken(t, "Time taken to print grp_nodes: ");

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
// relative reference suffix

// relative reference
// relative reference prefix ?

// cymarsflos trie
// cysmartflo trie
// flo smart trie
// mars flo trie
// cymars kebab trie
// cyclic sort matching algorithm with rank stacked front loaded succinct trie
// kebab hill

#endif
