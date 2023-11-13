#ifndef STATIC_DICT_H
#define STATIC_DICT_H

#include <stdlib.h>
#include <math.h>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>
#include <immintrin.h>

using namespace std;

namespace squeezed {

#define INSERT_AFTER 1
#define INSERT_BEFORE 2
#define INSERT_LEAF 3
#define INSERT_EMPTY 4
#define INSERT_THREAD 5
#define INSERT_CONVERT 6
#define INSERT_CHILD_LEAF 7

#define bm_init_mask 0x0000000000000001UL
#define term_divisor 512
#define nodes_per_bv_block 512
#define bytes_per_bv_block 768
#define nodes_per_bv_block7 64
#define bytes_per_bv_block7 96

class dict_iter_ctx {
  public:
    int32_t cur_idx;
    int32_t key_pos;
    std::vector<uint8_t> key;
    std::vector<uint8_t> node_frag;
    std::vector<uint32_t> node_path;
    std::vector<uint32_t> child_count;
    //std::vector<uint32_t> ptr_bit_count;
    std::vector<uint32_t> last_tail_len;
    dict_iter_ctx() {
      cur_idx = key_pos = 0;
      node_frag.push_back(0);
      node_path.push_back(0);
      child_count.push_back(0);
      //ptr_bit_count.push_back(0);
      last_tail_len.push_back(0);
    }
};

struct child_cache {
  uint8_t node_id1;
  uint8_t node_id2;
  uint8_t node_id3;
  uint8_t child_count1;
  uint8_t child_count2;
  uint8_t child_count3;
  uint8_t ptr_bit_count1;
  uint8_t ptr_bit_count2;
  uint8_t ptr_bit_count3;
};

struct tail_cache {
  uint8_t grp_no;
  uint8_t tail_ptr1;
  uint8_t tail_ptr2;
  uint8_t tail_ptr3;
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
    static uint32_t read_uint32(uint8_t *pos) {
      return *((uint32_t *) pos);
      // uint32_t ret = 0;
      // int i = 4;
      // while (i--) {
      //   ret <<= 8;
      //   ret += *pos++;
      // }
      // return ret;
    }
    static uint32_t read_uint24(uint8_t *pos) {
      uint32_t ret = *pos++;
      ret <<= 8;
      ret |= *pos++;
      ret <<= 8;
      ret |= *pos;
      return ret;
    }
    static uint32_t read_uint16(uint8_t *pos) {
      uint32_t ret = *pos++;
      ret <<= 8;
      ret |= *pos;
      return ret;
    }
    static uint8_t *read_uint64(uint8_t *t, uint64_t& u64) {
      u64 = *((uint64_t *) t);
      return t + 8;
      // u64 = 0;
      // for (int v = 0; v < 8; v++) {
      //   u64 <<= 8;
      //   u64 |= *t++;
      // }
      // return t;
    }
    static uint32_t read_vint32(const uint8_t *ptr, int8_t *vlen = 0) {
        uint32_t ret = 0;
        int8_t len = 5; // read max 5 bytes
        do {
            ret <<= 7;
            ret += *ptr & 0x7F;
            len--;
        } while ((*ptr++ & 0x80) == 0x80 && len);
        if (vlen)
            *vlen = 5 - len;
        return ret;
    }
    static uint32_t min(uint32_t v1, uint32_t v2) {
      return v1 < v2 ? v1 : v2;
    }
};

class grp_ptr_data_map {
  private:
    uint8_t *ptr_lookup_tbl_loc;
    uint8_t *grp_data_loc;
    uint8_t start_bits;
    uint32_t two_byte_tail_count;
    uint32_t idx2_ptr_count;
    uint8_t idx2_ptr_size;
    int8_t grp_idx_limit;
    uint8_t *ptrs_loc;
    uint8_t *two_byte_tails_loc;
    uint8_t *idx2_ptrs_map_loc;
    uint8_t last_grp_no;
    uint8_t *code_lookup_tbl;
    std::vector<uint8_t *> grp_data;

    uint8_t *dict_buf;
    uint8_t *trie_loc;
    uint32_t start_node_id;
    uint32_t block_start_node_id;
    uint32_t end_node_id;

    int idx_map_arr[6] = {0, 384, 3456, 28032, 224640, 1797504};
    uint32_t (*read_idx_ptr)(uint8_t *) = &cmn::read_uint24;
    uint32_t read_ptr_from_idx(uint32_t grp_no, uint32_t ptr) {
      int idx_map_start = idx_map_arr[grp_no];
      ptr = read_idx_ptr(idx2_ptrs_map_loc + idx_map_start + ptr * idx2_ptr_size);
      return ptr;
    }

    uint32_t scan_ptr_bits_tail(uint32_t node_id, uint8_t *t, uint32_t ptr_bit_count) {
      uint64_t bm_mask = bm_init_mask;
      uint64_t bm_ptr;
      t = cmn::read_uint64(t + 24, bm_ptr);
      uint8_t *t_upto = t + (node_id % 64);
      if (t - 32 == trie_loc) {
        int diff = start_node_id % 64;
        t += diff;
        bm_mask <<= diff; // if (diff > 0)
      }
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
      uint64_t bm_ptr, bm_leaf;
      cmn::read_uint64(t, bm_leaf);
      t = cmn::read_uint64(t + 24, bm_ptr);
      uint8_t *t_upto = t + (node_id % 64);
      if (t - 32 == trie_loc) {
        int diff = start_node_id % 64;
        t += diff;
        bm_mask <<= diff; // if (diff > 0)
      }
      while (t < t_upto) {
        if (bm_leaf & bm_mask) {
          uint8_t ptr = read_ptr_bits8(node_id, ptr_bit_count);
          ptr_bit_count += code_lookup_tbl[ptr * 2];
        }
        bm_mask <<= 1;
        t++;
      }
      return ptr_bit_count;
    }

    uint32_t get_ptr_bit_count_tail(uint32_t node_id) {
      uint32_t block_count = (node_id - block_start_node_id) / 64;
      uint32_t ptr_bit_count = cmn::read_uint32(ptr_lookup_tbl_loc + block_count * 4);
      uint8_t *t = trie_loc + block_count * 96;
      return scan_ptr_bits_tail(node_id, t, ptr_bit_count);
    }

    uint32_t get_ptr_bit_count_val(uint32_t node_id) {
      uint32_t block_count = (node_id - block_start_node_id) / 64;
      uint32_t ptr_bit_count = cmn::read_uint32(ptr_lookup_tbl_loc + block_count * 4);
      uint8_t *t = trie_loc + block_count * 4 * 24;
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

    grp_ptr_data_map() {
    }

    void init(uint8_t *_dict_buf, uint8_t *_trie_loc, uint32_t _start_node_id, 
        uint32_t _block_start_node_id, uint32_t _end_node_id, uint8_t *tails_loc) {

      dict_buf = _dict_buf;
      trie_loc = _trie_loc;
      start_node_id = _start_node_id;
      block_start_node_id = _block_start_node_id;
      end_node_id = _end_node_id;

      ptr_lookup_tbl_loc = dict_buf + cmn::read_uint32(tails_loc);
      grp_data_loc = dict_buf + cmn::read_uint32(tails_loc + 4);
      two_byte_tail_count = cmn::read_uint32(tails_loc + 8);
      idx2_ptr_count = cmn::read_uint32(tails_loc + 12);
      idx2_ptr_size = idx2_ptr_count & 0x80000000 ? 3 : 2;
      start_bits = (idx2_ptr_count >> 20) & 0x0F;
      grp_idx_limit = (idx2_ptr_count >> 24) & 0x7F;
      idx2_ptr_count &= 0x000FFFFF;
      //dict_printf("Start_bits: %d, idx2_ptr_size: %d, idx2_ptr_count, %d, grp_idx_limit: %d\n", (int) start_bits, (int) idx2_ptr_size, idx2_ptr_count, grp_idx_limit);
      ptrs_loc = dict_buf + cmn::read_uint32(tails_loc + 16);
      two_byte_tails_loc = dict_buf + cmn::read_uint32(tails_loc + 20);
      idx2_ptrs_map_loc = dict_buf + cmn::read_uint32(tails_loc + 24);

      last_grp_no = *grp_data_loc;
      code_lookup_tbl = grp_data_loc + 2;
      uint8_t *grp_data_idx_start = code_lookup_tbl + 512;
      for (int i = 0; i <= last_grp_no; i++)
        grp_data.push_back(dict_buf + cmn::read_uint32(grp_data_idx_start + i * 4));
      int _start_bits = start_bits;
      for (int i = 1; i <= grp_idx_limit; i++) {
        idx_map_arr[i] = idx_map_arr[i - 1] + pow(2, _start_bits) * idx2_ptr_size;
        _start_bits += 3;
        //dict_printf("idx_map_arr[%d] = %d\n", i, idx_map_arr[i]);
      }
      if (idx2_ptr_size == 2)
        read_idx_ptr = &cmn::cmn::read_uint16;
    }

    void get_val(uint32_t node_id, int *in_size_out_value_len, uint8_t *ret_val) {
      uint32_t ptr_bit_count = get_ptr_bit_count_val(node_id);
      // std::cout << "NODID: " << node_id + 1 << ", block_bit_count: " << ptr_bit_count;
      uint32_t ptr = read_ptr_bits8(node_id, ptr_bit_count);
      uint8_t *lookup_tbl_ptr = code_lookup_tbl + (ptr & 0xFF) * 2;
      uint8_t bit_len = *lookup_tbl_ptr++;
      uint8_t grp_no = *lookup_tbl_ptr & 0x1F;
      uint8_t code_len = *lookup_tbl_ptr >> 5;
      ptr = read_extra_ptr(node_id, ptr_bit_count, bit_len);
      ptr &= ((1 << (bit_len - code_len)) - 1);
      if (grp_no < grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      // std::cout << ", GRP NO: " << (int) grp_no << ", bitlen: " << (int) bit_len << ", ptr: " << ptr << std::endl;
      uint8_t *val_loc = grp_data[grp_no] + ptr;
      int8_t len_of_len;
      uint32_t val_len = cmn::read_vint32(val_loc, &len_of_len);
      val_loc += len_of_len;
      //val_len = cmn::min(*in_size_out_value_len, val_len);
      *in_size_out_value_len = val_len;
      memcpy(ret_val, val_loc, val_len);
    }

    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint8_t& grp_no) {
      uint8_t *lookup_tbl_ptr = code_lookup_tbl + node_byte * 2;
      uint8_t bit_len = *lookup_tbl_ptr++;
      grp_no = *lookup_tbl_ptr & 0x1F;
      uint8_t code_len = *lookup_tbl_ptr >> 5;
      uint8_t node_val_bits = 8 - code_len;
      uint32_t ptr = node_byte & ((1 << node_val_bits) - 1);
      if (bit_len > 0) {
        if (ptr_bit_count == 0xFFFFFFFF)
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
      //ptr_bit_count = 0xFFFFFFFF;
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      get_tail_str(ret, tail_ptr, grp_no, max_tail_len);
    }

    uint8_t get_first_byte(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint32_t& tail_ptr, uint8_t& grp_no) {
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_data[grp_no];
      return tail[tail_ptr];
    }

    //const int max_tailset_len = 129;
    void get_tail_str(byte_str& ret, uint32_t tail_ptr, uint8_t grp_no, int max_tailset_len) {
      uint8_t *tail = grp_data[grp_no];
      ret.clear();
      uint8_t *t = tail + tail_ptr;
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

class fragment {
  private:
    uint8_t frag_id;
    uint8_t *dict_buf;
    uint8_t *fragment_loc;

  public:
    grp_ptr_data_map tail_map;
    grp_ptr_data_map val_map;
    uint8_t *trie_loc;
    uint32_t trie_size;
    uint32_t start_node_id;
    uint32_t block_start_node_id;
    uint32_t end_node_id;
    fragment(uint8_t _frag_id, uint8_t *_dict_buf, uint8_t *_fragment_loc, uint32_t _start_nid, uint32_t _block_start_nid, uint32_t _end_nid)
        : frag_id (_frag_id), dict_buf (_dict_buf), fragment_loc (_fragment_loc), start_node_id (_start_nid),
          block_start_node_id (_block_start_nid), end_node_id (_end_nid) {
      trie_loc = fragment_loc + 8;
      trie_size = cmn::read_uint32(fragment_loc);
      uint8_t *tails_loc = trie_loc + trie_size;
      uint32_t tail_map_size = cmn::read_uint32(_fragment_loc + 4);
      tail_map.init(_dict_buf, _fragment_loc + 8, _start_nid, _block_start_nid, _end_nid, trie_loc + trie_size);
      if (tail_map_size > 0) // todo: fix
        val_map.init(_dict_buf, _fragment_loc + 8, _start_nid, _block_start_nid, _end_nid, trie_loc + trie_size + tail_map_size);
    }

    void dump_vals() {
      int block_count = (end_node_id - start_node_id) / 64;
      uint32_t node_id = start_node_id;
      uint64_t bm_leaf;
      uint64_t bm_ptr;
      uint8_t *t = trie_loc;
      int val_len;
      uint8_t val_buf[100];
      cmn::read_uint64(t, bm_leaf);
      cmn::read_uint64(t + 24, bm_ptr);
      int start_diff = (start_node_id - block_start_node_id);
      uint64_t bm_mask = bm_init_mask;
      t += 32;
      t += start_diff;
      bm_mask <<= start_diff;
      for (int i = 0; i < block_count; i++) {
        do {
          if (bm_leaf & bm_mask) {
            val_map.get_val(node_id, &val_len, val_buf);
            printf("[%.*s]\n", val_len, val_buf);
          }
          node_id++;
          t++;
          bm_mask <<= 1;
        } while (node_id % 64);
        cmn::read_uint64(t, bm_leaf);
        cmn::read_uint64(t + 24, bm_ptr);
        t += 32;
        bm_mask = bm_init_mask;
      }
    }

};

class static_dict {

  private:
    uint8_t *dict_buf;
    size_t dict_size;

    uint32_t node_count;
    uint32_t common_node_count;
    uint32_t child_cache_count;
    uint32_t bv_block_count;
    uint32_t max_tail_len;
    uint8_t *common_nodes_loc;
    uint8_t *child_cache_loc;
    uint8_t *trie_bv_loc;
    uint8_t *leaf_bv_loc;
    uint8_t *select_lkup_loc;
    uint8_t *fragment_tbl_loc;

    uint8_t fragment_count;
    std::vector<fragment> fragments;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

  public:
    uint32_t sec_cache_count;
    uint8_t *sec_cache_loc;
    static_dict() {
      dict_buf = NULL;
    }

    ~static_dict() {
      if (dict_buf != NULL)
        free(dict_buf);
    }

    void set_print_enabled(bool to_print_messages = true) {
      is_dict_print_enabled = to_print_messages;
    }

    void load(std::string filename) {

      struct stat file_stat;
      memset(&file_stat, '\0', sizeof(file_stat));
      stat(filename.c_str(), &file_stat);
      dict_size = file_stat.st_size;
      dict_buf = (uint8_t *) malloc(dict_size);

      FILE *fp = fopen(filename.c_str(), "rb+");
      fread(dict_buf, dict_size, 1, fp);
      fclose(fp);

      fragment_count = dict_buf[2];
      node_count = cmn::read_uint32(dict_buf + 3);
      bv_block_count = node_count / nodes_per_bv_block;
      common_node_count = cmn::read_uint32(dict_buf + 7);
      max_tail_len = cmn::read_uint32(dict_buf + 11) + 1;
      child_cache_count = cmn::read_uint32(dict_buf + 15);
      sec_cache_count = cmn::read_uint32(dict_buf + 19);
      child_cache_loc = dict_buf + cmn::read_uint32(dict_buf + 23);
      sec_cache_loc = dict_buf + cmn::read_uint32(dict_buf + 27);

      select_lkup_loc =  dict_buf + cmn::read_uint32(dict_buf + 31);
      trie_bv_loc = dict_buf + cmn::read_uint32(dict_buf + 35);
      leaf_bv_loc = dict_buf + cmn::read_uint32(dict_buf + 39);
      fragment_tbl_loc = dict_buf + cmn::read_uint32(dict_buf + 43);

      uint32_t start_node_id = 0;
      for (int i = 0; i < fragment_count; i++) {
        uint32_t fragment_loc = cmn::read_uint32(fragment_tbl_loc + i * 12);
        //std::cout << "Fragment loc: " << fragment_loc << std::endl;
        uint32_t block_start_node_id = cmn::read_uint32(fragment_tbl_loc + 4 + i * 12);
        uint32_t end_node_id = cmn::read_uint32(fragment_tbl_loc + 8 + i * 12);
        fragments.push_back(fragment(i, dict_buf, dict_buf + fragment_loc, start_node_id, block_start_node_id, end_node_id));
        start_node_id = end_node_id;
      }
    }

    template <typename T>
    string operator[](uint32_t id) const {
      string ret;
      return ret;
    }
    uint32_t find_match(string key) {
      return 0;
    }

    int bin_srch_bv_term(uint32_t first, uint32_t last, uint32_t term_count) {
      while (first < last) {
        uint32_t middle = (first + last) >> 1;
        uint32_t term_at = cmn::read_uint32(trie_bv_loc + middle * 22);
        if (term_at < term_count)
          first = middle + 1;
        else if (term_at > term_count)
          last = middle;
        else
          return middle;
      }
      return last;
    }

    // https://stackoverflow.com/a/76608807/5072621
    // https://vigna.di.unimi.it/ftp/papers/Broadword.pdf
    const uint64_t sMSBs8 = 0x8080808080808080ull;
    const uint64_t sLSBs8 = 0x0101010101010101ull;
    inline uint64_t leq_bytes(uint64_t pX, uint64_t pY) {
      return ((((pY | sMSBs8) - (pX & ~sMSBs8)) ^ pX ^ pY) & sMSBs8) >> 7;
    }
    inline uint64_t gt_zero_bytes(uint64_t pX) {
      return ((pX | ((pX | sMSBs8) - sLSBs8)) & sMSBs8) >> 7;
    }
    inline uint64_t find_nth_set_bit(uint64_t pWord, uint64_t pR) {
      const uint64_t sOnesStep4  = 0x1111111111111111ull;
      const uint64_t sIncrStep8  = 0x8040201008040201ull;
      uint64_t byte_sums = pWord - ((pWord & 0xA*sOnesStep4) >> 1);
      byte_sums = (byte_sums & 3*sOnesStep4) + ((byte_sums >> 2) & 3*sOnesStep4);
      byte_sums = (byte_sums + (byte_sums >> 4)) & 0xF*sLSBs8;
      byte_sums *= sLSBs8;
      const uint64_t k_step_8 = pR * sLSBs8;
      const uint64_t place = (leq_bytes( byte_sums, k_step_8 ) * sLSBs8 >> 53) & ~0x7;
      const int byte_rank = pR - (((byte_sums << 8) >> place) & 0xFF);
      const uint64_t spread_bits = (pWord >> place & 0xFF) * sLSBs8 & sIncrStep8;
      const uint64_t bit_sums = gt_zero_bytes(spread_bits) * sLSBs8;
      const uint64_t byte_rank_step_8 = byte_rank * sLSBs8;
      return place + (leq_bytes( bit_sums, byte_rank_step_8 ) * sLSBs8 >> 56);
    }

    uint8_t *scan_block64(uint32_t& node_id, uint32_t& child_count, uint32_t& term_count, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr, uint32_t target_term_count) {

      int frag_no = 0;
      // if (frag_no > 0)
      //   std::cout << "~";
      fragment *frag = which_fragment(node_id, frag_no);
      uint8_t *t = frag->trie_loc + (node_id - frag->block_start_node_id) / nodes_per_bv_block7 * bytes_per_bv_block7;
      t = cmn::read_uint64(t, bm_leaf);
      t = cmn::read_uint64(t, bm_term);
      t = cmn::read_uint64(t, bm_child);
      t = cmn::read_uint64(t, bm_ptr);
      int i = target_term_count - term_count - 1;
      uint64_t isolated_bit = _pdep_u64(1ULL << i, bm_term);
      size_t pos = _tzcnt_u64(isolated_bit) + 1;
      // size_t pos = find_nth_set_bit(bm_term, i) + 1;
      node_id += pos;
      if (node_id >= frag->end_node_id) {
        int frag_no2 = 0;
        frag = which_fragment(node_id, frag_no2);
        t = frag->trie_loc + (node_id - frag->block_start_node_id) / nodes_per_bv_block7 * bytes_per_bv_block7;
        if (node_id % 64) {
          t += 32;
          t += pos;
        }
      } else
        t += pos;
      term_count = target_term_count;
      child_count = child_count + __builtin_popcountll(bm_child & ((isolated_bit << 1) - 1));
      // size_t k = 0;
      // while (term_count < target_term_count) {
      //   if (bm_child & bm_mask)
      //     child_count++;
      //   if (bm_term & bm_mask)
      //     term_count++;
      //   node_id++;
      //   t++;
      //   bm_mask <<= 1;
      //   k++;
      // }
      // if (child_count != child_count1)
      //   printf("Node_Id: %u,%u\tChild count: %u,%u\n", node_id, node_id1, child_count, child_count1);
      return t;
    }

    fragment* which_fragment(uint32_t node_id, int& cur_frag_idx) {
      fragment *cur_frag;
      do {
        cur_frag = &fragments[cur_frag_idx];
        if (node_id < cur_frag->end_node_id)
          return cur_frag;
      } while (++cur_frag_idx < fragment_count);
      return cur_frag;
    }

    uint8_t *find_child(uint32_t& node_id, uint32_t& child_count, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr) {
      uint32_t target_term_count = child_count;
      uint32_t child_block;
      uint8_t *select_loc = select_lkup_loc + target_term_count / term_divisor * 3;
      if ((target_term_count % term_divisor) == 0) {
        child_block = cmn::read_uint24(select_loc);
      } else {
        uint32_t start_block = cmn::read_uint24(select_loc);
        uint32_t end_block = cmn::read_uint24(select_loc + 3);
        if (start_block + 8 >= end_block) {
          do {
            start_block++;
          } while (cmn::read_uint32(trie_bv_loc + start_block * 22) < target_term_count && start_block <= end_block);
          child_block = start_block - 1;
        } else {
          child_block = bin_srch_bv_term(start_block, end_block, target_term_count);
        }
        // printf("%u,%u,%.0f\t%u,%u,%.0f,%u\n", node_id_block, bv_block_count, ceil(log2(bv_block_count - node_id_block)), start_block, end_block, ceil(log2(end_block - start_block)), child_block);
      }
      // uint32_t node_id_block = node_id / nodes_per_bv_block;
      // child_block = bin_srch_bv_term(node_id_block, bv_block_count, target_term_count);
      child_block++;
      uint32_t term_count;
      do {
        child_block--;
        term_count = cmn::read_uint32(trie_bv_loc + child_block * 22);
        child_count = cmn::read_uint32(trie_bv_loc + child_block * 22 + 4);
        node_id = child_block * nodes_per_bv_block;
      } while (term_count >= target_term_count);
      uint8_t *bv7_term = trie_bv_loc + child_block * 22 + 8;
      uint8_t *bv7_child = bv7_term + 7;
      for (int pos7 = 0; pos7 < 7 && node_id + nodes_per_bv_block7 < node_count; pos7++) {
        uint8_t term7 = bv7_term[pos7];
        if (term_count + term7 < target_term_count) {
          term_count += term7;
          child_count += bv7_child[pos7];
          node_id += nodes_per_bv_block7;
        } else
          break;
      }
      return scan_block64(node_id, child_count, term_count, bm_leaf, bm_term, bm_child, bm_ptr, target_term_count);
    }

    uint8_t *read_flags(uint8_t *t, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr) {
      t = cmn::read_uint64(t, bm_leaf);
      t = cmn::read_uint64(t, bm_term);
      t = cmn::read_uint64(t, bm_child);
      return cmn::read_uint64(t, bm_ptr);
    }

    uint8_t *restore_from_child_cache(fragment **cur_frag, uint32_t &child_count, uint32_t &node_id, int &cur_frag_idx, 
                  uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr, uint32_t& ptr_bit_count) {
      child_cache *cc3 = (child_cache *) (child_cache_loc + (child_count - 1) * sizeof(child_cache));
      node_id = cmn::read_uint24(&cc3->node_id1);
      child_count = cmn::read_uint24(&cc3->child_count1);
      (*cur_frag) = which_fragment(node_id, cur_frag_idx);
      uint8_t *t = (*cur_frag)->trie_loc + (node_id - (*cur_frag)->block_start_node_id) / nodes_per_bv_block7 * bytes_per_bv_block7;
      if (node_id % 64) {
        t = read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
        t += node_id % 64;
      }
      ptr_bit_count = cmn::read_uint24(&cc3->ptr_bit_count1);
      return t;
    }

    uint32_t lookup(const uint8_t *key, int key_len, int& result, int& cur_frag_idx) {
      int key_pos = 0;
      uint32_t node_id = 0;
      uint8_t key_byte = key[key_pos];
      cur_frag_idx = 0;
      fragment *cur_frag = &fragments[cur_frag_idx];
      uint8_t *t = cur_frag->trie_loc;
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      uint8_t trie_byte;
      uint8_t grp_no;
      uint32_t tail_ptr = 0;
      uint32_t child_count = 0;
      uint32_t ptr_bit_count = 0;
      uint8_t tail_str_buf[max_tail_len];
      byte_str tail_str(tail_str_buf, max_tail_len);
      do {
        if ((node_id % 64) == 0) {
          bm_mask = bm_init_mask;
          t = read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
        }
        uint8_t node_byte = trie_byte = *t++;
        if (bm_mask & bm_ptr) {
          // if (node_id < sec_cache_count) {
          //   ptr_bit_count = 0xFFFFFFFF;
          //   tail_ptr = 0;
          //   trie_byte = sec_cache_loc[node_id];
          // } else
            trie_byte = cur_frag->tail_map.get_first_byte(node_byte, node_id, ptr_bit_count, tail_ptr, grp_no);
        }
        if (bm_mask & bm_child)
          child_count++;
        if (key_byte != trie_byte) {
          if (bm_mask & bm_term) {
            result = ~INSERT_AFTER;
            return node_id;
          }
          bm_mask <<= 1;
          node_id++;
          continue;
        }
        if (key_byte == trie_byte) {
          int cmp = 0;
          uint32_t tail_len = 1;
          if (bm_mask & bm_ptr) {
            // if (tail_ptr == 0)
            //   tail_ptr = cur_frag->tail_map.get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
            cur_frag->tail_map.get_tail_str(tail_str, tail_ptr, grp_no, max_tail_len);
            tail_len = tail_str.length();
            cmp = cmn::compare(tail_str.data(), tail_len, key + key_pos, key_len - key_pos);
          }
          key_pos += tail_len;
          if (key_pos < key_len && (cmp == 0 || cmp - 1 == tail_len)) {
            if ((bm_mask & bm_child) == 0) {
              result = ~INSERT_LEAF;
              return node_id;
            }
            if (child_count <= child_cache_count) {
              t = restore_from_child_cache(&cur_frag, child_count, node_id, cur_frag_idx, bm_leaf, bm_term, bm_child, bm_ptr, ptr_bit_count);
            } else {
              t = find_child(node_id, child_count, bm_leaf, bm_term, bm_child, bm_ptr);
              cur_frag = which_fragment(node_id, cur_frag_idx);
              ptr_bit_count = 0xFFFFFFFF;
            }
            bm_mask = (bm_init_mask << (node_id % nodes_per_bv_block7));
            key_byte = key[key_pos];
            continue;
          }
          if (cmp == 0 && key_pos == key_len && (bm_leaf & bm_mask)) {
            result = node_byte;
            result <<= 8;
            result += (bm_ptr & bm_mask ? 1 : 0);
            return node_id;
          }
          result = ~INSERT_THREAD;
        }
        result = ~INSERT_BEFORE;
        return node_id;
      } while (node_id < node_count);
      result = ~INSERT_EMPTY;
      return node_id;
    }

    bool get(const uint8_t *key, int key_len, int *in_size_out_value_len, uint8_t *val) {
      int result, key_pos, cmp, frag_idx;
      std::vector<uint8_t> val_str;
      uint32_t node_id = lookup(key, key_len, result, frag_idx);
      fragment *cur_frag = &fragments[frag_idx];
      if (result >= 0) {
        cur_frag->val_map.get_val(node_id, in_size_out_value_len, val);
        return true;
      }
      return false;
    }

    struct ctx_vars {
      uint8_t *t;
      int cur_frag_idx;
      fragment *cur_frag;
      uint32_t node_id, child_count;
      uint64_t bm_leaf, bm_term, bm_child, bm_ptr, bm_mask;
      byte_str tail;
      uint32_t tail_ptr;
      uint32_t ptr_bit_count;
      uint8_t grp_no;
      ctx_vars() {
        memset(this, '\0', sizeof(*this));
        ptr_bit_count = 0xFFFFFFFF;
      }
    };

    void push_to_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      ctx.cur_idx++;
      if (ctx.cur_idx < ctx.node_path.size()) {
        //ctx.last_tail_len[ctx.cur_idx] = 0;
        cv.tail.clear();
        update_ctx(ctx, cv);
      } else {
        ctx.child_count.push_back(cv.child_count);
        ctx.node_path.push_back(cv.node_id);
        ctx.node_frag.push_back(cv.cur_frag_idx);
        //ctx.ptr_bit_count.push_back(cv.ptr_bit_count);
        ctx.last_tail_len.push_back(0); //cv.tail.length());
      }
    }

    void update_ctx(dict_iter_ctx& ctx, ctx_vars& cv) {
      ctx.child_count[ctx.cur_idx] = cv.child_count;
      ctx.node_path[ctx.cur_idx] = cv.node_id;
      ctx.node_frag[ctx.cur_idx] = cv.cur_frag_idx;
      //ctx.ptr_bit_count[ctx.cur_idx] = cv.ptr_bit_count;
      ctx.key.resize(ctx.key.size() - ctx.last_tail_len[ctx.cur_idx]);
      ctx.last_tail_len[ctx.cur_idx] = cv.tail.length();
      for (int i = 0; i < cv.tail.length(); i++)
        ctx.key.push_back(cv.tail[i]);
    }

    void clear_last_tail(dict_iter_ctx& ctx) {
      ctx.key.resize(ctx.key.size() - ctx.last_tail_len[ctx.cur_idx]);
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
      cv.cur_frag_idx = ctx.node_frag[ctx.cur_idx];
      cv.cur_frag = &fragments[cv.cur_frag_idx];
      //cv.ptr_bit_count = ctx.ptr_bit_count[ctx.cur_idx];
      cv.t = cv.cur_frag->trie_loc + (cv.node_id - cv.cur_frag->block_start_node_id) / nodes_per_bv_block7 * bytes_per_bv_block7;
      if (cv.node_id % 64) {
        cv.t = read_flags(cv.t, cv.bm_leaf, cv.bm_term, cv.bm_child, cv.bm_ptr);
        cv.t += cv.node_id % 64;
        cv.bm_mask = bm_init_mask << (cv.node_id % 64);
      }
    }

    void read_flags_block_begin(ctx_vars& cv) {
      if ((cv.node_id % 64) == 0) {
        cv.bm_mask = bm_init_mask;
        cv.t = read_flags(cv.t, cv.bm_leaf, cv.bm_term, cv.bm_child, cv.bm_ptr);
      }
    }

    int next(dict_iter_ctx& ctx, uint8_t *key_buf, uint8_t *val_buf = NULL, int *val_buf_len = NULL) {
      ctx_vars cv;
      uint8_t tail[max_tail_len + 1];
      cv.tail.set_buf_max_len(tail, max_tail_len);
      read_from_ctx(ctx, cv);
      bool to_skip_first_leaf = (ctx.key.size() > 0);
      do {
        read_flags_block_begin(cv);
        if (cv.bm_mask & cv.bm_leaf) {
          if (to_skip_first_leaf) {
            if ((cv.bm_mask & cv.bm_child) == 0) {
              while (cv.bm_mask & cv.bm_term) {
                if (ctx.cur_idx == 0)
                  return 0;
                pop_from_ctx(ctx, cv);
                read_flags_block_begin(cv);
              }
              cv.bm_mask <<= 1;
              cv.node_id++;
              cv.t++;
              update_ctx(ctx, cv);
              to_skip_first_leaf = false;
              continue;
            }
          } else {
            cv.cur_frag->tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, max_tail_len, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no, cv.bm_mask & cv.bm_ptr);
            update_ctx(ctx, cv);
            int key_len = ctx.key.size();
            memcpy(key_buf, ctx.key.data(), key_len);
            cv.cur_frag->val_map.get_val(cv.node_id, val_buf_len, val_buf);
            return key_len;
          }
        }
        to_skip_first_leaf = false;
        if (cv.bm_mask & cv.bm_child) {
          cv.child_count++;
          cv.cur_frag->tail_map.get_tail_str(cv.tail, cv.node_id, *cv.t, max_tail_len, cv.tail_ptr, cv.ptr_bit_count, cv.grp_no, cv.bm_mask & cv.bm_ptr);
          update_ctx(ctx, cv);
          if (cv.child_count <= child_cache_count) {
            cv.t = restore_from_child_cache(&cv.cur_frag, cv.child_count, cv.node_id, cv.cur_frag_idx, cv.bm_leaf, cv.bm_term, cv.bm_child, cv.bm_ptr, cv.ptr_bit_count);
          } else {
            cv.t = find_child(cv.node_id, cv.child_count, cv.bm_leaf, cv.bm_term, cv.bm_child, cv.bm_ptr);
            cv.cur_frag = which_fragment(cv.node_id, cv.cur_frag_idx);
            cv.ptr_bit_count = 0xFFFFFFFF;
          }
          cv.bm_mask = (bm_init_mask << (cv.node_id % nodes_per_bv_block7));
          push_to_ctx(ctx, cv);
        }
      } while (cv.node_id < node_count);
      return 0;
    }

    void dump_vals() {
      fragments[0].dump_vals();
    }

};

}
#endif
