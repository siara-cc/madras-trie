#ifndef STATIC_DICT_H
#define STATIC_DICT_H

#include <stdlib.h>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>
#include <immintrin.h>

#include "bit_vector.h"
#include "var_array.h"
#include "squeezed_builder.h"

#define INSERT_AFTER 1
#define INSERT_BEFORE 2
#define INSERT_LEAF 3
#define INSERT_EMPTY 4
#define INSERT_THREAD 5
#define INSERT_CONVERT 6
#define INSERT_CHILD_LEAF 7

using namespace std;

namespace squeezed {

class byte_str {
  int max_len;
  int len;
  uint8_t *buf;
  public:
    byte_str(uint8_t *_buf, int _max_len) {
      max_len = _max_len;
      buf = _buf;
      len = 0;
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
    size_t length() {
      return len;
    }
    void clear() {
      len = 0;
    }
};

class static_dict {

  private:
    uint8_t *dict_buf;
    size_t dict_size;

    uint32_t node_count;
    uint32_t common_node_count;
    uint32_t two_byte_tail_count;
    uint32_t idx2_ptr_count;
    uint32_t bv_block_count;
    uint32_t max_tail_len;
    uint8_t *grp_tails_loc;
    uint8_t *grp_vals_loc;
    uint8_t *cache_loc;
    uint8_t *ptr_lookup_tbl_loc;
    uint8_t *trie_bv_loc;
    uint8_t *leaf_bv_loc;
    uint8_t *select_lkup_loc;
    uint8_t *select_lkup_loc_end;
    uint8_t *tail_ptrs_loc;
    uint8_t *trie_loc;
    uint8_t *two_byte_tails_loc;
    uint8_t *idx2_ptrs_map_loc;
    uint8_t *common_nodes_loc;

    uint8_t grp_count;
    int8_t grp_idx_limit;
    uint8_t idx2_ptr_size;
    uint8_t *code_lookup_tbl;
    std::vector<uint8_t *> grp_tails;
    builder *sb;

    int idx_map_arr[6] = {0, 384, 3456, 28032, 224640, 1797504};
    uint32_t (*read_idx_ptr)(uint8_t *) = &read_uint24;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

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

    uint32_t read_ptr_from_idx(uint32_t grp_no, uint32_t ptr) {
      int idx_map_start = idx_map_arr[grp_no];
      // std::cout << (int) grp_no << ", " << (int) grp_idx_limit << ", " << idx_map_start << ", " << ptr << std::endl;
      ptr = read_idx_ptr(idx2_ptrs_map_loc + idx_map_start + ptr * idx2_ptr_size);
      return ptr;
    }

  public:
    static_dict(std::string filename, builder *_sb = NULL) {

      sb = _sb;

      struct stat file_stat;
      memset(&file_stat, '\0', sizeof(file_stat));
      stat(filename.c_str(), &file_stat);
      dict_size = file_stat.st_size;
      dict_buf = (uint8_t *) malloc(dict_size);

      FILE *fp = fopen(filename.c_str(), "rb+");
      fread(dict_buf, dict_size, 1, fp);
      fclose(fp);

      grp_tails_loc = dict_buf + 2 + 15 * 4; // 62
      node_count = read_uint32(dict_buf + 2);
      common_node_count = read_uint32(dict_buf + 6);
      two_byte_tail_count = read_uint32(dict_buf + 10);
      idx2_ptr_count = read_uint32(dict_buf + 14);
      idx2_ptr_size = idx2_ptr_count & 0x80000000 ? 3 : 2;
      grp_idx_limit = (idx2_ptr_count >> 23) & 0x7F;
      idx2_ptr_count &= 0x00FFFFFF;
      max_tail_len = read_uint32(dict_buf + 18);
      bv_block_count = node_count / nodes_per_bv_block;
      grp_vals_loc = dict_buf + read_uint32(dict_buf + 22);
      cache_loc = dict_buf + read_uint32(dict_buf + 24);
      ptr_lookup_tbl_loc = dict_buf + read_uint32(dict_buf + 30);
      trie_bv_loc = dict_buf + read_uint32(dict_buf + 34);
      leaf_bv_loc = dict_buf + read_uint32(dict_buf + 38);
      select_lkup_loc =  dict_buf + read_uint32(dict_buf + 42);
      tail_ptrs_loc = dict_buf + read_uint32(dict_buf + 46);
      select_lkup_loc_end = tail_ptrs_loc;
      two_byte_tails_loc = dict_buf + read_uint32(dict_buf + 50);
      idx2_ptrs_map_loc = dict_buf + read_uint32(dict_buf + 54);
      trie_loc = dict_buf + read_uint32(dict_buf + 58);

      grp_count = *grp_tails_loc;
      code_lookup_tbl = grp_tails_loc + 1;
      uint8_t *grp_tails_idx_start = code_lookup_tbl + 512;
      for (int i = 0; i < grp_count; i++)
        grp_tails.push_back(dict_buf + read_uint32(grp_tails_idx_start + i * 4));

      printf("%u,%ld,%ld,%ld,%ld,%ld,%ld,%ld\n", node_count, cache_loc-dict_buf, ptr_lookup_tbl_loc-dict_buf, trie_bv_loc-dict_buf, 
                leaf_bv_loc-dict_buf, select_lkup_loc-dict_buf, tail_ptrs_loc-dict_buf, trie_loc-dict_buf);

      if (idx2_ptr_size == 2) {
        idx_map_arr[1] = 256;
        idx_map_arr[2] = 256 + 2048;
        idx_map_arr[3] = 256 + 2048 + 16384;
        read_idx_ptr = &read_uint16;
      }

    }

    ~static_dict() {
    }

    template <typename T>
    string operator[](uint32_t id) const {
      string ret;
      return ret;
    }
    uint32_t find_match(string key) {
      return 0;
    }

    uint32_t read_len(uint8_t *tail, uint32_t ptr, uint8_t& len_len) {
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

    void scan_nodes_ptr_bits(uint32_t node_id, uint8_t *t, uint32_t& ptr_bit_count) {
      uint64_t bm_ptr;
      uint64_t bm_mask = bm_init_mask;
      read_uint64(t + 24, bm_ptr);
      t += 32;
      uint8_t *t_upto = t + (node_id % 64);
      while (t < t_upto) {
        if (bm_ptr & bm_mask)
          ptr_bit_count += code_lookup_tbl[*t * 2 + 1];
        bm_mask <<= 1;
        t++;
      }
    }

    void get_ptr_bit_count(uint32_t node_id, uint32_t& ptr_bit_count) {
      uint32_t block_count = node_id / 64;
      ptr_bit_count = read_uint32(ptr_lookup_tbl_loc + block_count * 4);
      uint8_t *t = trie_loc + block_count * 96;
      scan_nodes_ptr_bits(node_id, t, ptr_bit_count);
    }

    // flag pos: 1 1 4 4 7 7 10 10 13 13
    // trie pos: 0 2 3 5 6 8  9 11 12 14
    // i:        0 1 2 3 4 5  6  7  8  9
    uint32_t read_extra_ptr(uint32_t node_id, uint32_t& ptr_bit_count, int8_t bit_len) {
      if (ptr_bit_count == 0xFFFFFFFF)
        get_ptr_bit_count(node_id, ptr_bit_count);
      uint8_t *ptr_loc = tail_ptrs_loc + ptr_bit_count / 8;
      uint8_t bits_left = 8 - (ptr_bit_count % 8);
      ptr_bit_count += bit_len;
      uint32_t ret = 0;
      while (bit_len > 0) {
        if (bit_len < bits_left) {
          bits_left -= bit_len;
          ret |= ((*ptr_loc >> bits_left) & ((1 << bit_len) - 1));
          bit_len = 0;
        } else {
          ret |= (*ptr_loc++ & ((1 << bits_left) - 1));
          bit_len -= bits_left;
          bits_left = 8;
          ret <<= (bit_len > 8 ? 8 : bit_len);
        }
      }
      return ret;
    }

    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint8_t& grp_no) {
      uint8_t *lookup_tbl_ptr = code_lookup_tbl + node_byte * 2;
      grp_no = *lookup_tbl_ptr & 0x1F;
      uint8_t code_len = *lookup_tbl_ptr++ >> 5;
      uint8_t bit_len = *lookup_tbl_ptr;
      uint8_t node_val_bits = 8 - code_len;
      uint32_t ptr = node_byte & ((1 << node_val_bits) - 1);
      if (bit_len > 0)
        ptr |= (read_extra_ptr(node_id, ptr_bit_count, bit_len) << node_val_bits);
      if (grp_no <= grp_idx_limit)
        ptr = read_ptr_from_idx(grp_no, ptr);
      return ptr;
    }

    uint8_t get_first_byte(uint8_t node_byte, uint64_t is_ptr, uint32_t node_id, uint32_t& ptr_bit_count, uint32_t& tail_ptr, uint8_t& grp_no) {
      if (!is_ptr)
        return node_byte;
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_tails[grp_no];
      return tail[tail_ptr];
    }

    //const int max_tailset_len = 129;
    void get_tail_str(byte_str& ret, uint32_t tail_ptr, uint8_t grp_no, int max_tailset_len) {
      uint32_t ptr = tail_ptr;
      uint8_t *tail = grp_tails[grp_no];
      ret.clear();
      int byt = tail[ptr++];
      while (byt > 31) {
        ret.append(byt);
        byt = tail[ptr++];
      }
      if (tail[--ptr] == 0)
        return;
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
        uint32_t prev_sfx_len = read_len(tail, ptr, len_len);
        last_str.append(prev_str.data() + prev_str.length()-prev_sfx_len, prev_sfx_len);
        ptr += len_len;
        ptr--;
        prev_str.clear();
        prev_str.append(last_str.data(), last_str.length());
        last_str.clear();
      }
      ret.append(prev_str.data() + prev_str.length()-sfx_len, sfx_len);
    }

    int bin_srch_bv_term(uint32_t first, uint32_t last, uint32_t term_count) {
      while (first < last) {
        uint32_t middle = (first + last) >> 1;
        uint32_t term_at = read_uint32(trie_bv_loc + middle * 22);
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

    uint8_t *scan_block64(uint8_t *t, uint32_t& node_id, uint32_t& child_count, uint32_t& term_count, uint32_t target_term_count) {

      uint64_t bm_term, bm_child;
      read_uint64(t + 8, bm_term);
      read_uint64(t + 16, bm_child);
      t += 32;
      int i = target_term_count - term_count - 1;
      uint64_t isolated_bit = _pdep_u64(1ULL << i, bm_term);
      size_t pos = _tzcnt_u64(isolated_bit) + 1;
      // size_t pos = find_nth_set_bit(bm_term, i) + 1;
      node_id = node_id + pos;
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

    const int term_divisor = 512;
    const int nodes_per_bv_block = 512;
    const int bytes_per_bv_block = 768;
    const int nodes_per_bv_block7 = 64;
    const int bytes_per_bv_block7 = 96;
    uint8_t *find_child(uint8_t *t, uint32_t& node_id, uint32_t& child_count, uint32_t& term_count) {
      uint32_t target_term_count = child_count;
      uint32_t child_block;
      uint8_t *select_loc = select_lkup_loc + target_term_count / term_divisor * 2;
      if ((target_term_count % term_divisor) == 0) {
        child_block = read_uint16(select_loc);
      } else {
        uint32_t start_block = read_uint16(select_loc);
        uint32_t end_block = read_uint16(select_loc + 2);
        if (start_block + 4 >= end_block) {
          do {
            start_block++;
          } while (read_uint32(trie_bv_loc + start_block * 22) < target_term_count && start_block <= end_block);
          child_block = start_block - 1;
        } else {
          child_block = bin_srch_bv_term(start_block, end_block, target_term_count);
        }
        // printf("%u,%u,%.0f\t%u,%u,%.0f,%u\n", node_id_block, bv_block_count, ceil(log2(bv_block_count - node_id_block)), start_block, end_block, ceil(log2(end_block - start_block)), child_block);
      }
      // uint32_t node_id_block = node_id / nodes_per_bv_block;
      // child_block = bin_srch_bv_term(node_id_block, bv_block_count, target_term_count);
      child_block++;
      do {
        child_block--;
        term_count = read_uint32(trie_bv_loc + child_block * 22);
        child_count = read_uint32(trie_bv_loc + child_block * 22 + 4);
        node_id = child_block * nodes_per_bv_block;
        t = trie_loc + child_block * bytes_per_bv_block;
      } while (term_count >= target_term_count);
      uint8_t *bv7_term = trie_bv_loc + child_block * 22 + 8;
      uint8_t *bv7_child = bv7_term + 7;
      for (int pos7 = 0; pos7 < 7 && node_id + nodes_per_bv_block7 < node_count; pos7++) {
        uint8_t term7 = bv7_term[pos7];
        if (term_count + term7 < target_term_count) {
          term_count += term7;
          child_count += bv7_child[pos7];
          node_id += nodes_per_bv_block7;
          t += bytes_per_bv_block7;
        } else
          break;
      }
      return scan_block64(t, node_id, child_count, term_count, target_term_count);
    }

    uint8_t *read_uint64(uint8_t *t, uint64_t& u64) {
      u64 = *((uint64_t *) t);
      return t + 8;
      // u64 = 0;
      // for (int v = 0; v < 8; v++) {
      //   u64 <<= 8;
      //   u64 |= *t++;
      // }
      // return t;
    }

    void read_flags(uint8_t *t, uint64_t& bm_leaf, uint64_t& bm_term, uint64_t& bm_child, uint64_t& bm_ptr) {
      t = read_uint64(t, bm_leaf);
      t = read_uint64(t, bm_term);
      t = read_uint64(t, bm_child);
      read_uint64(t, bm_ptr);
    }

    static const uint64_t bm_init_mask = 0x0000000000000001UL;
    int lookup(std::string& key) {
      int key_pos = 0;
      uint32_t node_id = 0;
      uint8_t key_byte = key[key_pos];
      uint8_t *t = trie_loc;
      uint8_t node_byte;
      uint64_t bm_leaf = 0;
      uint64_t bm_term = 0;
      uint64_t bm_child = 0;
      uint64_t bm_ptr = 0;
      uint64_t bm_mask = bm_init_mask;
      uint8_t grp_no;
      uint8_t trie_byte;
      uint32_t ptr_bit_count = 0;
      uint32_t tail_ptr = 0;
      uint32_t term_count = 0;
      uint32_t child_count = 0;
      uint8_t tail_str_buf[max_tail_len];
      byte_str tail_str(tail_str_buf, max_tail_len);
      do {
        if ((node_id % 64) == 0) {
          bm_mask = bm_init_mask;
          read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
          t += 32;
        }
        node_byte = *t++;
        trie_byte = get_first_byte(node_byte, (bm_ptr & bm_mask), node_id, ptr_bit_count, tail_ptr, grp_no);
        node_id++;
        if (bm_mask & bm_child)
          child_count++;
        if (bm_mask & bm_term)
          term_count++;
        if (key_byte != trie_byte) {
          if (bm_mask & bm_term)
            return ~INSERT_AFTER;
          bm_mask <<= 1;
          continue;
        }
        if (key_byte == trie_byte) {
          int cmp = 0;
          uint32_t tail_len = 1;
          if (bm_mask & bm_ptr) {
            get_tail_str(tail_str, tail_ptr, grp_no, max_tail_len + 1);
            tail_len = tail_str.length();
            cmp = compare(tail_str.data(), tail_len,
                    (const uint8_t *) key.c_str() + key_pos, key.size() - key_pos);
            // printf("%d\t%d\t%.*s =========== ", cmp, tail_len, tail_len, tail_data);
            // printf("%d\t%.*s\n", (int) key.size() - key_pos, (int) key.size() - key_pos, key.data() + key_pos);
          }
          key_pos += tail_len;
          if (cmp == 0 && key_pos == key.size() && (bm_leaf & bm_mask))
            return 0;
          if (cmp == 0 || cmp - 1 == tail_len) {
            if ((bm_mask & bm_child) == 0)
              return ~INSERT_LEAF;
            t = find_child(t, node_id, child_count, term_count);
            read_flags(trie_loc + (node_id / 64) * 96, bm_leaf, bm_term, bm_child, bm_ptr);
            bm_mask = (bm_init_mask << (node_id % 64));
            key_byte = key[key_pos];
            ptr_bit_count = 0xFFFFFFFF;
            continue;
          }
          return ~INSERT_THREAD;
        }
        return ~INSERT_BEFORE;
      } while (node_id < node_count);
      return ~INSERT_EMPTY;
    }

};

}
#endif
