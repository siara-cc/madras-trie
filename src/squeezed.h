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
    uint32_t bv_block_count;
    uint32_t max_tail_len;
    uint8_t *grp_tails_loc;
    uint8_t *cache_loc;
    uint8_t *ptr_lookup_tbl_loc;
    uint8_t *term_bv_loc;
    uint8_t *child_bv_loc;
    uint8_t *leaf_bv_loc;
    uint8_t *select_lkup_loc;
    uint8_t *tail_ptrs_loc;
    uint8_t *trie_loc;

    uint8_t grp_count;
    uint8_t *code_lookup_tbl;
    std::vector<uint8_t *> grp_tails;
    builder *sb;

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

      grp_tails_loc = dict_buf + 2 + 10 * 4; // 42
      node_count = read_uint32(dict_buf + 2);
      max_tail_len = read_uint32(dict_buf + 6);
      bv_block_count = node_count / nodes_per_bv_block;
      cache_loc = dict_buf + read_uint32(dict_buf + 10);
      ptr_lookup_tbl_loc = dict_buf + read_uint32(dict_buf + 14);
      term_bv_loc =  dict_buf + read_uint32(dict_buf + 18);
      child_bv_loc =  dict_buf + read_uint32(dict_buf + 22);
      leaf_bv_loc =  dict_buf + read_uint32(dict_buf + 26);
      select_lkup_loc =  dict_buf + read_uint32(dict_buf + 30);
      tail_ptrs_loc = dict_buf + read_uint32(dict_buf + 34);
      trie_loc = dict_buf + read_uint32(dict_buf + 38);

      grp_count = *grp_tails_loc;
      code_lookup_tbl = grp_tails_loc + 1;
      uint8_t *grp_tails_idx_start = code_lookup_tbl + 512;
      for (int i = 0; i < grp_count; i++)
        grp_tails.push_back(dict_buf + read_uint32(grp_tails_idx_start + i * 4));

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
      ret |= (tail[ptr] & 0x3F);
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
      uint8_t ptr8 = node_byte & ((1 << node_val_bits) - 1);
      return (read_extra_ptr(node_id, ptr_bit_count, bit_len) << node_val_bits) | ptr8;
    }

    uint8_t get_first_byte(uint8_t node_byte, uint64_t is_ptr, uint32_t node_id, uint32_t& ptr_bit_count, uint32_t& tail_ptr, uint8_t& grp_no) {
      if (!is_ptr)
        return node_byte;
      tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_tails[grp_no];
      return tail[tail_ptr];
    }

    const int max_tailset_len = 65;
    void get_tail_str(byte_str& ret, uint32_t tail_ptr, uint8_t grp_no) {
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
        uint32_t term_at = read_uint32(term_bv_loc + middle * 7);
        if (term_at < term_count)
          first = middle + 1;
        else if (term_at > term_count)
          last = middle;
        else
          return middle - 1;
      }
      return first;
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

    void scan_block64(uint32_t& node_id, uint32_t term_count, uint32_t target_term_count) {
      uint64_t bm_term;
      uint8_t *t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
      read_uint64(t + 8, bm_term);
      t += 32;
      int i = target_term_count - term_count - 1;
      uint64_t isolated_bit = _pdep_u64(1ULL << i, bm_term);
      size_t bit_pos = __builtin_popcountll(isolated_bit - 1) + 1;
      //size_t bit_pos = _tzcnt_u64(isolated_bit) + 1;
      // size_t bit_pos = find_nth_set_bit(bm_term, i) + 1;
      node_id += bit_pos;
      // term_count = target_term_count;
      // child_count = child_count + __builtin_popcountll(bm_child & ((isolated_bit << 1) - 1));
    }
 
    uint32_t find_child_rank(uint32_t node_id, uint64_t bm_child, uint64_t mask) {
      uint8_t *child_rank_ptr = child_bv_loc + node_id / nodes_per_bv_block * 7;
      uint32_t child_rank = read_uint32(child_rank_ptr);
      int pos = (node_id / nodes_per_bv_block3) % 4;
      if (pos > 0) {
        uint8_t *bv_child3 = child_rank_ptr + 4;
        //while (pos--)
         child_rank += bv_child3[pos - 1];
        // child_rank += get_rank7(bv_child7, pos - 1);
      }
      return child_rank + __builtin_popcountll(bm_child & (mask - 1));
    }

    const int term_divisor = 512;
    const int nodes_per_bv_block = 256;
    const int bytes_per_bv_block = 384;
    const int nodes_per_bv_block3 = 64;
    const int bytes_per_bv_block3 = 96;
    void find_child(uint32_t& node_id, uint32_t& target_term_count) {
      uint32_t child_block;
      uint8_t *select_loc = select_lkup_loc + target_term_count / term_divisor * 4;
      if ((target_term_count % term_divisor) == 0) {
        child_block = read_uint32(select_loc);
      } else {
        uint32_t start_block = read_uint32(select_loc);
        select_loc += 4;
        uint32_t end_block = read_uint32(select_loc);
        if (start_block + 9 >= end_block) {
          do {
            start_block++;
          } while (read_uint32(term_bv_loc + start_block * 7) < target_term_count && start_block <= end_block);
          child_block = start_block - 1;
        } else {
          child_block = bin_srch_bv_term(start_block, end_block, target_term_count);
        }
        // printf("%u,%u,%.0f\t%u,%u,%.0f,%u\n", node_id_block, bv_block_count, ceil(log2(bv_block_count - node_id_block)), start_block, end_block, ceil(log2(end_block - start_block)), child_block);
      }
      // uint32_t node_id_block = node_id / nodes_per_bv_block;
      // child_block = bin_srch_bv_term(node_id_block, bv_block_count, target_term_count);
      uint32_t term_count = read_uint32(term_bv_loc + child_block * 7);
      child_block++;
      do {
        // printf("Child block: %u\n", child_block);
        child_block--;
        term_count = read_uint32(term_bv_loc + child_block * 7);
      } while (term_count >= target_term_count);
      node_id = child_block * nodes_per_bv_block;
      uint8_t *bv3_term = term_bv_loc + child_block * 7 + 4;
      if (term_count + bv3_term[2] < target_term_count) {
        term_count += bv3_term[2];
        node_id += nodes_per_bv_block3 * 3;
      } else {
        if (term_count + bv3_term[1] < target_term_count) {
          term_count += bv3_term[1];
          node_id += nodes_per_bv_block3 * 2;
        } else {
          if (term_count + *bv3_term < target_term_count) {
            term_count += *bv3_term;
            node_id += nodes_per_bv_block3;
          }
        }
      }
      scan_block64(node_id, term_count, target_term_count);
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
        if (key_byte > trie_byte) {
          if (bm_mask & bm_term)
            return ~INSERT_AFTER;
          bm_mask <<= 1;
          continue;
        }
        if (key_byte == trie_byte) {
          int cmp = 0;
          uint32_t tail_len = 1;
          if (bm_mask & bm_ptr) {
            get_tail_str(tail_str, tail_ptr, grp_no);
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
            term_count = child_count;
            find_child(node_id, child_count);
            // t = find_child1(t, node_id, child_count, term_count);
            // read_flags(t - (node_id % nodes_per_bv_block3) - 32, bm_leaf, bm_term, bm_child, bm_ptr);
            // bm_mask = (bm_init_mask << (node_id % nodes_per_bv_block3));
            t = trie_loc + node_id / nodes_per_bv_block3 * bytes_per_bv_block3;
            read_flags(t, bm_leaf, bm_term, bm_child, bm_ptr);
            bm_mask = (bm_init_mask << (node_id % nodes_per_bv_block3));
            child_count = find_child_rank(node_id, bm_child, bm_mask);
            t += (bm_mask == bm_init_mask ? 0 : 32);
            t += node_id % nodes_per_bv_block3;
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
