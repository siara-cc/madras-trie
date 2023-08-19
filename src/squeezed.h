#ifndef STATIC_DICT_H
#define STATIC_DICT_H

#include <stdlib.h>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>

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

class static_dict {

  private:
    uint8_t *dict_buf;
    size_t dict_size;

    uint32_t node_count;
    uint8_t *grp_tails_loc;
    uint8_t *cache_loc;
    uint8_t *ptr_lookup_tbl_loc;
    uint8_t *bit_vectors_loc;
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

    static uint32_t read_uint32(uint8_t *pos) {
      uint32_t ret = 0;
      int i = 4;
      while (i--) {
        ret <<= 8;
        ret += *pos++;
      }
      return ret;
    }

  public:
    static_dict(std::string filename, builder *_sb) {

      sb = _sb;

      struct stat file_stat;
      memset(&file_stat, '\0', sizeof(file_stat));
      stat(filename.c_str(), &file_stat);
      dict_size = file_stat.st_size;
      dict_buf = (uint8_t *) malloc(dict_size);

      FILE *fp = fopen(filename.c_str(), "rb+");
      fread(dict_buf, dict_size, 1, fp);
      fclose(fp);

      grp_tails_loc = dict_buf + 2 + 6 * 4; // 26
      node_count = read_uint32(dict_buf + 2);
      cache_loc = dict_buf + read_uint32(dict_buf + 6);
      ptr_lookup_tbl_loc = dict_buf + read_uint32(dict_buf + 10);
      bit_vectors_loc =  dict_buf + read_uint32(dict_buf + 14);
      tail_ptrs_loc = dict_buf + read_uint32(dict_buf + 18);
      trie_loc = dict_buf + read_uint32(dict_buf + 22);

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

    // flag pos: 1 1 4 4 7 7 10 10 13 13
    // trie pos: 0 2 3 5 6 8  9 11 12 14
    // i:        0 1 2 3 4 5  6  7  8  9
    uint32_t read_extra_ptr(uint32_t node_id, uint32_t& ptr_bit_count, int8_t bit_len) {
      if (ptr_bit_count == 0xFFFFFFFF) {
        uint32_t block_count = node_id / 42;
        ptr_bit_count = block_count ? read_uint32(ptr_lookup_tbl_loc + (block_count - 1) * 4) : 0;
        uint8_t *t = trie_loc + block_count * 63;
        int upto_node_id = node_id % 42;
        uint8_t flags, node_byte;
        for (int i = 0; i < upto_node_id; i++) {
          if (i % 2) {
            flags = (*t++ & 0x0F);
            node_byte = *t++;
          } else {
            node_byte = *t++;
            flags = (*t >> 4);
          }
          ptr_bit_count += (flags & TRIE_FLAGS_PTR ? code_lookup_tbl[node_byte * 2 + 1] : 0);
        }
      }
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

    uint32_t get_tail_ptr(uint8_t node_byte, uint32_t node_id, uint32_t& ptr_bit_count, uint8_t& grp_no, bool str = false) {
      uint8_t *lookup_tbl_ptr = code_lookup_tbl + node_byte * 2;
      grp_no = *lookup_tbl_ptr & 0x1F;
      uint8_t code_len = *lookup_tbl_ptr++ >> 5;
      uint8_t bit_len = *lookup_tbl_ptr;
      uint8_t node_val_bits = 8 - code_len;
      uint32_t ptr = node_byte & ((1 << node_val_bits) - 1);
      ptr_bit_count -= (str ? bit_len : 0);
      ptr |= (read_extra_ptr(node_id, ptr_bit_count, bit_len) << node_val_bits);
      return ptr;
    }

    uint8_t get_first_char(uint8_t node_byte, uint8_t flags, uint32_t node_id, uint32_t& ptr_bit_count) {
      if ((flags & TRIE_FLAGS_PTR) == 0)
        return node_byte;
      uint8_t grp_no;
      uint32_t ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
      uint8_t *tail = grp_tails[grp_no];
      return tail[ptr];
    }

    std::string get_tail_str(uint8_t node_byte, uint8_t flags, uint32_t node_id, uint32_t ptr_bit_count) {
      uint8_t grp_no;
      uint32_t tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no, true);
      uint32_t ptr = tail_ptr;
      uint8_t *tail = grp_tails[grp_no];
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

    int bin_srch_bv_term(uint32_t first, uint32_t size, uint32_t term_count) {
      uint32_t middle = (first + size) >> 4;
      while (first < size) {
        uint32_t term_at = read_uint32(bit_vectors_loc + middle * 22);
        if (term_at < term_count)
          first = middle + 1;
        else if (term_at > term_count)
          size = middle;
        else
          return middle;
        middle = (first + size) >> 1;
      }
      return size;
    }

    const int nodes_per_bv_block = 336;
    uint8_t *find_child(uint8_t *t, uint32_t& node_id, uint32_t& child_count, uint32_t& term_count) {
      uint32_t target_term_count = child_count;
      uint32_t first_term_count = read_uint32(bit_vectors_loc);
      uint32_t child_block = bin_srch_bv_term(node_id / nodes_per_bv_block, node_count / nodes_per_bv_block, target_term_count);
      child_block++;
      do {
        child_block--;
        term_count = read_uint32(bit_vectors_loc + child_block * 22);
        child_count = read_uint32(bit_vectors_loc + child_block * 22 + 4);
        node_id = child_block * nodes_per_bv_block;
        t = trie_loc + child_block * 8 * 63;
      } while (term_count >= target_term_count);
      uint8_t *bv7_term = bit_vectors_loc + child_block * 22 + 8;
      uint8_t *bv7_child = bit_vectors_loc + child_block * 22 + 15;
      for (int pos7 = 0; pos7 < 7 && node_id + 42 < node_count; pos7++) {
        uint8_t term7 = bv7_term[pos7];
        if (term_count + term7 < target_term_count) {
          term_count += term7;
          child_count += bv7_child[pos7];
          node_id += 42;
          t += 63;
        } else
          break;
      }
      while (term_count < target_term_count) {
        uint8_t flags;
        if (node_id % 2) {
          flags = (*t++ & 0x0F);
          t++;
        } else {
          t++;
          flags = (*t >> 4);
        }
        node_id++;
        if (flags & TRIE_FLAGS_CHILD)
          child_count++;
        if (flags & TRIE_FLAGS_TERM)
          term_count++;
        if (term_count == target_term_count)
          break;
      }
      return t;
    }

    static const char TRIE_FLAGS_LEAF = 0x01;
    static const char TRIE_FLAGS_CHILD = 0x02;
    static const char TRIE_FLAGS_PTR = 0x04;
    static const char TRIE_FLAGS_TERM = 0x08;
    int lookup(std::string key) {
      int key_pos = 0;
      uint32_t node_id = 0;
      uint8_t key_byte = key[key_pos];
      uint8_t *t = trie_loc;
      uint8_t node_byte;
      uint8_t flags;
      uint8_t trie_byte;
      uint32_t ptr_bit_count = 0;
      uint32_t term_count = 0;
      uint32_t child_count = 0;
      while (node_id < node_count) {
        if (node_id % 2) {
          flags = (*t++ & 0x0F);
          node_byte = *t++;
        } else {
          node_byte = *t++;
          flags = (*t >> 4);
        }
        trie_byte = get_first_char(node_byte, flags, node_id, ptr_bit_count);
        node_id++;
        if (flags & TRIE_FLAGS_CHILD)
          child_count++;
        if (flags & TRIE_FLAGS_TERM)
          term_count++;
        if (key_byte > trie_byte) {
          if (flags & TRIE_FLAGS_TERM)
            return ~INSERT_AFTER;
          continue;
        }
        if (key_byte == trie_byte) {
          int cmp;
          uint32_t tail_len = 1;
          if (flags & TRIE_FLAGS_PTR) {
            std::string tail_str = get_tail_str(node_byte, flags, node_id - 1, ptr_bit_count);
            tail_len = tail_str.length();
            cmp = compare((const uint8_t *) tail_str.c_str(), tail_len,
                    (const uint8_t *) key.c_str() + key_pos, key.size() - key_pos);
            // printf("%d\t%d\t%.*s =========== ", cmp, tail_len, tail_len, tail_data);
            // printf("%d\t%.*s\n", (int) key.size() - key_pos, (int) key.size() - key_pos, key.data() + key_pos);
          } else
            cmp = 0;
          if (cmp == 0 && key_pos + tail_len == key.size() && (flags & TRIE_FLAGS_LEAF))
            return 0;
          if (cmp == 0 || abs(cmp) - 1 == tail_len) {
            key_pos += tail_len;
            if ((flags & TRIE_FLAGS_CHILD) == 0)
              return ~INSERT_LEAF;
            t = find_child(t, node_id, child_count, term_count);
            if (key_pos >= key.size())
              return ~INSERT_THREAD;
            key_byte = key[key_pos];
            ptr_bit_count = 0xFFFFFFFF;
            continue;
          }
          return ~INSERT_THREAD;
        }
        return ~INSERT_BEFORE;
      }
      return ~INSERT_EMPTY;
    }

    uint8_t make_flags(node *cur_node) {
      return (cur_node->is_leaf ? 1 : 0) +
            (cur_node->first_child != NULL ? 2 : 0) + (cur_node->tail_len > 1 ? 4 : 0) +
            (cur_node->next_sibling == NULL ? 8 : 0);
    }

    void dump_tail_ptrs() {
      uint32_t node_id = 0;
      uint32_t ptr_bit_count = 0xFFFFFFFF;
      uint8_t flags, node_byte, grp_no;
      uint8_t *t = trie_loc;
      uint32_t i = 0, j = 0;
      printf("%u,%u,%u,%u,%u,%u\n", node_count, cache_loc-dict_buf, ptr_lookup_tbl_loc-dict_buf, bit_vectors_loc-dict_buf, tail_ptrs_loc-dict_buf, trie_loc-dict_buf);
      for (; node_id < node_count; node_id++) {
        node *cur_node = sb->get_node(i, j);
        if (node_id % 2) {
          flags = (*t++ & 0x0F);
          node_byte = *t++;
        } else {
          node_byte = *t++;
          flags = (*t >> 4);
        }
        // printf("id:%u,%u\tnb:%u,%u\tfl:%u,%u\tptr:%u,\n", node_id, cur_node->node_id, node_byte, sb->get_first_char(cur_node), flags, make_flags(cur_node), sb->get_ptr(cur_node));
        if (flags & TRIE_FLAGS_PTR) {
          uniq_tails_info *ti = sb->get_ti(cur_node);
          // std::string s = get_tail_str(node_byte, flags, node_id, ptr_bit_count);
          // // std::cout << s << std::endl;
          // std::string s1((const char *) sb->sort_tails.data() + cur_node->tail_pos, cur_node->tail_len);
          // if (s.compare(s1) != 0)
          //   std::cout << "[Mismatch: (" << s << "), (" << s1 << ")" << std::endl;
          uint32_t tail_ptr = get_tail_ptr(node_byte, node_id, ptr_bit_count, grp_no);
          // ptr_bit_count = 0xFFFFFFFF;
          if (tail_ptr != ti->tail_ptr)
            printf("Mismatch: %u\n", node_id);
          // printf("%u\t%u,%u\n", tail_ptr, ti->grp_no - 1, grp_no);
        }
        if ((node_id % 100000) == 0) {
          cout << ".";
          cout.flush();
        }
      }
      std::cout << "Completed" << std::endl;
    }

};

}
#endif
