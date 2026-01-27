#ifndef LEOPARD_H
#define LEOPARD_H

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
#ifndef _WIN32
#include <sys/mman.h>
#endif

#include "madras/dv1/ds_common/bv.hpp"
#include "madras/dv1/ds_common/vint.hpp"
#include "madras/dv1/ds_common/byte_blocks.hpp"

namespace madras { namespace dv1 { namespace memtrie {

#ifndef UINTXX_WIDTH
#define UINTXX_WIDTH 32
#endif

#if UINTXX_WIDTH == 32
#define uintxx_t uint32_t
#define PRIuXX PRIu32
#define UINTXX_MAX UINT32_MAX
#else
#define uintxx_t uint64_t
#define PRIuXX PRIu64
#define UINTXX_MAX UINT64_MAX
#endif

#if defined(__GNUC__) || defined(__clang__)
#define PACKED_STRUCT __attribute__((packed))
#elif defined(_MSC_VER)
#define PACKED_STRUCT
#pragma pack(push, 1)
#else
#define PACKED_STRUCT
#endif

#define LPD_FIND_FOUND 0
#define LPD_INSERT_AFTER -2
#define LPD_INSERT_BEFORE -3
#define LPD_INSERT_LEAF -4
#define LPD_INSERT_EMPTY -5
#define LPD_INSERT_CHILDREN -6
#define LPD_INSERT_CHILD_LEAF -8

typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uint8_t *> byte_ptr_vec;

#define NFLAG_LEAF 1
#define NFLAG_CHILD 2
#define NFLAG_TAIL 4
#define NFLAG_TERM 8
#define NFLAG_NULL 16
#define NFLAG_EMPTY 32

#define NODE_SET_LEAP 64
#define NODE_SET_LEAP2 128
#define NODE_SET_SORTED 128
#define NFLAG_MARK 128

// TODO: Revisit
#define LPDU_BIN 0x20
#define LPDU_NULL 0x40
#define LPDU_EMPTY 0x80

class node_set_vars {
  public:
    int key_pos;
    int cmp;
    int8_t find_state;
    uint8_t cur_node_idx;
    uint16_t level;
    uintxx_t node_set_pos;
    node_set_vars() {
      memset(this, '\0', sizeof(*this));
    }
};

typedef struct PACKED_STRUCT {
  uint8_t flags;
  uint8_t last_node_idx;
  union {
    uintxx_t alloc_len;
    uintxx_t freq;
    uintxx_t count;
  };
  union {
    uintxx_t swap_pos;
    uintxx_t node_id;
  };
} node_set_header;

typedef struct PACKED_STRUCT {
  uintxx_t col_val;
  uintxx_t tail;
  uintxx_t child;
  uint8_t b;
  uint8_t flags;
} node_s;

#if UINTXX_WIDTH == 32
#define node_size 14
static uint8_t leap_node[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\x40'};
#else
#define node_size 26
static uint8_t leap_node[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\x40'};
#endif
class node {
  public:
    node_s *ns;
    node() {
      ns = NULL;
    }
    node_s *get_node_struct() {
      return ns;
    }
    void operator=(uint8_t *_n) {
      ns = (node_s *) _n;
    }
    bool operator==(const uint8_t *_n) {
      return _n == (uint8_t *) ns;
    }
    bool operator!=(const uint8_t *_n) {
      return _n != (uint8_t *) ns;
    }
    uint8_t get_byte() {
      return ns->b;
    }
    uint8_t get_flags() {
      return ns->flags;
    }
    uintxx_t get_tail() {
      return ns->tail;
    }
    uintxx_t get_child() {
      return ns->child;
    }
    uintxx_t get_col_val() {
      return ns->col_val;
    }
    void set_byte(uint8_t b) {
      ns->b = b;
    }
    void set_flags(uint8_t flags) {
      ns->flags = flags;
    }
    void set_tail(uintxx_t pos) {
      ns->tail = pos;
    }
    void set_child(uintxx_t pos) {
      ns->child = pos;
    }
    void set_col_val(uintxx_t pos) {
      ns->col_val = pos;
    }
    void set_values(uint8_t b, uint8_t flags, uintxx_t tail_pos, uintxx_t child_pos, uintxx_t col_val_pos) {
      ns->b = b;
      ns->flags = flags;
      ns->tail = tail_pos;
      ns->child = child_pos;
      ns->col_val = col_val_pos;
    }
    uint8_t next() {
      if (ns->flags & NFLAG_TERM) {
        ns = nullptr;
        return 0;
      }
      ns++;
      return ns->b;
    }
};

class node_set_handler {
  private:
    size_t vec_pos;
    uint8_t *node_set;
    byte_ptr_vec& node_set_vec;
    node_set_header *ns_hdr;
  public:
    node_set_handler(byte_ptr_vec& _node_set_vec, int _vec_pos)
        : node_set_vec (_node_set_vec), vec_pos (_vec_pos) {
      set_pos(vec_pos);
    }
    void set_pos(int _vec_pos) {
      vec_pos = _vec_pos;
      node_set = node_set_vec.at(vec_pos);
      ns_hdr = (node_set_header *) node_set;
    }
    node first_node() {
      node n;
      n = node_set + sizeof(node_set_header);
      return n;
    }
    node operator[](int idx) {
      node n;
      if (idx > ns_hdr->last_node_idx)
        return n;
      n = node_set + sizeof(node_set_header) + idx * node_size;
      return n;
    }
    int get_ns_len() {
      return sizeof(node_set_header) + node_size * ns_hdr->last_node_idx + node_size;
    }
    node_set_header *hdr() {
      return ns_hdr;
    }
    uint8_t last_node_idx() {
      return ns_hdr->last_node_idx;
    }
    static int create_node_set(byte_ptr_vec& node_set_vec, int node_count) {
      int alloc_len = sizeof(node_set_header) + node_size * node_count;
      uint8_t *new_node_set = new uint8_t[alloc_len];
      int vec_pos = node_set_vec.size();
      node_set_vec.push_back(new_node_set);
      memset(new_node_set, '\0', alloc_len);
      node_set_header *ns_hdr = (node_set_header *) new_node_set;
      ns_hdr->alloc_len = alloc_len;
      ns_hdr->last_node_idx = node_count - 1;
      return vec_pos;
    }
    void ensure_capacity(node_set_vars& nsv, int addl_len, int& old_node_set_len) {
      old_node_set_len = get_ns_len();
      int remain_len = ns_hdr->alloc_len - get_ns_len();
      if (remain_len >= addl_len)
        return;
      // if (nsv.level < 10)
      //   addl_len *= lpd_level_mul[nsv.level];
      int new_len = old_node_set_len + addl_len;
      uint8_t *new_bytes = new uint8_t[new_len];
      memcpy(new_bytes, node_set, old_node_set_len);
      delete [] node_set;
      node_set_vec[vec_pos] = new_bytes;
      set_pos(vec_pos);
      ns_hdr->alloc_len = new_len;
    }
    uint8_t insert_sibling(node_set_vars& nsv, uint8_t b, uintxx_t tail_pos, uintxx_t tail_len, uintxx_t val_pos) {
      int old_node_set_len;
      uint8_t flags = NFLAG_LEAF | (tail_len > 1 ? NFLAG_TAIL : 0);
      ensure_capacity(nsv, node_size, old_node_set_len);
      uint8_t *new_node_pos = node_set + sizeof(node_set_header) + nsv.cur_node_idx * node_size;
      if (ns_hdr->last_node_idx < nsv.cur_node_idx) {
        node last_node;
        last_node = node_set + sizeof(node_set_header) + ns_hdr->last_node_idx * node_size;
        uint8_t cur_node_flag = last_node.get_flags();
        // if (cur_node_flag & NFLAG_TERM)
          last_node.set_flags(cur_node_flag & ~NFLAG_TERM);
        flags |= NFLAG_TERM;
      }
      ns_hdr->last_node_idx++;
      memmove(new_node_pos + node_size, new_node_pos, (ns_hdr->last_node_idx - nsv.cur_node_idx) * node_size);
      node n;
      n = new_node_pos;
      n.set_values(b, flags, tail_pos, 0, val_pos == UINTXX_MAX ? 0 : val_pos);
      return ns_hdr->last_node_idx;
    }
};

class node_iterator {
  private:
    size_t cur_nsh_idx;
    uint8_t cur_sib_id;
    node cur_node;
    node_set_handler nsh;
    byte_ptr_vec& node_sets;
    uintxx_t node_id;
  public:
    node_iterator(byte_ptr_vec& _node_sets, size_t start_idx = 0) : node_sets (_node_sets), nsh (_node_sets, start_idx) {
      cur_nsh_idx = start_idx;
      cur_sib_id = 0;
      cur_node = nullptr;
      node_id = UINTXX_MAX;
    }
    node next() {
      if (cur_node == nullptr) {
        if (nsh.hdr()->flags & NODE_SET_LEAP)
          cur_node = leap_node;
        else
          cur_node = nsh.first_node();
        cur_sib_id = 0;
      } else {
        if (cur_node == leap_node)
          cur_node = nsh.first_node();
        else {
          cur_node.next();
          cur_sib_id++;
        }
        if (cur_node == nullptr) {
          cur_nsh_idx++;
          cur_sib_id = 0;
          if (cur_nsh_idx < node_sets.size()) {
            nsh.set_pos(cur_nsh_idx);
            if (nsh.hdr()->flags & NODE_SET_LEAP)
              cur_node = leap_node;
            else
              cur_node = nsh.first_node();
          }
        }
      }
      node_id++;
      return cur_node;
    }
    node_set_handler *get_cur_nsh() {
      return &nsh;
    }
    uintxx_t get_cur_nsh_id() {
      return cur_nsh_idx;
    }
    uint8_t get_cur_sib_id() {
      return cur_sib_id;
    }
    uintxx_t get_node_id() {
      return node_id;
    }
    node_set_header *hdr() {
      return nsh.hdr();
    }
    uint8_t last_node_idx() {
      return nsh.hdr()->last_node_idx;
    }
};

class in_mem_trie {
  private:

  public:
    gen::byte_blocks *all_tails;
    byte_vec prev_key;
    uintxx_t node_count;
    uintxx_t node_set_count;
    uintxx_t key_count;
    uintxx_t max_key_len;
    uintxx_t max_tail_len;
    byte_ptr_vec all_node_sets;
    uint8_t min_loc[65536];
    uint8_t null_value[10];
    size_t null_value_len;
    uint8_t empty_value[10];
    size_t empty_value_len;
    in_mem_trie(const uint8_t *_null_value = (const uint8_t *) " ", size_t _null_value_len = 1, const uint8_t *_empty_value = (const uint8_t *) "!", size_t _empty_value_len = 1) {
      memcpy(null_value, _null_value, _null_value_len);
      null_value_len = _null_value_len;
      memcpy(empty_value, _empty_value, _empty_value_len);
      empty_value_len = _empty_value_len;
      all_tails = new gen::byte_blocks();
      node_set_handler::create_node_set(all_node_sets, 1);
      node_set_handler nsh(all_node_sets, 0);
      node n = nsh.first_node();
      n.set_byte(0);
      n.set_child(1); // The root node is just a placeholder not used in lookup,
      n.set_flags(NFLAG_TERM | NFLAG_CHILD); // so these settings don't mean much
      node_count = 1;
      node_set_count = 1;
      key_count = max_key_len = max_tail_len = 0;
      memset(min_loc, 0, 65536);
    }

    virtual ~in_mem_trie() {
      if (all_tails != NULL)
        delete all_tails;
      for (size_t i = 0; i < all_node_sets.size(); i++)
        delete [] all_node_sets[i];
    }

    uint8_t *operator[](int arr_pos) {
      return all_node_sets[arr_pos];
    }

    uintxx_t append_tail(gen::byte_blocks& vec, const uint8_t *byte_str, int byte_str_len) {
      return vec.push_back_with_vlen(byte_str, byte_str_len);
    }

    void set_print_enabled(bool to_print_messages = true) {
      gen::is_gen_print_enabled = to_print_messages;
    }

    void set_first_node(const uint8_t *key, int key_len, uintxx_t val_pos = UINTXX_MAX) {
      node_set_handler::create_node_set(all_node_sets, 1);
      uintxx_t tail_pos = append_tail(*all_tails, key, key_len);
      node_set_handler ns(all_node_sets, 1);
      node n = ns[0];
      n.set_values(key[0], NFLAG_LEAF | NFLAG_TERM | (key_len > 1 ? NFLAG_TAIL : 0), tail_pos, 0, val_pos == UINTXX_MAX ? 0 : val_pos);
      node_count++;
      node_set_count++;
    }

    bool lookup(const uint8_t *key, int key_len, node_set_vars& nsv) {
      if (all_node_sets.size() == 1) {
        nsv.find_state = LPD_INSERT_EMPTY;
        return false;
      }
      if (key == NULL) {
        key = null_value;
        key_len = null_value_len;
      }
      if (key_len == 0) {
        key = empty_value;
        key_len = empty_value_len;
      }
      nsv.level = 0;
      nsv.key_pos = 0;
      nsv.node_set_pos = 1;
      node_set_handler nsh(all_node_sets, 1);
      uint8_t key_byte = key[nsv.key_pos];
      int min_loc_offset = nsh.last_node_idx() * 256 + key_byte;
      uint8_t min_pos = min_loc[min_loc_offset];
      node n = nsh[min_pos];
      while (n.get_byte() > key_byte && min_pos > 0) {
        n = nsh[--min_pos];
        min_loc[min_loc_offset]--;
      }
      nsv.cur_node_idx = min_pos;
      do {
        uint8_t trie_byte = n.get_byte();
        if (key_byte > trie_byte) {
          nsv.cur_node_idx++;
          trie_byte = n.next();
          if (n == NULL) {
            nsv.find_state = LPD_INSERT_AFTER;
            return false;
          }
          continue;
        }
        if (key_byte == trie_byte) {
          int tail_len = 1;
          uint8_t flags = n.get_flags();
          nsv.cmp = 0;
          if (flags & NFLAG_TAIL) {
            size_t vlen;
            uint8_t *tail = (*all_tails)[n.get_tail()];
            tail_len = gen::read_vint32(tail, &vlen);
            tail += vlen;
            nsv.cmp = gen::compare(tail, tail_len, key + nsv.key_pos, key_len - nsv.key_pos);
          }
          if (nsv.cmp == 0 && nsv.key_pos + tail_len == key_len && (flags & NFLAG_LEAF)) {
            nsv.find_state = LPD_FIND_FOUND;
            return true;
          }
          if (nsv.cmp == 0 || abs(nsv.cmp) - 1 == tail_len) {
            nsv.key_pos += tail_len;
            if (nsv.key_pos >= key_len) {
              nsv.find_state = LPD_INSERT_LEAF;
              return false;
            }
            if ((flags & NFLAG_CHILD) == 0) {
              nsv.find_state = LPD_INSERT_CHILD_LEAF;
              return false;
            }
            nsv.node_set_pos = n.get_child();
            nsv.level++;
            nsh.set_pos(nsv.node_set_pos);
            key_byte = key[nsv.key_pos];
            min_loc_offset = nsh.last_node_idx() * 256 + key_byte;
            min_pos = min_loc[min_loc_offset];
            n = nsh[min_pos];
            while (n.get_byte() > key_byte && min_pos > 0) {
              n = nsh[--min_pos];
              min_loc[min_loc_offset]--;
            }
            nsv.cur_node_idx = min_pos;
            continue;
          }
          if (abs(nsv.cmp) - 1 == key_len - nsv.key_pos) {
            nsv.find_state = LPD_INSERT_CHILD_LEAF;
            return false;
          }
          nsv.find_state = LPD_INSERT_CHILDREN;
          return false;
        }
        nsv.find_state = LPD_INSERT_BEFORE;
        return false;
      } while (n != NULL);
      nsv.find_state = LPD_INSERT_AFTER;
      return false;
    }

    node_set_vars insert(const uint8_t *key, int key_len, uintxx_t val_pos = UINTXX_MAX) {
      if (key == NULL || key_len == 0) {
        key_len = (key == NULL ? null_value_len : empty_value_len);
        key = (key == NULL ? null_value : empty_value);
      }
      node_set_vars nsv;
      if (node_count == 1) {
        key_count++;
        max_key_len = key_len;
        set_first_node(key, key_len, val_pos);
        nsv.find_state = LPD_INSERT_EMPTY;
        nsv.node_set_pos = 1;
        nsv.cur_node_idx = 0;
        return nsv;
      }
      bool is_found = lookup(key, key_len, nsv);
      if (is_found) {
        node_set_handler nsh(all_node_sets, nsv.node_set_pos);
        node n = nsh[nsv.cur_node_idx];
        if (val_pos != UINTXX_MAX)
          n.set_col_val(val_pos);
        return nsv;
      }
      key_count++;
      if (max_key_len < key_len)
        max_key_len = key_len;
      if (nsv.find_state == LPD_INSERT_CHILDREN) {
        if (nsv.cmp != 0)
          nsv.key_pos += (abs(nsv.cmp) - 1);
      }
      uintxx_t tail_pos = 0;
      uintxx_t tail_len = key_len - nsv.key_pos;
      if (tail_len > 1)
        tail_pos = append_tail(*all_tails, key + nsv.key_pos, tail_len);
      node_set_handler nsh(all_node_sets, nsv.node_set_pos);
      switch (nsv.find_state) {
        case LPD_INSERT_BEFORE:
        case LPD_INSERT_AFTER: {
          uint8_t b = key[nsv.key_pos];
          uint8_t new_len = nsh.insert_sibling(nsv, b, tail_pos, tail_len, val_pos);
          int min_loc_offset = new_len * 256 + b;
          if (min_loc[min_loc_offset] == 0 || min_loc[min_loc_offset] > nsv.cur_node_idx)
            min_loc[min_loc_offset] = nsv.cur_node_idx;
          node_count++;
          } break;
        case LPD_INSERT_LEAF: {
          node n = nsh[nsv.cur_node_idx];
          n.set_flags(n.get_flags() | NFLAG_LEAF);
          if (val_pos != UINTXX_MAX)
            n.set_col_val(val_pos);
        } break;
        case LPD_INSERT_CHILDREN:
          add_children(nsh, nsv, key, key_len, tail_pos, tail_len, val_pos);
          node_set_count++;
          node_count += 2;
          break;
        case LPD_INSERT_CHILD_LEAF:
          add_children(nsh, nsv, key, key_len, tail_pos, tail_len, val_pos);
          node_set_count++;
          node_count++;
          break;
      }
      return nsv;
    }

    void add_children(node_set_handler& nsh, node_set_vars& nsv, const uint8_t *key, int key_len, uintxx_t tail_pos, uintxx_t tail_len, uintxx_t val_pos) {
      bool swap = (nsv.cmp > 0);
      if (nsv.cmp != 0)
        nsv.cmp = abs(nsv.cmp) - 1;
      uintxx_t child_at = nsv.cmp;
      uintxx_t new_ns_pos = node_set_handler::create_node_set(all_node_sets, (nsv.find_state == LPD_INSERT_CHILD_LEAF ? 1 : 2));
      node last_child = nsh[nsv.cur_node_idx];
      size_t vlen;
      uintxx_t last_child_tail_len = gen::read_vint32((*all_tails)[last_child.get_tail()], &vlen);
      uint8_t last_child_flags = last_child.get_flags();
      uintxx_t last_child_tail_pos = last_child.get_tail();
      uintxx_t last_child_val_pos = last_child.get_col_val();
      uintxx_t last_child_child_pos = last_child.get_child();
      uintxx_t split_len = last_child_tail_len - child_at;
      uint8_t *split_str = (*all_tails)[last_child_tail_pos] + vlen + child_at;
      uintxx_t split_tail_pos = 0;
      // if (split_len == 0 && nsv.find_state != LPD_INSERT_CHILD_LEAF)
      //   std::cout << "WARNING: WARNING: split len is 0 for state: " << (int) nsv.find_state << std::endl;
      if (split_len > 1)
        split_tail_pos = append_tail(*all_tails, split_str, split_len);
      if (child_at == 1) {
        last_child.set_tail(0);
        last_child_flags &= ~NFLAG_TAIL;
      }
      uint8_t split_byte;
      if (nsv.find_state == LPD_INSERT_CHILDREN) {
        split_byte = *split_str;
        last_child.set_flags((last_child_flags | NFLAG_CHILD) & ~NFLAG_LEAF);
        last_child_flags &= ~NFLAG_TAIL;
        last_child.set_child(new_ns_pos);
        last_child.set_col_val(0);
        gen::copy_vint32(child_at, (*all_tails)[last_child_tail_pos], vlen);
        nsh.set_pos(new_ns_pos);
        nsv.node_set_pos = new_ns_pos;
        nsv.cur_node_idx = swap ? 0 : 1;
        node new_child = nsh[nsv.cur_node_idx];
        node split_child = nsh[swap ? 1 : 0];
        new_child.set_values(key[nsv.key_pos], NFLAG_LEAF | (tail_len > 1 ? NFLAG_TAIL : 0), tail_pos, 0, val_pos == UINTXX_MAX ? 0 : val_pos);
        split_child.set_values(split_byte, (last_child_flags & ~NFLAG_TERM) | (split_len > 1 ? NFLAG_TAIL : 0), split_tail_pos, last_child_child_pos, last_child_val_pos);
        node term = nsh[1];
        term.set_flags(term.get_flags() | NFLAG_TERM);
        min_loc[0 * 256 + split_byte] = 0;
        min_loc[0 * 256 + key[nsv.key_pos]] = 0;
        min_loc[1 * 256 + split_byte] = 0;
        min_loc[1 * 256 + key[nsv.key_pos]] = 0;
      } else {
        last_child.set_flags(last_child_flags | NFLAG_CHILD | NFLAG_LEAF);
        last_child_flags &= ~NFLAG_TAIL;
        last_child.set_child(new_ns_pos);
        if (swap && val_pos != UINTXX_MAX)
          last_child.set_col_val(val_pos);
        nsh.set_pos(new_ns_pos);
        node new_child = nsh.first_node();
        if (swap) {
          split_byte = *split_str;
          gen::copy_vint32(child_at, (*all_tails)[last_child_tail_pos], vlen);
          new_child.set_values(split_byte, (last_child_flags | NFLAG_TERM) | (split_len > 1 ? NFLAG_TAIL : 0), split_tail_pos, last_child_child_pos, last_child_val_pos);
          min_loc[0 * 256 + split_byte] = 0;
        } else {
          new_child.set_values(key[nsv.key_pos], (NFLAG_LEAF | NFLAG_TERM) | (tail_len > 1 ? NFLAG_TAIL : 0), tail_pos, 0, val_pos == UINTXX_MAX ? 0 : val_pos);
          min_loc[0 * 256 + key[nsv.key_pos]] = 0;
          nsv.node_set_pos = new_ns_pos;
          nsv.cur_node_idx = 0;
        }
      }
    }

    void split_tails() {
      clock_t t = clock();
      typedef struct {
        uint8_t *part;
        uintxx_t uniq_arr_idx;
        uintxx_t ns_id;
        uint16_t part_len;
        uint8_t node_idx;
      } tail_part;
      typedef struct {
        uint8_t *uniq_part;
        uintxx_t freq_count;
        uint16_t uniq_part_len;
      } tail_uniq_part;
      std::vector<tail_part> tail_parts;
      std::vector<tail_uniq_part> tail_uniq_parts;
      node_iterator ni(all_node_sets, 1);
      node cur_node = ni.next();
      while (cur_node != nullptr) {
        if (cur_node.get_flags() & NFLAG_TAIL) {
          uint8_t *tail = (*all_tails)[cur_node.get_tail()];
          size_t vlen;
          uintxx_t tail_len = gen::read_vint32(tail, &vlen);
          tail += vlen;
          int last_word_len = 0;
          bool is_prev_non_word = false;
          std::vector<tail_part> tail_parts1;
          for (size_t i = 0; i < tail_len; i++) {
            uint8_t c = tail[i];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c > 127) {
              if (last_word_len >= 5 && is_prev_non_word) {
                if (tail_len - i < 5) {
                  last_word_len += (tail_len - i);
                  break;
                }
                if (i > last_word_len)
                  tail_parts1.push_back((tail_part) {tail + i - last_word_len, 0, (uintxx_t) ni.get_cur_nsh_id(), (uint16_t) last_word_len, ni.get_cur_sib_id()});
                last_word_len = 0;
              }
              is_prev_non_word = false;
            } else {
              is_prev_non_word = true;
            }
            last_word_len++;
          }
          if (last_word_len < tail_len)
            tail_parts1.push_back((tail_part) {tail + tail_len - last_word_len, 0, (uintxx_t) ni.get_cur_nsh_id(), (uint16_t) last_word_len, ni.get_cur_sib_id()});
          if (tail_parts1.size() > 0) {
            tail_parts.insert(tail_parts.end(), tail_parts1.begin(), tail_parts1.end());
            // printf("Tail: [%.*s]\n", (int) tail_len, tail);
            // for (size_t i = 0; i < tail_parts1.size(); i++) {
            //   tail_part *tp = &tail_parts1[i];
            //   printf("[%.*s]\n", tp->part_len, tp->part);
            // }
          }
        }
        cur_node = ni.next();
      }
      gen::gen_printf("Parts count: %lu\n", tail_parts.size());
      if (tail_parts.size() == 0)
        return;
      std::sort(tail_parts.begin(), tail_parts.end(), [](const tail_part& lhs, const tail_part& rhs) -> bool {
        return gen::compare(lhs.part, lhs.part_len, rhs.part, rhs.part_len) < 0;
      });
      tail_part *prev_tp = &tail_parts[0];
      uintxx_t freq_count = 0;
      uintxx_t uniq_arr_idx = 0;
      for (size_t i = 0; i < tail_parts.size(); i++) {
        tail_part *tp = &tail_parts[i];
        tp->uniq_arr_idx = uniq_arr_idx;
        int cmp = gen::compare(tp->part, tp->part_len, prev_tp->part, prev_tp->part_len);
        if (cmp == 0) {
          freq_count++;
        } else {
          tail_uniq_parts.push_back((tail_uniq_part) {prev_tp->part, freq_count, prev_tp->part_len});
          uniq_arr_idx++;
          freq_count = 1;
        }
        prev_tp = tp;
      }
      tail_uniq_parts.push_back((tail_uniq_part) {prev_tp->part, freq_count, prev_tp->part_len});
      std::sort(tail_parts.begin(), tail_parts.end(), [](const tail_part& lhs, const tail_part& rhs) -> bool {
        return lhs.part > rhs.part;
      });
      gen::gen_printf("Uniq Parts count: %lu\n", tail_uniq_parts.size());
      // std::sort(tail_uniq_parts.begin(), tail_uniq_parts.end(), [](const tail_uniq_part& lhs, const tail_uniq_part& rhs) -> bool {
      //   return lhs.freq_count > rhs.freq_count;
      // });
      // for (size_t i = 0; i < tail_uniq_parts.size(); i++) {
      //   tail_uniq_part *tup = &tail_uniq_parts[i];
      //   printf("%u\t[%.*s]\n", tup->freq_count, (int) tup->uniq_part_len, tup->uniq_part);
      // }
      node new_node;
      node_set_handler new_nsh(all_node_sets, 0);
      node_set_handler nsh(all_node_sets, 0);
      for (size_t i = 0; i < tail_parts.size(); i++) {
        tail_part *tp = &tail_parts[i];
        tail_uniq_part *utp = &tail_uniq_parts[tp->uniq_arr_idx];
        // if (utp->freq_count > 1) {
          nsh.set_pos(tp->ns_id);
          cur_node = nsh[tp->node_idx];
          uint8_t *tail = (*all_tails)[cur_node.get_tail()];
          size_t vlen;
          uintxx_t tail_len = gen::read_vint32(tail, &vlen);
          uintxx_t orig_tail_new_len = tp->part - tail - vlen;
          uintxx_t new_tail_len = tail_len - orig_tail_new_len;
          gen::copy_vint32(orig_tail_new_len, tail, vlen);
          //printf("Tail: [%.*s], pos: %u, len: %u, new len: %u, Part: [%.*s], len: %u\n", (int) tail_len, tail + vlen, cur_node.get_tail(), tail_len, orig_tail_new_len, (int) tp->part_len, tp->part, new_tail_len);
          size_t new_tail_pos = all_tails->push_back_with_vlen(tp->part, new_tail_len);
          uintxx_t new_ns_pos = node_set_handler::create_node_set(all_node_sets, 1);
          new_nsh.set_pos(new_ns_pos);
          new_node = new_nsh.first_node();
          new_node.set_flags(cur_node.get_flags() | NFLAG_TERM);
          new_node.set_child(cur_node.get_child());
          new_node.set_col_val(cur_node.get_col_val());
          new_node.set_byte(*tp->part);
          //printf("New tail pos: %lu\n", new_tail_pos);
          new_node.set_tail(new_tail_pos);
          cur_node.set_flags((cur_node.get_flags() | NFLAG_CHILD) & ~NFLAG_LEAF);
          cur_node.set_child(new_ns_pos);
          cur_node.set_col_val(0);
          node_set_count++;
          node_count++;
        // }
      }
      gen::gen_printf("New NS count: %lu\n", all_node_sets.size());
      gen::print_time_taken(t, "Time taken for tail split: ");
    }

    size_t size() {
      return key_count;
    }

    size_t get_size_in_bytes() {
      size_t sz = 0;
      node_set_handler cur_ns(all_node_sets, 0);
      for (size_t i = 0; i < all_node_sets.size(); i++) {
        cur_ns.set_pos(i);
        sz += cur_ns.hdr()->alloc_len;
      }
      sz += all_tails->get_size_in_bytes();
      sz += all_node_sets.size() * sizeof(uint8_t *);
      //sz += sizeof(trie);
      return sz;
    }

    void recreate_min_loc() {
      size_t max_len = 0;
      size_t min_len = 0xFF;
      memset(min_loc, 0xFF, 65536);
      node_set_handler cur_ns(all_node_sets, 1);
      for (size_t i = 1; i < all_node_sets.size(); i++) {
        cur_ns.set_pos(i);
        if (min_len > cur_ns.last_node_idx())
          min_len = cur_ns.last_node_idx();
        if (max_len < cur_ns.last_node_idx())
          max_len = cur_ns.last_node_idx();
        node n = cur_ns.first_node();
        for (size_t k = 0; k <= cur_ns.last_node_idx(); k++) {
          size_t min_loc_offset = cur_ns.last_node_idx() * 256 + n.get_byte();
          if (min_loc[min_loc_offset] > k)
            min_loc[min_loc_offset] = k;
          n.next();
        }
      }
      for (size_t i = 0; i < 65536; i++) {
        if (min_loc[i] == 0xFF)
          min_loc[i] = 0;
      }
#ifndef _WIN32
      madvise(min_loc + min_len * 256, (max_len - min_len) * 256, MADV_WILLNEED);
#endif
    }

};

#if defined(_MSC_VER)
#pragma pack(pop)
#endif
#undef PACKED_STRUCT

}}}

#endif
