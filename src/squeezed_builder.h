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

const uint8_t UTI_FLAG_SUFFIX_FULL = 0x01;
const uint8_t UTI_FLAG_SUFFIX_PARTIAL = 0x02;
const uint8_t UTI_FLAG_SUFFIXES = 0x03;
const uint8_t UTI_FLAG_HAS_SUFFIX = 0x04;
struct uniq_tails_info {
    uint32_t tail_pos;
    uint32_t tail_len;
    uint32_t fwd_pos;
    uint32_t cmp_fwd;
    uint32_t rev_pos;
    uint32_t cmp_rev_min;
    uint32_t cmp_rev_max;
    uint32_t freq_count;
    uint32_t tail_ptr;
    uint32_t avg_nid;
      uint32_t link_rev_idx;
      uint32_t node_id;
    uint32_t token_arr_pos;
    uint8_t grp_no;
    uint8_t flags;
    uniq_tails_info(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _rev_pos, uint32_t _freq_count) {
      tail_pos = _tail_pos; tail_len = _tail_len;
      rev_pos = _rev_pos;
      cmp_fwd = 0;
      cmp_rev_min = 0xFFFFFFFF;
      cmp_rev_max = 0;
      freq_count = _freq_count;
      link_rev_idx = 0xFFFFFFFF;
      token_arr_pos = 0xFFFFFFFF;
      tail_ptr = 0;
      avg_nid = 0;
      grp_no = 0;
      flags = 0;
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
};

class byte_block {
  private:
    std::vector<uint8_t *> blocks;
    bit_vector is_allocated;
    size_t block_remaining;
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
    const static size_t block_size = 256;
    byte_block() {
      block_remaining = 0;
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

typedef std::map<std::string, uint64_t> tail_map;
typedef std::multimap<uint32_t, std::string> tail_freq_map;
typedef std::map<uint32_t, std::string> tail_link_map;
typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uniq_tails_info *> uniq_tails_info_vec;

struct node_cache {
  uint32_t child;
  uint32_t child_ptr_bits;
  uint8_t grp_no;
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

static const int NFLAG_LEAF = 1;
static const int NFLAG_TERM = 2;
static const int NFLAG_NODEID_SET = 4;
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
  union {
    uint32_t swap_pos;
    uint32_t v0;
  };
  node() {
    memset(this, '\0', sizeof(node));
  }
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
        do {
          node *n = swap_nodes(nxt_n, node_id);
          nxt_n = n->next_sibling;
          n->flags |= (nxt_n == 0 ? NFLAG_TERM : 0);
          n->node_id = node_id++;
        } while (nxt_n != 0);
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
    byte_vec uniq_tails;
    uniq_tails_info_vec uniq_tails_rev;
    std::vector<freq_grp> freq_grp_vec;
    std::vector<byte_vec> grp_tails;
    byte_vec tail_ptrs;
    byte_vec two_byte_tails;
    byte_vec idx2_ptrs_map;
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

    void append(std::string key) {
      if (key == prev_key)
         return;
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
              node new_node = node();
              uint32_t new_node_pos = all_nodes.size();
              new_node.flags |= NFLAG_LEAF;
              //new_node->parent = last_child->parent;
              new_node.next_sibling = 0;
              //if (new_node->val == string(" discuss"))
              //  cout << "New node: " << key << endl;
              last_child->next_sibling = new_node_pos;
              all_nodes.push_back(new_node);
              append_tail_vec(key.substr(key_pos), new_node_pos);
              last_children[level] = new_node_pos;
              last_children.resize(level + 1);
              node_count++;
            } else {
              node child1 = node();
              node child2 = node();
              uint32_t child1_pos = all_nodes.size();
              uint32_t child2_pos = child1_pos + 1;
              child1.flags |= (last_child->flags & NFLAG_LEAF);
              //child1.parent = last_child;
              child1.next_sibling = child2_pos;
              child1.tail_pos = last_child->tail_pos + i;
              child1.tail_len = last_child->tail_len - i;
              //if (child1->val == string(" discuss"))
              //  cout << "Child1 node: " << key << endl;
              if (last_child->first_child != 0) {
                child1.first_child = last_child->first_child;
              }
              last_children.resize(level + 1);
              last_children.push_back(child2_pos);
              child2.flags |= NFLAG_LEAF;
              //child2->parent = last_child;
              child2.next_sibling = 0;
              //if (child2->val == string(" discuss"))
              //  cout << "Child2 node: " << key << endl;
              last_child->first_child = child1_pos;
              last_child->flags |= NFLAG_LEAF;
              last_child->tail_len = i; // do these before push_back all_nodes
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
          node child1 = node();
          uint32_t child1_pos = all_nodes.size();
          child1.flags |= NFLAG_LEAF;
          //child1->parent = last_child;
          child1.next_sibling = 0;
          //    if (child1->val == string(" discuss"))
          //      cout << "Ext node: " << key << endl;
          last_child->first_child = child1_pos;
          all_nodes.push_back(child1);
          append_tail_vec(key.substr(key_pos), child1_pos);
          last_children.resize(level + 1);
          last_children.push_back(child1_pos);
          node_count++;
          return;
        }
        level++;
        last_child = &all_nodes[last_children[level]];
      } while (last_child != 0);
    }

    struct tails_sort_data {
      uint8_t *tail_data;
      uint32_t tail_len;
      uint32_t n;
    };

    uint32_t make_uniq_tails(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_rev) {
      clock_t t = clock();
      std::vector<tails_sort_data> nodes_for_sort;
      for (uint32_t i = i; i < all_nodes.size(); i++) {
        node *n = &all_nodes[i];
        uint8_t *v = sort_tails[n->tail_pos];
        n->v0 = v[0];
        if (n->tail_len > 1)
          nodes_for_sort.push_back((struct tails_sort_data) { v, n->tail_len, i } );
      }
      t = gen::print_time_taken(t, "Time taken for adding to nodes_for_sort: ");
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this](const struct tails_sort_data& lhs, const struct tails_sort_data& rhs) -> bool {
        return gen::compare_rev(lhs.tail_data, lhs.tail_len, rhs.tail_data, rhs.tail_len) < 0;
      });
      t = gen::print_time_taken(t, "Time taken to sort: ");
      uint32_t vec_pos = 0;
      uint32_t freq_count = 0;
      uint32_t tot_freq_count = 0;
      std::vector<tails_sort_data>::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->tail_data;
      uint32_t prev_val_len = it->tail_len;
      uniq_tails_info *ti_ptr = new uniq_tails_info((uint32_t) uniq_tails.size(), prev_val_len, vec_pos, freq_count);
      uniq_tails_info *prev_ti_ptr = ti_ptr;
      while (it != nodes_for_sort.end()) {
        int cmp = gen::compare_rev(it->tail_data, it->tail_len, prev_val, prev_val_len);
        if (cmp == 0) {
          freq_count++;
        } else {
          ti_ptr->freq_count = freq_count;
          if (ti_ptr->freq_count == 1)
            ti_ptr->node_id = (it - 1)->n;
          tot_freq_count += freq_count;
          uniq_tails_rev.push_back(ti_ptr);
          for (int i = 0; i < prev_val_len; i++)
            uniq_tails.push_back(prev_val[i]);
          freq_count = 1;
          prev_val = it->tail_data;
          prev_val_len = it->tail_len;
          prev_ti_ptr = ti_ptr;
          ti_ptr = new uniq_tails_info((uint32_t) uniq_tails.size(), prev_val_len, ++vec_pos, freq_count);
        }
        all_nodes[it->n].rev_node_info_pos = vec_pos;
        it++;
      }
      ti_ptr->freq_count = freq_count;
      tot_freq_count += freq_count;
      uniq_tails_rev.push_back(ti_ptr);
      for (int i = 0; i < prev_val_len; i++)
        uniq_tails.push_back(prev_val[i]);
      t = gen::print_time_taken(t, "Time taken for uniq_tails_rev: ");
      sort_tails.release_blocks();

      return tot_freq_count;

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

    uint32_t check_next_grp(std::vector<freq_grp>& freq_grp_vec, uint32_t& grp_no, uint32_t cur_limit, uint32_t len) {
      if ((freq_grp_vec[grp_no].grp_size + len) >= (cur_limit - 1)) {
        cur_limit = pow(2, log2(cur_limit) + 3);
        grp_no++;
        freq_grp_vec.push_back((freq_grp) {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0});
      }
      return cur_limit;
    }

    void update_current_grp(std::vector<freq_grp>& freq_grp_vec, uint32_t grp_no, int32_t len, int32_t freq) {
      freq_grp_vec[grp_no].grp_size += len;
      freq_grp_vec[grp_no].freq_count += freq;
      freq_grp_vec[grp_no].count += (len < 0 ? -1 : 1);
    }

    uint32_t append_to_grp_tails(byte_vec& uniq_tails, std::vector<byte_vec>& grp_tails,
              uniq_tails_info *ti, uint32_t grp_no, uint32_t len = 0) {
      grp_no--;
      while (grp_no >= grp_tails.size()) {
        byte_vec tail;
        tail.push_back(0);
        grp_tails.push_back(tail);
      }
      uint32_t ptr = grp_tails[grp_no].size();
      if (len == 0)
        len = ti->tail_len;
      for (int k = 0; k < len; k++)
        grp_tails[grp_no].push_back(uniq_tails[ti->tail_pos + k]);
      if (len == ti->tail_len)
        grp_tails[grp_no].push_back(0);
      return ptr;
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

    const static uint32_t suffix_grp_limit = 3;
    void build_tail_maps(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd, uniq_tails_info_vec& uniq_tails_rev,
                 std::vector<freq_grp>& freq_grp_vec, std::vector<byte_vec>& grp_tails, byte_vec& tail_ptrs) {

      uint32_t tot_freq_count = make_uniq_tails(uniq_tails, uniq_tails_rev);

      clock_t t = clock();
      uniq_tails_fwd = uniq_tails_rev;
      std::sort(uniq_tails_fwd.begin(), uniq_tails_fwd.end(), [uniq_tails](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return gen::compare(uniq_tails.data() + lhs->tail_pos, lhs->tail_len, uniq_tails.data() + rhs->tail_pos, rhs->tail_len) < 0;
      });
      for (int i = 0; i < uniq_tails_fwd.size(); i++)
        uniq_tails_fwd[i]->fwd_pos = i;
      t = gen::print_time_taken(t, "Time taken for uniq_tails fwd sort: ");

      //uint32_t log10_max_freq_count = ceil(log10(max_freq_count));
      uniq_tails_info_vec uniq_tails_freq = uniq_tails_rev;
      std::sort(uniq_tails_freq.begin(), uniq_tails_freq.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / lhs->tail_len;
        uint32_t rhs_freq = rhs->freq_count / rhs->tail_len;
        lhs_freq = ceil(log10(lhs_freq));
        rhs_freq = ceil(log10(rhs_freq));
        return (lhs_freq == rhs_freq) ? (lhs->rev_pos > rhs->rev_pos) : (lhs_freq > rhs_freq);
      });
      t = gen::print_time_taken(t, "Time taken for uniq_tails freq: ");

      make_two_byte_tails(uniq_tails_freq, tot_freq_count);

      uint32_t freq_pos = 0;
      uint32_t savings_full = 0;
      uint32_t savings_count_full = 0;
      uniq_tails_info *ti0 = uniq_tails_freq[freq_pos];
      uint8_t *prev_val = uniq_tails.data() + ti0->tail_pos;
      uint32_t prev_val_len = ti0->tail_len;
      uint32_t prev_val_idx = freq_pos;
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->grp_no != 0)
          continue;
        int cmp = gen::compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
        cmp = cmp ? abs(cmp) - 1 : 0;
        uniq_tails_info *prev_ti = uniq_tails_freq[prev_val_idx];
        if (cmp == ti->tail_len) {
          ti->flags |= UTI_FLAG_SUFFIX_FULL;
          if (prev_ti->cmp_rev_min > cmp)
            prev_ti->cmp_rev_min = cmp;
          if (prev_ti->cmp_rev_max < cmp)
            prev_ti->cmp_rev_max = cmp;
          prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
          ti->link_rev_idx = prev_ti->rev_pos;
        } else {
          prev_val = uniq_tails.data() + ti->tail_pos;
          prev_val_len = ti->tail_len;
          prev_val_idx = freq_pos - 1;
        }
      }

      freq_pos = 0;
      uint32_t grp_no = 1;
      uint32_t cur_limit = 128;
      freq_grp_vec.push_back((freq_grp) {0, 0, 0, 0, 0, 0});
      freq_grp_vec.push_back((freq_grp) {grp_no, (uint8_t) log2(cur_limit), cur_limit, 0, 0, 0});
      uint32_t savings_partial = 0;
      uint32_t savings_count_partial = 0;
      uint32_t sfx_set_len = 0;
      uint32_t sfx_set_max = 64;
      uint32_t sfx_set_count = 0;
      uint32_t sfx_set_tot_len = 0;
      ti0 = uniq_tails_freq[freq_pos];
      prev_val = uniq_tails.data() + ti0->tail_pos;
      prev_val_len = ti0->tail_len;
      prev_val_idx = freq_pos;
      while (freq_pos < uniq_tails_freq.size()) {
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->grp_no != 0)
          continue;
        if (ti->flags & UTI_FLAG_SUFFIX_FULL) {
          savings_full += ti->tail_len;
          savings_full++;
          savings_count_full++;
          uniq_tails_info *link_ti = uniq_tails_rev[ti->link_rev_idx];
          if (link_ti->grp_no == 0) {
            cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, link_ti->tail_len + 1);
            link_ti->grp_no = grp_no;
            update_current_grp(freq_grp_vec, link_ti->grp_no, link_ti->tail_len + 1, link_ti->freq_count);
            link_ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, link_ti, grp_no);
          }
          update_current_grp(freq_grp_vec, link_ti->grp_no, 0, ti->freq_count);
          ti->tail_ptr = link_ti->tail_ptr + link_ti->tail_len - ti->tail_len;
          ti->grp_no = link_ti->grp_no;
        } else {
          int cmp = gen::compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
          cmp = cmp ? abs(cmp) - 1 : 0;
          uniq_tails_info *prev_ti = uniq_tails_freq[prev_val_idx];
          if (cmp > 1) {
            if (cmp >= ti->cmp_rev_min) {
              cmp = ti->cmp_rev_min - 1;
            }
            uint32_t remain_len = ti->tail_len - cmp;
            uint32_t len_len = get_len_len(cmp);
            remain_len += len_len;
            uint32_t new_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, remain_len);
            if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
              ti->cmp_rev_min = 0;
              sfx_set_len += remain_len;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              update_current_grp(freq_grp_vec, grp_no, remain_len, ti->freq_count);
              //ti->link_rev_idx = prev_ti->rev_pos;
              ti->flags |= UTI_FLAG_SUFFIX_PARTIAL;
              if (ti->cmp_rev_max < cmp)
                ti->cmp_rev_max = cmp;
              if (prev_ti->cmp_rev_min > cmp)
                prev_ti->cmp_rev_min = cmp;
              if (prev_ti->cmp_rev_max < cmp)
                prev_ti->cmp_rev_max = cmp;
              prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
              remain_len -= len_len;
              ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no, remain_len);
              get_len_len(cmp, &grp_tails[grp_no - 1]);
            } else {
              sfx_set_len = ti->cmp_rev_max + 1;
              sfx_set_tot_len += sfx_set_len;
              sfx_set_count++;
              update_current_grp(freq_grp_vec, grp_no, ti->tail_len + 1, ti->freq_count);
              ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
            cur_limit = new_limit;
          } else {
            sfx_set_len = ti->cmp_rev_max + 1;
            sfx_set_tot_len += sfx_set_len;
            sfx_set_count++;
            cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, ti->tail_len + 1);
            update_current_grp(freq_grp_vec, grp_no, ti->tail_len + 1, ti->freq_count);
            ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
          }
          ti->grp_no = grp_no;
        }
        if ((ti->flags & UTI_FLAG_SUFFIX_FULL) == 0) {
          prev_val = uniq_tails.data() + ti->tail_pos;
          prev_val_len = ti->tail_len;
          prev_val_idx = freq_pos - 1;
        }
      }
      printf("Savings full: %u, %u\n", savings_full, savings_count_full);
      printf("Savings partial: %u, %u\n", savings_partial, savings_count_partial);
      printf("Suffix set: %u, %u\n", sfx_set_tot_len, sfx_set_count);

      uint32_t savings_prefix = 0;
      uint32_t savings_count_prefix = 0;
      // uniq_tails_info *prev_fwd_ti = uniq_tails_fwd[0];
      // uint32_t fwd_pos = 1;
      // while (fwd_pos < uniq_tails_fwd.size()) {
      //   uniq_tails_info *ti = uniq_tails_fwd[fwd_pos];
      //   fwd_pos++;
      //   if (ti->flags &UTI_FLAG_SUFFIX_FULL)
      //     continue;
      //   int cmp = gen::compare(uniq_tails.data() + prev_fwd_ti->tail_pos, prev_fwd_ti->tail_len,
      //                   uniq_tails.data() + ti->tail_pos, ti->tail_len);
      //   cmp = abs(cmp) - 1;
      //   if (cmp > ti->tail_len - ti->cmp_rev_max - 1)
      //     cmp = ti->tail_len - ti->cmp_rev_max - 1;
      //   if (cmp > 4) {
      //     savings_prefix += cmp;
      //     savings_prefix -= 4;
      //     savings_count_prefix++;
      //     ti->cmp_fwd = cmp;
      //   }
      //   prev_fwd_ti = ti;
      // }

      uint32_t remain_tot = 0;
      uint32_t remain_cnt = 0;
      uint32_t cmp_rev_min_tot = 0;
      uint32_t cmp_rev_min_cnt = 0;
      uint32_t free_tot = 0;
      uint32_t free_cnt = 0;
      FILE *fp = fopen("remain.txt", "w+");
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
        // fprintf(fp, "%u\t%u\t%u\t%u\t%u\n", ti->freq_count, ti->tail_len, ti->node_id, ti->rev_pos, ti->cmp_rev_max);
        if (ti->cmp_rev_min != 0xFFFFFFFF && ti->cmp_rev_min > 4) {
          cmp_rev_min_tot += (ti->cmp_rev_min - 1);
          cmp_rev_min_cnt++;
        }
        uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, ti->grp_no, savings_prefix, savings_count_prefix, ti->cmp_rev_max, fp);
      }
      fclose(fp);
      printf("Free entries: %u, %u\n", free_tot, free_cnt);
      printf("Savings prefix: %u, %u\n", savings_prefix, savings_count_prefix);
      printf("Remaining: %u, %u\n", remain_tot, remain_cnt);
      printf("Cmp_rev_min_tot: %u, %u\n", cmp_rev_min_tot, cmp_rev_min_cnt);

      std::vector<uint32_t> freqs;
      for (int i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      huffman<uint32_t> _huffman(freqs);
      printf("bits\tcd\tct\tfct\tlen\tcdln\tmxsz\n");
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp& fg = freq_grp_vec[i];
        int len;
        fg.code = (uint8_t) _huffman.get_code(i - 1, len);
        fg.code_len = len;
        fg.grp_log2 = ceil(log2(fg.grp_size)) - 8 + len;
        printf("%d\t%2x\t%u\t%u\t%u\t%d\t%u\n", (int) fg.grp_log2, fg.code, fg.count, fg.freq_count, fg.grp_size, fg.code_len, fg.grp_limit);
      }
    }

    uint32_t find_prefix(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd, uniq_tails_info *ti, uint32_t grp_no, uint32_t& savings_prefix, uint32_t& savings_count_prefix, int cmp_sfx, FILE *fp) {
      if ((ti->flags & UTI_FLAG_SUFFIX_FULL) == 0) {
        uint32_t limit = ti->fwd_pos + 30;
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
            savings_prefix += cmp;
            savings_prefix -= 4;
            savings_count_prefix++;
            ti->cmp_fwd = cmp;
            ti_fwd->cmp_fwd = cmp;
            uint32_t remain_len = ti->tail_len - cmp_sfx - cmp;
            fprintf(fp, "%u\t%u\t%u\t%u\t<%.*s>%.*s[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, grp_no, remain_len, cmp, uniq_tails.data() + ti->tail_pos, 
                    remain_len, uniq_tails.data() + ti->tail_pos + cmp,
                    ti->tail_len - remain_len - cmp, uniq_tails.data() + ti->tail_pos + cmp + remain_len);
            return cmp;
          }
        }
      }
      fprintf(fp, "%u\t%u\t%u\t%u\t%.*s[%.*s]\n", (uint32_t) ceil(log10(ti->freq_count/ti->tail_len)), ti->freq_count, grp_no, ti->tail_len - cmp_sfx, ti->tail_len - cmp_sfx, uniq_tails.data() + ti->tail_pos, cmp_sfx, uniq_tails.data() + ti->tail_pos + ti->tail_len - cmp_sfx);
      // fprintf(fp, "%.*s\n", ti->tail_len - cmp_sfx, uniq_tails.data() + ti->tail_pos);
      return 0;
    }

    int append_bits(byte_vec& ptr_str, int last_byte_bits, int bits_to_append, int ptr) {
      int last_idx = ptr_str.size() - 1;
      while (bits_to_append > 0) {
        if (bits_to_append < last_byte_bits) {
          last_byte_bits -= bits_to_append;
          ptr_str[last_idx] |= (ptr << last_byte_bits);
          bits_to_append = 0;
        } else {
          bits_to_append -= last_byte_bits;
          ptr_str[last_idx] |= (ptr >> bits_to_append);
          last_byte_bits = 8;
          ptr_str.push_back(0);
          last_idx++;
        }
      }
      return last_byte_bits;
    }

    uint8_t get_tail_ptr(node *cur_node, uniq_tails_info_vec& uniq_tail_vec, byte_vec& uniq_tails,
             std::vector<freq_grp>& freq_grp_vec, std::vector<byte_vec>& grp_tails, byte_vec& tail_ptrs,
             int& last_byte_bits) {
      uint8_t node_val;
      uniq_tails_info *ti = uniq_tail_vec[cur_node->rev_node_info_pos];
      uint32_t ptr = 0;
      uint8_t grp_no = ti->grp_no;
      if (grp_no == 0 || ti->tail_ptr == 0)
        printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
      grp_no--;
      ptr = ti->tail_ptr;
      freq_grp _freq_grp = freq_grp_vec[grp_no + 1];
      uint8_t huf_code = _freq_grp.code;
      uint8_t huf_code_len = _freq_grp.code_len;
      int node_val_bits = 8 - huf_code_len;
      node_val = (huf_code << node_val_bits) | (ptr & ((1 << node_val_bits) - 1));
      ptr >>= node_val_bits;
      last_byte_bits = append_bits(tail_ptrs, last_byte_bits, _freq_grp.grp_log2, ptr);
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
      uniq_tails_info_vec uniq_tails_fwd;
      sort_nodes();
      build_tail_maps(uniq_tails, uniq_tails_fwd, uniq_tails_rev, freq_grp_vec, grp_tails, tail_ptrs);
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
      int last_byte_bits;
      //trie.reserve(node_count + (node_count >> 1));
      tail_ptrs.push_back(0);
      last_byte_bits = 8;
      uint32_t ptr_count = 0;
      int32_t ptr_delta_savings = 0;
      uint32_t ptrs_upto[20];
      int32_t block_delta_savings[20];
      memset(ptrs_upto, '\0', sizeof(ptrs_upto));
      memset(block_delta_savings, '\0', sizeof(block_delta_savings));
      FILE *fp = fopen("ptrs_deltas.txt", "wb+");
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if (cur_node->tail_len > 1) {
            uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
            if (ti->flags & UTI_FLAG_SUFFIX_FULL)
              sfx_full_count++;
            if (ti->flags & UTI_FLAG_SUFFIX_PARTIAL)
              sfx_partial_count++;
            int32_t assigned_bit_len = freq_grp_vec[ti->grp_no].grp_log2 + (8-freq_grp_vec[ti->grp_no].code_len);
            int32_t delta_bit_len = ceil(log2(abs((int)(ti->tail_ptr - ptrs_upto[ti->grp_no])) + 1));
            int32_t actual_bit_len = assigned_bit_len;
            if (ptrs_upto[ti->grp_no] == 0)
              actual_bit_len = delta_bit_len = assigned_bit_len;
            else {
              if (delta_bit_len >= (assigned_bit_len - 1))
                actual_bit_len = assigned_bit_len + 1;
              else {
                if (delta_bit_len < assigned_bit_len / 4)
                  actual_bit_len = assigned_bit_len / 4 + 4;
                else if (delta_bit_len < assigned_bit_len / 2)
                  actual_bit_len = assigned_bit_len / 2 + 4;
                else if (delta_bit_len < assigned_bit_len * 3 / 4)
                  actual_bit_len = assigned_bit_len * 3 / 4 + 3;
                else
                  actual_bit_len = assigned_bit_len;
              }
            }
            fprintf(fp, "%u\t%u\t%u\t%d\t%d\t%d\t%d\t%d\n", cur_node->node_id / 64, ti->grp_no,
                ti->tail_ptr, assigned_bit_len, ti->tail_ptr - ptrs_upto[ti->grp_no], delta_bit_len, actual_bit_len, assigned_bit_len - actual_bit_len);
            block_delta_savings[ti->grp_no] += (assigned_bit_len - actual_bit_len);
            ptrs_upto[ti->grp_no] = ti->tail_ptr;
          }
          if (cur_node->flags & NFLAG_TERM)
            term_count++;
          uint8_t flags = (cur_node->flags & NFLAG_LEAF) +
            (cur_node->first_child != 0 ? 2 : 0) + (cur_node->tail_len > 1 ? 4 : 0) +
            (cur_node->flags & NFLAG_TERM ? 8 : 0);
          flag_counts[flags & 0x07]++;
          uint8_t node_val;
          if (cur_node->tail_len == 1) {
            node_val = cur_node->v0;
          } else {
            char_counts[(cur_node->tail_len > 8) ? 7 : (cur_node->tail_len-2)]++;
            node_val = get_tail_ptr(cur_node, uniq_tails_rev, uniq_tails, 
                         freq_grp_vec, grp_tails, tail_ptrs, last_byte_bits);
            ptr_count++;
          }
          if (node_count && (node_count % 64) == 0) {
            for (int j = 1; j < freq_grp_vec.size(); j++) {
              if (block_delta_savings[j] > 0)
                ptr_delta_savings += block_delta_savings[j];
            }
            memset(block_delta_savings, '\0', sizeof(block_delta_savings));
            memset(ptrs_upto, '\0', sizeof(ptrs_upto));
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
      fclose(fp);
      if (node_count % 64) {
        append_flags(trie, bm_leaf, bm_term, bm_child, bm_ptr);
        append_byte_vec(trie, byte_vec64);
      }
      std::cout << std::endl;
      std::cout << "Ptr delta savings: " << ptr_delta_savings / 8 << std::endl;
      for (int i = 0; i < 8; i++) {
        std::cout << "Flag " << i << ": " << flag_counts[i] << "\t";
        std::cout << "Char " << i + 2 << ": " << char_counts[i] << std::endl;
      }
      // for (int i = 0; i < 8; i++)
      //   std::cout << "Val lens " << i << ": " << val_len_counts[i] << std::endl;
      uint32_t total_tails = 0;
      for (int i = 0; i < grp_tails.size(); i++)
        total_tails += grp_tails[i].size();
      std::cout << "Total pointer count: " << ptr_count << std::endl;
      std::cout << "Full suffix count: " << sfx_full_count << std::endl;
      std::cout << "Partial suffix count: " << sfx_partial_count << std::endl;
      std::cout << "Node count: " << node_count << std::endl;
      std::cout << "Trie size: " << trie.size() << std::endl;
      std::cout << "Term count: " << term_count << std::endl;
      std::cout << "Max tail len: " << max_tail_len << std::endl;
      std::cout << "Total tail size: " << total_tails << std::endl;
      std::cout << "Uniq tail count: " << uniq_tails_rev.size() << std::endl;
      std::cout << "Tail ptr size: " << tail_ptrs.size() << std::endl;
      uint32_t ptr_lookup_tbl = (ceil(node_count/64) + 1) * 4;
      uint32_t trie_bv = (ceil(node_count/nodes_per_bv_block) + 1) * 11 * 2;
      uint32_t leaf_bv = (ceil(node_count/nodes_per_bv_block) + 1) * 11;
      uint32_t select_lookup = (ceil(term_count/term_divisor) + 1) * 4;
      std::cout << "Pointer lookup table: " << ptr_lookup_tbl << std::endl;
      std::cout << "Trie bit vectors: " << trie_bv << std::endl;
      std::cout << "Leaf bit vectors: " << leaf_bv << std::endl;
      std::cout << "Select lookup table: " << select_lookup << std::endl;
      uint32_t cache_size = key_count / 512 * sizeof(node_cache);
      std::cout << "Cache size: " << cache_size << std::endl;
      uint32_t total_size = 2 + 9 * 4 + trie.size()
          + ptr_lookup_tbl + trie_bv + leaf_bv + select_lookup
          + cache_size
          + total_tails + 512 + 1 + grp_tails.size() * 4 // tails
          + tail_ptrs.size();  // tail pointers
      std::cout << "Total size: " << total_size << std::endl;
      std::cout << "Node struct size: " << sizeof(node) << std::endl;

      fp = fopen(out_filename.c_str(), "wb+");
      fputc(0xA5, fp); // magic byte
      fputc(0x01, fp); // version 1.0
      uint32_t grp_tails_loc = 2 + 14 * 4; // 58
      uint32_t grp_tails_size = 513 // group_count + huffman lookup table
                       + grp_tails.size() * 4 // tail_locations
                       + total_tails;
      uint32_t cache_loc = grp_tails_loc + grp_tails_size;
      uint32_t ptr_lookup_tbl_loc = cache_loc + cache_size;
      uint32_t trie_bv_loc = ptr_lookup_tbl_loc + ptr_lookup_tbl;
      uint32_t leaf_bv_loc = trie_bv_loc + trie_bv;
      uint32_t select_lkup_loc = leaf_bv_loc + leaf_bv;
      uint32_t tail_ptrs_loc = select_lkup_loc + select_lookup;
      uint32_t two_byte_tails_loc = tail_ptrs_loc + tail_ptrs.size();
      uint32_t idx2_ptrs_map_loc = two_byte_tails_loc + two_byte_tails.size();
      uint32_t common_nodes_loc = idx2_ptrs_map_loc + idx2_ptrs_map.size();
      uint32_t trie_loc = common_nodes_loc + ceil(common_node_count * 1.5);
      two_byte_count = two_byte_tails.size() / 2;
      idx2_ptr_count = idx2_ptrs_map.size() / 3;
      printf("%u,%u,%u,%u,%u,%u,%u,%u\n", node_count, cache_loc, ptr_lookup_tbl_loc, trie_bv_loc, leaf_bv_loc, select_lkup_loc, tail_ptrs_loc, trie_loc);
      write_uint32(node_count, fp);
      write_uint32(common_node_count, fp);
      write_uint32(two_byte_count, fp);
      write_uint32(idx2_ptr_count, fp);
      write_uint32(max_tail_len, fp);
      write_uint32(cache_loc, fp);
      write_uint32(ptr_lookup_tbl_loc, fp);
      write_uint32(trie_bv_loc, fp);
      write_uint32(leaf_bv_loc, fp);
      write_uint32(select_lkup_loc, fp);
      write_uint32(tail_ptrs_loc, fp);
      write_uint32(two_byte_tails_loc, fp);
      write_uint32(idx2_ptrs_map_loc, fp);
      write_uint32(trie_loc, fp);
      write_grp_tails(freq_grp_vec, grp_tails, grp_tails_loc + 513, fp); // group count, 512 lookup tbl, tail locs, tails
      //write_cache(fp, cache_size);
      fwrite(trie.data(), cache_size, 1, fp);
      write_ptr_lookup_tbl(freq_grp_vec, uniq_tails_rev, fp);
      write_trie_bv(fp);
      write_leaf_bv(fp);
      write_select_lkup(fp);
      fwrite(tail_ptrs.data(), tail_ptrs.size(), 1, fp);
      fwrite(trie.data(), two_byte_tails.size(), 1, fp);
      fwrite(trie.data(), trie.size(), 1, fp);
      fclose(fp);

      // std::vector<nodes_ptr_grp> ptr_grp_vec;
      // for (uint32_t i = 1; i < all_nodes.size(); i++) {
      //   node *cur_node = &all_nodes[i];
      //   if (cur_node->tail_len > 1) {
      //     uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
      //     ptr_grp_vec.push_back((nodes_ptr_grp) {i, (ti->grp_no << 28) | ti->tail_ptr});
      //   }
      // }
      // std::sort(ptr_grp_vec.begin(), ptr_grp_vec.end(), [this](const struct nodes_ptr_grp& lhs, const struct nodes_ptr_grp& rhs) -> bool {
      //   return lhs.ptr == rhs.ptr ? (lhs.node_id < rhs.node_id) : (lhs.ptr < rhs.ptr);
      // });
      // uint32_t ptr_grp_count = 0;
      // nodes_ptr_grp& prev_ptr_grp = ptr_grp_vec[0];
      // fp = fopen("node_ptrs.txt", "wb+");
      // for (uint32_t i = 0; i < ptr_grp_vec.size(); i++) {
      //   nodes_ptr_grp& ptr_grp = ptr_grp_vec[i];
      //   if (ptr_grp.ptr == prev_ptr_grp.ptr) {
      //     ptr_grp_count++;
      //   } else {
      //     fprintf(fp, "%8u\t%u\t%u\t%u\t%u\t%u\n", ptr_grp_count, all_nodes[prev_ptr_grp.node_id].tail_len, prev_ptr_grp.node_id, all_nodes[prev_ptr_grp.node_id].rev_node_info_pos,
      //             prev_ptr_grp.ptr >> 28, prev_ptr_grp.ptr & 0x0FFFFFFF);
      //     prev_ptr_grp = ptr_grp;
      //     ptr_grp_count = 1;
      //   }
      // }
      // fclose(fp);

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

    static void write_uint32(uint32_t input, FILE *fp) {
      // int i = 4;
      // while (i--)
      //   fputc((input >> (8 * i)) & 0xFF, fp);
      fputc(input & 0xFF, fp);
      fputc((input >> 8) & 0xFF, fp);
      fputc((input >> 16) & 0xFF, fp);
      fputc(input >> 24, fp);
    }

    const int nodes_per_bv_block7 = 64;
    const int nodes_per_bv_block = 512;
    const int term_divisor = 512;
    void write_bv7(uint32_t node_id, uint32_t& term1_count, uint32_t& child_count,
                    uint32_t& term1_count7, uint32_t& child_count7,
                    uint8_t *term1_buf7, uint8_t *child_buf7, uint8_t& pos7, FILE *fp) {
      if (node_id && (node_id % nodes_per_bv_block) == 0) {
        fwrite(term1_buf7, 7, 1, fp);
        fwrite(child_buf7, 7, 1, fp);
        write_uint32(term1_count, fp);
        write_uint32(child_count, fp);
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
      write_uint32(0, fp);
      write_uint32(0, fp);
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
        write_uint32(leaf_count, fp);
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
      write_uint32(0, fp);
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
      write_uint32(0, fp);
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if (cur_node->flags & NFLAG_TERM) {
            if (term_count && (term_count % term_divisor) == 0) {
              write_uint32(node_id / nodes_per_bv_block, fp);
              //printf("%u\t%u\n", term_count, node_id / nodes_per_bv_block);
            }
            term_count++;
          }
          node_id++;
      }
    }

    const int nodes_per_ptr_block = 64;
    void write_ptr_lookup_tbl(std::vector<freq_grp>& freq_grp_vec, uniq_tails_info_vec& uniq_tails_rev, FILE* fp) {
      uint32_t node_id = 0;
      uint32_t bit_count = 0;
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if ((node_id % nodes_per_ptr_block) == 0)
            write_uint32(bit_count, fp);
          if (cur_node->tail_len > 1) {
            uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
            bit_count += freq_grp_vec[ti->grp_no].grp_log2;
          }
          node_id++;
        }
    }

    void write_code_lookup_tbl(std::vector<freq_grp>& freq_grp_vec, FILE* fp) {
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
          printf("Code not found: %d", i);
          fputc(0, fp);
          fputc(0, fp);
        }
      }
    }

    void write_grp_tails(std::vector<freq_grp>& freq_grp_vec, std::vector<byte_vec>& grp_tails, uint32_t offset, FILE* fp) {
      uint8_t grp_count = grp_tails.size();
      fputc(grp_count, fp);
      write_code_lookup_tbl(freq_grp_vec, fp);
      uint32_t total_tail_size = 0;
      for (int i = 0; i < grp_count; i++) {
        write_uint32(offset + grp_count * 4 + total_tail_size, fp);
        total_tail_size += grp_tails[i].size();
      }
      for (int i = 0; i < grp_count; i++) {
        fwrite(grp_tails[i].data(), grp_tails[i].size(), 1, fp);
      }
    }

    uint8_t get_first_char(node *n) {
      if (n->tail_len == 1)
        return n->v0;
      // return *(sort_tails.data() + n->tail_pos);
      uniq_tails_info *ti = uniq_tails_rev[n->rev_node_info_pos];
      uint32_t ptr = ti->tail_ptr;
      uint32_t grp_no = ti->grp_no;
      byte_vec& tail = grp_tails[grp_no - 1];
      return *(tail.data() + ti->tail_ptr);
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
      ret |= (tail[ptr] & 0x3F);
      return ret + 15;
    }

    const uint8_t *get_tail_str(node *n, uint32_t& tail_len) {
      tail_len = n->tail_len;
      return sort_tails[n->tail_pos];
    }

    std::string get_tail_str(node *n) {
      uniq_tails_info *ti = uniq_tails_rev[n->rev_node_info_pos];
      uint32_t ptr = ti->tail_ptr;
      uint32_t grp_no = ti->grp_no;
      std::string ret;
      byte_vec& tail = grp_tails[grp_no - 1];
      int byt = tail[ptr++];
      while (byt > 31) {
        ret.append(1, byt);
        byt = tail[ptr++];
      }
      if (tail[--ptr] == 0)
        return ret;
      uint8_t len_len = 0;
      uint32_t sfx_len = read_len(tail, ptr, len_len);
      uint32_t ptr_end = ti->tail_ptr;
      ptr = ti->tail_ptr;
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

    int lookup(std::string key) {
      int key_pos = 0;
      uint8_t key_char = key[key_pos];
      node *cur_node = &all_nodes[1];
      uint8_t trie_char = get_first_char(cur_node);
      do {
        while (key_char > trie_char) {
          cur_node = &all_nodes[cur_node->next_sibling];
          if (cur_node == NULL)
            return ~INSERT_AFTER;
          trie_char = get_first_char(cur_node);
        }
        if (key_char == trie_char) {
          int cmp;
          if (cur_node->tail_len > 1) {
            uint32_t tail_len;
            // const uint8_t *tail_str = get_tail_str(cur_node, tail_len);
            // cmp = gen::compare(tail_str, tail_len, 
            std::string tail_str = get_tail_str(cur_node);
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
            cur_node = &all_nodes[cur_node->first_child];
            if (key_pos >= key.size())
              return ~INSERT_THREAD;
            key_char = key[key_pos];
            trie_char = get_first_char(cur_node);
            continue;
          }
          return ~INSERT_THREAD;
        }
        return ~INSERT_BEFORE;
      } while (1);
      return 0;
    }

    uniq_tails_info *get_ti(node *n) {
      return uniq_tails_rev[n->rev_node_info_pos];
    }

    uint32_t get_ptr(node *cur_node) {
      uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
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
          uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
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
            uniq_tails_info *ti1 = uniq_tails_rev[node1->rev_node_info_pos];
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
