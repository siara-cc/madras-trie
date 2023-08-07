#ifndef builder_H
#define builder_H

#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>
#include <time.h>

#include "var_array.h"
#include "bit_vector.h"
#include "bitset_vector.h"
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
const uint8_t UTI_FLAG_SFXGRP_TIKTOK = 0x08;
struct uniq_tails_info {
    uint32_t tail_pos;
    uint32_t tail_len;
    uint32_t rev_pos;
    uint32_t cmp_rev;
    uint32_t cmp_rev_min;
    uint32_t freq_count;
    uint32_t tail_ptr;
    uint32_t link_rev_idx;
    uint32_t token_arr_pos;
    uint8_t grp_no;
    uint8_t flags;
    uniq_tails_info(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _rev_pos, uint32_t _freq_count) {
      tail_pos = _tail_pos; tail_len = _tail_len;
      rev_pos = _rev_pos;
      cmp_rev = 0;
      cmp_rev_min = 0xFFFFFFFF;
      freq_count = _freq_count;
      link_rev_idx = 0xFFFFFFFF;
      token_arr_pos = 0xFFFFFFFF;
      tail_ptr = 0;
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

struct node {
  public:
    node *first_child;
    //node *parent;
    node *next_sibling;
    uint32_t tail_pos;
    uint32_t rev_node_info_pos;
    uint32_t tail_len;
    uint8_t is_leaf;
    uint8_t level;
    uint8_t v0;
    node() {
      memset(this, '\0', sizeof(node));
    }
};

int compare(const uint8_t *v1, int len1, const uint8_t *v2,
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

int compare_rev(const uint8_t *v1, int len1, const uint8_t *v2,
        int len2) {
    int lim = (len2 < len1 ? len2 : len1);
    int k = 1;
    do {
        uint8_t c1 = v1[len1 - k];
        uint8_t c2 = v2[len2 - k];
        if (c1 < c2)
            return -k;
        else if (c1 > c2)
            return k;
    } while (k++ < lim);
    if (len1 == len2)
        return 0;
    return (len1 < len2 ? -k : k);
}

class builder_abstract {
  public:
    std::vector<uint8_t> sort_tails;
};

class builder : public builder_abstract {

  private:
    node *root;
    node *first_node;
    int node_count;
    int key_count;
    std::vector<vector<node *> > level_nodes;
    std::string prev_key;
    //dfox uniq_basix_map;
    //basix uniq_basix_map;
    //art_tree at;

    builder(builder const&);
    builder& operator=(builder const&);

    node *get_last_child(node *node) {
      node = node->first_child;
      while (node != NULL && node->next_sibling != NULL)
        node = node->next_sibling;
      return node;
    }

    void add_to_level_nodes(node *new_node) {
        if (level_nodes.size() < new_node->level)
          level_nodes.push_back(vector<node *>());
        level_nodes[new_node->level - 1].push_back(new_node);
    }

    void append_tail_vec(std::string val, node *n) {
      n->tail_pos = sort_tails.size();
      n->tail_len = val.length();
      for (int i = 0; i < val.length(); i++)
        sort_tails.push_back(val[i]);
    }

    bool compareNodeTails(const node *lhs, const node *rhs) const {
      return compare(sort_tails.data() + lhs->tail_pos, lhs->tail_len,
                     sort_tails.data() + rhs->tail_pos, rhs->tail_len);
    }

  public:
    byte_vec uniq_tails;
    uniq_tails_info_vec uniq_tails_rev;
    std::vector<byte_vec> grp_tails;
    std::vector<byte_vec> grp_tail_ptrs;
    builder() {
      root = new node;
      node_count = 0;
      key_count = 0;
      //root->parent = NULL;
      root->next_sibling = NULL;
      root->is_leaf = 0;
      root->level = 0;
      first_node = new node;
      //first_node->parent = root;
      first_node->next_sibling = NULL;
      first_node->level = 1;
      add_to_level_nodes(first_node);
      root->first_child = first_node;
      util::generate_bit_counts();
      //art_tree_init(&at);
    }

    ~builder() {
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++)
          delete cur_lvl_nodes[j];
      }
      // for (int i = 0; i < uniq_tails_fwd.size(); i++) {
      //   delete uniq_tails_fwd[i];
      // }
      delete root;
    }

    void append(string key) {
      if (key == prev_key)
         return;
      key_count++;
      if (node_count == 0) {
        append_tail_vec(key, first_node);
        first_node->is_leaf = 1;
        node_count++;
        return;
      }
      prev_key = key;
      int key_pos = 0;
      node *last_child = get_last_child(root);
      do {
        std::string val((const char *) sort_tails.data() + last_child->tail_pos, last_child->tail_len);
        int i = 0;
        for (; i < val.length(); i++) {
          if (key[key_pos] != val[i]) {
            if (i == 0) {
              node *new_node = new node();
              new_node->is_leaf = 1;
              new_node->level = last_child->level;
              //new_node->parent = last_child->parent;
              new_node->next_sibling = NULL;
              append_tail_vec(key.substr(key_pos), new_node);
              //if (new_node->val == string(" discuss"))
              //  cout << "New node: " << key << endl;
              add_to_level_nodes(new_node);
              last_child->next_sibling = new_node;
              node_count++;
            } else {
              node *child1 = new node();
              node *child2 = new node();
              child1->is_leaf = last_child->is_leaf;
              child1->level = last_child->level + 1;
              //child1->parent = last_child;
              child1->next_sibling = child2;
              child1->tail_pos = last_child->tail_pos + i;
              child1->tail_len = last_child->tail_len - i;
              //if (child1->val == string(" discuss"))
              //  cout << "Child1 node: " << key << endl;
              if (last_child->first_child != NULL) {
                node *node = last_child->first_child;
                child1->first_child = node;
                do {
                  //node->parent = child1;
                  node->level++;
                  node = node->next_sibling;
                } while (node != NULL);
              }
              child2->is_leaf = 1;
              child2->level = last_child->level + 1;
              //child2->parent = last_child;
              child2->next_sibling = NULL;
              append_tail_vec(key.substr(key_pos), child2);
              //if (child2->val == string(" discuss"))
              //  cout << "Child2 node: " << key << endl;
              last_child->first_child = child1;
              last_child->is_leaf = 0;
              node_count += 2;
              add_to_level_nodes(child1);
              add_to_level_nodes(child2);
              //last_child->tail_pos += i;
              last_child->tail_len = i;
            }
            return;
          }
          key_pos++;
        }
        if (i == val.length() && key_pos < key.length()
            && last_child->is_leaf && last_child->first_child == NULL) {
          node *child1 = new node();
          child1->is_leaf = 1;
          child1->level = last_child->level + 1;
          //child1->parent = last_child;
          child1->next_sibling = NULL;
          append_tail_vec(key.substr(key_pos), child1);
          //    if (child1->val == string(" discuss"))
          //      cout << "Ext node: " << key << endl;
          last_child->first_child = child1;
          node_count++;
          add_to_level_nodes(child1);
          return;
        }
        last_child = get_last_child(last_child);
      } while (last_child != NULL);
    }

    struct tails_sort_data {
      uint8_t *tail_data;
      uint32_t tail_len;
      node *n;
    };

    clock_t print_time_taken(clock_t t, const char *msg) {
      t = clock() - t;
      double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
      std::cout << msg << time_taken << std::endl;
      return clock();
    }

    uint32_t make_uniq_tails(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_rev, uniq_tails_info_vec& uniq_tails_freq) {
      clock_t t = clock();
      uint32_t total_ptrs = 0;
      std::vector<tails_sort_data> nodes_for_sort;
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *n = cur_lvl_nodes[j];
          if (n->tail_len > 1) {
            total_ptrs++;
            nodes_for_sort.push_back((struct tails_sort_data) { sort_tails.data() + n->tail_pos, n->tail_len, n } );
          } else {
            uint8_t *v = sort_tails.data() + n->tail_pos;
            n->v0 = v[0];
          }
        }
      }
      t = print_time_taken(t, "Time taken for adding to nodes_for_sort: ");
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this](const struct tails_sort_data& lhs, const struct tails_sort_data& rhs) -> bool {
        return compare_rev(lhs.tail_data, lhs.tail_len, rhs.tail_data, rhs.tail_len) < 0;
      });
      t = print_time_taken(t, "Time taken to sort: ");
      uint32_t vec_pos = 0;
      uint32_t freq_count = 0;
      std::vector<tails_sort_data>::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->tail_data;
      uint32_t prev_val_len = it->tail_len;
      uniq_tails_info *ti_ptr = new uniq_tails_info((uint32_t) uniq_tails.size(), prev_val_len, vec_pos, freq_count);
      uniq_tails_info *prev_ti_ptr = ti_ptr;
      while (it != nodes_for_sort.end()) {
        int cmp = compare_rev(it->tail_data, it->tail_len, prev_val, prev_val_len);
        if (cmp == 0) {
          freq_count++;
        } else {
          ti_ptr->freq_count = freq_count;
          uniq_tails_rev.push_back(ti_ptr);
          for (int i = 0; i < prev_val_len; i++)
            uniq_tails.push_back(prev_val[i]);
          freq_count = 1;
          prev_val = it->tail_data;
          prev_val_len = it->tail_len;
          prev_ti_ptr = ti_ptr;
          ti_ptr = new uniq_tails_info((uint32_t) uniq_tails.size(), prev_val_len, ++vec_pos, freq_count);
        }
        it->n->rev_node_info_pos = vec_pos;
        it++;
      }
      ti_ptr->freq_count = freq_count;
      uniq_tails_rev.push_back(ti_ptr);
      for (int i = 0; i < prev_val_len; i++)
        uniq_tails.push_back(prev_val[i]);
      t = print_time_taken(t, "Time taken for uniq_tails_rev: ");
      uniq_tails_freq = uniq_tails_rev;
      std::sort(uniq_tails_freq.begin(), uniq_tails_freq.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / 10;
        uint32_t rhs_freq = rhs->freq_count / 10;
        return (lhs_freq == rhs_freq) ? lhs->rev_pos > rhs->rev_pos : (lhs_freq > rhs_freq);
      });
      t = print_time_taken(t, "Time taken for uniq_tails freq: ");

      return total_ptrs;
    }

    uint32_t get_var_len(uint32_t len, byte_vec *vec = NULL) {
      uint32_t var_len = (len < 64 ? 2 : (len < 8192 ? 3 : (len < 1024768 ? 4 : 5)));
      if (vec != NULL) {
        int bit7s = var_len - 2;
        for (int i = bit7s - 1; i >= 0; i--)
          vec->push_back((len >> (i * 7 + 6)) & 0x7F);
        vec->push_back(0x40 + (len & 0x3F));
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

    uint32_t append_to_grp_tails(byte_vec& uniq_tails, vector<byte_vec>& grp_tails,
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

    const static uint32_t suffix_grp_limit = 3;
    void build_tail_maps(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd, uniq_tails_info_vec& uniq_tails_rev,
                 std::vector<freq_grp>& freq_grp_vec, vector<byte_vec>& grp_tails, vector<byte_vec>& grp_tail_ptrs) {
      uniq_tails_info_vec uniq_tails_freq = uniq_tails_rev;
      uint32_t total_ptrs = make_uniq_tails(uniq_tails, uniq_tails_rev, uniq_tails_freq);
      clock_t t = clock();
      uniq_tails_fwd = uniq_tails_rev;
      std::sort(uniq_tails_fwd.begin(), uniq_tails_fwd.end(), [uniq_tails](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return compare(uniq_tails.data() + lhs->tail_pos, lhs->tail_len, uniq_tails.data() + rhs->tail_pos, rhs->tail_len) < 0;
      });
      t = print_time_taken(t, "Time taken for uniq_tails fwd sort: ");

      uint32_t s_no = 0;
      uint32_t grp_no = 1;
      uint32_t cur_limit = 128;
      int freq_pos = 0;
      freq_grp_vec.push_back((freq_grp) {0, 0, 0, 0, 0, 0});
      freq_grp_vec.push_back((freq_grp) {1, 7, 128, 0, 0, 0});
      while (freq_pos < uniq_tails_freq.size()) {
        s_no++;
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        ti->tail_ptr = 0;
        freq_pos++;
        if (ti->grp_no != 0) {
          if (cur_limit < 128)
            printf("%u*\t%u*\t%u\t[%.*s]\t0\t[1]\n", s_no, freq_grp_vec[grp_no].count, ti->freq_count, ti->tail_len, uniq_tails.data() + ti->tail_pos);
          continue;
        }
        cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, ti->tail_len + 1);
        if (grp_no > suffix_grp_limit)
          break;
        if (cur_limit < 128) {
          printf("%u\t%u\t%u\t[%.*s]\t0\t[%.*s]\n", s_no, freq_grp_vec[grp_no].count, ti->freq_count, ti->tail_len, uniq_tails.data() + ti->tail_pos,
                  ti->link_rev_idx == 0xFFFFFFFF ? 1 : uniq_tails_rev[ti->link_rev_idx]->tail_len,
                  ti->link_rev_idx == 0xFFFFFFFF ? "-" : (const char *) uniq_tails.data() + uniq_tails_rev[ti->link_rev_idx]->tail_pos);
        }
        update_current_grp(freq_grp_vec, grp_no, ti->tail_len + 1, ti->freq_count);
        ti->grp_no = grp_no;
        if (ti->tail_len > 2) {
          uint8_t *val = uniq_tails.data() + ti->tail_pos;
          for (int j = ti->rev_pos - 1; j > 0; j--) {
            uniq_tails_info *ti_rev = uniq_tails_rev[j];
            if (ti_rev->grp_no != 0 && ti_rev->grp_no != ti->grp_no)
              continue;
            if (ti_rev->tail_len >= ti->tail_len)
              continue;
            int cmp = compare_rev(val, ti->tail_len, uniq_tails.data() + ti_rev->tail_pos, ti_rev->tail_len);
            if (cmp < 3)
              break;
            cmp--;
            if (cmp == ti_rev->tail_len) {
              if ((ti_rev->flags & UTI_FLAG_SUFFIX_FULL) == 0 || ti_rev->grp_no == 0) {
                if (cur_limit < 128) {
                  printf("%u\t%u*\t%u\t[%.*s]\t0\t[%.*s]\t%s\n", s_no, freq_grp_vec[grp_no].count,
                          ti_rev->freq_count, ti_rev->tail_len, uniq_tails.data() + ti_rev->tail_pos,
                          ti->tail_len, val, (ti_rev->freq_count > ti->freq_count ? "**" : ""));
                }
                update_current_grp(freq_grp_vec, grp_no, 0, ti_rev->freq_count);
                ti_rev->tail_ptr = 0;
                if ((ti_rev->flags & UTI_FLAG_SUFFIX_FULL) == 0 && ti_rev->grp_no != 0)
                  update_current_grp(freq_grp_vec, grp_no, -ti_rev->tail_len-1, -ti_rev->freq_count);
                ti_rev->link_rev_idx = ti->rev_pos;
                ti_rev->flags |= UTI_FLAG_SUFFIX_FULL;
                ti_rev->grp_no = ti->grp_no;
              } else {
                if (uniq_tails_rev[ti_rev->link_rev_idx]->tail_len < ti->tail_len) {
                  ti_rev->link_rev_idx = ti->rev_pos;
                  ti_rev->flags |= UTI_FLAG_SUFFIX_FULL;
                  ti_rev->tail_ptr = 0;
                }
              }
            }
          }
        }
      }

      for (int i = 0; i < uniq_tails_freq.size(); i++) {
        uniq_tails_info *ti = uniq_tails_freq[i];
        if (ti->grp_no == 0)
          continue;
        if (ti->flags & UTI_FLAG_SUFFIXES) {
          uniq_tails_info *ti_link = uniq_tails_rev[ti->link_rev_idx];
          if (ti_link->tail_ptr == 0)
            ti_link->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti_link, ti_link->grp_no);
          ti->tail_ptr = ti_link->tail_ptr + ti_link->tail_len - ti->tail_len;
        } else {
          if (ti->tail_ptr == 0)
            ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, ti->grp_no);
        }
      }
      t = print_time_taken(t, "Time taken for suffix_grp_limit: ");

      int32_t rev_pos = uniq_tails_rev.size() - 1;
      uniq_tails_info *ti0 = uniq_tails_rev[rev_pos];
      uint8_t *prev_val = uniq_tails.data() + ti0->tail_pos;
      uint32_t prev_val_len = ti0->tail_len;
      uint32_t prev_val_idx = rev_pos;
      while (--rev_pos >= 0) {
        s_no++;
        uniq_tails_info *ti = uniq_tails_rev[rev_pos];
        if (ti->grp_no != 0)
          continue;
        int cmp = compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
        cmp = cmp ? abs(cmp) - 1 : 0;
        uniq_tails_info *prev_ti = uniq_tails_rev[prev_val_idx];
        if (cmp == ti->tail_len) {
          ti->flags |= UTI_FLAG_SUFFIX_FULL;
          ti->link_rev_idx = prev_ti->rev_pos;
          ti->cmp_rev = cmp;
          prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
          if (prev_ti->freq_count < ti->freq_count)
            prev_ti->freq_count = ti->freq_count;
          if (prev_ti->cmp_rev_min > cmp)
            prev_ti->cmp_rev_min = cmp;
        } else {
          prev_val = uniq_tails.data() + ti->tail_pos;
          prev_val_len = ti->tail_len;
          prev_val_idx = rev_pos;
        }
      }
      t = print_time_taken(t, "Time taken for suffix full: ");

      freq_pos--;
      std::sort(uniq_tails_freq.begin() + freq_pos, uniq_tails_freq.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        uint32_t lhs_freq = lhs->freq_count / 10;
        uint32_t rhs_freq = rhs->freq_count / 10;
        return (lhs_freq == rhs_freq) ? lhs->rev_pos > rhs->rev_pos : (lhs_freq > rhs_freq);
      });

      uint32_t sfx_set_len = 0;
      uint32_t sfx_set_max = 64;
      ti0 = uniq_tails_freq[freq_pos];
      prev_val = uniq_tails.data() + ti0->tail_pos;
      prev_val_len = ti0->tail_len;
      prev_val_idx = freq_pos;
      uint32_t savings_full = 0;
      uint32_t savings_partial = 0;
      uint32_t savings_count = 0;
      while (freq_pos < uniq_tails_freq.size()) {
        s_no++;
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->grp_no != 0)
          continue;
        int cmp = 0;
        if (ti->flags & UTI_FLAG_HAS_SUFFIX) {
          sfx_set_len = ti->tail_len + 1;
          cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, sfx_set_len);
          update_current_grp(freq_grp_vec, grp_no, sfx_set_len, ti->freq_count);
          ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
        } else
        if (ti->flags & UTI_FLAG_SUFFIX_FULL) {
          cmp = ti->tail_len;
          update_current_grp(freq_grp_vec, grp_no, 0, ti->freq_count);
          uniq_tails_info *link_ti = uniq_tails_rev[ti->link_rev_idx];
          if (link_ti->tail_ptr == 0 | link_ti->grp_no != grp_no)
            std::cout << "Unexpected: link_ti->tail_ptr = 0: " << std::endl;
          ti->tail_ptr = link_ti->tail_ptr + link_ti->tail_len - ti->tail_len; // + link_ti->cmp_rev;
          savings_full += cmp;
          savings_full++;
          savings_count++;
        } else {
            sfx_set_len = ti->tail_len + 1;
            cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, sfx_set_len);
            update_current_grp(freq_grp_vec, grp_no, sfx_set_len, ti->freq_count);
            ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
          // cmp = compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
          // cmp = cmp ? abs(cmp) - 1 : 0;
          // if (cmp > 1) {
          //   uniq_tails_info *prev_ti = uniq_tails_freq[prev_val_idx];
          //   if (cmp >= prev_ti->cmp_rev_min)
          //     cmp = prev_ti->cmp_rev_min - 1;
          //   uint32_t remain_len = ti->tail_len - cmp;
          //   uint32_t len_len = get_len_len(cmp);
          //   remain_len += len_len;
          //   uint32_t new_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, remain_len);
          //   if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
          //     sfx_set_len += remain_len;
          //     savings_partial += cmp;
          //     savings_partial -= len_len;
          //     savings_count++;
          //     update_current_grp(freq_grp_vec, grp_no, remain_len, ti->freq_count);
          //     ti->link_rev_idx = prev_ti->rev_pos;
          //     ti->flags |= UTI_FLAG_SUFFIX_PARTIAL;
          //     prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
          //     remain_len -= len_len;
          //     ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no, remain_len);
          //     get_len_len(cmp, &grp_tails[grp_no - 1]);
          //   } else {
          //     sfx_set_len = ti->tail_len + 1;
          //     update_current_grp(freq_grp_vec, grp_no, sfx_set_len, ti->freq_count);
          //     ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
          //   }
          //   cur_limit = new_limit;
          // } else {
          //   sfx_set_len = ti->tail_len + 1;
          //   cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, sfx_set_len);
          //   update_current_grp(freq_grp_vec, grp_no, sfx_set_len, ti->freq_count);
          //   ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
          // }
        }
        ti->grp_no = grp_no;
        if (cmp != ti->tail_len) {
          prev_val = uniq_tails.data() + ti->tail_pos;
          prev_val_len = ti->tail_len;
          prev_val_idx = freq_pos - 1;
        }
      }
      printf("Suffix savings: %u, %u, %u\n", savings_full, savings_partial, savings_count);

      vector<uint32_t> freqs;
      for (int i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      huffman<uint32_t> _huffman(freqs);
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        freq_grp& fg = freq_grp_vec[i];
        int len;
        fg.code = (uint8_t) _huffman.get_code(i - 1, len);
        fg.code_len = len;
        printf("%d\t%u\t%u\t%u\t%u\t%2x\t%d\n", (int) fg.grp_log2, fg.grp_limit, fg.count, fg.freq_count, fg.grp_size, fg.code, fg.code_len);
      }
    }

    int append_bits(byte_vec& ptr_str, int last_byte_bits, int bits_to_append, int ptr) {
      int last_idx = ptr_str.size() - 1;
      int remaining_bits = 8 - last_byte_bits;
      while (bits_to_append > 0) {
        ptr_str[last_idx] |= ptr & ((1 << remaining_bits) -1);
        ptr >>= remaining_bits;
        if (bits_to_append > remaining_bits) {
          bits_to_append -= remaining_bits;
          remaining_bits = 8;
        } else {
          remaining_bits -= bits_to_append;
          bits_to_append = 0;
        }
        if (bits_to_append) {
          ptr_str.push_back(0);
          last_idx++;
        }
      }
      last_byte_bits = 8 - remaining_bits;
      return last_byte_bits;
    }

    uint8_t get_tail_ptr(node *cur_node, uniq_tails_info_vec& uniq_tail_vec, byte_vec& uniq_tails,
             vector<freq_grp>& freq_grp_vec, vector<byte_vec>& grp_tails, vector<byte_vec>& grp_tail_ptrs,
             vector<int>& last_byte_bits) {
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
      node_val = huf_code | ((ptr & 0xFF) << huf_code_len);
      // if (grp_no == 2 || grp_no == 3)
      //   std::cout << "Ptr: " << ptr << " " << grp_no << std::endl;
      // if (grp_no == 1 && ptr > (part1 - 1))
      //   std::cout << "ERROR: " << ptr << " > " << part1 << std::endl;
      // if (grp_no == 2 && ptr > (part2 - 1))
      //   std::cout << "ERROR: " << ptr << " > " << part2 << std::endl;
      ptr >>= node_val_bits;
      //  std::cout << ceil(log2(ptr)) << " ";
      last_byte_bits[grp_no] = append_bits(grp_tail_ptrs[grp_no], last_byte_bits[grp_no], ceil(log2(_freq_grp.grp_size)) - node_val_bits, ptr);
      return node_val;
    }

    void build() {
      std::cout << std::endl;
      byte_vec trie;
      uniq_tails_info_vec uniq_tails_fwd;
      std::vector<freq_grp> freq_grp_vec;
      build_tail_maps(uniq_tails, uniq_tails_fwd, uniq_tails_rev, freq_grp_vec, grp_tails, grp_tail_ptrs);
      uint32_t flag_counts[8];
      uint32_t char_counts[8];
      memset(flag_counts, '\0', sizeof(uint32_t) * 8);
      memset(char_counts, '\0', sizeof(uint32_t) * 8);
      uint32_t node_count = 0;
      uint8_t pending_byte = 0;
      vector<int> last_byte_bits;
      //trie.reserve(node_count + (node_count >> 1));
      for (int i = 1; i < freq_grp_vec.size(); i++) {
        byte_vec tail_ptr;
        tail_ptr.push_back(0);
        grp_tail_ptrs.push_back(tail_ptr);
        last_byte_bits.push_back(0);
      }
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *cur_node = cur_lvl_nodes[j];
          uint8_t flags = (cur_node->is_leaf ? 1 : 0) +
            (cur_node->first_child != NULL ? 2 : 0) + (cur_node->tail_len > 1 ? 4 : 0) +
            (cur_node->next_sibling == NULL ? 8 : 0);
          flag_counts[flags & 0x07]++;
          uint8_t node_val;
          if (cur_node->tail_len == 1) {
            node_val = cur_node->v0;
          } else {
            char_counts[(cur_node->tail_len > 7) ? 7 : (cur_node->tail_len-2)]++;
            node_val = get_tail_ptr(cur_node, uniq_tails_rev, uniq_tails, 
                         freq_grp_vec, grp_tails, grp_tail_ptrs,
                         last_byte_bits);
          }
          if ((node_count % 2) == 0) {
            trie.push_back(node_val);
            pending_byte = flags << 4;
          } else {
            trie.push_back(pending_byte | flags);
            trie.push_back(node_val);
            pending_byte = 0;
          }
          node_count++;
          //fwrite(&flags, 1, 1, fout);
          //fwrite(&node_val, 1, 1, fout);
        }
        if ((node_count % 2) == 1) {
          trie.push_back(pending_byte);
        }
      }
      std::cout << std::endl;
      for (int i = 0; i < 8; i++) {
        std::cout << "Flag " << i << ": " << flag_counts[i] << "\t";
        std::cout << "Char " << i + 2 << ": " << char_counts[i] << std::endl;
      }
      // for (int i = 0; i < 8; i++)
      //   std::cout << "Val lens " << i << ": " << val_len_counts[i] << std::endl;
      std::cout << "Trie size: " << trie.size() << std::endl;
      std::cout << "Node count: " << node_count << std::endl;
      uint32_t total_tails = 0;
      for (int i = 0; i < grp_tails.size(); i++) {
        total_tails += grp_tails[i].size();
        std::cout << "Tail" << i << " size: " << grp_tails[i].size() << std::endl;
      }
      uint32_t total_tail_ptrs = 0;
      for (int i = 0; i < grp_tail_ptrs.size(); i++) {
        total_tail_ptrs += grp_tail_ptrs[i].size();
        std::cout << "Tail" << i << " ptr size: " << grp_tail_ptrs[i].size() << std::endl;
      }
      std::cout << "Total tail size: " << total_tails << std::endl;
      std::cout << "Total tail ptr size: " << total_tail_ptrs << std::endl;
      uint32_t total_size = trie.size()
          +(ceil(node_count/320)*11*4) // bit vectoors
          +total_tails // tails
          +total_tail_ptrs;  // tail pointers
      std::cout << "Total size: " << total_size << std::endl;
      std::cout << "Node struct size: " << sizeof(node) << std::endl;
    }

    uint8_t get_first_char(node *n) {
      if (n->tail_len == 1)
        return n->v0;
      uniq_tails_info *ti = uniq_tails_rev[n->rev_node_info_pos];
      uint8_t *tail_data = uniq_tails.data() + ti->tail_pos;
      return tail_data[0];
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
        last_str.append(prev_str.substr(prev_sfx_len));
        ptr += len_len;
        prev_str = last_str;
      }
      ret.append(prev_str.substr(prev_str.length()-sfx_len));
      return ret;
    }

    int lookup(std::string key, vector<node *>& nodes) {
      int key_pos = 0;
      uint8_t key_char = key[key_pos];
      node *cur_node = first_node;
      if (key == std::string("a 20 bomb"))
        key_pos = 0;
      vector<node *> ret;
      ret.push_back(cur_node);
      uint8_t trie_char = get_first_char(cur_node);
      do {
        while (key_char > trie_char) {
          cur_node = cur_node->next_sibling;
          ret[ret.size()-1] = cur_node;
          if (cur_node == NULL)
            return ~INSERT_AFTER;
          trie_char = get_first_char(cur_node);
        }
        if (key_char == trie_char) {
          int cmp;
          if (cur_node->tail_len > 1) {
            uint32_t tail_len;
            std::string tail_str = get_tail_str(cur_node);
            cmp = compare((const uint8_t *) tail_str.c_str(), tail_str.length(),
                    (const uint8_t *) key.c_str() + key_pos, key.size() - key_pos);
            // printf("%d\t%d\t%.*s =========== ", cmp, tail_len, tail_len, tail_data);
            // printf("%d\t%.*s\n", (int) key.size() - key_pos, (int) key.size() - key_pos, key.data() + key_pos);
          } else
            cmp = 0;
          if (cmp == 0 && key_pos + cur_node->tail_len == key.size() && cur_node->is_leaf)
            return 0;
          if (cmp == 0 || abs(cmp) - 1 == cur_node->tail_len) {
            key_pos += cur_node->tail_len;
            cur_node = cur_node->first_child;
            ret.push_back(cur_node);
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
