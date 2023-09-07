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
    uint32_t tail_ptr;
    uint32_t link_rev_idx;
    uint32_t token_arr_pos;
    uint8_t grp_no;
    uint8_t flags;
    uniq_tails_info(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _rev_pos, uint32_t _freq_count) {
      tail_pos = _tail_pos; tail_len = _tail_len;
      rev_pos = _rev_pos;
      cmp_fwd = 0;
      cmp_rev = 0;
      cmp_rev_min = 0xFFFFFFFF;
      cmp_rev_max = 0;
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
struct node {
  uint32_t node_id;
  uint32_t first_child;
  uint32_t next_sibling;
  //node *parent;
  uint32_t tail_pos;
  uint32_t rev_node_info_pos;
  uint32_t tail_len;
  uint8_t flags;
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
};

class builder : public builder_abstract {

  private:
    node root;
    node first_node;
    int node_count;
    int key_count;
    vector<node> all_nodes;
    vector<uint32_t> last_children;
    std::string prev_key;
    //dfox uniq_basix_map;
    //basix uniq_basix_map;
    //art_tree at;

    //builder(builder const&);
    //builder& operator=(builder const&);

    void sort_nodes() {
      clock_t t = clock();
      uint32_t nxt = 0;
      uint32_t node_id = 1;
      std::vector<node> new_all_nodes;
      new_all_nodes.push_back(all_nodes[0]);
      while (node_id < all_nodes.size()) {
        if (new_all_nodes[nxt].first_child == 0) {
          nxt++;
          continue;
        }
        uint32_t nxt_n = new_all_nodes[nxt].first_child;
        new_all_nodes[nxt].first_child = node_id;
        node n = all_nodes[nxt_n];
        do {
          all_nodes[nxt_n].node_id = node_id;
          nxt_n = n.next_sibling;
          n.flags |= (nxt_n == 0 ? NFLAG_TERM : 0);
          n.node_id = node_id++;
          n.next_sibling = (nxt_n == 0 ? 0 : node_id);
          new_all_nodes.push_back(n);
          n = all_nodes[nxt_n];
        } while (nxt_n != 0);
        nxt++;
      }
      all_nodes = new_all_nodes;
      print_time_taken(t, "Time taken for sort_nodes(): ");
      printf("New all_nodes size: %lu\n", all_nodes.size());
    }

    void append_tail_vec(std::string val, uint32_t node_pos) {
      all_nodes[node_pos].tail_pos = sort_tails.size();
      all_nodes[node_pos].tail_len = val.length();
      for (int i = 0; i < val.length(); i++)
        sort_tails.push_back(val[i]);
    }

    bool compareNodeTails(const node *lhs, const node *rhs) const {
      return compare(sort_tails.data() + lhs->tail_pos, lhs->tail_len,
                     sort_tails.data() + rhs->tail_pos, rhs->tail_len);
    }

  public:
    byte_vec sort_tails;
    byte_vec uniq_tails;
    uniq_tails_info_vec uniq_tails_rev;
    std::vector<freq_grp> freq_grp_vec;
    std::vector<byte_vec> grp_tails;
    byte_vec tail_ptrs;
    std::string out_filename;
    builder(const char *out_file) {
      node_count = 0;
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

    void append(string key) {
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
        std::string val((const char *) sort_tails.data() + last_child->tail_pos, last_child->tail_len);
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
      node *n;
    };

    clock_t print_time_taken(clock_t t, const char *msg) {
      t = clock() - t;
      double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
      std::cout << msg << time_taken << std::endl;
      return clock();
    }

    void make_uniq_tails(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_rev, uniq_tails_info_vec& uniq_tails_freq) {
      clock_t t = clock();
      std::vector<tails_sort_data> nodes_for_sort;
      for (int i = 0; i < all_nodes.size(); i++) {
        node *n = &all_nodes[i];
        uint8_t *v = sort_tails.data() + n->tail_pos;
        n->v0 = v[0];
        if (n->tail_len > 1)
          nodes_for_sort.push_back((struct tails_sort_data) { v, n->tail_len, n } );
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

    }

    uint32_t get_var_len(uint32_t len, byte_vec *vec = NULL) {
      uint32_t var_len = (len < 64 ? 2 : (len < 8192 ? 3 : (len < 1024768 ? 4 : 5)));
      if (vec != NULL) {
        vec->push_back(15);
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
                 std::vector<freq_grp>& freq_grp_vec, vector<byte_vec>& grp_tails, byte_vec& tail_ptrs) {
      uniq_tails_info_vec uniq_tails_freq = uniq_tails_rev;
      make_uniq_tails(uniq_tails, uniq_tails_rev, uniq_tails_freq);
      clock_t t = clock();
      uniq_tails_fwd = uniq_tails_rev;
      std::sort(uniq_tails_fwd.begin(), uniq_tails_fwd.end(), [uniq_tails](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return compare(uniq_tails.data() + lhs->tail_pos, lhs->tail_len, uniq_tails.data() + rhs->tail_pos, rhs->tail_len) < 0;
      });
      for (int i = 0; i < uniq_tails_fwd.size(); i++)
        uniq_tails_fwd[i]->fwd_pos = i;
      t = print_time_taken(t, "Time taken for uniq_tails fwd sort: ");

      uint32_t savings_full = 0;
      uint32_t savings_count_full = 0;

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
          savings_full += ti->tail_len;
          savings_count_full++;
        } else {
          if (ti->tail_ptr == 0)
            ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, ti->grp_no);
        }
      }

      freq_pos--;
      uint32_t freq_pos_save = freq_pos;

      // uint32_t freq_pos_save = 0;

      uniq_tails_info *ti0 = uniq_tails_freq[freq_pos];
      uint8_t *prev_val = uniq_tails.data() + ti0->tail_pos;
      uint32_t prev_val_len = ti0->tail_len;
      uint32_t prev_val_idx = freq_pos;
      uint32_t savings_partial = 0;
      uint32_t savings_count_partial = 0;
      while (freq_pos < uniq_tails_freq.size()) {
        s_no++;
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->grp_no != 0)
          continue;
        int cmp = compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
        cmp = cmp ? abs(cmp) - 1 : 0;
        uniq_tails_info *prev_ti = uniq_tails_freq[prev_val_idx];
        if (cmp == ti->tail_len) {
          ti->flags |= UTI_FLAG_SUFFIX_FULL;
          ti->cmp_rev = cmp;
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

      freq_pos = freq_pos_save;
      uint32_t sfx_set_len = 0;
      uint32_t sfx_set_max = 64;
      uint32_t savings_prefix = 0;
      uint32_t savings_count_prefix = 0;
      ti0 = uniq_tails_freq[freq_pos];
      prev_val = uniq_tails.data() + ti0->tail_pos;
      prev_val_len = ti0->tail_len;
      prev_val_idx = freq_pos;
      FILE *fp = fopen("remain.txt", "w+");
      while (freq_pos < uniq_tails_freq.size()) {
        s_no++;
        uniq_tails_info *ti = uniq_tails_freq[freq_pos];
        freq_pos++;
        if (ti->grp_no != 0)
          continue;
        if (ti->flags & UTI_FLAG_SUFFIX_FULL) {
            savings_full += ti->tail_len;
            savings_full++;
            savings_count_full++;
            uniq_tails_info *link_ti = uniq_tails_rev[ti->link_rev_idx];
            update_current_grp(freq_grp_vec, link_ti->grp_no, 0, ti->freq_count);
            ti->tail_ptr = link_ti->tail_ptr + link_ti->tail_len - ti->tail_len;
            ti->grp_no = link_ti->grp_no;
        } else {
          int cmp = compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
          cmp = cmp ? abs(cmp) - 1 : 0;
          uniq_tails_info *prev_ti = uniq_tails_freq[prev_val_idx];
          if (cmp > 1) {
            if (cmp >= ti->cmp_rev_min) // TODO: could reduce cmp_rev_min?
              cmp = ti->cmp_rev_min - 1;
            uint32_t remain_len = ti->tail_len - cmp;
            uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, grp_no, savings_prefix, savings_count_prefix, cmp, fp);
            uint32_t len_len = get_len_len(cmp);
            remain_len += len_len;
            uint32_t new_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, remain_len);
            if (sfx_set_len + remain_len <= sfx_set_max && cur_limit == new_limit) {
              sfx_set_len += remain_len;
              savings_partial += cmp;
              savings_partial -= len_len;
              savings_count_partial++;
              update_current_grp(freq_grp_vec, grp_no, remain_len, ti->freq_count);
              ti->link_rev_idx = prev_ti->rev_pos;
              ti->flags |= UTI_FLAG_SUFFIX_PARTIAL;
              ti->cmp_rev = cmp;
              if (prev_ti->cmp_rev_min > cmp)
                prev_ti->cmp_rev_min = cmp;
              prev_ti->flags |= UTI_FLAG_HAS_SUFFIX;
              remain_len -= len_len;
              ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no, remain_len);
              get_len_len(cmp, &grp_tails[grp_no - 1]);
            } else {
              uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, grp_no, savings_prefix, savings_count_prefix, ti->cmp_rev_max, fp);
              sfx_set_len = ti->tail_len + 1;
              update_current_grp(freq_grp_vec, grp_no, sfx_set_len, ti->freq_count);
              ti->tail_ptr = append_to_grp_tails(uniq_tails, grp_tails, ti, grp_no);
            }
              //printf("%u\t%u\t%u\t%u\t%u\t%u\t%.*s\n", grp_no, ti->cmp_rev, ti->cmp_rev_min, ti->tail_ptr, remain_len, ti->tail_len, ti->tail_len, uniq_tails.data() + ti->tail_pos);
            cur_limit = new_limit;
          } else {
            uint32_t prefix_len = find_prefix(uniq_tails, uniq_tails_fwd, ti, grp_no, savings_prefix, savings_count_prefix, ti->cmp_rev_max, fp);
            sfx_set_len = ti->tail_len + 1;
            cur_limit = check_next_grp(freq_grp_vec, grp_no, cur_limit, sfx_set_len);
            update_current_grp(freq_grp_vec, grp_no, sfx_set_len, ti->freq_count);
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
      fclose(fp);
      printf("Savings full: %u, %u\n", savings_full, savings_count_full);
      printf("Savings partial: %u, %u\n", savings_partial, savings_count_partial);
      printf("Savings prefix: %u, %u\n", savings_prefix, savings_count_prefix);

      vector<uint32_t> freqs;
      for (int i = 1; i < freq_grp_vec.size(); i++)
        freqs.push_back(freq_grp_vec[i].freq_count);
      huffman<uint32_t> _huffman(freqs);
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
      uint32_t limit = ti->fwd_pos + 30;
      if (limit > uniq_tails_fwd.size())
        limit = uniq_tails_fwd.size();
      for (uint32_t i = ti->fwd_pos; i < limit; i++) {
        uniq_tails_info *ti_fwd = uniq_tails_fwd[i];
        int cmp = compare(uniq_tails.data() + ti_fwd->tail_pos, ti_fwd->tail_len, uniq_tails.data() + ti->tail_pos, ti->tail_len);
        cmp = abs(cmp) - 1;
        if (cmp > ti->tail_len - cmp_sfx)
          cmp = ti->tail_len - cmp_sfx;
        if (cmp > 4 && grp_no == ti_fwd->grp_no && ti_fwd->cmp_fwd == 0) {
          savings_prefix += cmp;
          savings_prefix -= 4;
          savings_count_prefix++;
          ti->cmp_fwd = cmp;
          ti_fwd->cmp_fwd = cmp;
          uint32_t remain_len = ti->tail_len - cmp_sfx - cmp;
          fprintf(fp, "[%.*s]%.*s[%.*s]\n", cmp, uniq_tails.data() + ti->tail_pos, 
                  remain_len, uniq_tails.data() + ti->tail_pos + cmp,
                  ti->tail_len - remain_len - cmp, uniq_tails.data() + ti->tail_pos + cmp + remain_len);
          return cmp;
        }
      }
      fprintf(fp, "%.*s[%.*s]\n", ti->tail_len - cmp_sfx, uniq_tails.data() + ti->tail_pos, cmp_sfx, uniq_tails.data() + ti->tail_pos + ti->tail_len - cmp_sfx);
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
             vector<freq_grp>& freq_grp_vec, vector<byte_vec>& grp_tails, byte_vec& tail_ptrs,
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
      // if (grp_no == 2 || grp_no == 3)
      //   std::cout << "Ptr: " << ptr << " " << grp_no << std::endl;
      // if (grp_no == 1 && ptr > (part1 - 1))
      //   std::cout << "ERROR: " << ptr << " > " << part1 << std::endl;
      // if (grp_no == 2 && ptr > (part2 - 1))
      //   std::cout << "ERROR: " << ptr << " > " << part2 << std::endl;
      ptr >>= node_val_bits;
      //  std::cout << ceil(log2(ptr)) << " ";
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
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if (cur_node->tail_len > 1) {
            uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
            if (ti->flags & UTI_FLAG_SUFFIX_FULL)
              sfx_full_count++;
          }
          cur_node->node_id = node_count + 1;
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
            char_counts[(cur_node->tail_len > 7) ? 7 : (cur_node->tail_len-2)]++;
            node_val = get_tail_ptr(cur_node, uniq_tails_rev, uniq_tails, 
                         freq_grp_vec, grp_tails, tail_ptrs, last_byte_bits);
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
      uint32_t total_tails = 0;
      for (int i = 0; i < grp_tails.size(); i++) {
        total_tails += grp_tails[i].size();
        std::cout << "Tail" << i << " size: " << grp_tails[i].size() << std::endl;
      }
      std::cout << "Total pointer count: " << ptr_count << std::endl;
      std::cout << "Full suffix count: " << sfx_full_count << std::endl;
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

      FILE *fp = fopen(out_filename.c_str(), "wb+");
      fputc(0xA5, fp); // magic byte
      fputc(0x01, fp); // version 1.0
      uint32_t grp_tails_loc = 2 + 9 * 4; // 26
      uint32_t grp_tails_size = 513 // group_count + huffman lookup table
                       + grp_tails.size() * 4 // tail_locations
                       + total_tails;
      uint32_t cache_loc = grp_tails_loc + grp_tails_size;
      uint32_t ptr_lookup_tbl_loc = cache_loc + cache_size;
      uint32_t trie_bv_loc = ptr_lookup_tbl_loc + ptr_lookup_tbl;
      uint32_t leaf_bv_loc = trie_bv_loc + trie_bv;
      uint32_t select_lkup_loc = leaf_bv_loc + leaf_bv;
      uint32_t tail_ptrs_loc = select_lkup_loc + select_lookup;
      uint32_t trie_loc = tail_ptrs_loc + tail_ptrs.size();
      printf("%u,%u,%u,%u,%u,%u,%u,%u\n", node_count, cache_loc, ptr_lookup_tbl_loc, trie_bv_loc, leaf_bv_loc, select_lkup_loc, tail_ptrs_loc, trie_loc);
      write_uint32(node_count, fp);
      write_uint32(max_tail_len, fp);
      write_uint32(cache_loc, fp);
      write_uint32(ptr_lookup_tbl_loc, fp);
      write_uint32(trie_bv_loc, fp);
      write_uint32(leaf_bv_loc, fp);
      write_uint32(select_lkup_loc, fp);
      write_uint32(tail_ptrs_loc, fp);
      write_uint32(trie_loc, fp);
      write_grp_tails(freq_grp_vec, grp_tails, grp_tails_loc + 513, fp); // group count, 512 lookup tbl, tail locs, tails
      //write_cache(fp, cache_size);
      fwrite(trie.data(), cache_size, 1, fp);
      write_ptr_lookup_tbl(freq_grp_vec, uniq_tails_rev, fp);
      write_trie_bv(fp);
      write_leaf_bv(fp);
      write_select_lkup(fp);
      fwrite(tail_ptrs.data(), tail_ptrs.size(), 1, fp);
      fwrite(trie.data(), trie.size(), 1, fp);
      fclose(fp);

      fp = fopen("nodes.txt", "wb+");
      //dump_nodes(first_node, fp);
      find_rpt_nodes(fp);
      fclose(fp);

      return out_filename;

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
      return sort_tails.data() + n->tail_pos;
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
/*
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
            // cmp = compare(tail_str, tail_len, 
            std::string tail_str = get_tail_str(cur_node);
            cmp = compare((const uint8_t *) tail_str.c_str(), tail_str.length(),
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
*/
    uniq_tails_info *get_ti(node *n) {
      return uniq_tails_rev[n->rev_node_info_pos];
    }

    uint32_t get_ptr(node *cur_node) {
      uniq_tails_info *ti = uniq_tails_rev[cur_node->rev_node_info_pos];
      return ti->tail_ptr;
    }

    struct nodes_for_grp {
      uint32_t start_node_id;
      uint8_t len;
      int32_t node_grp_id;
    };

    uint32_t get_node_val(node *n) {
      uniq_tails_info *n1 = uniq_tails_rev[n->rev_node_info_pos];
      return n->tail_len > 1 ? (n1->grp_no << 28) + n1->tail_ptr : n->v0;
    }

    void find_rpt_nodes(FILE *fp) {
      clock_t t = clock();
      // std::sort(all_nodes.begin(), all_nodes.end(), [this](const node *lhs, const node *rhs) -> bool {
      //   return lhs->node_id < rhs->node_id;
      // });
      // t = print_time_taken(t, "Time taken to sort all_nodes: ");
      std::vector<nodes_for_grp> for_node_grp;
      for (int i = 1; i < all_nodes.size(); i++) {
          node *cur_node = &all_nodes[i];
          if (cur_node->flags & NFLAG_TERM)
            continue;
          node *next_node = &all_nodes[i + 1];
          if (cur_node->first_child == 0 && next_node->first_child == 0) {
            for_node_grp.push_back((nodes_for_grp) {cur_node->node_id, 2, 0});
          }
      }
      t = print_time_taken(t, "Time taken to push to for_node_grp: ");
      std::sort(for_node_grp.begin(), for_node_grp.end(), [this](const struct nodes_for_grp& lhs, const struct nodes_for_grp& rhs) -> bool {
        node *lhs_node1 = &all_nodes[lhs.start_node_id];
        node *lhs_node2 = &all_nodes[lhs.start_node_id + 1];
        node *rhs_node1 = &all_nodes[rhs.start_node_id];
        node *rhs_node2 = &all_nodes[rhs.start_node_id + 1];
        return get_node_val(lhs_node1) == get_node_val(rhs_node1) ? 
                (get_node_val(lhs_node2) < get_node_val(rhs_node2)) :
                (get_node_val(lhs_node1) < get_node_val(rhs_node1));
      });
      t = print_time_taken(t, "Time taken to sort for_node_grp: ");
      fprintf(fp, "Total node grps: %lu\n", for_node_grp.size());
      node *prev_node = &all_nodes[for_node_grp[0].start_node_id];
      uint32_t count = 0;
      for (int i = 0; i < for_node_grp.size(); i++) {
        if (get_node_val(&all_nodes[for_node_grp[i].start_node_id]) == get_node_val(prev_node)) {
          count++;
        } else {
          node *node1 = &all_nodes[for_node_grp[i].start_node_id];
          node *node2 = &all_nodes[for_node_grp[i].start_node_id + 1];
          uniq_tails_info *ti1 = uniq_tails_rev[node1->rev_node_info_pos];
          uniq_tails_info *ti2 = uniq_tails_rev[node2->rev_node_info_pos];
          fprintf(fp, "%010u\t", count);
          if (node1->tail_len == 1)
            fprintf(fp, "[%c]\t", node1->v0);
          else
            fprintf(fp, "[%d][%u]\t", ti1->grp_no, ti1->tail_ptr);
          if (node2->tail_len == 1)
            fprintf(fp, "[%c]\n", node2->v0);
          else
            fprintf(fp, "[%d][%u]\n", ti2->grp_no, ti2->tail_ptr);
          count = 0;
          prev_node = &all_nodes[for_node_grp[i].start_node_id];
        }
      }
      t = print_time_taken(t, "Time taken to push to print for_node_grp: ");
      
    }

/*
    uint8_t get_node_val(node *n) {
      uint8_t node_val;
      uniq_tails_info *ti = uniq_tails_rev[n->rev_node_info_pos];
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
      return node_val;
    }

    void dump_nodes(node *n, FILE *fp) {
      if (n == NULL)
        return;
      int count = 0;
      int ptr_count = 0;
      int bit_count = 0;
      node *n1 = n;
      bool no_child = true;
      while (n1 != NULL) {
        node *ch = n1->first_child;
        if (n1->first_child != NULL)
          no_child = false;
        if (n1->is_leaf && n1->next_sibling == NULL && ch != NULL && ch->next_sibling == NULL && ch->first_child == NULL) {
          no_child = true;
        }
        if (n1->tail_len > 1) {
          uniq_tails_info *ti = uniq_tails_rev[n1->rev_node_info_pos];
          bit_count += freq_grp_vec[ti->grp_no].grp_log2;
          ptr_count++;
        }
        n1 = n1->next_sibling;
        if (ch != NULL)
          n1 = ch;
        count++;
      }
      // if (no_child && ptr_count > 0 && count > 1)
        fprintf(fp, "\t%d\t%d\t", count, bit_count);
      n1 = n;
      while (n1 != NULL) {
        uint8_t node_val;
        uniq_tails_info *ti = uniq_tails_rev[n1->rev_node_info_pos];
          uint8_t flags = (n1->is_leaf ? 1 : 0) +
            (n1->first_child != NULL ? 2 : 0) + (n1->tail_len > 1 ? 4 : 0) +
            (n1->next_sibling == NULL ? 8 : 0);
        node *ch = n1->first_child;
        // if (no_child && ptr_count > 0 && count > 1)
          fprintf(fp, "[%1x]", flags);
        if (n1->tail_len == 1) {
          node_val = n1->v0;
          // if (no_child && ptr_count > 0 && count > 1)
            fputc(node_val, fp);
        } else {
          node_val = get_node_val(n);
          //node_val = get_first_char(n1);
          // if (no_child && ptr_count > 0 && count > 1)
            fprintf(fp, "<%02u%08u>", ti->grp_no, ti->tail_ptr);
        }
        // if (ch != NULL && no_child && ptr_count > 0 && count > 1) {
        //   flags = (ch->is_leaf ? 1 : 0) +
        //     (ch->first_child != NULL ? 2 : 0) + (ch->tail_len > 1 ? 4 : 0) +
        //     (ch->next_sibling == NULL ? 8 : 0);
        //   fprintf(fp, "[%1x]", flags);
        //   if (ch->tail_len == 1)
        //     fputc(ch->v0, fp);
        //   else {
        //     uniq_tails_info *ti_ch = uniq_tails_rev[ch->rev_node_info_pos];
        //     fprintf(fp, "<%02u%08u>", ti_ch->grp_no, ti_ch->tail_ptr);
        //   }
        // }
        n1 = n1->next_sibling;
        // dump_nodes(n1->first_child, fp);
        // n1 = n1->next_sibling;
      }
      // if (count > 10)
      // if (no_child && ptr_count > 0 && count > 1)
        fputc('\n', fp);
      n1 = n;
      while (n1 != NULL) {
        dump_nodes(n1->first_child, fp);
        n1 = n1->next_sibling;
      }
    }

    struct sort_nodes {
      node *start_node;
      node *common_node;
      int count;
      int cmp;
    };

    int compare_nodes(sort_nodes& sn1, sort_nodes& sn2) {
      int lim = (sn2.count < sn1.count ? sn2.count : sn1.count);
      do {
        k++;
        node *n1 = sn1.start_node;
        node *n2 = sn2.start_node;
        uint32_t v1 = n1->tail_len > 1 ? (n1->grp_no << 28) + n1->tail_ptr : n1->v0;
        uint32_t v2 = n2->tail_len > 1 ? (n2->grp_no << 28) + n2->tail_ptr : n2->v0;
        if (v1 < v2)
          return -k;
        else if (v1 > v2)
          return k;
      } while (k < lim);
      if (sn1.count == sn2.count)
        return 0;
      k++;
      return (sn1.count < sn2.count ? -k : k);
    }

    void find_rpt_nodes(FILE *fp) {
      vector<sort_nodes> nodes_for_sort;
      add_to_sort_nodes(first_node, nodes_for_sort);
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this](const struct sort_nodes& lhs, const struct sort_nodes& rhs) -> bool {
        int cmp = this->compare_nodes(lhs, rhs);
        if (cmp > lhs.cmp) {
          lhs.cmp = cmp;
          if (lhs.count < rhs.count)
            lhs.other_node = rhs.start_node;
        }
        if (cmp > rhs.cmp)
          rhs.cmp = cmp;
        return cmp < 0;
      });
    }

    void add_to_sort_nodes(node* n, vector<sort_nodes>& nodes_for_sort) {
      if (n == NULL)
        return;
      node *n1 = n;
      while (n1 != NULL) {
        if (n1->next_sibling != NULL && n1->next_sibling->first_child == NULL) {
          nodes_for_sort.push_back((sort_nodes) {n, NULL, 0, 0});
          int node_pos = nodes_for_sort.size();
          sort_nodes *n2;
          do {
            node_pos--;
            n2 = &nodes_for_sort[node_pos];
            n2->count++;
          } while (n2->start_node != n);
        }
        n1 = n1->next_sibling;
      }
      n1 = n;
      while (n1 != NULL) {
        add_to_sort_nodes(n1->first_child, nodes_for_sort);
        n1 = n1->next_sibling;
      }
    }
    */

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
