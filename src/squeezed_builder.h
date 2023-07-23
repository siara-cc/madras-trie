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

namespace squeezed {

class uniq_tails_info {
  public:
    uint32_t tail_pos;
    uint32_t tail_len;
    uint32_t fwd_pos;
    uint32_t rev_pos;
    uint32_t cmp_fwd;
    uint32_t cmp_rev;
    union {
      uint32_t freq_count;
    };
      uint32_t tail_ptr;
    uint32_t link_fwd_idx;
    uint8_t grp_no;
    uint8_t flags;
    uniq_tails_info(uint32_t _tail_pos, uint32_t _tail_len, uint32_t _fwd_pos, uint32_t _cmp_fwd, uint32_t _freq_Count) {
      tail_pos = _tail_pos; tail_len = _tail_len;
      fwd_pos = _fwd_pos; cmp_fwd = _cmp_fwd;
      freq_count = _freq_Count;
      link_fwd_idx = 0xFFFFFFFF;
      tail_ptr = 0;
      grp_no = 0;
      flags = 0;
    }
};

typedef std::map<std::string, uint64_t> tail_map;
typedef std::multimap<uint32_t, std::string> tail_freq_map;
typedef std::map<uint32_t, std::string> tail_link_map;
typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uniq_tails_info *> uniq_tails_info_vec;

class builder_abstract {
  public:
    std::vector<uint8_t> tails;
};

class node {
  public:
    node *first_child;
    //node *parent;
    node *next_sibling;
    union {
      uint32_t tail_pos;
    };
      uint32_t fwd_node_info_pos;
    uint32_t tail_len;
    uint8_t is_leaf;
    uint8_t level;
    uint8_t v0;
    node() {
      //parent = NULL;
      first_child = next_sibling = NULL;
      is_leaf = 0;
      level = 0;
      tail_pos = 0;
      tail_len = 0;
      fwd_node_info_pos = 0;
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
      n->tail_pos = tails.size();
      n->tail_len = val.length();
      for (int i = 0; i < val.length(); i++)
        tails.push_back(val[i]);
    }

    bool compareNodeTails(const node *lhs, const node *rhs) const {
      return compare(tails.data() + lhs->tail_pos, lhs->tail_len,
                     tails.data() + rhs->tail_pos, rhs->tail_len);
    }

  public:
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
        std::string val((const char *) tails.data() + last_child->tail_pos, last_child->tail_len);
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

    uint32_t make_uniq_tails(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd) {
      clock_t t = clock();
      uint32_t total_ptrs = 0;
      std::vector<tails_sort_data> nodes_for_sort;
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *n = cur_lvl_nodes[j];
          if (n->tail_len > 1) {
            total_ptrs++;
            nodes_for_sort.push_back((struct tails_sort_data) { tails.data() + n->tail_pos, n->tail_len, n } );
          }
        }
      }
      t = print_time_taken(t, "Time taken for adding to nodes_for_sort: ");
      std::sort(nodes_for_sort.begin(), nodes_for_sort.end(), [this](const struct tails_sort_data& lhs, const struct tails_sort_data& rhs) -> bool {
        return compare(lhs.tail_data, lhs.tail_len, rhs.tail_data, rhs.tail_len) < 0;
      });
      t = print_time_taken(t, "Time taken to sort: ");
      uint32_t vec_pos = 0;
      uint32_t freq_count = 0;
      std::vector<tails_sort_data>::iterator it = nodes_for_sort.begin();
      uint8_t *prev_val = it->tail_data;
      uint32_t prev_val_len = it->tail_len;
      uniq_tails_info *ti_ptr = new uniq_tails_info((uint32_t) uniq_tails.size(), prev_val_len, vec_pos, 0, freq_count);
      while (it != nodes_for_sort.end()) {
        it->n->fwd_node_info_pos = uniq_tails_fwd.size();
        int cmp = compare(it->tail_data, it->tail_len, prev_val, prev_val_len);
        if (cmp == 0) {
          freq_count++;
        } else {
          ti_ptr->freq_count = freq_count;
          uniq_tails_fwd.push_back(ti_ptr);
          for (int i = 0; i < prev_val_len; i++)
            uniq_tails.push_back(prev_val[i]);
          freq_count = 1;
          prev_val = it->tail_data;
          prev_val_len = it->tail_len;
          ti_ptr = new uniq_tails_info((uint32_t) uniq_tails.size(), prev_val_len, ++vec_pos, 0, freq_count);
        }
        it++;
      }
      ti_ptr->freq_count = freq_count;
      uniq_tails_fwd.push_back(ti_ptr);
      for (int i = 0; i < prev_val_len; i++)
        uniq_tails.push_back(prev_val[i]);
      t = print_time_taken(t, "Time taken for uniq_tails_fwd: ");
      return total_ptrs;
    }

    const static uint32_t part1 = 128;
    const static uint32_t part2 = 4096;
    const static uint32_t part3 = (1 << 31);
    void build_tail_maps(byte_vec& uniq_tails, uniq_tails_info_vec& uniq_tails_fwd, uniq_tails_info_vec& uniq_tails_rev) {
      uint32_t total_ptrs = make_uniq_tails(uniq_tails, uniq_tails_fwd);
      clock_t t = clock();
      uniq_tails_rev = uniq_tails_fwd;
      std::sort(uniq_tails_rev.begin(), uniq_tails_rev.end(), [uniq_tails](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return compare_rev(uniq_tails.data() + lhs->tail_pos, lhs->tail_len, uniq_tails.data() + rhs->tail_pos, rhs->tail_len) < 0;
      });
      int i = uniq_tails_rev.size() - 1;
      uniq_tails_info *ti0 = uniq_tails_rev[i];
      uint8_t *prev_val = uniq_tails.data() + ti0->tail_pos;
      uint32_t prev_val_len = ti0->tail_len;
      uint32_t prev_val_idx = i;
      ti0->rev_pos = i;
      ti0->cmp_rev = 0;
      // uint32_t savings = 0;
      // FILE *fp = fopen("suffix_match.txt", "w+");
      while (--i >= 0) {
        uniq_tails_info *ti = uniq_tails_rev[i];
        int cmp = compare_rev(uniq_tails.data() + ti->tail_pos, ti->tail_len, prev_val, prev_val_len);
        cmp = cmp ? abs(cmp) - 1 : 0;
        ti->rev_pos = i;
        ti->cmp_rev = cmp;
        if (cmp == ti->tail_len) {
          ti->link_fwd_idx = uniq_tails_rev[prev_val_idx]->fwd_pos;
          // fprintf(fp, "%d\t[%.*s]\t[%.*s]\n", cmp, ti->tail_len, uniq_tails.data() + ti->tail_pos,
          //                 prev_val_len, prev_val);
          // savings += ti->tail_len;
          // savings++;
        } else {
          prev_val = uniq_tails.data() + ti->tail_pos;
          prev_val_len = ti->tail_len;
          prev_val_idx = i;
        }
      }
      // fprintf(fp, "Savings: %u\n", savings);
      // fclose(fp);
      t = print_time_taken(t, "Time taken for uniq_tails rev: ");
      uniq_tails_info_vec uniq_tails_freq = uniq_tails_fwd;
      std::sort(uniq_tails_freq.begin(), uniq_tails_freq.end(), [this](const struct uniq_tails_info *lhs, const struct uniq_tails_info *rhs) -> bool {
        return lhs->freq_count > rhs->freq_count;
      });
      t = print_time_taken(t, "Time taken for uniq_tails freq: ");
      uint32_t s_no = 0;
      uint32_t grp_no = 1;
      uint32_t grp_tails_len = 0;
      uint32_t grp_tails_count = 0;
      uint32_t cur_limit = part1;
      uint32_t total_tails_len = 0;
      uint32_t tails_freq_count = 0;
      for (int i = 0; i < uniq_tails_freq.size(); i++) {
        s_no++;
        uniq_tails_info *ti = uniq_tails_freq[i];
        if (ti->grp_no == 0) {
          if (cur_limit == part3 && ti->link_fwd_idx != 0xFFFFFFFF) {
            grp_tails_count++;
            tails_freq_count += ti->freq_count;
            ti->grp_no = grp_no;
            continue;
          }
          if (cur_limit < part3 && ti->link_fwd_idx != 0xFFFFFFFF) {
            if (ti->grp_no == 0)
              ti->link_fwd_idx = 0xFFFFFFFF;
            else
              continue;
          }
          if (cur_limit < part1) {
            printf("%u\t%u\t%u\t[%.*s]\t0\t[%.*s]\n", s_no, grp_tails_count, ti->freq_count, ti->tail_len, uniq_tails.data() + ti->tail_pos,
                    ti->link_fwd_idx == 0xFFFFFFFF ? 1 : uniq_tails_fwd[ti->link_fwd_idx]->tail_len,
                    ti->link_fwd_idx == 0xFFFFFFFF ? "-" : (const char *) uniq_tails.data() + uniq_tails_fwd[ti->link_fwd_idx]->tail_pos);
          }
          if ((grp_tails_len + ti->tail_len + 1) > cur_limit) {
            std::cout << log2(cur_limit) << "\t" << cur_limit << "\t" << grp_tails_count << "\t" << tails_freq_count << "\t" << grp_tails_len << std::endl;
            cur_limit = (cur_limit == part1 ? part2 : part3);
            total_tails_len += grp_tails_len;
            grp_tails_len = 0;
            grp_tails_count = 0;
            tails_freq_count = 0;
            grp_no++;
          }
          grp_tails_len += ti->tail_len;
          grp_tails_len++;
          grp_tails_count++;
          tails_freq_count += ti->freq_count;
          ti->grp_no = grp_no;
          if (ti->tail_len > 2 && cur_limit < part1) {
            uint8_t *val = uniq_tails.data() + ti->tail_pos;
            for (int j = ti->rev_pos - 1; j > 0; j--) {
              uniq_tails_info *ti_rev = uniq_tails_rev[j];
              if (ti_rev->grp_no != 0 && ti_rev->grp_no != ti->grp_no)
                continue;
              if (ti_rev->tail_len >= ti->tail_len)
                continue;
              int cmp = compare_rev(val, ti->tail_len, uniq_tails.data() + ti_rev->tail_pos, ti_rev->tail_len);
              if (cmp == 2 || cmp == 1)
                break;
              else {
                if ((cmp - 1) == ti_rev->tail_len) {
                  if (ti_rev->link_fwd_idx == 0xFFFFFFFF || ti_rev->grp_no == 0) {
                    if (cur_limit < part1) {
                      printf("%u\t%u*\t%u\t[%.*s]\t0\t[%.*s]\t%s\n", s_no, grp_tails_count, ti_rev->freq_count, ti_rev->tail_len, uniq_tails.data() + ti_rev->tail_pos,
                              ti->tail_len, val, (ti_rev->freq_count > ti->freq_count ? "**" : ""));
                    }
                    grp_tails_count++;
                    tails_freq_count += ti_rev->freq_count;
                    if (ti_rev->link_fwd_idx == 0xFFFFFFFF && ti_rev->grp_no != 0) {
                      grp_tails_len -= ti_rev->tail_len;
                      grp_tails_len--;
                      tails_freq_count -= ti_rev->freq_count;
                      grp_tails_count--;
                    }
                    ti_rev->link_fwd_idx = ti->fwd_pos;
                    ti_rev->grp_no = ti->grp_no;
                  } else {
                    if (uniq_tails_fwd[ti_rev->link_fwd_idx]->tail_len < ti->tail_len)
                      ti_rev->link_fwd_idx = ti->fwd_pos;
                  }
                // } else {
                //   if ((cmp - 1) > ti_rev->tail_len)
                //     break;
                }
              }
            }
          }
        } else {
          if (cur_limit < part1)
            printf("%u*\t%u*\t%u\t[%.*s]\t0\t[1]\n", s_no, grp_tails_count, ti->freq_count, ti->tail_len, uniq_tails.data() + ti->tail_pos);
        }
        ti->tail_ptr = 0;
      }
      std::cout << log2(cur_limit) << "\t" << cur_limit << "\t" << grp_tails_count << "\t" << tails_freq_count << "\t" << grp_tails_len << std::endl;
      // 1. build freq sort map - done
      // 2. assign rev sort idx - done
      // 3. Go through uniq_tails_freq desc, assign initial
      //    3.1 Determine shape
      //    3.2 Determine number of sections
      //    3.3 Don't do greedy suffix matching for 10% of uniq_len or until last chunk reached
      // 4. Match suffixes for rest
      // 5. Match prefixes or suffixes for rest
      // 6. Add rest to dict_fwd map and sort
      //    6.1 Copy to dict_rev map and sort
      //    6.2 Find longest prefix/suffix and add to freq_dict
      //    6.3 Add encoded tail with pointers to uniq_tails and link it to uniq_tails_fwd
      // 7. Discard dict_fwd and dict_rev map, crop special characters and repeat step 6 until no more
      // 8. Go through freq_dict desc, make dict_fwd and dict_rev
      // 9. Match suffixes, prefixes and link them to dict_fwd
      // 10. Go through freq_dict desc, form dict_64, dict_8192, dict_1m, dict_128m byte_vecs, assign ptrs
      // 11. Go through encoded strings in uniq_tails_fwd and assign ptrs from dict
      // 12. Go through uniq_tails_freq form section tails, assign sections
    }

    int append_bits(std::vector<uint8_t>& ptr_str, std::vector<uint32_t>& tail_ptr_counts, int addl_bit_count, bool is_next, int last_byte_bits, int ptr) {
      if (is_next)
        tail_ptr_counts.push_back(1);
      else {
        int last_idx = tail_ptr_counts.size() - 1;
        tail_ptr_counts[last_idx]++;
      }
      if (addl_bit_count == 0)
        return last_byte_bits;
      if (is_next) {
        ptr_str.push_back(0);
        last_byte_bits = 0;
      }
      int last_idx = ptr_str.size() - 1;
      int remaining_bits = 8 - last_byte_bits;
      int bits_to_append = addl_bit_count;
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
             std::vector<uint8_t>& tails0, std::vector<uint8_t>& tails1, std::vector<uint8_t>& tails2,
             std::vector<uint8_t>& tail_ptrs1, std::vector<uint8_t>& tail_ptrs2,
             uint32_t& tail_ptr_counts0, std::vector<uint32_t>& tail_ptr_counts1, std::vector<uint32_t>& tail_ptr_counts2,
             int& addl_bit_count1, int& last_byte_bits1,
             int& addl_bit_count2, int& last_byte_bits2) {
      uint8_t node_val;
      uniq_tails_info *ti = uniq_tail_vec[cur_node->fwd_node_info_pos];
      uint32_t ptr = 0;
      uint8_t grp_no = ti->grp_no;
      if (grp_no == 0)
        printf("ERROR: not marked: [%.*s]\n", ti->tail_len, uniq_tails.data() + ti->tail_pos);
      std::vector<uint8_t>& tails = (grp_no == 1 ? tails0 : (grp_no == 2 ? tails1 : tails2));
      std::vector<uint8_t>& tail_ptrs = (grp_no == 1 ? tail_ptrs1 : (grp_no == 2 ? tail_ptrs1 : tail_ptrs2));
      int& addl_bit_count = (grp_no == 1 ? addl_bit_count1 : (grp_no == 2 ? addl_bit_count1 : addl_bit_count2));
      int& last_byte_bits = (grp_no == 1 ? last_byte_bits1 : (grp_no == 2 ? last_byte_bits1 : last_byte_bits2));
      std::vector<uint32_t>& tail_ptr_counts = (grp_no == 1 ? tail_ptr_counts1 : (grp_no == 2 ? tail_ptr_counts1 : tail_ptr_counts2));
      if (ti->tail_ptr == 0) {
        if (ti->link_fwd_idx == 0xFFFFFFFF) {
          ptr = tails.size();
          for (int k = 0; k < ti->tail_len; k++)
            tails.push_back(uniq_tails[ti->tail_pos + k]);
          tails.push_back(0);
        } else {
          uniq_tails_info *ti_link = uniq_tail_vec[ti->link_fwd_idx];
          if (ti_link->grp_no != grp_no)
            printf("WARN: mismatch grp %u, [%.*s], [%.*s]\n", grp_no, ti->tail_len, uniq_tails.data() + ti->tail_pos, ti_link->tail_len, uniq_tails.data() + ti_link->tail_pos);
          if (ti_link->tail_ptr == 0) {
            ti_link->tail_ptr = tails.size();
            ptr = tails.size() + ti_link->tail_len - ti->tail_len;
            for (int k = 0; k < ti_link->tail_len; k++)
              tails.push_back(uniq_tails[ti_link->tail_pos + k]);
            tails.push_back(0);
          } else {
            ptr = ti_link->tail_ptr + ti_link->tail_len - ti->tail_len;
          }
        }
        ti->tail_ptr = ptr;
      } else {
        ptr = ti->tail_ptr;
      }
      int node_val_bits = (grp_no == 1 ? 7 : 6);
      node_val = ptr & ((1 << node_val_bits) - 1);
      node_val |= (grp_no << 6);
      // if (grp_no == 2 || grp_no == 3)
      //   std::cout << "Ptr: " << ptr << " " << grp_no << std::endl;
      if (grp_no == 1 && ptr > (part1 - 1))
        std::cout << "ERROR: " << ptr << " > " << part1 << std::endl;
      if (grp_no == 2 && ptr > (part2 - 1))
        std::cout << "ERROR: " << ptr << " > " << part2 << std::endl;
      ptr >>= node_val_bits;
      //  std::cout << ceil(log2(ptr)) << " ";
      if (grp_no == 1) {
        tail_ptr_counts0++;
      } else {
        if (ptr >= (1 << addl_bit_count)) {
          std::cout << "Tail ptr: " << ptr << " " << grp_no << std::endl;
          addl_bit_count++;
          last_byte_bits = append_bits(tail_ptrs, tail_ptr_counts, addl_bit_count, true, 0, ptr);
        } else {
          last_byte_bits = append_bits(tail_ptrs, tail_ptr_counts, addl_bit_count, false, last_byte_bits, ptr);
        }
      }
      return node_val;
    }

    void build() {
      std::cout << std::endl;
      byte_vec trie, tails0, tails1, tails2;
      byte_vec tail_ptrs1, tail_ptrs2;
      byte_vec uniq_tails;
      uniq_tails_info_vec uniq_tails_fwd, uniq_tails_rev;
      build_tail_maps(uniq_tails, uniq_tails_fwd, uniq_tails_rev);
      uint32_t tail_ptr_counts0;
      std::vector<uint32_t> tail_ptr_counts1, tail_ptr_counts2;
      //std::vector<uint8_t> tails;
      uint32_t flag_counts[8];
      memset(flag_counts, '\0', sizeof(uint32_t) * 8);
      uint32_t node_count = 0;
      uint8_t pending_byte = 0;
      int addl_bit_count1 = 0;
      int last_byte_bits1 = 0;
      int addl_bit_count2 = 0;
      int last_byte_bits2 = 0;
      trie.reserve(node_count + (node_count >> 1));
      tail_ptr_counts0 = 0;
      tail_ptr_counts1.push_back(0);
      tail_ptr_counts2.push_back(0);
      tail_ptrs1.push_back(0);
      tail_ptrs2.push_back(0);
      tails0.push_back(0); // waste 1 byte
      tails1.push_back(0); // waste 1 byte
      tails2.push_back(0); // waste 1 byte
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *cur_node = cur_lvl_nodes[j];
          std::string val((const char *) tails.data() + cur_node->tail_pos, cur_node->tail_len);
          uint8_t flags = (cur_node->is_leaf ? 1 : 0) +
            (cur_node->first_child != NULL ? 2 : 0) + (val.length() > 1 ? 4 : 0) +
            (cur_node->next_sibling == NULL ? 8 : 0);
          flag_counts[flags & 0x07]++;
          uint8_t node_val;
          if (val.length() == 1) {
            node_val = val[0];
          } else {
            node_val = get_tail_ptr(cur_node, uniq_tails_fwd, uniq_tails, 
                         tails0, tails1, tails2,
                         tail_ptrs1, tail_ptrs2, 
                         tail_ptr_counts0, tail_ptr_counts1, tail_ptr_counts2,
                         addl_bit_count1, last_byte_bits1,
                         addl_bit_count2, last_byte_bits2);
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
      uint32_t total = tail_ptr_counts0;
      std::cout << "Count0: " << tail_ptr_counts0 << std::endl;
      for (int i = 0; i < tail_ptr_counts1.size(); i++) {
        std::cout << "Counts for bit " << i + 6 << ": " << tail_ptr_counts1[i] << std::endl;
        total += tail_ptr_counts1[i];
      }
      for (int i = 0; i < tail_ptr_counts2.size(); i++) {
        std::cout << "Counts for bit " << i + 6 << ": " << tail_ptr_counts2[i] << std::endl;
        total += tail_ptr_counts2[i];
      }
      for (int i = 0; i < 8; i++)
        std::cout << "Flag " << i << ": " << flag_counts[i] << std::endl;
      // for (int i = 0; i < 8; i++)
      //   std::cout << "Val lens " << i << ": " << val_len_counts[i] << std::endl;
      std::cout << "Total ptrs: " << total << std::endl;
      std::cout << "Trie size: " << trie.size() << std::endl;
      std::cout << "Node count: " << node_count << std::endl;
      std::cout << "Tail0 size: " << tails0.size() << std::endl;
      std::cout << "Tail1 size: " << tails1.size() << std::endl;
      std::cout << "Tail2 size: " << tails2.size() << std::endl;
      std::cout << "Tail Ptrs1 size: " << tail_ptrs1.size() << std::endl;
      std::cout << "Tail Ptrs2 size: " << tail_ptrs2.size() << std::endl;
      std::cout << "Tail Ptr Counts 0: " << tail_ptr_counts0 << std::endl;
      std::cout << "Tail Ptr Counts 1: " << tail_ptr_counts1.size() << std::endl;
      std::cout << "Tail Ptr Counts 2: " << tail_ptr_counts2.size() << std::endl;
      uint32_t total_size = trie.size()
          +(ceil(node_count/320)*11*4) // bit vectoors
          +tails0.size()+tails1.size()+tails2.size() // tails
          +tail_ptrs1.size()+tail_ptrs2.size()  // tail pointers
          +(tail_ptr_counts1.size()+tail_ptr_counts2.size())*4; // tail ptr info
      std::cout << "Total size: " << total_size << std::endl;
      std::cout << "Node struct size: " << sizeof(node) << std::endl;
    }

};

}

// 1xxxxxxx 1xxxxxxx 1xxxxxxx 01xxxxxx
// 16 - followed by pointer
// 0 2-15/18-31 bytes 16 ptrs bytes 1

#endif
