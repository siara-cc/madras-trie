#ifndef STATIC_DICT_BUILDER_H
#define STATIC_DICT_BUILDER_H

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <math.h>

#include "var_array.h"
#include "bit_vector.h"
#include "bitset_vector.h"

using namespace std;

enum {SRCH_ST_UNKNOWN, SRCH_ST_NEXT_SIBLING, SRCH_ST_NOT_FOUND, SRCH_ST_NEXT_CHAR};

namespace squeezed {

class node {
  public:
    node *first_child;
    node *parent;
    node *next_sibling;
    string val;
    bool is_leaf;
    int level;
    node() {
      first_child = parent = next_sibling = NULL;
      is_leaf = false;
      level = 0;
    }
};

class static_dict_builder {

  private:
    node *root;
    node *first_node;
    int node_count;
    double oct_node_count;
    int key_count;
    vector<node *> all_nodes;
    vector<vector<node *> > level_nodes;
    string prev_key;

    bitset_vector ptrs;
    vector<uint8_t> suffixes;
    bitset_vector payloads;

    static_dict_builder(static_dict_builder const&);
    static_dict_builder& operator=(static_dict_builder const&);

    node *get_last_child(node *node) {
      node = node->first_child;
      while (node != NULL && node->next_sibling != NULL)
        node = node->next_sibling;
      return node;
    }

    node *find_next_sibling_having_same_level(node *n, int level) {
        node *parent = n;
        n = NULL;
        while (parent != root) {
          node *next_sibling = parent->next_sibling;
          while (next_sibling != NULL) {
            node *first_child = next_sibling->first_child;
            while (first_child != NULL) {
              if (first_child->level == level) {
                n = first_child;
                break;
              }
              first_child = first_child->first_child;
            }
            if (n != NULL)
              break;
            next_sibling = next_sibling->next_sibling;
          }
          if (n != NULL)
            break;
          parent = parent->parent;
        }
        return n;
    }

  public:
    static_dict_builder() {
      root = new node;
      node_count = 0;
      oct_node_count = 1;
      key_count = 0;
      root->parent = NULL;
      root->next_sibling = NULL;
      root->is_leaf = false;
      root->level = 0;
      first_node = new node;
      first_node->parent = root;
      first_node->next_sibling = NULL;
      first_node->level = 1;
      all_nodes.push_back(first_node);
      add_to_level_nodes(first_node);
      root->first_child = first_node;
    }

    ~static_dict_builder() {
      for (int i = 0; i < all_nodes.size(); i++)
        delete all_nodes[i];
      delete root;
    }

    void add_to_level_nodes(node *new_node) {
        if (level_nodes.size() < new_node->level)
          level_nodes.push_back(vector<node *>());
        level_nodes[new_node->level - 1].push_back(new_node);
    }
    void append(string key) {
      if (key == prev_key)
         return;
      key_count++;
      if (node_count == 0) {
        first_node->val = key;
        first_node->is_leaf = true;
        node_count++;
        return;
      }
      prev_key = key;
      int key_pos = 0;
      node *last_child = get_last_child(root);
      do {
        const string *val = &last_child->val;
        int i = 0;
        for (; i < val->length(); i++) {
          if (key[key_pos] != val->at(i)) {
            if (i == 0) {
              node *new_node = new node();
              new_node->is_leaf = true;
              new_node->level = last_child->level;
              new_node->parent = last_child->parent;
              new_node->next_sibling = NULL;
              new_node->val = key.substr(key_pos);
              //if (new_node->val == string(" discuss"))
              //  cout << "New node: " << key << endl;
              all_nodes.push_back(new_node);
              add_to_level_nodes(new_node);
              last_child->next_sibling = new_node;
              node_count++;
              if ((last_child->val[0] >> 3) != (new_node->val[0] >> 3))
                oct_node_count++;
            } else {
              node *child1 = new node();
              node *child2 = new node();
              child1->is_leaf = last_child->is_leaf;
              child1->level = last_child->level + 1;
              child1->parent = last_child;
              child1->next_sibling = child2;
              child1->val = val->substr(i);
              //if (child1->val == string(" discuss"))
              //  cout << "Child1 node: " << key << endl;
              if (last_child->first_child != NULL) {
                node *node = last_child->first_child;
                child1->first_child = node;
                do {
                  node->parent = child1;
                  node->level++;
                  node = node->next_sibling;
                } while (node != NULL);
              }
              all_nodes.push_back(child1);
              child2->is_leaf = true;
              child2->level = last_child->level + 1;
              child2->parent = last_child;
              child2->next_sibling = NULL;
              child2->val = key.substr(key_pos);
              //if (child2->val == string(" discuss"))
              //  cout << "Child2 node: " << key << endl;
              last_child->first_child = child1;
              last_child->is_leaf = false;
              node_count += 2;
              oct_node_count += ((child1->val[0] >> 3) == (child2->val[0] >> 3) ? 1 : 1.5);
              all_nodes.push_back(child2);
              add_to_level_nodes(child1);
              add_to_level_nodes(child2);
              last_child->val.erase(i);
            }
            return;
          }
          key_pos++;
        }
        if (i == val->length() && key_pos < key.length()
            && last_child->is_leaf && last_child->first_child == NULL) {
          node *child1 = new node();
          child1->is_leaf = true;
          child1->level = last_child->level + 1;
          child1->parent = last_child;
          child1->next_sibling = NULL;
          child1->val = key.substr(key_pos);
          //    if (child1->val == string(" discuss"))
          //      cout << "Ext node: " << key << endl;
          last_child->first_child = child1;
          node_count++;
          all_nodes.push_back(child1);
          add_to_level_nodes(child1);
          oct_node_count++;
          return;
        }
        last_child = get_last_child(last_child);
      } while (last_child != NULL);
    }

    string build() {
      uint32_t node_count = all_nodes.size();
      uint32_t two_byte_suffix_count = 0;
      uint32_t three_byte_suffix_count = 0;
      uint32_t four_byte_suffix_count = 0;
      uint32_t one_byte_leaf_count = 0;
      uint32_t total_suffix_count = 0;
      uint32_t total_prefix_count = 0;
      uint32_t total_suffix_len = 0;
      uint32_t total_uniq_suffix_len = 0;
      uint32_t total_uniq_prefix_len = 0;
      uint32_t uniq_suffix_len_lt5 = 0;
      uint32_t uniq_prefix_len_lt5 = 0;
      //set<string> uniq_suffix;
      //set<string> uniq_prefix;
      map<string, int> uniq_map;
      FILE *fout = fopen("tails.txt", "w+");
      for (int i = 0; i < node_count; i++) {
        string& suffix = all_nodes[i]->val;
        int val_len = suffix.length();
        total_suffix_len += val_len;
        if (all_nodes[i]->is_leaf && all_nodes[i]->first_child == NULL)
          one_byte_leaf_count++;
        if (val_len > 1) {
          if (val_len == 2)
            two_byte_suffix_count++;
          if (val_len == 3)
            three_byte_suffix_count++;
          if (val_len == 4)
            four_byte_suffix_count++;
          fwrite(suffix.c_str(), suffix.length(), 1, fout);
          fwrite("\n", 1, 1, fout);
          total_suffix_count++;
          if (!all_nodes[i]->is_leaf)
              total_prefix_count++;
          //map<string, int>& uniq_map = all_nodes[i]->is_leaf ? uniq_suffix : uniq_prefix;
          if (uniq_map.find(suffix) == uniq_map.end()) {
            uniq_map.insert(pair<string, int>(suffix, 0));
            if (all_nodes[i]->is_leaf) {
              total_uniq_suffix_len += suffix.length();
              if (suffix.length() < 5)
                uniq_suffix_len_lt5 += suffix.length();
            } else {
              total_uniq_prefix_len += suffix.length();
              if (suffix.length() < 5)
                uniq_prefix_len_lt5 += suffix.length();
            }
          }
        }
      }
      fclose(fout);
      int ext_louds_len = node_count / 2;
      ext_louds_len++;
      int total_len = node_count + ext_louds_len; // trie
      total_len += total_suffix_len;
      total_len += total_suffix_count * 3;
      string ret(total_len, 0);
      node *start_node = root->first_child;
      node *n = start_node;
      int level = 1;
      int ext_louds_pos = 0;
      int node_pos = ext_louds_len;
      int ptr_pos = ext_louds_len + node_count;
      int suffix_pos = total_len - total_suffix_len;
      cout << "Node count: " << node_count << endl;
      cout << "Oct Node count: " << oct_node_count << endl;
      cout << "One byte leaf count: " << one_byte_leaf_count << endl;
      cout << "Tail count: " << total_suffix_count << endl;
      cout << "Tail len: " << total_suffix_len - total_suffix_count << endl;
      cout << "Prefix count: " << total_prefix_count << endl;
      cout << "Suffix count: " << total_suffix_count - total_prefix_count << endl;
      cout << "2 byte Suffix count: " << two_byte_suffix_count << endl;
      cout << "3 byte Suffix count: " << three_byte_suffix_count << endl;
      cout << "4 byte Suffix count: " << four_byte_suffix_count << endl;
      cout << "Uniq Prefix len: " << total_uniq_prefix_len << endl;
      cout << "Uniq Suffix len: " << total_uniq_suffix_len << endl;
      cout << "Uniq Prefix len lt 5: " << uniq_prefix_len_lt5 << endl;
      cout << "Uniq Suffix len lt 5: " << uniq_suffix_len_lt5 << endl;
                    // trie_chars   bitmaps          pointers                                                   tails
      int total_size = node_count + node_count / 2 + uniq_map.size() + total_uniq_prefix_len + total_uniq_suffix_len
                       + total_suffix_count * (ceil(log2(total_uniq_prefix_len + total_uniq_suffix_len))-8)/8;
      cout << "Estimated size: " << total_size << endl;
      uint32_t gt5_count = total_suffix_count - two_byte_suffix_count - three_byte_suffix_count - four_byte_suffix_count;
      uint32_t gt5_uniq_len = total_suffix_len - (uniq_prefix_len_lt5 + uniq_suffix_len_lt5);
      uint32_t lt5_count = two_byte_suffix_count + three_byte_suffix_count + four_byte_suffix_count;
      uint32_t lt5_uniq_len = (uniq_prefix_len_lt5 + uniq_suffix_len_lt5);
      total_size = node_count + node_count / 2 +  uniq_map.size() + total_uniq_prefix_len + total_uniq_suffix_len
                       + gt5_count * (ceil(log2(gt5_uniq_len))-8)/8
                       + lt5_count * (ceil(log2(lt5_uniq_len))-8)/8;
      cout << "Estimated size 2: " << total_size << endl;
      string octrie;
      for (int i = 0; i < level_nodes.size(); i++) {
        vector<node *> cur_lvl_nodes = level_nodes[i];
        uint8_t oct = 0;
        uint8_t leaf_bm = 0;
        uint8_t child_bm = 0;
        string leaf_tails;
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *cur_node = cur_lvl_nodes[j];
          uint8_t cur_byte = cur_node->val[0];
          if ((oct >> 3) != (cur_byte >> 3)) {
            if (j != 0) {
              if (child_bm != 0)
                  oct |= 0x02;
              if (cur_node->next_sibling == NULL)
                  oct |= 0x04;
              octrie.append(1, oct);
              if (child_bm != 0)
                octrie.append(1, child_bm);
              octrie.append(1, leaf_bm);
            }
            oct = leaf_bm = child_bm = 0;
          }
          node *parent = cur_node->parent;
          if (parent != NULL && parent->first_child == cur_node && parent->val.length() > 1)
            append_tail(uniq_map, octrie, parent->val.substr(1), false, octrie.length());
          oct = cur_byte & 0xF8;
          uint8_t mask = 1 << (cur_byte & 0x07);
          if (cur_node->first_child != NULL)
            child_bm |= mask;
          if (cur_node->is_leaf) {
            leaf_bm |= mask;
            append_tail(uniq_map, leaf_tails, cur_node->val.substr(1), true, octrie.length());
          }
        }
        if (!(oct == 0 && leaf_bm == 0 && child_bm == 0)) {
            if (child_bm != 0)
                oct |= 0x02;
            oct |= 0x04;
            octrie.append(1, oct);
            if (child_bm != 0)
              octrie.append(1, child_bm);
            octrie.append(1, leaf_bm);
        }
        octrie.append(leaf_tails);
      }
      cout << "Octire len: " << octrie.length() << endl;
      return ret;
    }

    void append_tail(map<string, int>& uniq_map, string& tail_str, string tail, bool is_leaf, int pos) {
      if (tail.length() == 0) {
        tail_str.append(1, (char) 1);
        return;
      }
      map<string, int>::iterator it = uniq_map.find(tail);
      if (it == uniq_map.end()) {
        append_vint(tail_str, tail.length());
        tail_str.append(tail);
        uniq_map[tail] = pos;
        return;
      }
      append_vint(tail_str, it->second);
    }

    void append_vint(string& tail_str, int i) {
      if (i < 8) {
        tail_str.append(1, (char) (i << 5) + 1);
        return;
      } else if (i < 1024) {
        tail_str.append(1, (char) (i << 5) + 1);
        tail_str.append(1, (char) (i >> 3));
      } else if (i < 131072) {
        tail_str.append(1, (char) (i << 5) + 1);
        tail_str.append(1, (char) ((i >> 3) & 0x7F) + 0x80);
        tail_str.append(1, (char) (i >> 10));
      } else {
        tail_str.append(1, (char) (i << 5) + 1);
        tail_str.append(1, (char) ((i >> 3) & 0x7F) + 0x80);
        tail_str.append(1, (char) ((i >> 10) & 0x7F) + 0x80);
        tail_str.append(1, (char) (i >> 17));
      }
    }

};

}

#endif
