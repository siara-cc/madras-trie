#ifndef STATIC_DICT_BUILDER_H
#define STATIC_DICT_BUILDER_H

#include <set>
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
    int key_count;
    vector<node *> all_nodes;
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
      root->first_child = first_node;
    }

    ~static_dict_builder() {
      for (int i = 0; i < all_nodes.size(); i++)
        delete all_nodes[i];
      delete root;
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
              last_child->next_sibling = new_node;
              node_count++;
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
              all_nodes.push_back(child2);
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
          return;
        }
        last_child = get_last_child(last_child);
      } while (last_child != NULL);
    }

    string build() {
      uint32_t node_count = all_nodes.size();
      uint32_t total_suffix_count = 0;
      uint32_t total_suffix_len = 0;
      for (int i = 0; i < node_count; i++) {
        int val_len = all_nodes[i]->val.length();
        total_suffix_len += val_len;
        if (val_len > 1)
          total_suffix_count++;
      }
      int ext_louds_len = node_count / 2;
      ext_louds_len++;
      int total_len = node_count + ext_louds_len; // trie
      total_suffix_len += total_suffix_count; // assuming each suffix < 255 bytes in len
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
      cout << "Suffix count: " << total_suffix_count << endl;
      cout << "Suffix len: " << total_suffix_len - total_suffix_count << endl;
      //cout << level << ": ";
      while (start_node != NULL) {
        node *next_sibling = n;
        //if (level != next_sibling->level)
        //  cout << "Level mismatch" << endl;
        while (next_sibling != NULL) {
          //cout << next_sibling->val << ", ";
          uint32_t ptr = node_pos << 2;
          string val = next_sibling->val;
          ret[node_pos++] = val.length() > 1 ? (ptr >> 24) : val[0];
          ret[ptr_pos++] = (ptr >> 16) & 0xFF;
          ret[ptr_pos++] = (ptr >> 8) & 0xFF;
          ret[ptr_pos++] = ptr & 0xFF;
          int suffix_len = val.length() - 1;
          ret[suffix_pos++] = suffix_len;
          for (int i = 1; i <= suffix_len; i++)
            ret[suffix_pos++] = val[i];
          uint8_t flags = val.length() > 1 ? 0x08 : 0; // ptr
          flags += next_sibling->is_leaf ? 0x04 : 0;                 // leaf
          flags += next_sibling->first_child == NULL ? 0 : 0x02;     // child
          flags += next_sibling->next_sibling == NULL ? 0 : 0x01;    // is_last
          if (node_pos & 0x01)
            flags <<= 4;
          ret[ext_louds_pos] |= flags;
          if ((node_pos & 0x01) == 0)
            ext_louds_pos++;
          next_sibling = next_sibling->next_sibling;
        }
        n = find_next_sibling_having_same_level(n, level);
        if (n == NULL) {
          level++;
          if (start_node->first_child != NULL) {
            start_node = start_node->first_child;
            n = start_node;
          } else {
            start_node = find_next_sibling_having_same_level(start_node, level);
            if (start_node != NULL)
              n = start_node;
          }
          //cout << endl;
          //cout << level << ": ";
        } else {
          //cout << ",, ";
        }
      }
      //cout << endl;
      return ret;
    }

};

}

#endif
