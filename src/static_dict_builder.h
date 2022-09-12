#ifndef STATIC_DICT_BUILDER_H
#define STATIC_DICT_BUILDER_H

#include <set>
#include <string>
#include <vector>

#include "var_array.h"
#include "bit_vector.h"
#include "bitset_vector.h"

using namespace std;

enum {SRCH_ST_UNKNOWN, SRCH_ST_NEXT_SIBLING, SRCH_ST_NOT_FOUND, SRCH_ST_NEXT_CHAR};

namespace amphisbaena {

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

    char *buf;
    bit_vector louds;
    bit_vector is_leaf;
    bit_vector is_ptr;
    vector<uint8_t> nodes;
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

    void append(string& key) {
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

};

}

#endif
