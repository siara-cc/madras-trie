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

enum {SRCH_ST_UNKNOWN, SRCH_ST_NEXT_SIBLING, SRCH_ST_NOT_FOUND, SRCH_ST_NEXT_CHAR};

namespace squeezed {

class node {
  public:
    node *first_child;
    node *parent;
    node *next_sibling;
    std::string val;
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
    std::vector<node *> all_nodes;
    std::vector<vector<node *> > level_nodes;
    std::string prev_key;

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

    int append_bits(std::vector<uint8_t>& ptr_str, std::vector<uint32_t>& tail_ptr_counts, int addl_bit_count, bool is_next, int last_byte_bits, int ptr) {
      if (is_next)
        tail_ptr_counts.push_back(1);
      else {
        int last_idx = tail_ptr_counts.size() - 1;
        tail_ptr_counts[last_idx] = tail_ptr_counts[last_idx] + 1;
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

    void build() {
      std::cout << std::endl;
      std::map<string, int> uniq_map;
      std::map<string, int> uniq_map_c;
      std::map<string, int> uniq_map_l;
      for (int i = 0; i < node_count; i++) {
        node *cur_node = all_nodes[i];
        string& val = cur_node->val;
        int val_len = val.length();
        if (val_len > 1) {
          std::map<std::string, int>::iterator it = uniq_map.find(val);
          if (it == uniq_map.end()) {
            uniq_map.insert(pair<string, int>(val, -1));
          } else {
            it->second--;
          }
          if (cur_node->first_child != NULL) {
            it = uniq_map_c.find(val);
            if (it == uniq_map_c.end()) {
              uniq_map_c.insert(pair<string, int>(val, -1));
            } else {
              it->second--;
            }
          }
          if (cur_node->is_leaf && cur_node->first_child == NULL) {
            it = uniq_map_l.find(val);
            if (it == uniq_map_l.end()) {
              uniq_map_l.insert(pair<string, int>(val, -1));
            } else {
              it->second--;
            }
          }
        }
      }
      FILE *fout = fopen("tail_freq_c.txt", "w+");
      for (std::map<std::string, int>::iterator it = uniq_map_c.begin(); it != uniq_map_c.end(); it++) {
        char print_str[200];
        sprintf(print_str, "%d\t[%s]\n", -it->second, it->first.c_str());
        fwrite(print_str, strlen(print_str), 1, fout);
      }
      fclose(fout);
      fout = fopen("tail_freq_l.txt", "w+");
      for (std::map<std::string, int>::iterator it = uniq_map_l.begin(); it != uniq_map_l.end(); it++) {
        char print_str[200];
        sprintf(print_str, "%d\t[%s]\n", -it->second, it->first.c_str());
        fwrite(print_str, strlen(print_str), 1, fout);
      }
      fclose(fout);
      std::vector<uint8_t> trie;
      std::vector<uint8_t> tail_ptrs;
      std::vector<uint32_t> tail_ptr_counts;
      std::vector<uint8_t> tails;
      uint32_t tail_entry_count = 0;
      uint32_t flag_counts[8];
      uint32_t val_len_counts[8];
      memset(flag_counts, '\0', sizeof(uint32_t) * 8);
      memset(val_len_counts, '\0', sizeof(uint32_t) * 8);
      uint32_t found_ptr_len = 0;
      uint32_t lower_bound_count = 0;
      uint32_t node_count = 0;
      uint8_t pending_byte = 0;
      int addl_bit_count = 0;
      int last_byte_bits = 0;
      tail_ptr_counts.push_back(0);
      tail_ptrs.push_back(0);
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *cur_node = cur_lvl_nodes[j];
          uint8_t flags = 0;
          uint8_t node_val;
          std::string& val = cur_node->val;
          // if (val.length() < 4)
          //   continue;
          // if (cur_node->first_child != NULL)
          //    continue;
          if (val.length() == 1)
            node_val = cur_node->val[0];
          else {
            std::map<std::string, int>::iterator it = uniq_map.find(val);
            if (it == uniq_map.end())
              std::cout << "Unexpected value not found" << std::endl;
            else {
              uint32_t ptr = 0;
              if (it->second < 0) {
                std::map<std::string, int>::iterator it2 = uniq_map.lower_bound(val);
                it2++;
                if (it2 != uniq_map.end() && it2->first.substr(0, val.length()).compare(val.c_str()) == 0 && it2->second >= 0)
                  lower_bound_count += val.length();
                ptr = tails.size();
                for (int k = 0; k < val.length(); k++)
                  tails.push_back(val[k]);
                tails.push_back(0);
                tail_entry_count++;
                it->second = ptr;
                if (val.length() > 2) {
                  int suffix_count = val.length() - 2;
                  for (int k = 0; k < suffix_count; k++) {
                    std::string suffix = val.substr(k + 1);
                    std::map<std::string, int>::iterator it1 = uniq_map.find(suffix);
                    if (it1 == uniq_map.end())
                      uniq_map.insert(std::pair<std::string, int>(suffix, ptr + k + 1));
                    else {
                      if (it1->second < 0)
                        it1->second = ptr + k + 1;
                      else {
                        if (tails[it1->second - 1] == 0)
                          found_ptr_len += suffix.length() + 1;
                        it1->second = ptr + k + 1;
                      }
                    }
                  }
                }
              } else {
                ptr = it->second;
              }
              node_val = ptr & 0xFF;
              ptr >>= 8;
              //  std::cout << ceil(log2(ptr)) << " ";
              if (tails.size() >= (1 << (8 + addl_bit_count))) {
                //std::cout << "Tail size: " << tails.size() << std::endl;
                addl_bit_count++;
                last_byte_bits = append_bits(tail_ptrs, tail_ptr_counts, addl_bit_count, true, 0, ptr);
              } else {
                last_byte_bits = append_bits(tail_ptrs, tail_ptr_counts, addl_bit_count, false, last_byte_bits, ptr);
              }
            }
            if (val.length() < 9)
              val_len_counts[val.length() - 2]++;
            else
              val_len_counts[7]++;
          }
          if (cur_node->is_leaf)
            flags |= 0x01;
          if (cur_node->first_child != NULL)
            flags |= 0x02;
          if (cur_node->val.length() > 1)
            flags |= 0x04;
          if (cur_node->next_sibling == NULL)
            flags |= 0x08;
          flag_counts[flags & 0x07]++;
          if ((node_count % 2) == 0) {
            trie.push_back(node_val);
            pending_byte = flags << 4;
          } else {
            trie.push_back(pending_byte | flags);
            trie.push_back(node_val);
            pending_byte = 0;
          }
          node_count++;
        }
        if ((node_count % 2) == 1) {
          trie.push_back(pending_byte);
        }
      }
      std::cout << std::endl;
      int total = 0;
      for (int i = 0; i < tail_ptr_counts.size(); i++) {
        std::cout << "Counts for bit " << i + 8 << ": " << tail_ptr_counts[i] << std::endl;
        total += tail_ptr_counts[i];
      }
      for (int i = 0; i < 8; i++)
        std::cout << "Flag " << i << ": " << flag_counts[i] << std::endl;
      // for (int i = 0; i < 8; i++)
      //   std::cout << "Val lens " << i << ": " << val_len_counts[i] << std::endl;
      std::cout << "Total ptrs: " << total << std::endl;
      std::cout << "Found ptr len: " << found_ptr_len << std::endl;
      std::cout << "Lower bound count: " << lower_bound_count << std::endl;
      std::cout << "Trie size: " << trie.size() << std::endl;
      std::cout << "Node count: " << all_nodes.size() << std::endl;
      std::cout << "Tail entry count: " << tail_entry_count << std::endl;
      std::cout << "Tail size: " << tails.size() << std::endl;
      std::cout << "Tail Ptrs size: " << tail_ptrs.size() << std::endl;
      std::cout << "Tail Ptr Counts size: " << tail_ptr_counts.size() << std::endl;
      std::cout << "Total size: " << trie.size()+tails.size()+tail_ptrs.size()+tail_ptr_counts.size()*4 << std::endl;
    }

};

}

#endif
