#ifndef builder_H
#define builder_H

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

typedef std::map<std::string, uint64_t> tail_map;
typedef std::multimap<uint32_t, std::string> tail_freq_map;
typedef std::map<uint32_t, std::string> tail_link_map;
typedef std::vector<uint8_t> byte_vec;

class node {
  public:
    node *first_child;
    //node *parent;
    node *next_sibling;
    std::string val;
    bool is_leaf;
    int level;
    node() {
      //parent = NULL;
      first_child = next_sibling = NULL;
      is_leaf = false;
      level = 0;
    }
};

class builder {

  private:
    node *root;
    node *first_node;
    int node_count;
    double oct_node_count;
    int key_count;
    std::vector<vector<node *> > level_nodes;
    std::string prev_key;

    builder(builder const&);
    builder& operator=(builder const&);

    node *get_last_child(node *node) {
      node = node->first_child;
      while (node != NULL && node->next_sibling != NULL)
        node = node->next_sibling;
      return node;
    }

  public:
    builder() {
      root = new node;
      node_count = 0;
      oct_node_count = 1;
      key_count = 0;
      //root->parent = NULL;
      root->next_sibling = NULL;
      root->is_leaf = false;
      root->level = 0;
      first_node = new node;
      //first_node->parent = root;
      first_node->next_sibling = NULL;
      first_node->level = 1;
      add_to_level_nodes(first_node);
      root->first_child = first_node;
    }

    ~builder() {
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++)
          delete cur_lvl_nodes[j];
      }
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
              //new_node->parent = last_child->parent;
              new_node->next_sibling = NULL;
              new_node->val = key.substr(key_pos);
              //if (new_node->val == string(" discuss"))
              //  cout << "New node: " << key << endl;
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
              //child1->parent = last_child;
              child1->next_sibling = child2;
              child1->val = val->substr(i);
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
              child2->is_leaf = true;
              child2->level = last_child->level + 1;
              //child2->parent = last_child;
              child2->next_sibling = NULL;
              child2->val = key.substr(key_pos);
              //if (child2->val == string(" discuss"))
              //  cout << "Child2 node: " << key << endl;
              last_child->first_child = child1;
              last_child->is_leaf = false;
              node_count += 2;
              oct_node_count += ((child1->val[0] >> 3) == (child2->val[0] >> 3) ? 1 : 1.5);
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
          //child1->parent = last_child;
          child1->next_sibling = NULL;
          child1->val = key.substr(key_pos);
          //    if (child1->val == string(" discuss"))
          //      cout << "Ext node: " << key << endl;
          last_child->first_child = child1;
          node_count++;
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

    void assign_tail_bucket(tail_map& uniq_map, tail_link_map& uniq_link_map, tail_freq_map& uniq_freq_map, int limit) {
      std::cout << "Limit: " << limit << ", Link map size: " << uniq_link_map.size() << endl;
      tail_freq_map::iterator it_freq = uniq_freq_map.end();
      int tails_len = 0;
      do {
        if (tails_len > limit)
          break;
        it_freq--;
        tail_map::iterator it_tails = uniq_map.find(it_freq->second);
        if (it_tails == uniq_map.end())
          std::cout << "Unexpected" << std::endl;
        else {
          uint32_t link_id = it_tails->second >> 32;
          if (link_id >= 0x80000000)
            continue;
          tail_link_map::iterator it_link = uniq_link_map.find(link_id);
          if (it_link != uniq_link_map.end())
            continue;
          tails_len += it_freq->second.length();
          tails_len++;
          std::string& val = it_freq->second;
          if (val.length() > 2) {
            int suffix_count = val.length() - 2;
            for (int k = 1; k <= suffix_count; k++) {
              std::string suffix = val.substr(k);
              tail_map::iterator it1 = uniq_map.find(suffix);
              if (it1 != uniq_map.end()) {
                uint32_t link_id = it1->second >> 32;
                if (link_id >= 0x80000000)
                  continue;
                uint32_t freq = it1->second & 0xFFFFFFFF;
                if ((it1->second >> 62) == 0) {
                  bool is_added = add2_link_map(link_id, uniq_link_map, val);
                  if (is_added)
                    std::cout << it_freq->first << ", suffix: " << suffix << ", val: " << val << std::endl;
                  if (is_added && freq > it_freq->first) {
                    tails_len -= suffix.length();
                    tails_len--;
                  }
                }
              }
            }
          }
        }
      } while (it_freq != uniq_freq_map.begin());
      it_freq = uniq_freq_map.end();
      tails_len = 0;
      int tail_count = 0;
      it_freq--;
      do {
        if (tails_len > limit)
          break;
        int sw = (limit == 126 ? 0 : 1);
        tail_map::iterator it_tails = uniq_map.find(it_freq->second);
        if (it_tails == uniq_map.end())
          std::cout << "Unexpected" << std::endl;
        else {
          uint32_t link_id = (it_tails->second >> 32) & 0x3FFFFFFF;
          if (it_tails->second < 0x8000000000000000) {
            it_tails->second |= (sw == 0 ? 0x8000000000000000 : 0xC000000000000000);
            std::string& appended_val = it_freq->second;
            tail_link_map::iterator it_link = uniq_link_map.find(link_id);
            if (it_link == uniq_link_map.end()) {
              std::cout << tail_count << "\t" << it_freq->first << "\t[";
              std::cout << it_freq->second << "]" << std::endl;
              tails_len += it_freq->second.length();
              tails_len++;
            } else {
              tail_map::iterator it_tails2 = uniq_map.find(it_link->second);
              if (it_tails2 == uniq_map.end())
                std::cout << "Unexpected" << std::endl;
              else {
                if (it_tails2->second < 0x8000000000000000) {
                  if (tails_len + it_link->second.length() < limit) {
                    tails_len += it_link->second.length();
                    tails_len++;
                    appended_val = it_link->second;
                    it_tails2->second |= (sw == 0 ? 0x8000000000000000 : 0xC000000000000000);
                    std::cout << tail_count << "\t" << it_freq->first << "\t[";
                    std::cout << it_freq->second << "]\t" << (it_tails2->second & 0xFFFFFFFF) << "\t[" << it_link->second << "]" << std::endl;
                  } else {
                    tails_len += it_freq->second.length();
                    tails_len++;
                    uniq_link_map.erase(link_id);
                    std::cout << tail_count << "\t" << it_freq->first << "\t[";
                    std::cout << it_freq->second << "]" << std::endl;
                  }
                } else {
                    std::cout << tail_count << "\t" << it_freq->first << "\t[";
                    std::cout << it_freq->second << "]*" << std::endl;
                    appended_val = string("a");
                }
              }
            }
            if (appended_val.length() > 2) {
              int suffix_count = appended_val.length() - 2;
              for (int k = 1; k <= suffix_count; k++) {
                std::string suffix = appended_val.substr(k);
                tail_map::iterator it1 = uniq_map.find(suffix);
                if (it1 != uniq_map.end()) {
                   if (it1->second < 0x8000000000000000)
                     it1->second |= (sw == 0 ? 0x8000000000000000 : 0xC000000000000000);
                }
              }
            }
          } else {
            tail_link_map::iterator it_link = uniq_link_map.find(link_id);
            std::cout << tail_count << "\t" << it_freq->first << "\t[";
            std::cout << it_freq->second << "]\t?\t[" << (it_link == uniq_link_map.end() ? "?" : it_link->second) << "]" << std::endl;
          }
        }
        uniq_freq_map.erase(it_freq--);
        tail_count++;
      } while (tail_count < uniq_freq_map.size());
      std::cout << "Tail count: " << tail_count << ", len: " << tails_len << std::endl;

    }

    void add2_uniq_map(tail_map& uniq_map, std::string& val, uint32_t& max_freq) {
      tail_map::iterator it = uniq_map.find(val);
      if (it == uniq_map.end()) {
        uint64_t val_id = uniq_map.size();
        val_id <<= 32;
        uniq_map.insert(pair<std::string, uint64_t>(val, 1 + val_id));
      } else {
        it->second++;
        if (it->second > max_freq)
          max_freq = it->second;
      }
    }

    bool add2_link_map(uint32_t link_id, tail_link_map& uniq_link_map, std::string& val) {
      tail_link_map::iterator it_link = uniq_link_map.find(link_id);
      bool to_ins = false;
      if (it_link == uniq_link_map.end())
        to_ins = true;
      else {
        if (it_link->second.length() < val.length())
          to_ins = true;
      }
      if (to_ins) {
        uniq_link_map.erase(link_id);
        //std::cout << "Added link: " << (unsigned long) &uniq_link_map << " " << link_id << " " << val << std::endl;
        uniq_link_map.insert(pair<uint32_t, std::string>(link_id, val));
      }
      return to_ins;
    }

    void build_tail_link_map2(tail_map& uniq_map, tail_link_map& uniq_link_map2, tail_freq_map& uniq_freq_map, uint32_t max_freq) {
      tail_map::iterator it;
      for (it = uniq_map.begin(); it != uniq_map.end(); it++) {
        if (it->second >> 62)
          continue;
        std::string val = it->first;
        if (val.length() > 2) {
          int suffix_count = val.length() - 2;
          for (int k = 1; k <= suffix_count; k++) {
            std::string suffix = val.substr(k);
            tail_map::iterator it1 = uniq_map.find(suffix);
            if (it1 != uniq_map.end()) {
              uint32_t link_id = it1->second >> 32;
              if (link_id < 0x80000000)
                add2_link_map(link_id, uniq_link_map2, val);
            }
          }
        }
      }
    }

    void build_tail_maps(tail_map& uniq_map, tail_link_map& uniq_link_map0, tail_link_map& uniq_link_map1, tail_link_map& uniq_link_map2) {
      uint32_t max_freq = 0;
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *cur_node = cur_lvl_nodes[j];
          std::string& val = cur_node->val;
          int val_len = val.length();
          if (val_len > 1) {
            add2_uniq_map(uniq_map, val, max_freq);
          }
        }
      }
      tail_freq_map uniq_freq_map;
      tail_map::iterator it;
      for (it = uniq_map.begin(); it != uniq_map.end(); it++) {
        std::string val = it->first;
        uniq_freq_map.insert(pair<uint32_t, std::string>(it->second & 0xFFFFFFFF, val));
      }
      std::cout << "Uniq map size: " << uniq_map.size() << std::endl;
      std::cout << "Uniq freq map size: " << uniq_freq_map.size() << std::endl;
      assign_tail_bucket(uniq_map, uniq_link_map0, uniq_freq_map, 126);
      assign_tail_bucket(uniq_map, uniq_link_map1, uniq_freq_map, 4094);
      build_tail_link_map2(uniq_map, uniq_link_map2, uniq_freq_map, max_freq);
      tail_map::iterator it_tails = uniq_map.begin();
      while (it_tails != uniq_map.end()) {
        it_tails->second &= 0xFFFFFFFF00000000;
        it_tails++;
      }
    }

    uint8_t get_tail_ptr(std::string& val, tail_map& uniq_map, tail_link_map& uniq_link_map0,
             tail_link_map& uniq_link_map1, tail_link_map& uniq_link_map2,
             std::vector<uint8_t>& tails0, std::vector<uint8_t>& tails1, 
             std::vector<uint8_t>& tails2, std::vector<uint8_t>& tail_ptrs1,
             std::vector<uint8_t>& tail_ptrs2, uint32_t& tail_ptr_counts0,
             std::vector<uint32_t>& tail_ptr_counts1, std::vector<uint32_t>& tail_ptr_counts2,
             int& addl_bit_count1, int& last_byte_bits1,
             int& addl_bit_count2, int& last_byte_bits2) {
      uint8_t node_val;
      tail_map::iterator it = uniq_map.find(val);
      if (it == uniq_map.end())
        std::cout << "Unexpected value not found" << std::endl;
      else {
        uint32_t ptr = 0;
        uint32_t link_id = (it->second >> 32);
        uint64_t which = link_id >> 30;
        link_id &= 0x3FFFFFFF;
        std::vector<uint8_t>& tails = (which == 2 ? tails0 : (which == 3 ? tails1 : tails2));
        std::vector<uint8_t>& tail_ptrs = (which == 2 ? tail_ptrs1 : (which == 3 ? tail_ptrs1 : tail_ptrs2));
        int& addl_bit_count = (which == 2 ? addl_bit_count1 : (which == 3 ? addl_bit_count1 : addl_bit_count2));
        int& last_byte_bits = (which == 2 ? last_byte_bits1 : (which == 3 ? last_byte_bits1 : last_byte_bits2));
        tail_link_map& uniq_link_map = (which == 2 ? uniq_link_map0 : (which == 3 ? uniq_link_map1 : uniq_link_map2));
        std::vector<uint32_t>& tail_ptr_counts = (which == 2 ? tail_ptr_counts1 : (which == 3 ? tail_ptr_counts1 : tail_ptr_counts2));
        if ((it->second & 0xFFFFFFFF) == 0) {
          tail_link_map::iterator it1 = uniq_link_map.find(link_id);
          if (it1 == uniq_link_map.end()) {
            ptr = tails.size();
            for (int k = 0; k < val.length(); k++)
              tails.push_back(val[k]);
            tails.push_back(0);
          } else {
            tail_map::iterator it2 = uniq_map.find(it1->second);
            if (it2 == uniq_map.end())
              std::cout << "Unexpected linked value not found" << std::endl;
            else {
              if ((it2->second & 0xFFFFFFFF) == 0) {
                it2->second = (it2->second & 0xFFFFFFFF00000000) + tails.size();
                ptr = tails.size() + it1->second.length() - val.length();
                for (int k = 0; k < it1->second.length(); k++)
                  tails.push_back(it1->second[k]);
                tails.push_back(0);
              } else {
                ptr = (it2->second & 0xFFFFFFFF) + it1->second.length() - val.length();
              }
            }
          }
          it->second = (it->second & 0xFFFFFFFF00000000) + ptr;
        } else {
          ptr = it->second & 0xFFFFFFFF;
        }
        int node_val_bits = (which == 2 ? 7 : 6);
        node_val = ptr & ((1 << node_val_bits) - 1);
        ptr >>= node_val_bits;
        //  std::cout << ceil(log2(ptr)) << " ";
        if (which == 2)
          tail_ptr_counts0++;
        else {
          if (tails.size() >= (1 << (node_val_bits + addl_bit_count))) {
            //std::cout << "Tail size: " << tails.size() << std::endl;
            addl_bit_count++;
            last_byte_bits = append_bits(tail_ptrs, tail_ptr_counts, addl_bit_count, true, 0, ptr);
          } else {
            last_byte_bits = append_bits(tail_ptrs, tail_ptr_counts, addl_bit_count, false, last_byte_bits, ptr);
          }
        }
      }
      return node_val;
    }

    void build() {
      std::cout << std::endl;
      byte_vec trie, tails0, tails1, tails2, tail_ptrs1, tail_ptrs2;
      tail_map uniq_map;
      tail_link_map uniq_link_map0, uniq_link_map1, uniq_link_map2;
      build_tail_maps(uniq_map, uniq_link_map0, uniq_link_map1, uniq_link_map2);
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
          std::string& val = cur_node->val;
          uint8_t flags = (cur_node->is_leaf ? 1 : 0) +
            (cur_node->first_child != NULL ? 2 : 0) + (val.length() > 1 ? 4 : 0) +
            (cur_node->next_sibling == NULL ? 8 : 0);
          flag_counts[flags & 0x07]++;
          uint8_t node_val;
          if (val.length() == 1) {
            node_val = cur_node->val[0];
          } else {
            node_val = get_tail_ptr(val, uniq_map, uniq_link_map0, uniq_link_map1, uniq_link_map2, tails0, tails1, tails2,
                         tail_ptrs1, tail_ptrs2, tail_ptr_counts0, tail_ptr_counts1, tail_ptr_counts2,
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
      std::cout << "Total size: " << trie.size()
          +tails0.size()+tails1.size()+tails2.size()
          +tail_ptrs1.size()+tail_ptrs2.size()
          +tail_ptr_counts1.size()*4+tail_ptr_counts2.size()*4 << std::endl;
    }

};

}

#endif
