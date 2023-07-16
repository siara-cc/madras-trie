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

typedef std::map<std::string, uint64_t> tail_map;
typedef std::multimap<uint32_t, std::string> tail_freq_map;
typedef std::map<uint32_t, std::string> tail_link_map;
typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uint8_t> all_tails;

class entry {
  public:
    std::string key;
    uint64_t tail;
    entry(std::string _key, uint64_t _tail) {
      key = _key;
      tail = _tail;
    }
};

typedef std::vector<entry> tail_vec;

bool comparePairs(const entry& lhs, const entry& rhs) {
  return lhs.key < rhs.key;
}

class node {
  public:
    node *first_child;
    //node *parent;
    node *next_sibling;
    uint32_t tail_pos;
    uint16_t tail_len;
    uint8_t is_leaf;
    uint8_t level;
    node() {
      //parent = NULL;
      first_child = next_sibling = NULL;
      is_leaf = 0;
      level = 0;
    }
};

class builder {

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
    tail_vec uniq_vec_map;
    std::vector<uint8_t> tails;

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

    bool add2_link_map(uint32_t link_id, tail_link_map& uniq_link_map, std::string& val) {
      tail_link_map::iterator it_link = uniq_link_map.find(link_id);
      bool to_ins = false;
      bool exists = false;
      if (it_link == uniq_link_map.end())
        to_ins = true;
      else {
        exists = true;
        if (it_link->second.length() < val.length())
          to_ins = true;
      }
      if (to_ins) {
        uniq_link_map.erase(link_id);
        //std::cout << "Added link: " << (unsigned long) &uniq_link_map << " " << link_id << " " << val << std::endl;
        uniq_link_map.insert(pair<uint32_t, std::string>(link_id, val));
      }
      return !exists;
    }

    const static uint32_t part1 = 128;
    const static uint32_t part2 = 4096;
    const static uint32_t part3 = (1 << 31);
    void assign_tail_bucket(tail_map& uniq_map, tail_link_map& uniq_link_map, tail_freq_map& uniq_freq_map) {
      tail_freq_map::iterator it_freq = uniq_freq_map.end();
      uint32_t tails_len = 0;
      uint32_t total_tails_len = 0;
      uint32_t tails_count = 0;
      uint32_t tails_freq_count = 0;
      uint32_t part = part1;
      uint32_t s_no = 0;
      while (it_freq != uniq_freq_map.begin()) {
        it_freq--;
        s_no++;
        tail_map::iterator it_tails = uniq_map.find(it_freq->second);
        if (it_tails == uniq_map.end())
          std::cout << "Unexpected" << std::endl;
        else {
          uint32_t link_id = it_tails->second >> 32;
          if (part == part1 && link_id >= 0x40000000) {
            std::cout << s_no << "*\t" << tails_count << "*\t" << it_freq->first << "\t[";
            std::cout << it_freq->second << "]\t" << 0 << "\t[" << 1 << "]" << std::endl;
          }
          if (link_id >= 0x40000000)
            continue;
          tail_link_map::iterator it_link = uniq_link_map.find(link_id);
          if (it_link != uniq_link_map.end())
            continue;
          if (part < part2) {
            std::cout << s_no << "\t" << tails_count << "\t" << it_freq->first << "\t[";
            std::cout << it_freq->second << "]\t" << 0 << "\t[" << (it_link == uniq_link_map.end() ? "-" : it_link->second) << "]\t" << tails_len << std::endl;
          }
          if ((tails_len + it_freq->second.length() + 1) > part) {
            std::cout << log2(part) << "\t" << part << "\t" << tails_count << "\t" << tails_freq_count << "\t" << tails_len << std::endl;
            part = (part == part1 ? part2 : part3);
            total_tails_len += tails_len;
            tails_len = tails_count = tails_freq_count = 0;
          }
          tails_len += it_freq->second.length();
          tails_len++;
          tails_count++;
          tails_freq_count += it_freq->first;
          it_tails->second |= (part == part1 ? 0x4000000000000000 : (part == part2 ? 0x8000000000000000 : 0xC000000000000000));
          std::string& val = it_freq->second;
          if (val.length() > 2) {
            int suffix_count = val.length() - 2;
            for (int k = 1; k <= suffix_count; k++) {
              std::string suffix = val.substr(k);
              tail_map::iterator it1 = uniq_map.find(suffix);
              if (it1 != uniq_map.end()) {
                uint8_t marked_part = (it1->second >> 62);
                uint8_t part_mark = (part == part1 ? 0x01 : (part == part2 ? 0x02 : 0x03));
                if (marked_part != 0 && marked_part != part_mark)
                  continue;
                uint32_t link_id = (it1->second >> 32) & 0x3FFFFFFF;
                uint32_t freq = it1->second & 0xFFFFFFFF;
                if (true) {
                  bool is_added = add2_link_map(link_id, uniq_link_map, val);
                  if (is_added) {
                    if (part < part2) {
                      std::cout << s_no << "\t" << tails_count << "*\t" << freq << "\t[";
                      std::cout << suffix << "]\t" << 0 << "\t[" << val << "]" << (freq > it_freq->first ? "**" : "") << "\t" << tails_len << std::endl;
                    }
                    it1->second |= (part == part1 ? 0x4000000000000000 : (part == part2 ? 0x8000000000000000 : 0xC000000000000000));
                    tails_count++;
                    tails_freq_count += freq;
                  }
                  if (is_added && marked_part != 0) {
                    tails_len -= suffix.length();
                    tails_len--;
                    tails_freq_count -= freq;
                    tails_count--;
                  }
                }
              }
            }
          }
        }
      }
      std::cout << log2(part) << "\t" << part << "\t" << tails_count << "\t" << tails_freq_count << "\t" << tails_len << std::endl;
      total_tails_len += tails_len;
      std::cout << "Total len: " << total_tails_len << std::endl;
    }

    void add2_uniq_map(tail_map& uniq_map, std::string& val, uint32_t& max_freq) {

      // uniq_vec_map.push_back(entry(val, 1));
      // uint64_t *out = (uint64_t *) art_search(&at, (uint8_t *) val.c_str(), val.length());
      // if (out == NULL) {
      //   uint64_t *in_val = new uint64_t();
      //   *in_val = 1;
      //   art_insert(&at, (const uint8_t *) val.c_str(), val.length(), (void *) in_val);
      // } else {
      //   (*out)++;
      // }

      // uint64_t tail_val = 1;
      // int tail_val_len = 8;
      // bool is_present = uniq_basix_map.get((const char *) val.c_str(), val.length(), &tail_val_len, (char *) &tail_val);
      // if (is_present)
      //   tail_val++;
      // uniq_basix_map.put(val.c_str(), val.length(), (const char *) &tail_val, tail_val_len);

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

    void build_tail_maps(tail_map& uniq_map, tail_link_map& uniq_link_map) {

      // clock_t t;
      // t = clock();

      uint32_t max_freq = 0;
      for (int i = 0; i < level_nodes.size(); i++) {
        std::vector<node *> cur_lvl_nodes = level_nodes[i];
        for (int j = 0; j < cur_lvl_nodes.size(); j++) {
          node *cur_node = cur_lvl_nodes[j];
          std::string val((const char *) tails.data() + cur_node->tail_pos, cur_node->tail_len);
          int val_len = val.length();
          if (val_len > 1) {
            add2_uniq_map(uniq_map, val, max_freq);
          }
        }
      }

      // t = clock() - t;
      // double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
      // std::cout << "Time taken to append vector: " << time_taken << std::endl;
      // t = clock();

      // std::sort(uniq_vec_map.begin(), uniq_vec_map.end(), comparePairs);
      // t = clock() - t;
      // time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
      // std::cout << "Time taken for sort: " << time_taken << std::endl;

      //uniq_basix_map.print_num_levels();
      //uniq_basix_map.print_stats(uniq_basix_map.size());
      std::cout << "Total Uniq: " << uniq_map.size() << std::endl;
      tail_freq_map uniq_freq_map;
      tail_map::iterator it;
      uint32_t tail_freq_count = 0;
      for (it = uniq_map.begin(); it != uniq_map.end(); it++) {
        std::string val = it->first;
        uint32_t freq = it->second & 0xFFFFFFFF;
        uniq_freq_map.insert(pair<uint32_t, std::string>(freq, val));
        tail_freq_count += freq;
      }
      std::cout << "Total Freq: " << tail_freq_count << std::endl;
      assign_tail_bucket(uniq_map, uniq_link_map, uniq_freq_map);
      tail_map::iterator it_tails = uniq_map.end();
      const std::string *prev_entry = NULL;
      int prev_prefix_len = 0;
      int group_len = 0;
      int max_group_len = 0;
      it_tails--;
      uint32_t savings = 0;
      FILE *fp = fopen("prefix_match.txt", "w+");
      char buf[1000];
      do {
        uint32_t link_id = (it_tails->second >> 32) & 0x3FFFFFFF;
        tail_link_map::iterator it_link = uniq_link_map.find(link_id);
        if (it_link != uniq_link_map.end()) {
          it_tails--;
          continue;
        }
        if (prev_entry == NULL) {
          prev_entry = &it_tails->first;
          // fwrite(it_tails->first.c_str(), it_tails->first.length(), 1, fp);
          // fwrite("\n", 1, 1, fp);
          group_len += it_tails->first.length();
          it_tails--;
          sprintf(buf, "[%s]", it_tails->first.c_str());
          fwrite(buf, strlen(buf), 1, fp);
          //std::cout << "[" << *prev_entry << "] ";
          continue;
        }
        int prefix_len = 0;
        int max_len = prev_entry->length() > it_tails->first.length() ? it_tails->first.length() : prev_entry->length();
        for (int i = 0; i < max_len && (*prev_entry)[i] == it_tails->first[i]; i++)
           prefix_len++;
        //std::cout << *prev_entry << " " << it_tails->first << " " << prefix_len << std::endl;
        if (prefix_len > 1 && group_len < 64) { // && (prefix_len >= prev_prefix_len || ((prev_prefix_len - prefix_len) < 4))) {
          //std::cout << prefix_len << "[" << it_tails->first << "] ";
          sprintf(buf, "%d{%s|%s}%lu ", prefix_len, it_tails->first.substr(0, prefix_len).c_str(), it_tails->first.substr(prefix_len).c_str(), it_tails->first.length()-prefix_len);
          fwrite(buf, strlen(buf), 1, fp);
          savings += prefix_len;
          savings -= (prefix_len / 13);
          savings--;
          group_len += prefix_len;
          prev_prefix_len = prefix_len;
        } else {
          // if (prefix_len > 1)
          //   std::cout << std::endl;
          if (group_len > max_group_len)
            max_group_len = group_len;
          prev_prefix_len = 0;
          group_len = 0;
          //std::cout << "[" << it_tails->first << "] ";
          // fwrite(it_tails->first.c_str(), it_tails->first.length(), 1, fp);
          fwrite("\n\n", 2, 1, fp);
          sprintf(buf, "[%s] ", it_tails->first.c_str());
          fwrite(buf, strlen(buf), 1, fp);
        }
        prev_entry = &it_tails->first;
        it_tails--;
      } while (it_tails != uniq_map.begin());
      fwrite("\n", 1, 1, fp);
      fclose(fp);
      std::cout << std::endl;
      std::cout << "Savings: " << savings << std::endl;
      std::cout << "Max group len: " << max_group_len << std::endl;
      it_tails = uniq_map.begin();
      while (it_tails != uniq_map.end()) {
        it_tails->second &= 0xFFFFFFFF00000000;
        it_tails++;
      }
    }

    uint8_t get_tail_ptr(std::string& val, tail_map& uniq_map, tail_link_map& uniq_link_map,
             std::vector<uint8_t>& tails0, std::vector<uint8_t>& tails1, std::vector<uint8_t>& tails2,
             std::vector<uint8_t>& tail_ptrs1, std::vector<uint8_t>& tail_ptrs2,
             uint32_t& tail_ptr_counts0, std::vector<uint32_t>& tail_ptr_counts1, std::vector<uint32_t>& tail_ptr_counts2,
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
        if (which == 0)
          std::cout << "ERROR: not marked: " << it->first << std::endl;
        std::vector<uint8_t>& tails = (which == 1 ? tails0 : (which == 2 ? tails1 : tails2));
        std::vector<uint8_t>& tail_ptrs = (which == 1 ? tail_ptrs1 : (which == 2 ? tail_ptrs1 : tail_ptrs2));
        int& addl_bit_count = (which == 1 ? addl_bit_count1 : (which == 2 ? addl_bit_count1 : addl_bit_count2));
        int& last_byte_bits = (which == 1 ? last_byte_bits1 : (which == 2 ? last_byte_bits1 : last_byte_bits2));
        std::vector<uint32_t>& tail_ptr_counts = (which == 1 ? tail_ptr_counts1 : (which == 2 ? tail_ptr_counts1 : tail_ptr_counts2));
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
              if ((it2->second >> 62) != which)
                std::cout << "WARN: " << which << ", " << (it2->second >> 62) << ", [" << it1->second << "], [" << it->first << std::endl;
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
        int node_val_bits = (which == 1 ? 7 : 6);
        node_val = ptr & ((1 << node_val_bits) - 1);
        node_val |= (which << 6);
        // if (which == 2 || which == 3)
        //   std::cout << "Ptr: " << ptr << " " << which << std::endl;
        if (which == 1 && ptr > (part1 - 1))
          std::cout << "ERROR: " << ptr << " > " << part1 << std::endl;
        if (which == 2 && ptr > (part2 - 1))
          std::cout << "ERROR: " << ptr << " > " << part2 << std::endl;
        ptr >>= node_val_bits;
        //  std::cout << ceil(log2(ptr)) << " ";
        if (which == 1) {
          tail_ptr_counts0++;
        } else {
          if (ptr >= (1 << addl_bit_count)) {
            std::cout << "Tail ptr: " << ptr << " " << which << std::endl;
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
      byte_vec trie, tails0, tails1, tails2;
      byte_vec tail_ptrs1, tail_ptrs2;
      tail_map uniq_map;
      tail_link_map uniq_link_map;
      build_tail_maps(uniq_map, uniq_link_map);
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
            node_val = get_tail_ptr(val, uniq_map, uniq_link_map, tails0, tails1, tails2,
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
