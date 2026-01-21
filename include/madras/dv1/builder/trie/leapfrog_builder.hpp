#ifndef __DV1_BUILDER_LEAPFROG__
#define __DV1_BUILDER_LEAPFROG__

#include "madras/dv1/common.hpp"
#include "madras/dv1/builder/output_writer.hpp"
#include "madras/dv1/builder/trie/memtrie/in_mem_trie.hpp"

namespace madras { namespace dv1 {

struct bldr_min_pos_stats {
  uint8_t min_b;
  uint8_t max_b;
  uint8_t min_len;
  uint8_t max_len;
  bldr_min_pos_stats() {
    max_b = max_len = 0;
    min_b = min_len = 0xFF;
  }
};

class leap_frog_builder {
  private:
    uint8_t min_pos[256][256];
    output_writer &output;
    memtrie::in_mem_trie &memtrie;

  public:
    leap_frog_builder(memtrie::in_mem_trie &_memtrie, output_writer &_output)
            : memtrie (_memtrie), output (_output) {
    }
    // uintxx_t min_len_count[256];
    // uintxx_t min_len_last_ns_id[256];
    bldr_min_pos_stats make_min_positions() {
      clock_t t = clock();
      bldr_min_pos_stats stats;
      memset(min_pos, 0xFF, 65536);
      // memset(min_len_count, 0, sizeof(uintxx_t) * 256);
      // memset(min_len_last_ns_id, 0, sizeof(uintxx_t) * 256);
      for (size_t i = 1; i < memtrie.all_node_sets.size(); i++) {
        memtrie::node_set_handler cur_ns(memtrie.all_node_sets, i);
        uint8_t len = cur_ns.last_node_idx();
        if (stats.min_len > len)
          stats.min_len = len;
        if (stats.max_len < len)
          stats.max_len = len;
        // if (i < memtrie.node_set_count / 4) {
        //   min_len_count[len]++;
        //   min_len_last_ns_id[len] = i;
        // }
        memtrie::node cur_node = cur_ns.first_node();
        for (size_t k = 0; k <= len; k++) {
          uint8_t b = cur_node.get_byte();
          if (min_pos[len][b] > k)
             min_pos[len][b] = k;
          if (stats.min_b > b)
            stats.min_b = b;
          if (stats.max_b < b)
            stats.max_b = b;
          cur_node.next();
        }
      }
      gen::print_time_taken(t, "Time taken for make_min_positions(): ");
      // for (int i = 0; i < 256; i++) {
      //   if (min_len_count[i] > 0)
      //     printf("%d\t%u\t%u\n", i, min_len_count[i], min_len_last_ns_id[i]);
      // }
      return stats;
    }

    uintxx_t decide_min_stat_to_use(bldr_min_pos_stats& stats) {
      clock_t t = clock();
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        int good_b_count = 0;
        for (int j = stats.min_b; j <= stats.max_b; j++) {
          if (min_pos[i][j] > 0 && min_pos[i][j] != 0xFF)
            good_b_count++;
        }
        if (good_b_count > (i >> 1)) {
          stats.min_len = i;
          break;
        }
      }
      // for (int i = stats.min_len; i <= stats.max_len; i++) {
      //   printf("Len: %d:: ", i);
      //   for (int j = stats.min_b; j <= stats.max_b; j++) {
      //     int min = min_pos[i][j];
      //     if (min != 255)
      //       printf("%c(%d): %d, ", j, j, min);
      //   }
      //   printf("\n\n");
      // }
      gen::print_time_taken(t, "Time taken for decide_min_stat_to_use(): ");
      return 0; //todo
    }

    void write_sec_cache(const bldr_min_pos_stats& stats, uintxx_t sec_cache_size) {
      for (int i = stats.min_len; i <= stats.max_len; i++) {
        for (int j = 0; j <= 255; j++) {
          uint8_t min_len = min_pos[i][j];
          if (min_len == 0xFF)
            min_len = 0;
          min_len++;
          output.write_byte(min_len);
        }
      }
    }

};

}}

#endif
