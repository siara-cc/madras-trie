#include <iostream>
#include <string>
#include <chrono>
#include <cstdio>
#include <sys/stat.h>
#include <fcntl.h>

#include "madras/dv1/reader/static_trie_map.hpp"
#include "madras/dv1/builder/madras_builder.hpp"
#include "madras/dv1/ds_common/vint.hpp"

using namespace std;

typedef struct {
  uint8_t *key;
  uint32_t key_len;
  union {
    uint32_t leaf_id;
    uint32_t leaf_seq;
  };
} key_ctx;

using steady_clock_t = std::chrono::steady_clock;
using time_point_t  = steady_clock_t::time_point;

double time_taken_in_secs(time_point_t start)
{
    auto end = steady_clock_t::now();
    std::chrono::duration<double> elapsed = end - start;
    return elapsed.count();
}

time_point_t print_time_taken(time_point_t start, const char* msg)
{
    double time_taken = time_taken_in_secs(start);
    std::printf("%s %lf\n", msg, time_taken);
    return steady_clock_t::now();
}

bool nodes_sorted_on_freq;
madras::dv1::static_trie *bench_build(int argc, char *argv[], std::vector<uint8_t>& output_buf, std::vector<key_ctx>& lines,
    bool is_sorted, int trie_count, size_t& trie_size, double& time_taken, double& keys_per_sec, bool as_int, int max_groups) {

  int asc = argc > 4 ? atoi(argv[4]) : 0;
  int leapfrog = argc > 5 ? atoi(argv[5]) : 0;
  int sfx_coding = argc > 8 ? atoi(argv[8]) : 1;

  auto t = steady_clock_t::now();

  size_t line_count = lines.size();
  madras::dv1::builder *sb;
  madras::dv1::bldr_options bldr_opts = madras::dv1::dflt_opts;
  bldr_opts.max_inner_tries = trie_count;
  bldr_opts.max_groups = max_groups;
  bldr_opts.sort_nodes_on_freq = asc > 0 ? false : true;
  bldr_opts.leap_frog = leapfrog > 0 ? true : false;
  bldr_opts.partial_sfx_coding = sfx_coding;
  sb = new madras::dv1::builder(nullptr, "kv_table,Key", 1, "t", "u", 0, 1, &bldr_opts);

  sb->set_print_enabled(false);

  int64_t ival;
  size_t isize;
  uint8_t istr[10];

  for (size_t i = 0; i < lines.size(); i++) {
    if (as_int) {
      ival = std::atoll((const char *) lines[i].key);
      isize = gen::get_svint60_len(ival);
      gen::copy_svint60(ival, (uint8_t *) istr, isize);
      memcpy(lines[i].key, istr, isize);
      lines[i].key_len = isize;
    }
    sb->insert(lines[i].key, lines[i].key_len, i);
  }
  //t = print_time_taken(t, "Time taken for insert/append: ");

  sb->set_out_vec(&output_buf);
  sb->build_and_write_all(true);

  uint32_t seq_idx = 0;
  nodes_sorted_on_freq = sb->get_opts()->sort_nodes_on_freq;
  // if (nodes_sorted_on_freq) {
  //   sb->set_leaf_seq(1, seq_idx, [&lines](uint32_t arr_idx, uint32_t leaf_seq) -> void {
  //     key_ctx *kc = &lines[arr_idx];
  //     kc->leaf_seq = leaf_seq;
  //     //printf("%u, %u\n", arr_idx, leaf_seq);
  //   });
  //   std::sort(lines.begin(), lines.end(), [](const key_ctx& lhs, const key_ctx& rhs) -> bool {
  //     return lhs.leaf_seq < rhs.leaf_seq;
  //   });
  //   // int idx = lines.size() - 1;
  //   // printf("%d, %u, [%.*s]\n", idx, lines[idx].leaf_seq, lines[idx].key_len, lines[idx].key);
  // }
  delete sb;

  madras::dv1::static_trie *trie_reader = new madras::dv1::static_trie();
  trie_reader->load_static_trie(output_buf.data());

  trie_size = sb->tp.total_idx_size;
  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return trie_reader;

}

bool bench_lookup(std::vector<key_ctx>& lines, madras::dv1::static_trie *trie_reader, double& time_taken, double& keys_per_sec) {

  madras::dv1::input_ctx in_ctx;

  auto t = steady_clock_t::now();

  key_ctx *kc;
  size_t line_count = lines.size();
  for (size_t i = 0; i < line_count; i++) {

    kc = &lines[i];

    in_ctx.key = kc->key;
    in_ctx.key_len = kc->key_len;

    if (!trie_reader->lookup(in_ctx)) {
      return false;
    }
    kc->leaf_id = trie_reader->leaf_rank1(in_ctx.node_id);

  }

  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return true;

}

bool bench_rev_lookup(std::vector<key_ctx>& lines, madras::dv1::static_trie *trie_reader, double& time_taken, double& keys_per_sec) {

  size_t out_key_len = 0;
  std::vector<uint8_t> out_key_buf(trie_reader->get_max_key_len() + 1);

  auto t = steady_clock_t::now();

  key_ctx *kc;
  size_t line_count = lines.size();
  for (size_t i = 0; i < line_count; i++) {

    kc = &lines[i];
    if (!trie_reader->reverse_lookup(kc->leaf_id, &out_key_len, out_key_buf.data()))
      return false;
    if (kc->key_len != out_key_len || memcmp(kc->key, out_key_buf.data(), kc->key_len) != 0)
      return false;

  }
  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return true;

}

bool bench_next(std::vector<key_ctx>& lines, madras::dv1::static_trie *trie_reader, double& time_taken, double& keys_per_sec) {

  size_t out_key_len = 0;
  std::vector<uint8_t> out_key_buf(trie_reader->get_max_key_len() + 1);
  madras::dv1::iter_ctx dict_ctx;
  dict_ctx.init(trie_reader->get_max_key_len(), trie_reader->get_max_level());

  auto t = steady_clock_t::now();

  key_ctx *kc;
  size_t line_count = lines.size();
  for (size_t i = 0; i < line_count; i++) {

    kc = &lines[i];

    out_key_len = trie_reader->next(dict_ctx, out_key_buf.data());
    if (kc->key_len != out_key_len || memcmp(kc->key, out_key_buf.data(), kc->key_len) != 0)
      return false;

  }
  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return true;

}

int main(int argc, char *argv[]) {

  if (argc < 2) {
    printf("Usage: madras_bench <input_file> [min_inner_tries] [max_inner_tries] [asc] [leapfrog] [numbers] [max_groups]\n");
    return 0;
  }

  int min_inner_tries = argc > 2 ? atoi(argv[2]) : 0;
  int max_inner_tries = argc > 3 ? atoi(argv[3]) : 2;
  // TODO: only 1 level works for as_int
  bool as_int = argc > 6 ? (atoi(argv[6]) == 1 ? true : false) : false;
  int max_groups = argc > 7 ? atoi(argv[7]) : 1;

  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(argv[1], &file_stat);
  uint8_t *file_buf = new uint8_t[file_stat.st_size + 1];
  printf("File_name: %s, size: %ld\n", argv[1], (long) file_stat.st_size);

  FILE *fp = fopen(argv[1], "rb");
  if (fp == NULL) {
    perror("Could not open file; ");
    free(file_buf);
    return 1;
  }
  size_t res = fread(file_buf, 1, file_stat.st_size, fp);
  if (res != file_stat.st_size) {
    perror("Error reading file: ");
    free(file_buf);
    return 1;
  }
  fclose(fp);

  size_t line_count = 0;
  bool is_sorted = true;
  std::vector<key_ctx> lines;
  const uint8_t *prev_line = (const uint8_t *) "";
  size_t prev_line_len = 0;
  size_t line_len = 0;
  uint8_t *line = gen::extract_line(file_buf, line_len, file_stat.st_size);
  do {
    if (gen::compare(line, line_len, prev_line, prev_line_len) != 0) {
      uint8_t *key = line;
      int key_len = line_len;
      if (gen::compare(key, key_len, prev_line, gen::min(prev_line_len, key_len)) < 0)
        is_sorted = false;
      lines.push_back(key_ctx{line, (uint32_t) line_len, 0});
      prev_line = line;
      prev_line_len = line_len;
      line_count++;
      if ((line_count % 100000) == 0) {
        printf(".");
        fflush(stdout);
      }
    }
    line = gen::extract_line(line, line_len, file_stat.st_size - (line - file_buf) - line_len);
  } while (line != NULL);
  printf("\n");
  std::cout << "Sorted? : " << is_sorted << std::endl;

//   if (!is_sorted) {
//     std::sort(lines.begin(), lines.end(), [as_int](const key_ctx& lhs, const key_ctx& rhs) -> bool {
//       if (as_int)
//         return atoll((const char *) lhs.key) < atoll((const char *) rhs.key);
//       return gen::compare(lhs.key, lhs.key_len, rhs.key, rhs.key_len) < 0;
//     });
//     is_sorted = true;
//   }

  size_t trie_size;
  double time_taken;
  double keys_per_sec;

  std::vector<uint8_t> output_buf;
  madras::dv1::static_trie *trie_reader;

  for (int i = min_inner_tries; i <= max_inner_tries; i++) {
    trie_reader = bench_build(argc, argv, output_buf, lines, is_sorted, i, trie_size, time_taken, keys_per_sec, as_int, max_groups);
    if (trie_size == 0) {
      printf("Build fail\n");
      return 1;
    }
    printf("%zu\t%lf\t", trie_size, keys_per_sec);
    bool is_success = bench_lookup(lines, trie_reader, time_taken, keys_per_sec);
    if (!is_success) {
      printf("Lookup fail\n");
      return 1;
    }
    printf("%lf\t", keys_per_sec);
    is_success = bench_rev_lookup(lines, trie_reader, time_taken, keys_per_sec);
    if (!is_success) {
      printf("Rev lookup fail\n");
      return 1;
    }
    printf("%lf\t", keys_per_sec);
    if (!nodes_sorted_on_freq && is_sorted) {
      is_success = bench_next(lines, trie_reader, time_taken, keys_per_sec);
      if (!is_success) {
        printf("Next fail\n");
        return 1;
      }
      printf("%lf\n", keys_per_sec);
    } else
      printf("\n");
    delete trie_reader;
    output_buf.clear();
  }

  delete [] file_buf;

  return 0;

}
