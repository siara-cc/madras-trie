#include <iostream>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>

#include "src/madras_dv1.hpp"
#include "src/madras_builder_dv1.hpp"

using namespace std;

typedef struct {
  uint8_t *key;
  uint32_t key_len;
  uint32_t leaf_id;
} key_ctx;

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %lf\n", msg, time_taken);
  return clock();
}

madras_dv1::static_trie_map *bench_build(int argc, char *argv[], std::vector<uint8_t>& output_buf, std::vector<key_ctx>& lines, bool is_sorted, int trie_count, size_t& trie_size, double& time_taken, double& keys_per_sec) {

  clock_t t = clock();

  size_t line_count = lines.size();
  madras_dv1::builder *sb;
  madras_dv1::bldr_options bldr_opts = madras_dv1::dflt_opts;
  bldr_opts.max_inner_tries = trie_count;
  sb = new madras_dv1::builder(nullptr, "kv_table,Key", 1, "t", "u", 0, false, false, bldr_opts);
  sb->set_print_enabled(false);

  for (size_t i = 0; i < lines.size(); i++) {
    sb->insert(lines[i].key, lines[i].key_len);
  }
  //t = print_time_taken(t, "Time taken for insert/append: ");

  sb->set_out_vec(&output_buf);
  sb->write_kv();

  sb->write_final_val_table();

  bool nodes_sorted_on_freq = sb->opts.sort_nodes_on_freq;
  delete sb;

  madras_dv1::static_trie_map *trie_reader = new madras_dv1::static_trie_map();
  trie_reader->set_print_enabled(false);
  trie_reader->load_from_mem(output_buf.data(), output_buf.size());

  trie_size = sb->tp.total_idx_size;
  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return trie_reader;

}

bool bench_lookup(int argc, char *argv[], std::vector<uint8_t>& output_buf, std::vector<key_ctx>& lines, bool is_sorted, int trie_count, madras_dv1::static_trie_map *trie_reader, double& time_taken, double& keys_per_sec) {

  size_t out_key_len = 0;
  uint8_t key_buf[trie_reader->get_max_key_len() + 1];
  madras_dv1::iter_ctx dict_ctx;
  dict_ctx.init(trie_reader->get_max_key_len(), trie_reader->get_max_level());

  madras_dv1::input_ctx in_ctx;

  //t = print_time_taken(t, "Time taken for load: ");

  clock_t t = clock();

  key_ctx *kc;
  size_t line_count = lines.size();
  for (size_t i = 0; i < line_count; i++) {
    // if (gen::compare(line, line_len, prev_line, prev_line_len) == 0)
    //   continue;
    // prev_line = line;
    // prev_line_len = line_len;

    kc = &lines[i];

    in_ctx.key = kc->key;
    in_ctx.key_len = kc->key_len;

    // if (strcmp((const char *) in_ctx.key, "a 10 episode") == 0)
    //   int ret = 1;

    // uint8_t *tab_loc = (uint8_t *) memchr(line, '\t', line_len);
    // if (tab_loc != NULL) {
    //   key_len = tab_loc - line;
    //   val = tab_loc + 1;
    //   val_len = line_len - key_len - 1;
    // } else {
    //   val_len = (line_len > 6 ? 7 : line_len);
    // }

    // if (is_sorted && !nodes_sorted_on_freq) {
    //   out_key_len = trie_reader.next(dict_ctx, key_buf);
    //   if (out_key_len != in_ctx.key_len)
    //     printf("Len mismatch: [%.*s], [%.*s], %d, %d, %d\n", in_ctx.key_len, in_ctx.key, (int) out_key_len, key_buf, in_ctx.key_len, (int) out_key_len, (int) out_val_len);
    //   else {
    //     if (memcmp(in_ctx.key, key_buf, in_ctx.key_len) != 0)
    //       printf("Key mismatch: E:[%.*s], A:[%.*s]\n", in_ctx.key_len, in_ctx.key, (int) out_key_len, key_buf);
    //     if (what == 2 && memcmp(in_ctx.key, val_buf, out_val_len) != 0)
    //       printf("n2:Val mismatch: E:[%.*s], A:[%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
    //     if (what == 0 && memcmp(val, val_buf, out_val_len) != 0)
    //       printf("Val mismatch: [%.*s], [%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
    //   }
    // }

    if (!trie_reader->lookup(in_ctx)) {
      return false;
    }
    kc->leaf_id = trie_reader->leaf_rank1(in_ctx.node_id);
    // else {
    //   uint32_t leaf_id = trie_reader.leaf_rank1(in_ctx.node_id);
    //   bool success = trie_reader.reverse_lookup(leaf_id, &out_key_len, key_buf);
    //   key_buf[out_key_len] = 0;
    //   if (strncmp((const char *) in_ctx.key, (const char *) key_buf, in_ctx.key_len) != 0)
    //     printf("Reverse lookup fail - e:[%s], a:[%.*s]\n", in_ctx.key, in_ctx.key_len, key_buf);
    // }

  }
  // out_key_len = trie_reader.next(dict_ctx, key_buf, val_buf, &out_val_len);
  // if (out_key_len != -1)
  //   printf("Expected Eof: [%.*s], %d\n", out_key_len, key_buf, out_key_len);

  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return true;

}

bool bench_rev_lookup(int argc, char *argv[], std::vector<uint8_t>& output_buf, std::vector<key_ctx>& lines, bool is_sorted, int trie_count, madras_dv1::static_trie_map *trie_reader, double& time_taken, double& keys_per_sec) {

  size_t out_key_len = 0;
  uint8_t out_key_buf[trie_reader->get_max_key_len() + 1];

  clock_t t = clock();

  key_ctx *kc;
  size_t line_count = lines.size();
  for (size_t i = 0; i < line_count; i++) {

    kc = &lines[i];
    if (!trie_reader->reverse_lookup(kc->leaf_id, &out_key_len, out_key_buf))
      return false;
    if (kc->key_len != out_key_len || memcmp(kc->key, out_key_buf, kc->key_len) != 0)
      return false;

  }
  time_taken = time_taken_in_secs(t);
  keys_per_sec = line_count / time_taken / 1000;

  return true;

}

int main(int argc, char *argv[]) {

  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(argv[1], &file_stat);
  uint8_t *file_buf = (uint8_t *) malloc(file_stat.st_size + 1);
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
      lines.push_back((key_ctx) {line, (uint32_t) line_len, 0});
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

  if (!is_sorted) {
    std::sort(lines.begin(), lines.end(), [](const key_ctx& lhs, const key_ctx& rhs) -> bool {
      return gen::compare(lhs.key, lhs.key_len, rhs.key, rhs.key_len) < 0;
    });
    is_sorted = true;
  }

  size_t trie_size;
  double time_taken;
  double keys_per_sec;

  std::vector<uint8_t> output_buf;
  madras_dv1::static_trie_map *trie_reader;

  for (int i = 0; i < 3; i++) {
    trie_reader = bench_build(argc, argv, output_buf, lines, is_sorted, i, trie_size, time_taken, keys_per_sec);
    if (trie_size == 0) {
      printf("Build fail\n");
      return 1;
    }
    printf("%lu     %lf     ", trie_size, keys_per_sec);
    bool is_success = bench_lookup(argc, argv, output_buf, lines, is_sorted, i, trie_reader, time_taken, keys_per_sec);
    if (!is_success) {
      printf("Lookup fail\n");
      return 1;
    }
    printf("%lf     ", keys_per_sec);
    is_success = bench_rev_lookup(argc, argv, output_buf, lines, is_sorted, i, trie_reader, time_taken, keys_per_sec);
    if (!is_success) {
      printf("Rev lookup fail\n");
      return 1;
    }
    printf("%lf\n", keys_per_sec);
    delete trie_reader;
    output_buf.clear();
  }

  free(file_buf);

  return 0;

}
