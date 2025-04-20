#include <cstring>
#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>
#include "../src/madras_dv1.hpp"

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %f\n", msg, time_taken);
  return clock();
}

int main(int argc, char *argv[]) {

  if (argc < 3) {
    printf("Usage: query_mdx <mdx_file> <column_index>");
    return 0;
  }

  clock_t t = clock();

  madras_dv1::static_trie_map dict_reader;
  dict_reader.set_print_enabled(true);
  dict_reader.load(argv[1]);
  //dict_reader.map_file_to_mem(argv[1]);

  t = print_time_taken(t, "Time taken for load: ");

  int column_idx = atoi(argv[2]);

  uint8_t col_val[20];
  size_t col_len = 0;
  uint32_t ptr_bit_count = UINT32_MAX;

  int row_count = dict_reader.get_key_count();
  if (row_count == 0)
    row_count = dict_reader.get_node_count();
  // else {
  //   printf("This utility only for mdx with no primary trie\n");
  //   return 1;
  // }
  char data_type = dict_reader.get_column_type(column_idx);
  printf("Row count: %d, Node count: %u, Col type: %c, name: %s\n", row_count, dict_reader.get_node_count(),
    data_type, dict_reader.get_column_name(column_idx));
  size_t val_len;
  madras_dv1::value_retriever *val_retriever = dict_reader.get_value_retriever(column_idx);
  madras_dv1::val_ctx vctx;
  if (data_type == MST_TEXT || data_type == MST_BIN) {
    vctx.init(dict_reader.get_max_val_len(column_idx) + 1, true, false);
    int64_t sum = 0;
    bool has_next = true;
    val_retriever->fill_val_ctx(0, vctx);
    do {
      has_next = val_retriever->next_val(vctx);
      if (*vctx.val_len != -1)
        sum += *vctx.val_len;
    } while (has_next);
    printf("Sum: %lld\n", sum);
  } else
  if (data_type == MST_INT) {
    vctx.init(32, true, true);
    int64_t sum = 0;
    bool has_next = true;
    val_retriever->fill_val_ctx(0, vctx);
    do {
      has_next = val_retriever->next_val(vctx);
      if (*vctx.val_len != -1)
        sum += *((int64_t *) vctx.val);
    } while (has_next);
    printf("Sum: %lld\n", sum);
  } else {
    vctx.init(32, true, true);
    double sum = 0;
    bool has_next = true;
    val_retriever->fill_val_ctx(0, vctx);
    size_t count = 0;
    do {
      count++;
      has_next = val_retriever->next_val(vctx);
      if (*vctx.val_len != -1)
        sum += *((double *) vctx.val);
    } while (has_next);
    printf("Sum: %lf, count: %lu\n", sum, count);
  }
  printf("vctx.node_id: %u\n", vctx.node_id);
  printf("eps: %lf, ", row_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for sum: ");

  return 1;
}
