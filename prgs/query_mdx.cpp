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
    row_count = dict_reader.get_node_count() - 1;
  else {
    printf("This utility only for mdx with no primary trie\n");
    return 1;
  }
  char data_type = dict_reader.get_column_type(column_idx);
  printf("Row count: %d, Col type: %c, name: %s\n", row_count,
    dict_reader.get_column_type(column_idx), dict_reader.get_column_name(column_idx));
  if (data_type == 't' || data_type == '*') {
    uint8_t val[dict_reader.get_max_val_len(column_idx)];
    //uint8_t val[1000]; // get_max_val_len not working for trie columns
    size_t val_len;
    int64_t sum = 0;
    for (size_t i = 0; i < row_count; i++) {
      bool is_success = dict_reader.get_col_val(i, column_idx, &val_len, val);
      if (is_success && val_len != -1)
        sum += val_len;
    }
    printf("Sum: %lld\n", sum);
  } else
  if (data_type == 'i') {
    madras_dv1::value_retriever *val_retriever = dict_reader.get_value_retriever(column_idx);
    madras_dv1::val_ctx vctx;
    vctx.init(8, true, true);
    val_retriever->init_val_ctx(0, vctx);
    int64_t sum = 0;
    for (int i = 0; i < row_count; i++) {
      bool is_success = val_retriever->next_val(vctx);
      if (is_success && *vctx.val_len != -1)
        sum += *((int64_t *) vctx.val);
    }
    printf("Sum: %lld\n", sum);
  } else {
    madras_dv1::value_retriever *val_retriever = dict_reader.get_value_retriever(column_idx);
    madras_dv1::val_ctx vctx;
    vctx.init(8, true, true);
    val_retriever->init_val_ctx(0, vctx);
    double sum = 0;
    for (int i = 0; i < row_count; i++) {
      bool is_success = val_retriever->next_val(vctx);
      if (is_success && *vctx.val_len != -1)
        sum += *((double *) vctx.val);
    }
    printf("Sum: %lf\n", sum);
  }
  t = print_time_taken(t, "Time taken for sum: ");

  return 1;
}
