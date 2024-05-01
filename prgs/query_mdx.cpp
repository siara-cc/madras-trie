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

  madras_dv1::static_dict dict_reader;
  dict_reader.set_print_enabled(true);
  //dict_reader.load(argv[1]);
  dict_reader.map_file_to_mem(argv[1]);

  t = print_time_taken(t, "Time taken for load: ");

  int column_idx = atoi(argv[2]);

  uint8_t col_val[20];
  int col_len = 0;
  uint32_t ptr_bit_count = UINT32_MAX;

  int row_count = dict_reader.key_count;
  if (row_count == 0)
    row_count = dict_reader.node_count;
  char data_type = dict_reader.get_column_type(column_idx);
  printf("Row count: %d, Col type: %c, name: %s\n", row_count,
    dict_reader.get_column_type(column_idx), dict_reader.get_column_name(column_idx));
  if (data_type == '0' || data_type == 'i') {
    int64_t sum = 0;
    for (int i = 0; i < row_count; i++) {
      bool is_success = dict_reader.get_col_val(i, column_idx, &col_len, col_val, &ptr_bit_count);
      if (is_success && col_len != -1)
        sum += *((int64_t *) col_val);
    }
    printf("Sum: %lld\n", sum);
  } else {
    double sum = 0;
    for (int i = 0; i < row_count; i++) {
      *((double *) col_val) = 0;
      bool is_success = dict_reader.get_col_val(i, column_idx, &col_len, col_val, &ptr_bit_count);
      if (is_success && col_len != -1)
        sum += *((double *) col_val);
    }
    printf("Sum: %lf\n", sum);
  }
  t = print_time_taken(t, "Time taken for sum: ");

  return 1;
}
