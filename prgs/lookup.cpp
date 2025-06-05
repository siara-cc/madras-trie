#include <cstring>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include "../src/madras_dv1.hpp"

double get_wall_time_seconds() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return t.tv_sec + t.tv_nsec / 1e9;
}

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %f (%lu, %lu)\n", msg, time_taken, clock() - t, CLOCKS_PER_SEC);
  return clock();
}

int main(int argc, char *argv[]) {


  clock_t t = clock();
  double start, end;

  madras_dv1::static_trie_map stm;
  stm.map_file_to_mem(argv[1]);

  madras_dv1::input_ctx in_ctx;
  char input[100];
  strcpy(input, "hello");
  while (input[0] != 0) {
    printf("Enter a string: ");
    int scan_res = scanf("%99[^\n]", input);  // reads until newline
    if (scan_res != 1)
      break;
    getchar();
    printf("You entered: %s\n", input);
    if (strlen(input) == 0)
      break;
    start = get_wall_time_seconds();
    in_ctx.key = (const uint8_t *) input;
    in_ctx.key_len = strlen(input);
    bool res = stm.lookup(in_ctx);
    end = get_wall_time_seconds();
    if (res)
      printf("Node id: %u\n", in_ctx.node_id);
    else
      printf("Not found\n");
    printf("Time taken: %.6f seconds\n", end - start);

  }

  return 1;
}
