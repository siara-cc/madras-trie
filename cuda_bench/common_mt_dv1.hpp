#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>
#include <chrono>
#include <thread>
#include <vector>

#include "../src/madras_dv1.hpp"

typedef struct {
  uint32_t key_loc;
  uint32_t key_len;
  union {
    uint32_t leaf_id;
    uint32_t leaf_seq;
  };
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

uint8_t *load_lines(const char *file_name, std::vector<key_ctx> &lines) {

  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(file_name, &file_stat);
  size_t file_size = file_stat.st_size;
  printf("File_name: %s, size: %lu\n", file_name, file_size);

  FILE *fp = fopen(file_name, "rb");
  if (fp == nullptr) {
    perror("Could not open file; ");
    return nullptr;
  }
  uint8_t *file_buf = new uint8_t[file_size + 1];
  size_t res = fread(file_buf, 1, file_size, fp);
  if (res != file_size) {
    perror("Error reading file: ");
    free(file_buf);
    return nullptr;
  }
  fclose(fp);

  size_t line_count = 0;
  bool is_sorted = true;
  const uint8_t *prev_line = (const uint8_t *) "";
  size_t prev_line_len = 0;
  size_t line_len = 0;
  uint8_t *line = gen::extract_line(file_buf, line_len, file_size);
  do {
    if (gen::compare(line, line_len, prev_line, prev_line_len) != 0) {
      uint8_t *key = line;
      int key_len = line_len;
      if (gen::compare(key, key_len, prev_line, gen::min(prev_line_len, key_len)) < 0)
        is_sorted = false;
      lines.push_back((key_ctx) {(uint32_t) (line - file_buf), (uint32_t) line_len, UINT32_MAX});
      prev_line = line;
      prev_line_len = line_len;
      line_count++;
      if ((line_count % 100000) == 0) {
        printf(".");
        fflush(stdout);
      }
    }
    line = gen::extract_line(line, line_len, file_size - (line - file_buf) - line_len);
  } while (line != NULL);
  printf("\n");
  printf("Sorted? : %d\n", is_sorted);

  return file_buf;

}

uint8_t *load_mdx_file(const char *file_name) {

  char mdx_file_name[strlen(file_name) + 5];
  strcpy(mdx_file_name, file_name);
  strcat(mdx_file_name, ".mdx");
  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(mdx_file_name, &file_stat);
  size_t mdx_file_size = file_stat.st_size;
  printf("MDX File_name: %s, size: %lu\n", mdx_file_name, mdx_file_size);
  FILE *fp = fopen(mdx_file_name, "rb");
  if (fp == NULL) {
    perror("Could not open mdx file; ");
    return nullptr;
  }
  uint8_t *mdx_file_buf = new uint8_t[mdx_file_size + 1];
  int res = fread(mdx_file_buf, 1, mdx_file_size, fp);
  if (res != mdx_file_size) {
    perror("Error reading mdx file: ");
    delete [] mdx_file_buf;
    return nullptr;
  }
  fclose(fp);

  return mdx_file_buf;

}
