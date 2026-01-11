#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>
#include <math.h>
#include <chrono>
#include <thread>
#include <vector>

size_t min_of(size_t n1, size_t n2) {
  return n1 > n2 ? n2 : n1;
}

static int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2) {
    int lim = (len2 < len1 ? len2 : len1);
    int k = 0;
    do {
        uint8_t c1 = v1[k];
        uint8_t c2 = v2[k];
        k++;
        if (c1 < c2)
            return -k;
        else if (c1 > c2)
            return k;
    } while (k < lim);
    if (len1 == len2)
        return 0;
    k++;
    return (len1 < len2 ? -k : k);
}

uint8_t *extract_line(uint8_t *last_line, size_t& last_line_len, size_t remaining) {
  if (remaining == 0)
    return nullptr;
  last_line += last_line_len;
  while (*last_line == '\r' || *last_line == '\n' || *last_line == '\0') {
    last_line++;
    remaining--;
    if (remaining == 0)
      return nullptr;
  }
  uint8_t *cr_pos = (uint8_t *) memchr(last_line, '\n', remaining);
  if (cr_pos != nullptr) {
    if (*(cr_pos - 1) == '\r')
      cr_pos--;
    *cr_pos = 0;
    last_line_len = cr_pos - last_line;
  } else
    last_line_len = remaining;
  return last_line;
}

typedef struct {
  uint32_t key_loc;
  uint32_t key_len;
  union {
    uint32_t leaf_id;
    uint32_t leaf_seq;
  };
} key_ctx;

double time_taken_in_secs(struct timespec t) {
  struct timespec t_end;
  clock_gettime(CLOCK_REALTIME, &t_end);
  return (t_end.tv_sec - t.tv_sec) + (t_end.tv_nsec - t.tv_nsec) / 1e9;
}

struct timespec print_time_taken(struct timespec t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %lf\n", msg, time_taken);
  clock_gettime(CLOCK_REALTIME, &t);
  return t;
}

uint8_t *load_lines(const char *file_name, std::vector<key_ctx> &lines, size_t& file_size) {

  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(file_name, &file_stat);
  file_size = file_stat.st_size;
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
  uint8_t *line = extract_line(file_buf, line_len, file_size);
  do {
    if (compare(line, line_len, prev_line, prev_line_len) != 0) {
      uint8_t *key = line;
      size_t key_len = line_len;
      if (compare(key, key_len, prev_line, min_of(prev_line_len, line_len)) < 0)
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
    line = extract_line(line, line_len, file_size - (line - file_buf) - line_len);
  } while (line != NULL);
  printf("\n");
  printf("Sorted? : %d\n", is_sorted);

  return file_buf;

}

uint8_t *load_mdx_file(const char *file_name, size_t& mdx_file_size) {

  char mdx_file_name[strlen(file_name) + 5];
  strcpy(mdx_file_name, file_name);
  strcat(mdx_file_name, ".mdx");
  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(mdx_file_name, &file_stat);
  mdx_file_size = file_stat.st_size;
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
