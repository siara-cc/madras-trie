#include <iostream>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>

#include "src/madras_dv1.hpp"
#include "src/madras_builder_dv1.hpp"

using namespace std;

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  std::cout << msg << time_taken << std::endl;
  return clock();
}

int main(int argc, char *argv[]) {

  madras_dv1::builder sb(argv[1]);
  sb.set_print_enabled(true);
  vector<uint8_t *> lines;

  clock_t t = clock();
  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(argv[1], &file_stat);
  uint8_t *file_buf = (uint8_t *) malloc(file_stat.st_size + 1);
  printf("File_size: %lld\n", file_stat.st_size);

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

  int line_count = 0;
  uint8_t *prev_line = {0};
  size_t prev_line_len = 0;
  uint8_t *line = file_buf;
  uint8_t *cr_pos = (uint8_t *) memchr(line, '\n', file_stat.st_size);
  if (cr_pos == NULL)
    cr_pos = file_buf + file_stat.st_size;
  else {
    if (cr_pos > line && *(cr_pos - 1) == '\r')
      cr_pos--;
  }
  *cr_pos = 0;
  size_t line_len = cr_pos - line;
  do {
    if (prev_line_len != line_len || strncmp((const char *) line, (const char *) prev_line, prev_line_len) != 0) {
      // sb.append(line, line_len, line, line_len > 6 ? 7 : line_len);
      // sb.append(line, line_len);
      sb.insert(line, line_len, line, line_len > 6 ? 7 : line_len);
      // sb.insert(line, line_len);
      lines.push_back(line);
      prev_line = line;
      prev_line_len = line_len;
      line_count++;
      if ((line_count % 100000) == 0) {
        cout << ".";
        cout.flush();
      }
    }
    line = cr_pos + (cr_pos[1] == '\n' ? 2 : 1);
    cr_pos = (uint8_t *) memchr(line, '\n', file_stat.st_size - (cr_pos - file_buf));
    if (cr_pos != NULL && cr_pos > line) {
      if (*(cr_pos - 1) == '\r')
        cr_pos--;
      *cr_pos = 0;
      line_len = cr_pos - line;
    }
  } while (cr_pos != NULL);
  std::cout << std::endl;
  t = print_time_taken(t, "Time taken for insert/append: ");

  std::string out_file = sb.build(std::string(argv[1]) + ".rst");
  printf("\nBuild Keys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for build: ");

  madras_dv1::static_dict dict_reader; //, &sb);
  dict_reader.set_print_enabled(true);
  dict_reader.load(out_file.c_str());

  int key_len = 0;
  int val_len = 0;
  uint8_t key_buf[1000];
  uint8_t val_buf[100];
  madras_dv1::dict_iter_ctx dict_ctx(dict_reader.get_max_key_len(), dict_reader.get_max_level());

  // dict_reader.reverse_lookup(1096762, &line_count, key_buf, &val_len, val_buf);
  // //dict_reader.reverse_lookup_from_node_id(65, &line_count, key_buf, &val_len, val_buf);
  // printf("Key: [%.*s]\n", line_count, key_buf);
  // printf("Val: [%.*s]\n", val_len, val_buf);

  // dict_reader.dump_vals();

  line_count = 0;
  bool success = false;
  for (int i = 0; i < lines.size(); i++) {
    line = lines[i];
    line_len = strlen((const char *) line);
    // if (line.compare("don't think there's anything wrong") == 0)
    //   std::cout << line << std::endl;;
    // if (line.compare("understand that there is a") == 0)
    //   std::cout << line << std::endl;;
    // if (line.compare("a 18 year old") == 0) // child aligns to 64
    //   std::cout << line << std::endl;;
    // if (line.compare("!Adios_Amigos!") == 0)
    //   std::cout << line << std::endl;
    // if (line.compare("National_Register_of_Historic_Places_listings_in_Jackson_County,_Missouri:_Downtown_Kansas_City") == 0)
    //   std::cout << line << std::endl;
    // if (line.compare("a 1tb") == 0)
    //   ret = 1;
    // if (line.compare("really act") == 0)
    //   ret = 1;
    // if (line.compare("they argued") == 0)
    //   ret = 1;
    // if (line.compare("they achieve") == 0)
    //   ret = 1;
    // if (line.compare("understand that there is a") == 0)
    //   ret = 1;

    key_len = dict_reader.next(dict_ctx, key_buf, val_buf, &val_len);
    if (key_len != line_len)
      printf("Len mismatch: [%.*s], %u, %u\n", (int) line_len, line, key_len, val_len);
    else {
      if (memcmp(line, key_buf, key_len) != 0)
        printf("Key mismatch: [%.*s], [%.*s]\n", (int) line_len, line, key_len, key_buf);
      if (memcmp(line, val_buf, val_len) != 0)
        printf("Val mismatch: [%.*s], [%.*s]\n", (int) (line_len > 6 ? 7 : line_len), line, val_len, val_buf);
    }

    uint32_t node_id;
    bool is_found = dict_reader.lookup(line, line_len, node_id);
    if (!is_found)
      std::cout << line << std::endl;
    uint32_t leaf_id = dict_reader.get_leaf_rank(node_id);
    bool success = dict_reader.reverse_lookup(leaf_id, &key_len, key_buf);
    key_buf[key_len] = 0;
    if (strncmp((const char *) line, (const char *) key_buf, line_len) != 0)
      printf("Reverse lookup fail - expected: [%s], actual: [%.*s]\n", line, key_len, key_buf);

    success = dict_reader.get(line, line_len, &val_len, val_buf);
    if (success) {
      val_buf[val_len] = 0;
      if (strncmp((const char *) line, (const char *) val_buf, 7) != 0)
        printf("key: [%.*s], val: [%.*s]\n", (int) line_len, line, val_len, val_buf);
    } else
      std::cout << line << std::endl;

    // success = sb.get(line, line_len, &val_len, val_buf);
    // if (success) {
    //   val_buf[val_len] = 0;
    //   if (strncmp((const char *) line, (const char *) val_buf, 7) != 0)
    //     printf("key: [%.*s], val: [%.*s]\n", (int) line_len, line, val_len, val_buf);
    // } else
    //   std::cout << line << std::endl;

    line_count++;
    if ((line_count % 100000) == 0) {
      cout << ".";
      cout.flush();
    }
  }
  printf("\nKeys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for retrieve: ");

  free(file_buf);

  return 0;

}
