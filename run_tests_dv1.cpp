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

  int what = 0;
  if (argc > 2)
   what = atoi(argv[2]);

  const char *data_types = nullptr;
  if (argc > 3)
    data_types = argv[3];

  const char *encoding = nullptr;
  if (argc > 4)
    encoding = argv[4];

  madras_dv1::builder *sb;
  if (what == 0)
    sb = new madras_dv1::builder(argv[1], "kv_table,Key,Value,Len,chksum", 4, data_types == nullptr ? "tttt" : data_types, encoding == nullptr ? "uuuu" : encoding);
  else if (what == 1) // Process only key
    sb = new madras_dv1::builder(argv[1], "kv_table,Key", 1, data_types == nullptr ? "t" : data_types, encoding == nullptr ? "u" : encoding);
  else if (what == 2) // Insert key as value to compare trie and prefix coding
    sb = new madras_dv1::builder(argv[1], "kv_table,Key,Value", 2, data_types == nullptr ? "tt" : data_types, encoding == nullptr ? "uu" : encoding);
  sb->set_print_enabled(true);
  vector<pair<uint8_t *, uint32_t>> lines;

  clock_t t = clock();
  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(argv[1], &file_stat);
  uint8_t *file_buf = (uint8_t *) malloc(file_stat.st_size + 1);
  printf("File_name: %s, size: %lld\n", argv[1], file_stat.st_size);

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
  bool is_sorted = true;
  const uint8_t *prev_line = (const uint8_t *) "";
  size_t prev_line_len = 0;
  size_t line_len = 0;
  uint8_t *line = gen::extract_line(file_buf, line_len, file_stat.st_size);
  do {
    if (gen::compare(line, line_len, prev_line, prev_line_len) != 0) {
      uint8_t *key = line;
      int key_len = line_len;
      uint8_t *val = line;
      int val_len;
      // uint8_t *tab_loc = (uint8_t *) memchr(line, '\t', line_len);
      // if (tab_loc != NULL) {
      //   key_len = tab_loc - line;
      //   val = tab_loc + 1;
      //   val_len = line_len - key_len - 1;
      // } else {
        val_len = (line_len > 6 ? 7 : line_len);
      // }
      if (gen::compare(key, key_len, prev_line, gen::min(prev_line_len, key_len)) < 0)
        is_sorted = false;
      if (what == 0)
        sb->insert(key, key_len, val, val_len);
      else if (what == 1)
        sb->insert(key, key_len);
      else if (what == 2)
        sb->insert(key, key_len, key, key_len);
      lines.push_back(make_pair(line, line_len));
      prev_line = line;
      prev_line_len = line_len;
      line_count++;
      if ((line_count % 100000) == 0) {
        cout << ".";
        cout.flush();
      }
    }
    line = gen::extract_line(line, line_len, file_stat.st_size - (line - file_buf) - line_len);
  } while (line != NULL);
  std::cout << std::endl;

  std::string out_file = argv[1];
  out_file += ".mdx";
  sb->write_kv(out_file.c_str());
  printf("\nBuild Keys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for build: ");
  std::cout << "Sorted? : " << is_sorted << std::endl;

  if (what == 0) {
    sb->reset_for_next_col();
    for (size_t i = 0; i < line_count; i++) {
      line = lines[i].first;
      line_len = lines[i].second;
      char val[30];
      snprintf(val, 30, "%zu", line_len);
      // printf("[%.*s]]\n", (int) strlen(val), val);
      sb->insert_col_val(val, strlen(val));
    }
    sb->build_and_write_col_val();

    sb->reset_for_next_col();
    for (size_t i = 0; i < line_count; i++) {
      line = lines[i].first;
      int checksum = 0;
      for (size_t j = 0; i < strlen((const char *) line); j++) {
        checksum += line[j];
      }
      char val[30];
      snprintf(val, 30, "%d", checksum);
      // printf("[%.*s]]\n", (int) strlen(val), val);
      sb->insert_col_val(val, strlen(val));
    }
    sb->build_and_write_col_val();
  }

  sb->write_final_val_table();

  t = print_time_taken(t, "Time taken for insert/append: ");

  madras_dv1::static_trie_map trie_reader;
  trie_reader.set_print_enabled(true);
  trie_reader.load(out_file.c_str());

  size_t out_key_len = 0;
  size_t out_val_len = 0;
  uint8_t key_buf[trie_reader.get_max_key_len() + 1];
  uint8_t val_buf[trie_reader.get_max_val_len() + 1];
  madras_dv1::iter_ctx dict_ctx;
  dict_ctx.init(trie_reader.get_max_key_len(), trie_reader.get_max_level());

  if (!is_sorted) {
    std::sort(lines.begin(), lines.end(), [](const pair<uint8_t *, int> lhs, const pair<uint8_t *, int> rhs) -> bool {
      return gen::compare(lhs.first, lhs.second, rhs.first, rhs.second) < 0;
    });
    is_sorted = true;
  }

  madras_dv1::input_ctx in_ctx;

  line_count = 0;
  bool success = false;
  for (size_t i = 0; i < lines.size(); i++) {
    line = lines[i].first;
    line_len = lines[i].second;
    // if (gen::compare(line, line_len, prev_line, prev_line_len) == 0)
    //   continue;
    // prev_line = line;
    // prev_line_len = line_len;
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
    // if (strcmp((const char *) line, "a 10 episode") == 0)
    //   int ret = 1;
    // if (line.compare("really act") == 0)
    //   ret = 1;
    // if (line.compare("they argued") == 0)
    //   ret = 1;
    // if (line.compare("they achieve") == 0)
    //   ret = 1;
    // if (line.compare("understand that there is a") == 0)
    //   ret = 1;

    uint8_t *val = line;
    size_t val_len;
    // uint8_t *tab_loc = (uint8_t *) memchr(line, '\t', line_len);
    // if (tab_loc != NULL) {
    //   key_len = tab_loc - line;
    //   val = tab_loc + 1;
    //   val_len = line_len - key_len - 1;
    // } else {
    //   val_len = (line_len > 6 ? 7 : line_len);
    // }

    in_ctx.key = line;
    in_ctx.key_len = line_len;

    if (is_sorted && !sb->opts.sort_nodes_on_freq) {
      out_key_len = trie_reader.next(dict_ctx, key_buf, val_buf, &out_val_len);
      if (out_key_len != in_ctx.key_len)
        printf("Len mismatch: [%.*s], [%.*s], %d, %d, %d\n", in_ctx.key_len, in_ctx.key, (int) out_key_len, key_buf, in_ctx.key_len, (int) out_key_len, (int) out_val_len);
      else {
        if (memcmp(in_ctx.key, key_buf, in_ctx.key_len) != 0)
          printf("Key mismatch: E:[%.*s], A:[%.*s]\n", in_ctx.key_len, in_ctx.key, (int) out_key_len, key_buf);
        if (what == 2 && memcmp(in_ctx.key, val_buf, out_val_len) != 0)
          printf("n2:Val mismatch: E:[%.*s], A:[%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
        if (what == 0 && memcmp(val, val_buf, out_val_len) != 0)
          printf("Val mismatch: [%.*s], [%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
      }
    }

    bool is_found = trie_reader.lookup(in_ctx);
    if (!is_found)
      std::cout << "Lookup fail: " << line << std::endl;
    else {
      uint32_t leaf_id = trie_reader.leaf_rank(in_ctx.node_id);
      bool success = trie_reader.reverse_lookup(leaf_id, &out_key_len, key_buf);
      key_buf[out_key_len] = 0;
      if (strncmp((const char *) in_ctx.key, (const char *) key_buf, in_ctx.key_len) != 0)
        printf("Reverse lookup fail - e:[%s], a:[%.*s]\n", in_ctx.key, in_ctx.key_len, key_buf);
    }

    success = trie_reader.get(in_ctx, &out_val_len, val_buf);
    if (success) {
      val_buf[out_val_len] = 0;
      if (what == 2 && memcmp(in_ctx.key, val_buf, out_val_len) != 0)
        printf("g2:Val mismatch: E:[%.*s], A:[%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
      if (what == 0 && strncmp((const char *) val, (const char *) val_buf, val_len) != 0)
        printf("key: [%.*s], val: [%.*s]\n", (int) out_key_len, in_ctx.key, (int) out_val_len, val_buf);
    } else
      std::cout << "Get fail: " << in_ctx.key << std::endl;

    if (what == 0) {
      trie_reader.get_col_val(in_ctx.node_id, 1, &out_val_len, val_buf);
      val_buf[out_val_len] = 0;
      if (atoi((const char *) val_buf) != line_len) {
        std::cout << "First val mismatch - expected: " << line_len << ", found: "
            << (const char *) val_buf << std::endl;
      }

      trie_reader.get_col_val(in_ctx.node_id, 2, &out_val_len, val_buf);
      val_buf[out_val_len] = 0;
      int checksum = 0;
      for (int i = 0; i < strlen((const char *) line); i++) {
        checksum += line[i];
      }
      if (atoi((const char *) val_buf) != checksum) {
        std::cout << "Second val mismatch - expected: " << checksum << ", found: "
            << (const char *) val_buf << std::endl;
      }
    }

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
  // out_key_len = trie_reader.next(dict_ctx, key_buf, val_buf, &out_val_len);
  // if (out_key_len != -1)
  //   printf("Expected Eof: [%.*s], %d\n", out_key_len, key_buf, out_key_len);

  printf("\nKeys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for retrieve: ");

  delete sb;

  free(file_buf);

  return 0;

}
