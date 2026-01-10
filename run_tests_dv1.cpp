#include <iostream>
#include <string>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "madras/dv1/reader/static_trie_map.hpp"
#include "madras/dv1/builder/madras_builder.hpp"

#include "../ds_common/src/vint.hpp"

using namespace std;

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

int main(int argc, char *argv[]) {

  if (argc < 2) {
    printf("Usage: ./run_tests <file_name> [mode=0*|1|2] [num_tries=3] [asc=0] [leapfrog=0] [num=0] [max_groups=16]\n");
    return 1;
  }

  int inner_trie_count = argc > 3 ? atoi(argv[3]) : 1;
  bool asc = argc > 4 ? atoi(argv[4]) == 1 ? true : false : false;
  bool leapfrog = argc > 5 ? atoi(argv[5]) == 1 ? true : false : false;
  // TODO: only 1 level works for as_int
  bool as_int = argc > 6 ? (atoi(argv[6]) == 1 ? true : false) : false;
  int max_groups = argc > 7 ? atoi(argv[7]) : 16;
  bool suffix_coding = argc > 8 ? (atoi(argv[8]) == 1 ? true : false) : true;
  printf("tries: %d, Asc? %d, Leapfrog?: %d, Int?: %d, Max Grps: %d, Sfx coding: %d\n",
      inner_trie_count, asc, leapfrog, as_int, max_groups, suffix_coding);

  madras::dv1::bldr_options bldr_opts = madras::dv1::dflt_opts;
  bldr_opts.max_inner_tries = inner_trie_count;
  bldr_opts.max_groups = max_groups;
  bldr_opts.sort_nodes_on_freq = asc ? 0 : 1;
  bldr_opts.leap_frog = leapfrog ? 1 : 0;
  bldr_opts.partial_sfx_coding = suffix_coding ? 1 : 0;

  int what = 0;
  if (argc > 2)
   what = atoi(argv[2]);

  madras::dv1::builder *sb;
  if (what == 0)
    sb = new madras::dv1::builder(argv[1], "kv_table,Key,Value,Len,chksum", 4, "ttii", "uuuu", 0, 1, &bldr_opts);
  else if (what == 1) // Process only key
    sb = new madras::dv1::builder(argv[1], "kv_table,Key", 1, "t", "u", 0, 1, &bldr_opts);
  else if (what == 2) // Insert key as value to compare trie and prefix coding
    sb = new madras::dv1::builder(argv[1], "kv_table,Key,Value", 2, "tt", "uu", 0, 1, &bldr_opts);
  sb->set_print_enabled(true);
  vector<pair<uint8_t *, uint32_t>> lines;

  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(argv[1], &file_stat);
  uint8_t *file_buf = (uint8_t *) malloc(file_stat.st_size + 1);
  printf("File_name: %s, size: %lu\n", argv[1], (long) file_stat.st_size);

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

  int64_t ival;
  size_t isize;
  uint8_t istr[10];
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
      // uint8_t *tab_loc = (uint8_t *) memchr(line, '\t', line_len);
      // if (tab_loc != NULL) {
      //   key_len = tab_loc - line;
      //   val = tab_loc + 1;
      //   val_len = line_len - key_len - 1;
      // } else {
      // }
      if (gen::compare(key, key_len, prev_line, gen::min(prev_line_len, key_len)) < 0)
        is_sorted = false;
      if (as_int) {
        ival = std::atoll((const char *) key);
        if (abs(ival) >= (1ULL << 60))
          printf("ERROR: overflow!!!!!!!!!!!!!!!!!!!!!: %" PRId64 "\n", ival);
        isize = gen::get_svint60_len(ival);
        gen::copy_svint60(ival, (uint8_t *) istr, isize);
        // isize = 8;
        // gen::int64ToUint8Sortable(ival, istr);
        key = istr;
        key_len = isize;
      }
      if (what == 0 || what == 2) {
        madras::dv1::mdx_val_in values[4];
        size_t value_lens[4];
        values[0].txt_bin = key;
        value_lens[0] = key_len;
        values[1].txt_bin = key;
        value_lens[1] = key_len;
        if (what == 0) {
          values[2].i64 = line_len;
          value_lens[2] = 8;
          int64_t checksum = 0;
          for (size_t j = 0; j < strlen((const char *) key); j++) {
            checksum += key[j];
          }
          values[3].i64 = checksum;
          value_lens[3] = 8;
        }
        sb->insert_record(values, value_lens);
      } else if (what == 1) {
        sb->insert(key, key_len);
      }
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
  out_file += ".mdsi";
  if (what == 2) {
    sb->build_kv();
    sb->write_kv(true, out_file.c_str());
  } else
    sb->build_and_write_all(true, out_file.c_str());
  printf("\nBuild Keys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for build: ");
  std::cout << "Sorted? : " << is_sorted << std::endl;

  t = print_time_taken(t, "Time taken for insert/append: ");

  bool nodes_sorted_on_freq = sb->opts->sort_nodes_on_freq;
  delete sb;

  madras::dv1::static_trie_map trie_reader;
  trie_reader.set_print_enabled(true);
  trie_reader.load(out_file.c_str());

  size_t out_key_len = 0;
  size_t out_val_len = 0;
  uint8_t key_buf[trie_reader.get_max_key_len() + 1];
  uint8_t val_buf[trie_reader.get_max_val_len() + 1];
  madras::dv1::mdx_val mv;
  mv.txt_bin = val_buf;
  madras::dv1::iter_ctx dict_ctx;
  dict_ctx.init(trie_reader.get_max_key_len(), trie_reader.get_max_level());

  if (!is_sorted) {
    std::sort(lines.begin(), lines.end(), [as_int](const pair<uint8_t *, int> lhs, const pair<uint8_t *, int> rhs) -> bool {
      if (as_int)
        return atoll((const char *) lhs.first) < atoll((const char *) rhs.first);
      return gen::compare(lhs.first, lhs.second, rhs.first, rhs.second) < 0;
    });
    is_sorted = true;
  }

  madras::dv1::input_ctx in_ctx;
  uint8_t *val;
  size_t val_len;

  t = print_time_taken(t, "Time taken for load: ");

  line_count = 0;
  size_t err_count = 0;
  bool success = false;
  for (size_t i = 0; i < lines.size(); i++) {
    // if (gen::compare(line, line_len, prev_line, prev_line_len) == 0)
    //   continue;
    // prev_line = line;
    // prev_line_len = line_len;

    in_ctx.key = lines[i].first;
    in_ctx.key_len = lines[i].second;

    if (as_int) {
      ival = std::atoll((const char *) in_ctx.key);
      isize = gen::get_svint60_len(ival);
      gen::copy_svint60(ival, (uint8_t *) istr, isize);
      // isize = 8;
      // gen::int64ToUint8Sortable(ival, istr);
      in_ctx.key = istr;
      in_ctx.key_len = isize;
    }

    // if (strcmp((const char *) in_ctx.key, "a 10 episode") == 0)
    //   int ret = 1;

    val = lines[i].first;
    val_len = lines[i].second;
    // uint8_t *tab_loc = (uint8_t *) memchr(line, '\t', line_len);
    // if (tab_loc != NULL) {
    //   key_len = tab_loc - line;
    //   val = tab_loc + 1;
    //   val_len = line_len - key_len - 1;
    // } else {
    //   val_len = (line_len > 6 ? 7 : line_len);
    // }

    if (is_sorted && !nodes_sorted_on_freq) {
      out_key_len = trie_reader.next(dict_ctx, key_buf);
      if (out_key_len != in_ctx.key_len) {
        printf("%lu: Len mismatch: [%.*s], [%.*s], %" PRIuXX ", %d, %d\n", i, (int) in_ctx.key_len, in_ctx.key, (int) out_key_len, key_buf, in_ctx.key_len, (int) out_key_len, (int) out_val_len);
        err_count++;
      } else {
        if (memcmp(in_ctx.key, key_buf, in_ctx.key_len) != 0) {
          err_count++;
          printf("%lu: Key mismatch: E:[%.*s], A:[%.*s], %" PRIuXX "\n", i, (int) in_ctx.key_len, in_ctx.key, (int) out_key_len, key_buf, in_ctx.key_len);
          if (as_int)
            printf("E: %s, A: %" PRId64 "\n", lines[i].first, gen::read_svint60(key_buf));
        }
        if (what == 0 || what == 2) {
          in_ctx.node_id = dict_ctx.node_path[dict_ctx.cur_idx];
          trie_reader.get_col_val(in_ctx.node_id, 1, &out_val_len, mv);
          if (what == 2 && memcmp(in_ctx.key, val_buf, out_val_len) != 0) {
            printf("n2:Val mismatch: E:[%.*s], A:[%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
            err_count++;
          }
          if (what == 0 && memcmp(val, val_buf, out_val_len) != 0) {
            printf("Val mismatch: [%.*s], [%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
            err_count++;
          }
        }
      }
    }

    bool is_found = trie_reader.lookup(in_ctx);
    if (!is_found) {
      std::cout << "Lookup fail: " << in_ctx.key << std::endl;
      err_count++;
    } else {
      uint32_t leaf_id = trie_reader.leaf_rank1(in_ctx.node_id);
      bool success = trie_reader.reverse_lookup(leaf_id, &out_key_len, key_buf);
      key_buf[out_key_len] = 0;
      if (strncmp((const char *) in_ctx.key, (const char *) key_buf, in_ctx.key_len) != 0) {
        printf("Reverse lookup fail - e:[%s], a:[%.*s]\n", in_ctx.key, (int) in_ctx.key_len, key_buf);
        err_count++;
      }
    }

    mv.txt_bin = val_buf;
    success = trie_reader.get(in_ctx, &out_val_len, mv);
    if (success) {
      val_buf[out_val_len] = 0;
      if (what == 0 && strncmp((const char *) val, (const char *) val_buf, val_len) != 0) {
        printf("key: [%.*s], val: [%.*s]\n", (int) out_key_len, in_ctx.key, (int) out_val_len, val_buf);
        err_count++;
      }
      if (what == 2 && memcmp(in_ctx.key, val_buf, out_val_len) != 0) {
        printf("g2:Val mismatch: E:[%.*s], A:[%.*s]\n", (int) val_len, val, (int) out_val_len, val_buf);
        err_count++;
      }
    } else {
      std::cout << "Get fail: " << in_ctx.key << std::endl;
      err_count++;
    }

    if (what == 0) {
      trie_reader.get_col_val(in_ctx.node_id, 2, &out_val_len, mv);
      int64_t out_len = mv.i64;
      if (out_len != in_ctx.key_len) {
        std::cout << "nid: " << in_ctx.node_id << ", First val mismatch - expected: " << in_ctx.key_len << ", found: "
            << out_len << std::endl;
        err_count++;
      }
      trie_reader.get_col_val(in_ctx.node_id, 3, &out_val_len, mv);
      int64_t checksum = 0;
      for (size_t i = 0; i < strlen((const char *) in_ctx.key); i++) {
        checksum += in_ctx.key[i];
      }
      int64_t out_chksum = mv.i64;
      if (out_chksum != checksum) {
        std::cout << "nid: " << in_ctx.node_id << ", Second val mismatch - expected: " << checksum << ", found: "
            << out_chksum << std::endl;
        err_count++;
      }
    }

    line_count++;
    // if ((line_count % 100000) == 0) {
    //   cout << ".";
    //   cout.flush();
    // }
  }
  // out_key_len = trie_reader.next(dict_ctx, key_buf, val_buf, &out_val_len);
  // if (out_key_len != -1)
  //   printf("Expected Eof: [%.*s], %d\n", out_key_len, key_buf, out_key_len);

  printf("\nKeys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  printf("Error count: %lu\n", err_count);
  t = print_time_taken(t, "Time taken for retrieve: ");

  free(file_buf);

  return 0;

}
