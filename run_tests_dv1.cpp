#include <fstream>
#include <iostream>
#include <string>

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
  vector<string> lines;

  clock_t t = clock();
  fstream infile;
  infile.open(argv[1], ios::in);
  int line_count = 0;
  if (infile.is_open()) {
    string line;
    string prev_line = "";
    while (getline(infile, line)) {
      if (line == prev_line)
         continue;
      // sb.append((const uint8_t *) line.c_str(), line.length(), (const uint8_t *) line.c_str(), line.length() > 6 ? 7 : line.length());
      // sb.append((const uint8_t *) line.c_str(), line.length());
      sb.insert((const uint8_t *) line.c_str(), line.length(), (const uint8_t *) line.c_str(), line.length() > 6 ? 7 : line.length());
      //sb.insert((const uint8_t *) line.c_str(), line.length());
      lines.push_back(line);
      prev_line = line;
      line_count++;
      if ((line_count % 100000) == 0) {
        cout << ".";
        cout.flush();
      }
    }
    infile.close();
  }
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
  madras_dv1::dict_iter_ctx dict_ctx;

  // dict_reader.reverse_lookup(1096762, &line_count, key_buf, &val_len, val_buf);
  // //dict_reader.reverse_lookup_from_node_id(65, &line_count, key_buf, &val_len, val_buf);
  // printf("Key: [%.*s]\n", line_count, key_buf);
  // printf("Val: [%.*s]\n", val_len, val_buf);

  // dict_reader.dump_vals();

  line_count = 0;
  bool success = false;
  for (int i = 0; i < lines.size(); i++) {
    std::string& line = lines[i];
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

    int key_len = dict_reader.next(dict_ctx, key_buf, val_buf, &val_len);
    if (key_len != line.length())
      printf("Len mismatch: [%.*s], %u, %u\n", (int) line.length(), line.c_str(), key_len, val_len);
    else {
      if (memcmp(line.c_str(), key_buf, key_len) != 0)
        printf("Key mismatch: [%.*s], [%.*s]\n", (int) line.length(), line.c_str(), key_len, key_buf);
      if (memcmp(line.substr(0, line.length() > 6 ? 7 : line.length()).c_str(), val_buf, val_len) != 0)
        printf("Val mismatch: [%.*s], [%.*s]\n", (int) (line.length() > 6 ? 7 : line.length()), line.c_str(), val_len, val_buf);
    }

    uint32_t node_id;
    bool is_found = dict_reader.lookup((const uint8_t *) line.c_str(), line.length(), node_id);
    if (!is_found)
      std::cout << line << std::endl;
    uint32_t leaf_id = dict_reader.get_leaf_rank(node_id);
    bool success = dict_reader.reverse_lookup(leaf_id, &key_len, key_buf);
    key_buf[key_len] = 0;
    if (line.compare((const char *) key_buf) != 0)
      printf("Reverse lookup fail - expected: [%s], actual: [%.*s]\n", line.c_str(), key_len, key_buf);

    success = dict_reader.get((const uint8_t *) line.c_str(), line.length(), &val_len, val_buf);
    if (success) {
      val_buf[val_len] = 0;
      if (line.substr(0, 7).compare((const char *) val_buf) != 0)
        printf("key: [%.*s], val: [%.*s]\n", (int) line.length(), line.c_str(), val_len, val_buf);
    } else
      std::cout << line << std::endl;

    success = sb.get((const uint8_t *) line.c_str(), line.length(), &val_len, val_buf);
    if (success) {
      val_buf[val_len] = 0;
      if (line.substr(0, 7).compare((const char *) val_buf) != 0)
        printf("key: [%.*s], val: [%.*s]\n", (int) line.length(), line.c_str(), val_len, val_buf);
    } else
      std::cout << line << std::endl;

    line_count++;
    if ((line_count % 100000) == 0) {
      cout << ".";
      cout.flush();
    }
  }
  printf("\nKeys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for retrieve: ");

  return 0;

}
