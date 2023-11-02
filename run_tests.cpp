#include <fstream>
#include <iostream>

#include "squeezed.h"
#include "squeezed_builder.h"

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

  squeezed::builder sb(argv[1]);
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
      sb.append((const uint8_t *) line.c_str(), line.length(), (const uint8_t *) line.c_str(), line.length() > 3 ? 4 : line.length());
      //sb.append((const uint8_t *) line.c_str(), line.length());
      //sb.insert((const uint8_t *) line.c_str(), line.length());
      lines.push_back(line);
      prev_line = line;
      line_count++;
      if ((line_count % 100000) == 0) {
        cout << ".";
        cout.flush();
      }
    }
  }
  infile.close();
  std::cout << std::endl;
  t = print_time_taken(t, "Time taken for insert/append: ");

  std::string out_file = sb.build();
  printf("\nBuild Keys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for build: ");

  squeezed::static_dict dict_reader(out_file); //, &sb);
  //dict_reader.dump_tail_ptrs();

  int val_len;
  uint8_t val_buf[100];

  // dict_reader.dump_vals();

  line_count = 0;
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
    int ret = 0;
    int frag_idx = 0;
    if (line.compare("a 1tb") == 0)
      ret = 1;
    if (line.compare("x dell ultrasharp 49 curved") == 0)
      ret = 1;
    if (line.compare("they arrest") == 0)
      ret = 1;
    if (line.compare("they argued") == 0)
      ret = 1;
    if (line.compare("they achieve") == 0)
      ret = 1;
    if (line.compare("State_Rd_66") == 0)
      ret = 1;
    // if (ret == 1)
      dict_reader.lookup((const uint8_t *) line.c_str(), line.length(), ret, frag_idx);
    // bool success = dict_reader.get((const uint8_t *) line.c_str(), line.length(), &val_len, val_buf);
    // if (success) {
    //   ret = 0;
    //   printf("key: [%.*s], val: [%.*s]\n", (int) line.length(), line.c_str(), val_len, val_buf);
    // }
    // int ret, key_pos, cmp;
    // uint32_t n_id;
    // std::vector<uint32_t> node_path;
    // squeezed::node *n = sb.lookup((const uint8_t *) line.c_str(), line.length(), ret, key_pos, cmp, n_id, node_path);
    if (ret < 0)
      std::cout << ret << ": " << line << std::endl;
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
