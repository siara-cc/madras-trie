#include <fstream>
#include <iostream>

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

  squeezed::builder sb;
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
      sb.append(line);
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

  sb.build();
  t = print_time_taken(t, "Time taken for build: ");

  line_count = 0;
  vector<squeezed::node *> ret_val;
  for (int i = 0; i < lines.size(); i++) {
    std::string line = lines[i];
    int ret = sb.lookup(line, ret_val);
    if (ret != 0)
      std::cout << ret << ": " << line << std::endl;
    line_count++;
    if ((line_count % 100000) == 0) {
      cout << ".";
      cout.flush();
    }
  }
  printf("Keys per sec: %lf\n", line_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for retrieve: ");

  return 0;

}
