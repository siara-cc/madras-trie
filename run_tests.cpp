#include <fstream>
#include <iostream>

#include "squeezed_builder.h"

using namespace std;

int main(int argc, char *argv[]) {

  squeezed::static_dict_builder sb;

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
      prev_line = line;
      line_count++;
      if ((line_count % 100000) == 0) {
        cout << ".";
        cout.flush();
      }
    }
  }
  infile.close();

  string s = sb.build();
  cout << s.length() << endl;

  return 0;

}
