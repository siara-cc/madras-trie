#include <fstream>
#include <iostream>
#include <string>

#include "src/squeezed.h"

int main(int argc, char *argv[]) {

  int fragment_count = 1;
  if (argc > 2)
    fragment_count = atoi(argv[2]);

  squeezed::static_dict sq_trie;
  sq_trie.load(string(argv[1]));

  int val_len;
  uint8_t key_buf[1000];
  uint8_t val_buf[100];
  squeezed::dict_iter_ctx dict_ctx;

  int key_len = 1;
  const char *key1 = "en difference between the projection";
  while (key_len > 0) {
    int key_len = sq_trie.next(dict_ctx, key_buf, val_buf, &val_len);
    if (key_len > 0) {
      if (memcmp(key_buf, key1, strlen(key1)) == 0)
        std::cout << "hello" << std::endl;
      printf("Key: [%.*s]\n", key_len, key_buf);
    }
  }

  return 0;

}
