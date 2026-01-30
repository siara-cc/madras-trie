#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#endif

#include <stdio.h>
#include "madras/dv1/reader/static_trie_map.hpp"

int main(int argc, char *argv[]) {

  if (argc < 4) {
    printf("Usage: node madras_lookup.js <NODEFS_PATH> <MDX_NAME> <lookup_key>");
    return 0;
  }

  printf("Arg 1: %s\n", argv[1]);
  printf("Arg 2: %s\n", argv[2]);
  printf("Arg 3: [%.*s], %lu\n", (int) strlen(argv[3]), argv[3], strlen(argv[3]));

#ifdef __EMSCRIPTEN__
    EM_ASM_({
        try {
            FS.mkdir('/test');
            FS.mount(NODEFS, { root: UTF8ToString($0) }, '/test');
        } catch (error) {
            console.error('Error mounting NODEFS:', error);
        }
    }, argv[1]);
#endif

  FILE *fp = fopen(argv[2], "rb");
  if (fp == NULL) {
    perror("Error opening file: ");
    return 0;
  }
  fclose(fp);

  madras::dv1::static_trie_map st;
  st.load(argv[2]);
  madras::dv1::input_ctx in_ctx;
  in_ctx.key = (const uint8_t *) argv[3];
  in_ctx.key_len = strlen(argv[3]);
  in_ctx.node_id = 0;
  if (st.lookup(in_ctx))
    printf("Found at %lu\n", in_ctx.node_id);
  else
    printf("Not found: %u\n", in_ctx.node_id);

  return 1;

}
