#ifndef BITSET_VECTOR_H
#define BITSET_VECTOR_H

#include <stdlib.h>
#include <stdint.h>

using namespace std;

namespace squeezed {

class bitset_vector {

  private:
    char *buf;
    size_t sz;

    bitset_vector(bitset_vector const&);
    bitset_vector& operator=(bitset_vector const&);

  public:
    bitset_vector() {
    }
    void init(int bit_count, char *buf, size_t size) {
    }
    uint32_t get(int pos) {
      return 0;
    }
    void set(int pos, uint32_t val) {
    }

};

}

#endif
