#ifndef BIT_VECTOR_H
#define BIT_VECTOR_H

#include <stdlib.h>
#include <stdint.h>

using namespace std;

namespace amphisbaena {

class bit_vector {

  private:
    char *buf;
    size_t sz;

    bit_vector(bit_vector const&);
    bit_vector& operator=(bit_vector const&);

  public:
    bit_vector();
    bool operator[](size_t i) const {
      return true;
    }
    size_t rank0(size_t i) const {
      return 0;
    }
    size_t rank1(size_t i) const {
      return 0;
    }
    size_t select0(size_t i) const {
      return 0;
    }
    size_t select1(size_t i) const {
      return 0;
    }
    void append(bool bit) {
    }

};

}

#endif
