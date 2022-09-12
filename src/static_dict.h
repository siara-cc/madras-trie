#ifndef STATIC_DICT_H
#define STATIC_DICT_H

#include <stdlib.h>
#include <string>

#include "bit_vector.h"
#include "var_array.h"

using namespace std;

namespace amphisbaena {

class static_dict {

  private:
    void *buf;
    size_t size;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

  public:
    static_dict();
    ~static_dict();
    template <typename T>
    string operator[](uint32_t id) const {
      string ret;
      return ret;
    }
    uint32_t find_match(string key) {
      return 0;
    }

};

}
#endif
