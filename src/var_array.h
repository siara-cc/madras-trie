#ifndef VAR_ARRAY_H
#define VAR_ARRAY_H

#include <stdlib.h>
#include <stdint.h>

using namespace std;

namespace squeezed {

class var_array {

  private:
    char *buf;
    size_t size;

    var_array(var_array const&);
    var_array& operator=(var_array const&);

  public:
    var_array();
    uint32_t operator[](size_t i) const {
      return get(i);
    }
    uint32_t get(size_t i) const {
      return 0;
    }
    void set(size_t i, uint32_t val) {
    }
    void append(uint32_t val, size_t size) {
    }

};

}

#endif
