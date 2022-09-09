#include <bit_vector.h>
#include <var_array.h>

using namespace std;

class dict {

  private:
    bit_vector louds;
    bit_vector is_leaf;
    bit_vector is_ptr;
    Vector<uint8_t> nodes;
    var_array ptrs;
    Vector<uint8_t> suffixes;
    Vector<uint8_t> payloads;
    size_t size;

    dict(dict const&);
    dict& operator=(dict const&);

  public:
    dict();
    template <typename T>
    string operator[](uint32_t id) const {
    }
    uint32_t find_match(const char *buf, size_t len) {
    }

}
