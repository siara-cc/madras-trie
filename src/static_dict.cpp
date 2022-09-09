#include <bit_vector.h>
#include <var_array.h>

using namespace std;

class static_dict {

  private:
    bit_vector louds;
    bit_vector is_leaf;
    bit_vector is_ptr;
    Vector<uint8_t> nodes;
    var_array ptrs;
    Vector<uint8_t> suffixes;
    Vector<uint8_t> payloads;
    size_t size;

    static_dict(static_dict const&);
    static_dict& operator=(static_dict const&);

  public:
    static_dict();
    template <typename T>
    string operator[](uint32_t id) const {
    }
    uint32_t find_match(const char *buf, size_t len) {
    }

}
