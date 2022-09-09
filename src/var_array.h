using namespace std;

class var_array {

  private:
    Vector<uint32_t> idx;
    Vector<uint8_t> bytes;
    size_t size;

    var_array(var_array const&);
    var_array& operator=(var_array const&);

  public:
    var_array();
    uint32_t operator[](size_t i) const {
      return get(i);
    }
    uint32_t get(size_t i) {
    }
    void set(size_t i, uint32_t val) {
    }
    void append(uint32_t val, size_t size) {
    }

}
