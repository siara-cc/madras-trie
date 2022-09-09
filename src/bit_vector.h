using namespace std;

class bit_vector {

  private:
    Vector<uint8_t> bits;
    size_t sz;

    bit_vector(bit_vector const&);
    bit_vector& operator=(bit_vector const&);

  public:
    bit_vector();
    bool operator[](size_t i) const {
    }
    size_t rank0(size_t i) const {
    }
    size_t rank1(size_t i) const {
    }
    size_t select0(size_t i) const {
    }
    size_t select1(size_t i) const {
    }

}
