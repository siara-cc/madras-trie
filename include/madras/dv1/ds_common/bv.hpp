#ifndef _BV_HPP_
#define _BV_HPP_

#include <vector>
#include <math.h>

namespace gen {

// Function qualifiers
#ifndef __fq1
#define __fq1
#endif

#ifndef __fq2
#define __fq2
#endif

typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uint8_t *> byte_ptr_vec;

__fq1 __fq2 static size_t get_lkup_tbl_size(size_t count, size_t block_size, size_t entry_size) {
  size_t ret = (count / block_size) + ((count % block_size) == 0 ? 0 : 1);
  ret *= entry_size;
  return ret;
}

__fq1 __fq2 static size_t get_lkup_tbl_size2(size_t count, size_t block_size, size_t entry_size) {
  size_t ret = get_lkup_tbl_size(count, block_size, entry_size) + entry_size;
  return ret;
}

template <class T>
class bit_vector {
  private:
    std::vector<T> bv;
    bool all_ones;
    size_t highest_bit_no;
  public:
    bit_vector(bool _all_ones = false) {
      all_ones = _all_ones;
      highest_bit_no = 0;
    }
    // bit_no starts from 0
    void set(size_t bit_no, bool val) {
      if (highest_bit_no < bit_no)
        highest_bit_no = bit_no;
      size_t bit_width = (sizeof(T) * 8);
      size_t pos = bit_no / bit_width;
      while (bv.size() <= pos)
        bv.push_back(all_ones ? (T) UINT64_MAX : 0x00);
      T mask = 1ULL << (bit_no % bit_width);
      if (val)
        bv[pos] |= mask;
      else
        bv[pos] &= ~mask;
    }
    bool operator[](size_t bit_no) {
      size_t bit_width = (sizeof(T) * 8);
      size_t pos = bit_no / bit_width;
      T mask = 1ULL << (bit_no % bit_width);
      return (bv[pos] & mask) != 0;
    }
    size_t get_highest() {
      return highest_bit_no;
    }
    size_t size_bytes() {
      return sizeof(T) * bv.size();
    }
    std::vector<T> *raw_data() {
      return &bv;
    }
    void reset() {
      bv.clear();
      highest_bit_no = 0;
    }
};

template <class T>
class bv_reader {
  private:
    T *bv;
    size_t bv_size;
    // todo: implement size check
  public:
    __fq1 __fq2 bool operator[](size_t bit_no) {
      size_t bit_width = (sizeof(T) * 8);
      size_t pos = bit_no / bit_width;
      if (pos >= bv_size)
        return false;
      T mask = 1ULL << (bit_no % bit_width);
      return (bv[pos] & mask) != 0;
    }
    __fq1 __fq2 void set_bv(T *_bv, size_t _bv_size) {
      bv = _bv;
      bv_size = _bv_size;
    }
};

class int_bit_vector {
private:
    byte_vec* int_ptrs;
    size_t bit_len;
    size_t count;
    size_t sz;
    size_t last_bits_used; // 0..64

    static void append_uint64(uint64_t u64, byte_vec& v) {
      // always little-endian storage
      v.push_back(u64 & 0xFF);
      v.push_back((u64 >> 8) & 0xFF);
      v.push_back((u64 >> 16) & 0xFF);
      v.push_back((u64 >> 24) & 0xFF);
      v.push_back((u64 >> 32) & 0xFF);
      v.push_back((u64 >> 40) & 0xFF);
      v.push_back((u64 >> 48) & 0xFF);
      v.push_back(u64 >> 56);
    }

public:
    int_bit_vector() : int_ptrs(nullptr), bit_len(0), count(0), sz(0), last_bits_used(0) {}

    int_bit_vector(byte_vec* _ptrs, size_t _bit_len, size_t _count) {
      init(_ptrs, _bit_len, _count);
    }

    void init(byte_vec* _ptrs, size_t _bit_len, size_t _count) {
      int_ptrs = _ptrs;
      bit_len = _bit_len;
      count = _count;
      int_ptrs->clear();

      // reserve space (bit_len*count bits + up to 64b slack)
      sz = bit_len * count / 8 + 8;
      int_ptrs->reserve(sz);

      // initial 64-bit slot
      append_uint64(0, *int_ptrs);
      last_bits_used = 0;
    }

    void append(uint32_t v) { append((uint64_t)v); }

    void append(uint64_t v) {
      size_t bits = bit_len;

      while (bits > 0) {
        uint64_t* last_ptr = (uint64_t*)(int_ptrs->data() + int_ptrs->size() - 8);
        size_t space = 64 - last_bits_used;
        size_t take = (bits < space ? bits : space);

        uint64_t chunk = v & ((1ULL << take) - 1);
        *last_ptr |= chunk << last_bits_used;

        v >>= take;
        bits -= take;
        last_bits_used += take;

        if (last_bits_used == 64) {
          append_uint64(0, *int_ptrs);
          last_bits_used = 0;
        }
      }
    }

    size_t size() { return sz; }
};

class int_bv_reader {
private:
    uint64_t* base;
    size_t bit_len;

public:
    int_bv_reader() : base(nullptr), bit_len(0) {}

    void init(uint8_t* _int_bv, size_t _bit_len) {
        base = (uint64_t *) _int_bv;
        bit_len = _bit_len;
    }

    inline uint64_t operator[](size_t pos) const noexcept {
      if (bit_len == 0) return 0;
      uint64_t bit = pos * bit_len;
      size_t w = bit >> 6;
      uint32_t s = bit & 63;

      #if defined(__BMI2__) && (defined(__x86_64__) || defined(_M_X64))
        // -------- Intel/AMD BMI2 fast path --------
        uint64_t lo = _shrx_u64(base[w], s);
        uint64_t merged = lo | (base[w+1] << (64 - s));
        return _bzhi_u64(merged, bit_len);

      #else
        // -------- Portable fallback --------
        uint64_t lo = base[w] >> s;
        uint64_t v = lo;
        if (s + bit_len > 64)
            v |= (base[w+1] << (64 - s));

        return (v & ((1ULL << bit_len) - 1));
      #endif
    }
};

}

#endif
