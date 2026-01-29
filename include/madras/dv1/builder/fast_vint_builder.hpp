#ifndef __DV1_BUILDER_FAST_VINT_HPP
#define __DV1_BUILDER_FAST_VINT_HPP

#include "madras/dv1/common.hpp"
#include "madras/dv1/allflic48/allflic48.hpp"

namespace madras { namespace dv1 {

class fast_vint {
  private:
    char data_type;
    uint8_t hdr_size;
    uint8_t dec_count;
    std::vector<int64_t> i64_data;
    std::vector<int32_t> i32_data;
    std::vector<double> dbl_data;
    std::vector<uint8_t> dbl_exceptions;
    ptr_groups *ptr_grps;
    byte_vec *block_data;
    int64_t for_val;
    size_t count;
    bool is64bit;
    bool is_dbl_exceptions;
  public:
    bool isAll32bit;
    fast_vint(char _data_type) {
      data_type = _data_type;
      hdr_size = data_type == MST_DECV ? 3 : 2;
      dec_count = 0;
      for_val = INT64_MAX;
      count = 0;
      is64bit = false;
      isAll32bit = true;
      is_dbl_exceptions = false;
      block_data = nullptr;
    }
    ~fast_vint() {
    }
    void reset_block() {
      for_val = INT64_MAX;
      count = 0;
      dec_count = 0;
      is64bit = false;
      is_dbl_exceptions = false;
      i64_data.clear();
      i32_data.clear();
      dbl_data.clear();
      dbl_exceptions.clear();
    }
    void set_block_data(byte_vec *bd) {
      block_data = bd;
    }
    void set_ptr_grps(ptr_groups *_ptr_grps) {
      ptr_grps = _ptr_grps;
    }
    void add(uintxx_t node_id, uint8_t *data_pos, size_t data_len) {
      int64_t i64;
      if (data_type == MST_DECV) {
        double dbl = *((double *) data_pos);
        if (gen::is_negative_zero(dbl)) {
          dbl = 0;
          ptr_grps->set_null(node_id);
        }
        uint8_t frac_width = allflic::allflic48::cvt_dbl2_i64(dbl, i64);
        if (frac_width == UINT8_MAX || std::abs(i64) > 18014398509481983LL) {
          dbl_exceptions.push_back(1);
          is_dbl_exceptions = true;
          is64bit = true;
          isAll32bit = false;
        } else {
          if (dec_count < frac_width)
            dec_count = frac_width;
          dbl_exceptions.push_back(0);
        }
        dbl_data.push_back(dbl);
      } else {
        allflic::allflic48::simple_decode(data_pos, 1, &i64);
        dbl_exceptions.push_back(0);
        if (i64 == -1) { // null
          i64 = 0;
          ptr_grps->set_null(node_id);
        }
        i64_data.push_back(i64);
        if (i64 > INT32_MAX) {
          is64bit = true;
          isAll32bit = false;
        } else
          i32_data.push_back(i64);
      }
      if (for_val > i64)
        for_val = i64;
      count++;
    }
    void print_bits(uint64_t u64) {
      uint64_t mask = (1ULL << 63);
      for (size_t i = 0; i < 64; i++) {
        printf("%d", u64 & mask ? 1 : 0);
        mask >>= 1;
      }
      printf("\n");
    }
    size_t build_block() {
      if (data_type == MST_DECV) {
        for_val = INT64_MAX;
        for (size_t i = 0; i < dbl_data.size(); i++) {
          int64_t i64;
          double dbl = dbl_data[i];
              // printf("dbl: %lu, %lf\n", i, dbl);
          i64 = static_cast<int64_t>(dbl * allflic::allflic48::tens()[dec_count]);
          if (dbl_exceptions[i] == 0) {
            double dbl_back = static_cast<double>(i64);
            dbl_back /= allflic::allflic48::tens()[dec_count];
            if (dbl != dbl_back) {
              dbl_exceptions[i] = 1;
              is_dbl_exceptions = true;
            }
          }
          if (dbl_exceptions[i] == 0) {
            i64 = allflic::allflic48::zigzag_encode(i64);
            i64_data.push_back(i64);
            if (i64 > INT32_MAX) {
              is64bit = true;
              isAll32bit = false;
            } else
              i32_data.push_back(i64);
          } else {
            // printf("Exception: %lf\n", dbl);
            memcpy(&i64, &dbl, 8);
            i64 = allflic::allflic48::zigzag_encode(i64);
            i64_data.push_back(i64);
            is64bit = true;
            isAll32bit = false;
          }
          // if (is_dbl_exceptions)
          //   print_bits(static_cast<uint64_t>(i64));
          // printf("for_val: %lld, i64: %lld\n", for_val, i64_data[i]);
          if (for_val > i64)
            for_val = i64;
        }
      }
      if (is_dbl_exceptions || for_val == INT64_MAX || i64_data.size() == 0)
        for_val = 0;
      for (size_t i = 0; i < i64_data.size(); i++) {
        i64_data[i] -= for_val;
        // printf("data64: %lld\n", i64_data[i]);
      }
      for (size_t i = 0; i < i32_data.size(); i++) {
        i32_data[i] -= for_val;
        // printf("data32: %u\n", i32_data[i]);
      }
      // printf("for val: %lld\n", for_val);
      uint8_t hdr_b1 = (is64bit ? 0x80 : 0x00);
      size_t last_size = block_data->size();
      block_data->resize(block_data->size() + i64_data.size() * 9 + hdr_size + 2);
      block_data->at(last_size + 0) = hdr_b1;
      block_data->at(last_size + 1) = count;
      if (hdr_size == 3)
        block_data->at(last_size + 2) = dec_count;
      // printf("Dec_count: %d\n", dec_count);
      size_t blk_size;
      if (is64bit)
        blk_size = allflic::allflic48::encode(i64_data.data(), count, block_data->data() + last_size + hdr_size, for_val, dbl_exceptions.data());
      else
        blk_size = allflic::allflic48::encode(i32_data.data(), count, block_data->data() + last_size + hdr_size, for_val);
      // printf("Total blk size: %lu, cur size: %lu\n", block_data->size(), blk_size);
      block_data->resize(last_size + blk_size + hdr_size);
      return blk_size + hdr_size;
    }
};

}}

#endif
