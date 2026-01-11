#ifndef __DV1_BUILDER_OUTPUT_WRITER_HPP
#define __DV1_BUILDER_OUTPUT_WRITER_HPP

#include <stdio.h>
#include <vector>
#include <madras/dv1/common.hpp>

namespace madras { namespace dv1 {

class output_writer {

  private:
    FILE *fp = nullptr;
    std::vector<uint8_t> *out_vec = nullptr;

  public:
    void set_fp(FILE *_fp) {
      if (fp != nullptr) fclose(fp);
      fp = _fp;
    }

    void set_out_vec(std::vector<uint8_t> *_out_vec) {
      out_vec = _out_vec;
    }

    FILE *get_fp() {
      return fp;
    }

    std::vector<uint8_t> * get_out_vec() {
        return out_vec;
    }

    void write_byte(uint8_t b) {
      if (fp == nullptr)
        out_vec->push_back(b);
      else
        fputc(b, fp);
    }

    void write_u32(uint32_t u32) {
      if (fp == nullptr)
        gen::append_uint32(u32, *out_vec);
      else
        gen::write_uint32(u32, fp);
    }

    void write_u64(uint64_t u64) {
      if (fp == nullptr)
        gen::append_uint64(u64, *out_vec);
      else
        gen::write_uint64(u64, fp);
    }

    void write_u16(uintxx_t u16) {
      if (fp == nullptr)
        gen::append_uint16(u16, *out_vec);
      else
        gen::write_uint16(u16, fp);
    }

    void write_u24(uintxx_t u24) {
      if (fp == nullptr)
        gen::append_uint24(u24, *out_vec);
      else
        gen::write_uint24(u24, fp);
    }

    void write_bytes(const uint8_t *b, size_t len) {
      if (fp == nullptr) {
        for (size_t i = 0; i < len; i++)
          out_vec->push_back(b[i]);
      } else
          fwrite(b, 1, len, fp);
    }

    void write_align8(size_t nopad_size) {
      if ((nopad_size % 8) == 0)
          return;
      size_t remaining = 8 - (nopad_size % 8);
      if (fp == nullptr) {
        for (size_t i = 0; i < remaining; i++)
          out_vec->push_back(' ');
      } else {
        const char *padding = "       ";
        fwrite(padding, 1, remaining, fp);
      }
    }

    void reserve(size_t size) {
        if (out_vec != nullptr) out_vec->reserve(size);
    }

    long get_current_pos() {
      if (fp == nullptr) return out_vec->size();
      return ftell(fp);
    }

    bool output_to_file() {
      return (fp != nullptr);
    }

    uint8_t *get_output_buf_at(size_t pos) {
        return out_vec->data() + pos;
    }

    void seek(long pos, int from) {
      fseek(fp, pos, from);
    }

    void close() {
      if (fp != NULL)
        fclose(fp);
      fp = NULL;
    }

};

}}

#endif
