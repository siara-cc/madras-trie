#ifndef _BYTE_BLOCKS_HPP_
#define _BYTE_BLOCKS_HPP_

#include "gen.hpp"

namespace gen {

class byte_blocks {
  private:
    const int block_size;
    bit_vector<uint8_t> is_allocated;
    bool is_released;
    size_t count;
  protected:
    size_t block_remaining;
    std::vector<uint8_t *> blocks;
    uint8_t *reserve(size_t val_len, size_t& pos) {
      if (val_len == 0)
        val_len = 1;
      if (val_len <= block_remaining) {
        uint8_t *ret = blocks[blocks.size() - 1];
        pos = blocks.size() * block_size;
        pos -= block_size;
        size_t block_pos = (block_size - block_remaining);
        pos += block_pos;
        block_remaining -= val_len;
        return ret + block_pos;
      }
      size_t needed_bytes = val_len;
      int needed_blocks = needed_bytes / block_size;
      if (needed_bytes % block_size)
        needed_blocks++;
      block_remaining = needed_blocks * block_size;
      uint8_t *new_block = new uint8_t[block_remaining];
      memset(new_block, '\0', block_remaining);
      pos = blocks.size() * block_size;
      for (int i = 0; i < needed_blocks; i++) {
        is_allocated.set(blocks.size() + i, i == 0);
        blocks.push_back(new_block + i * block_size);
      }
      block_remaining -= val_len;
      return new_block;
    }
  public:
    byte_blocks(int _block_size = 4096)
           : block_size (_block_size) {
      count = 0;
      block_remaining = 0;
      is_released = false;
    }
    ~byte_blocks() {
      release_blocks();
    }
    size_t get_block_size() {
      return block_size;
    }
    void reset() {
      release_blocks();
      is_released = false;
    }
    void release_blocks() {
      for (size_t i = 0; i < blocks.size(); i++) {
        if (is_allocated[i])
          delete [] blocks[i];
      }
      blocks.clear();
      is_allocated.reset();
      is_released = true;
      block_remaining = 0;
      count = 0;
    }
    size_t get_size_in_bytes() {
      return block_size * blocks.size() + is_allocated.size_bytes();
    }
    size_t push_back(const uint8_t c) {
      size_t pos;
      uint8_t *buf = reserve(1, pos);
      *buf = c;
      return pos;
    }
    size_t push_back(const void *val, size_t val_len) {
      size_t pos;
      uint8_t *buf = reserve(val_len, pos);
      memcpy(buf, val, val_len);
      count++;
      return pos;
    }
    size_t push_back_rev(const void *val, size_t val_len) {
      size_t pos;
      uint8_t *buf = reserve(val_len, pos);
      for (int i = val_len - 1; i >= 0; i--)
        buf[val_len - i - 1] = ((uint8_t *) val)[i];
      count++;
      return pos;
    }
    size_t push_back_with_vlen(const void *val, size_t val_len, size_t extra = 0) {
      size_t pos;
      int8_t vlen = gen::get_vlen_of_uint32(val_len);
      uint8_t *buf = reserve(val_len + vlen + extra, pos);
      gen::copy_vint32(val_len, buf, vlen);
      memcpy(buf + vlen, val, val_len);
      count++;
      return pos;
    }
    size_t push_back_fvint(const void *val, size_t val_len) {
      uint8_t *fvint;
      size_t vlen = gen::copy_fvint64(fvint, val_len);
      size_t pos;
      uint8_t *buf = reserve(val_len + vlen, pos);
      memcpy(buf, fvint, vlen);
      memcpy(buf + vlen, val, val_len);
      count++;
      return pos;
    }
    uint8_t *operator[](size_t pos) {
      return blocks[pos / block_size] + (pos % block_size);
    }
    size_t size() {
      return block_size * blocks.size() - block_remaining;
    }
    size_t entry_count() {
      return count;
    }
    uint32_t append_uint32(uint32_t u32) {
      uint8_t s32[4];
      s32[0] = u32 & 0xFF;
      s32[1] = ((u32 >> 8) & 0xFF);
      s32[2] = ((u32 >> 16) & 0xFF);
      s32[3] = (u32 >> 24);
      return push_back(s32, 4);
    }
    uint32_t append_svint60(int64_t input) {
      size_t pos;
      size_t vlen = gen::get_svint60_len(input);
      uint8_t *buf = reserve(vlen, pos);
      gen::copy_svint60(input, buf, vlen);
      count++;
      return pos;
    }
    uint32_t append_svint61(uint64_t input) {
      size_t pos;
      size_t vlen = gen::get_svint61_len(input);
      uint8_t *buf = reserve(vlen, pos);
      gen::copy_svint61(input, buf, vlen);
      count++;
      return pos;
    }
    uint32_t append_svint15(uint64_t input) {
      size_t pos;
      int8_t vlen = gen::get_svint15_len(input);
      uint8_t *buf = reserve(vlen, pos);
      gen::copy_svint15(input, buf, vlen);
      count++;
      return pos;
    }
};

static size_t append_fvint32(byte_blocks& vec, uint32_t u32) {
  int8_t len = 0;
  uint8_t buf[5];
  do {
    uint8_t b = u32 & 0x7F;
    u32 >>= 7;
    if (u32 > 0)
      b |= 0x80;
    buf[len++] = b;
  } while (u32 > 0);
  return vec.push_back(buf, len);
}

class var_struct {
  private:
    byte_blocks data;
  public:
    var_struct() {
    }
    size_t add_vint(uint32_t u32) {
      return append_fvint32(data, u32);
    }
    size_t add_uint32(uint32_t u32) {
      size_t ret = data.push_back(&u32, 4);
      copy_uint32(u32, data[ret]);
      return ret;
    }
};

}

#endif
