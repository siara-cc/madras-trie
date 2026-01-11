#ifndef _DS_COMMON_HPP_
#define _DS_COMMON_HPP_

#include <stdarg.h>
#include <stdint.h>
#include <cstring>
#include <vector>
#include <cmath>

namespace gen {

// Function qualifiers
#ifndef __fq1
#define __fq1
#endif

#ifndef __fq2
#define __fq2
#endif

// Attribute qualifiers
#ifndef __gq1
#define __gq1
#endif

#ifndef __gq2
#define __gq2
#endif

typedef std::vector<uint8_t> byte_vec;
typedef std::vector<uint8_t *> byte_ptr_vec;

__gq1 __gq2 static bool is_gen_print_enabled = false;
__fq1 __fq2 static void gen_printf(const char* format, ...) {
  if (!is_gen_print_enabled)
    return;
  va_list args;
  va_start(args, format);
  vprintf(format, args);
  va_end(args);
}

static constexpr double dbl_div[] = {1.000000000000001, 10.000000000000001, 100.000000000000001, 1000.000000000000001, 10000.000000000000001, 100000.000000000000001, 1000000.000000000000001, 10000000.000000000000001, 100000000.000000000000001, 1000000000.000000000000001};
static int count_decimal_digits(double d) {
  int counter = 0;
  while (d != (int) d) {
    d *= 10;
    counter++;
    if (counter > 9)
      break;
  }
  return counter;
}
bool is_negative_zero(double x) {
  return x == 0.0 && std::signbit(x);
}
const static char *dt_formats[] = {"%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M %p", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"};
const static size_t dt_format_lens[] = {10, 10, 10, 19, 19, 19, 19, 19, 19};

#define SECONDS_PER_DAY 86400

static int64_t days_from_civil(int y, int m, int d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const int yoe = y - era * 400;                       // [0, 399]
    const int doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1; // [0, 365]
    const int doe = yoe * 365 + yoe / 4 - yoe / 100 + yoe / 400 + doy;
    return era * 146097 + doe - 719468;
}

static int64_t tm_to_epoch_seconds(const struct tm *tm) {
    int64_t days = days_from_civil(tm->tm_year + 1900,
                                    tm->tm_mon + 1,
                                    tm->tm_mday);
    return days * SECONDS_PER_DAY +
           tm->tm_hour * 3600 +
           tm->tm_min * 60 +
           tm->tm_sec;
}
static void civil_from_days(int64_t z, int *y, int *m, int *d) {
    z += 719468;
    const int era = (z >= 0 ? z : z - 146096) / 146097;
    const int doe = z - era * 146097;                    // [0, 146096]
    const int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int y1 = yoe + era * 400;
    const int doy = doe - (365 * yoe + yoe / 4 - yoe / 100 + yoe / 400);
    const int mp = (5 * doy + 2) / 153;
    *d = doy - (153 * mp + 2) / 5 + 1;
    *m = mp + (mp < 10 ? 3 : -9);
    *y = y1 + (*m <= 2 ? 1 : 0);
}
static void epoch_seconds_to_tm(int64_t seconds, struct tm *out_tm) {
    memset(out_tm, 0, sizeof(*out_tm));
    int64_t days = seconds / SECONDS_PER_DAY;
    int64_t rem = seconds % SECONDS_PER_DAY;
    if (rem < 0) {
        rem += SECONDS_PER_DAY;
        days -= 1;
    }
    int y, m, d;
    civil_from_days(days, &y, &m, &d);
    out_tm->tm_year = y - 1900;
    out_tm->tm_mon  = m - 1;
    out_tm->tm_mday = d;
    out_tm->tm_hour = rem / 3600;
    rem %= 3600;
    out_tm->tm_min  = rem / 60;
    out_tm->tm_sec  = rem % 60;
}
size_t bits_needed(size_t num) {
  if (num == 0)
    return 1;
  return ceil(log2(num + 1));
}
size_t bytes_needed(size_t num) {
  size_t bits = bits_needed(num);
  return (bits + 7) / 8;
}
static double pow10(int p) {
  return dbl_div[p];
}
static uint32_t log4(uint32_t val) {
  return (val < 4 ? 1 : (val < 16 ? 2 : (val < 64 ? 3 : (val < 256 ? 4 : (val < 1024 ? 5 : (val < 4096 ? 6 : (val < 16384 ? 7 : (val < 65536 ? 8 : 9))))))));
}
static uint32_t log5(uint32_t val) {
  return (val < 5 ? 1 : (val < 25 ? 2 : (val < 125 ? 3 : (val < 625 ? 4 : (val < 3125 ? 5 : (val < 15625 ? 6 : (val < 78125 ? 7 : (val < 390625 ? 8 : 9))))))));
}
static uint32_t log8(uint32_t val) {
  return (val < 8 ? 1 : (val < 64 ? 2 : (val < 512 ? 3 : (val < 4096 ? 4 : (val < 32768 ? 5 : (val < 262144 ? 6 : (val < 2097152 ? 7 : 8)))))));
}
static uint32_t lg10(uint32_t val) {
  return (val < 10 ? 1 : (val < 100 ? 2 : (val < 1000 ? 3 : (val < 10000 ? 4 : (val < 100000 ? 5 : (val < 1000000 ? 6 : (val < 10000000 ? 7 : 8)))))));
}
static uint32_t log12(uint32_t val) {
  return (val < 12 ? 1 : (val < 96 ? 2 : (val < 768 ? 3 : (val < 6144 ? 4 : (val < 49152 ? 5 : (val < 393216 ? 6 : (val < 3145728 ? 7 : 8)))))));
}
static uint32_t log16(uint32_t val) {
  return (val < 16 ? 1 : (val < 256 ? 2 : (val < 4096 ? 3 : (val < 65536 ? 4 : (val < 1048576 ? 5 : (val < 16777216 ? 6 : (val < 268435456 ? 7 : 8)))))));
}
static uint32_t log256(uint32_t val) {
  return (val < 256 ? 1 : (val < 65536 ? 2 : (val < 16777216 ? 3 : 4)));
}
static size_t max(size_t v1, size_t v2) {
  return v1 > v2 ? v1 : v2;
}
static size_t min(size_t v1, size_t v2) {
  return v1 < v2 ? v1 : v2;
}
static size_t size_align8(size_t input) {
  if ((input % 8) == 0)
    return input;
  return input + (8 - input % 8);
}
__fq1 __fq2 static clock_t print_time_taken(clock_t t, const char *msg) {
  t = clock() - t;
  double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds
  gen_printf("%s%.5f\n", msg, time_taken);
  return clock();
}
static int compare(const uint8_t *v1, const uint8_t *v2) {
  int k = 0;
  uint8_t c1, c2;
  do {
    c1 = v1[k];
    c2 = v2[k];
    if (c1 == 0 && c2 == 0)
      return 0;
    k++;
  } while (c1 == c2);
  return (c1 < c2 ? -k : k);
}
static int compare(const uint8_t *v1, int len1, const uint8_t *v2) {
  if (len1 == 0 && v2[0] == 0)
    return 0;
  int k = 0;
  uint8_t c1, c2;
  c1 = c2 = 0;
  while (k < len1 && c1 == c2) {
    c1 = v1[k];
    c2 = v2[k];
    k++;
    if (k == len1 && c2 == 0)
      return 0;
  }
  return (c1 < c2 ? -k : k);
}
static int compare(const uint8_t *v1, const uint8_t *v2, int len2) {
  if (len2 == 0 && v1[0] == 0)
    return 0;
  int k = 0;
  uint8_t c1, c2;
  c1 = c2 = 0;
  while (k < len2 && c1 == c2) {
    c1 = v1[k];
    c2 = v2[k];
    k++;
    if (k == len2 && c1 == 0)
      return 0;
  }
  return (c1 < c2 ? -k : k);
}
static int compare(const uint8_t *v1, int len1, const uint8_t *v2, int len2) {
    int lim = (len2 < len1 ? len2 : len1);
    int k = 0;
    do {
        uint8_t c1 = v1[k];
        uint8_t c2 = v2[k];
        k++;
        if (c1 < c2)
            return -k;
        else if (c1 > c2)
            return k;
    } while (k < lim);
    if (len1 == len2)
        return 0;
    k++;
    return (len1 < len2 ? -k : k);
}
static int compare_rev(const uint8_t *v1, int len1, const uint8_t *v2,
        int len2) {
    int lim = (len2 < len1 ? len2 : len1);
    int k = 0;
      while (k++ < lim) {
        uint8_t c1 = v1[len1 - k];
        uint8_t c2 = v2[len2 - k];
        if (c1 < c2)
            return -k;
        else if (c1 > c2)
            return k;
    }
    if (len1 == len2)
        return 0;
    return (len1 < len2 ? -k : k);
}
__fq1 __fq2 static void copy_uint16(uint16_t input, uint8_t *out) {
  *out++ = input & 0xFF;
  *out = (input >> 8);
}
__fq1 __fq2 static void copy_uint24(uint32_t input, uint8_t *out) {
  *out++ = input & 0xFF;
  *out++ = (input >> 8) & 0xFF;
  *out = (input >> 16);
}
__fq1 __fq2 static void copy_uint32(uint32_t input, uint8_t *out) {
  *out++ = input & 0xFF;
  *out++ = (input >> 8) & 0xFF;
  *out++ = (input >> 16) & 0xFF;
  *out = (input >> 24);
}
__fq1 __fq2 static void copy_uint64(uint64_t input, uint8_t *out) {
  *out++ = input & 0xFF;
  *out++ = (input >> 8) & 0xFF;
  *out++ = (input >> 16) & 0xFF;
  *out++ = (input >> 24) & 0xFF;
  *out++ = (input >> 32) & 0xFF;
  *out++ = (input >> 40) & 0xFF;
  *out++ = (input >> 48) & 0xFF;
  *out = (input >> 56);
}
static void copy_vint32(uint32_t input, uint8_t *out, size_t vlen) {
  for (int i = vlen - 1; i > 0; i--)
    *out++ = 0x80 + ((input >> (7 * i)) & 0x7F);
  *out = input & 0x7F;
}
static uint32_t read_vint32(const uint8_t *ptr, size_t *vlen = NULL) {
  uint32_t ret = 0;
  size_t len = 5; // read max 5 bytes
  do {
    ret <<= 7;
    ret += *ptr & 0x7F;
    len--;
  } while ((*ptr++ >> 7) && len);
  if (vlen != NULL)
    *vlen = 5 - len;
  return ret;
}
static void write_uint16(uint32_t input, FILE *fp) {
  fputc(input & 0xFF, fp);
  fputc(input >> 8, fp);
}
static void write_uint24(uint32_t input, FILE *fp) {
  fputc(input & 0xFF, fp);
  fputc((input >> 8) & 0xFF, fp);
  fputc(input >> 16, fp);
}
static void write_uint32(uint32_t input, FILE *fp) {
  fputc(input & 0xFF, fp);
  fputc((input >> 8) & 0xFF, fp);
  fputc((input >> 16) & 0xFF, fp);
  fputc(input >> 24, fp);
}
static void write_uint64(uint64_t input, FILE *fp) {
  fputc(input & 0xFF, fp);
  fputc((input >> 8) & 0xFF, fp);
  fputc((input >> 16) & 0xFF, fp);
  fputc((input >> 24) & 0xFF, fp);
  fputc((input >> 32) & 0xFF, fp);
  fputc((input >> 40) & 0xFF, fp);
  fputc((input >> 48) & 0xFF, fp);
  fputc(input >> 56, fp);
}
static void append_uint16(uint16_t u16, byte_vec& v) {
  v.push_back(u16 & 0xFF);
  v.push_back(u16 >> 8);
}
static void append_uint24(uint32_t u24, byte_vec& v) {
  v.push_back(u24 & 0xFF);
  v.push_back((u24 >> 8) & 0xFF);
  v.push_back(u24 >> 16);
}
static void append_uint32(uint32_t u32, byte_vec& v) {
  v.push_back(u32 & 0xFF);
  v.push_back((u32 >> 8) & 0xFF);
  v.push_back((u32 >> 16) & 0xFF);
  v.push_back(u32 >> 24);
}
static void append_uint64(uint64_t u64, byte_vec& v) {
  v.push_back(u64 & 0xFF);
  v.push_back((u64 >> 8) & 0xFF);
  v.push_back((u64 >> 16) & 0xFF);
  v.push_back((u64 >> 24) & 0xFF);
  v.push_back((u64 >> 32) & 0xFF);
  v.push_back((u64 >> 40) & 0xFF);
  v.push_back((u64 >> 48) & 0xFF);
  v.push_back(u64 >> 56);
}
static void append_byte_vec(byte_vec& vec, uint8_t *val, size_t val_len) {
  for (size_t i = 0; i < val_len; i++)
    vec.push_back(val[i]);
}
static uint32_t read_uint16(byte_vec& v, size_t pos) {
  uint32_t ret = v[pos++];
  ret |= (v[pos] << 8);
  return ret;
}
static uint32_t read_uint16(const uint8_t *ptr) {
#ifndef __CUDA_ARCH__
  uint16_t tmp;
  memcpy(&tmp, ptr, sizeof(tmp));
  return tmp; // faster endian dependent
#else
  uint32_t ret = *ptr++;
  ret |= (*ptr << 8);
  return ret;
#endif
}

static uint32_t read_uint24(byte_vec& v, size_t pos) {
  uint32_t ret = v[pos++];
  ret |= (uint32_t(v[pos++]) << 8);
  ret |= (uint32_t(v[pos]) << 16);
  return ret;
}

static uint32_t read_uint24(const uint8_t *ptr) {
#ifndef __CUDA_ARCH__
  uint32_t tmp = 0;
  memcpy(&tmp, ptr, 3);
  return tmp & 0x00FFFFFF; // faster endian dependent
#else
  uint32_t ret = *ptr++;
  ret |= (*ptr++ << 8);
  ret |= (*ptr << 16);
  return ret;
#endif
}

static uint32_t read_uint32(uint8_t *ptr) {
#ifndef __CUDA_ARCH__
  uint32_t tmp;
  memcpy(&tmp, ptr, sizeof(tmp));
  return tmp;
#else
  uint32_t ret = *ptr++;
  ret |= (*ptr++ << 8);
  ret |= (*ptr++ << 16);
  ret |= (*ptr << 24);
  return ret;
#endif
}

static uint64_t read_uint40(uint8_t *ptr) {
#ifndef __CUDA_ARCH__
  uint32_t lo;
  memcpy(&lo, ptr, sizeof(lo));
  uint64_t ret = lo;
  return ret | ((uint64_t) ptr[4] << 32);
#else
  uint64_t ret = *ptr++;
  ret |= ((uint64_t)*ptr++ << 8);
  ret |= ((uint64_t)*ptr++ << 16);
  ret |= ((uint64_t)*ptr++ << 24);
  ret |= ((uint64_t)*ptr << 32);
  return ret;
#endif
}

static uint64_t read_uint64(uint8_t *t) {
#ifndef __CUDA_ARCH__
  uint64_t tmp;
  memcpy(&tmp, t, sizeof(tmp));
  return tmp;
#else
  uint64_t ret = *t++;
  ret |= ((uint64_t)*t++ << 8);
  ret |= ((uint64_t)*t++ << 16);
  ret |= ((uint64_t)*t++ << 24);
  ret |= ((uint64_t)*t++ << 32);
  ret |= ((uint64_t)*t++ << 40);
  ret |= ((uint64_t)*t++ << 48);
  ret |= ((uint64_t)*t << 56);
  return ret;
#endif
}

static uint8_t *read_uint64(uint8_t *t, uint64_t& u64) {
#ifndef __CUDA_ARCH__
  memcpy(&u64, t, sizeof(u64)); // faster endian dependent
  return t + 8;
#else
  u64 = *t++;
  u64 |= ((uint64_t)*t++ << 8);
  u64 |= ((uint64_t)*t++ << 16);
  u64 |= ((uint64_t)*t++ << 24);
  u64 |= ((uint64_t)*t++ << 32);
  u64 |= ((uint64_t)*t++ << 40);
  u64 |= ((uint64_t)*t++ << 48);
  u64 |= ((uint64_t)*t << 56);
  return t;
#endif
  // u64 = 0;
  // for (int v = 0; v < 8; v++) {
  //   u64 <<= 8;
  //   u64 |= *t++;
  // }
  // return t;
}

static uint32_t read_uintx(const uint8_t *ptr, uint32_t mask) {
#ifndef __CUDA_ARCH__
  uint32_t tmp;
  memcpy(&tmp, ptr, sizeof(tmp));
  return tmp & mask; // faster endian dependent
#else
  uint32_t ret = *ptr++;
  ret |= (*ptr++ << 8);
  ret |= (*ptr++ << 16);
  ret |= (*ptr << 24);
  return ret & mask;
#endif
}
static uint8_t *extract_line(uint8_t *last_line, size_t& last_line_len, size_t remaining) {
  if (remaining == 0)
    return nullptr;
  last_line += last_line_len;
  while (*last_line == '\r' || *last_line == '\n' || *last_line == '\0') {
    last_line++;
    remaining--;
    if (remaining == 0)
      return nullptr;
  }
  uint8_t *cr_pos = (uint8_t *) memchr(last_line, '\n', remaining);
  if (cr_pos != nullptr) {
    if (*(cr_pos - 1) == '\r')
      cr_pos--;
    *cr_pos = 0;
    last_line_len = cr_pos - last_line;
  } else
    last_line_len = remaining;
  return last_line;
}

class byte_str {
  size_t max_len;
  size_t len;
  uint8_t *buf;
  public:
    __fq1 __fq2 byte_str() {
    }
    __fq1 __fq2 byte_str(uint8_t *_buf, size_t _max_len) {
      set_buf_max_len(_buf, _max_len);
    }
    __fq1 __fq2 void set_buf_max_len(uint8_t *_buf, size_t _max_len) {
      len = 0;
      buf = _buf;
      max_len = _max_len;
    }
    __fq1 __fq2 void append(uint8_t b) {
      if (len < max_len)
        buf[len++] = b;
    }
    __fq1 __fq2 void append(uint8_t *b, size_t blen) {
      size_t start = 0;
      while (len < max_len && start < blen) {
        buf[len++] = *b++;
        start++;
      }
    }
    __fq1 __fq2 uint8_t *data() {
      return buf;
    }
    __fq1 __fq2 uint8_t operator[](size_t idx) const {
      return buf[idx];
    }
    __fq1 __fq2 size_t length() {
      return len;
    }
    __fq1 __fq2 void set_length(size_t _len) {
      len = _len;
    }
    __fq1 __fq2 size_t get_limit() {
      return max_len;
    }
    __fq1 __fq2 void clear() {
      len = 0;
    }
};

}
#endif
