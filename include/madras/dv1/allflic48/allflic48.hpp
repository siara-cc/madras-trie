#ifndef __ALLFLIC48__
#define __ALLFLIC48__

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>

namespace allflic {

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

#if defined(_MSC_VER)
  #pragma message("_MSC_VER")
    /* Microsoft C/C++-compatible compiler */
    #if (defined(_M_IX86) || defined(_M_AMD64))
        #include <intrin.h>
    #elif defined(_M_ARM64)
        #include <arm_neon.h>
    #endif

    #include <stdint.h>
    #define __restrict__ __restrict
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
  #warning "_GNUC_X86_64"
    /* GCC-compatible compiler, targeting x86/x86-64 */
    #include <x86intrin.h>
#elif defined(__aarch64__)
  #warning "_aarch64_"
    /* GCC-compatible compiler, targeting ARM with NEON */
    #include <arm_neon.h>
#elif defined(__GNUC__) && defined(__IWMMXT__)
  #warning "_IWMMXT__"
    /* GCC-compatible compiler, targeting ARM with WMMX */
    #include <mmintrin.h>
#elif (defined(__GNUC__) || defined(__xlC__)) && (defined(__VEC__) || defined(__ALTIVEC__))
  #warning "_xlC_VEC_ALTIVEC__"
    /* XLC or GCC-compatible compiler, targeting PowerPC with VMX/VSX */
    #include <altivec.h>
#elif defined(__GNUC__) && defined(__SPE__)
  #warning "_SPE__"
    /* GCC-compatible compiler, targeting PowerPC with SPE */
    #include <spe.h>
#endif

#ifdef _MSC_VER
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanReverse64)
#endif

#ifdef __SSSE3__
typedef __m128i xmm_t;
#endif

#ifndef __CUDA_ARCH__
static const uint8_t lengthTable[256] = {
    4,  5,  6,  7,  5,  6,  7,  8,  6,  7,  8,  9,  7,  8,  9,  10, 5,  6,  7,
    8,  6,  7,  8,  9,  7,  8,  9,  10, 8,  9,  10, 11, 6,  7,  8,  9,  7,  8,
    9,  10, 8,  9,  10, 11, 9,  10, 11, 12, 7,  8,  9,  10, 8,  9,  10, 11, 9,
    10, 11, 12, 10, 11, 12, 13, 5,  6,  7,  8,  6,  7,  8,  9,  7,  8,  9,  10,
    8,  9,  10, 11, 6,  7,  8,  9,  7,  8,  9,  10, 8,  9,  10, 11, 9,  10, 11,
    12, 7,  8,  9,  10, 8,  9,  10, 11, 9,  10, 11, 12, 10, 11, 12, 13, 8,  9,
    10, 11, 9,  10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 6,  7,  8,  9,  7,
    8,  9,  10, 8,  9,  10, 11, 9,  10, 11, 12, 7,  8,  9,  10, 8,  9,  10, 11,
    9,  10, 11, 12, 10, 11, 12, 13, 8,  9,  10, 11, 9,  10, 11, 12, 10, 11, 12,
    13, 11, 12, 13, 14, 9,  10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 12, 13,
    14, 15, 7,  8,  9,  10, 8,  9,  10, 11, 9,  10, 11, 12, 10, 11, 12, 13, 8,
    9,  10, 11, 9,  10, 11, 12, 10, 11, 12, 13, 11, 12, 13, 14, 9,  10, 11, 12,
    10, 11, 12, 13, 11, 12, 13, 14, 12, 13, 14, 15, 10, 11, 12, 13, 11, 12, 13,
    14, 12, 13, 14, 15, 13, 14, 15, 16};

static const int8_t shuffleTable[256][16] = {
  {0, -1, -1, -1, 1, -1, -1, -1, 2, -1, -1, -1, 3, -1, -1, -1}, // 1111
  {0, 1, -1, -1, 2, -1, -1, -1, 3, -1, -1, -1, 4, -1, -1, -1},  // 2111
  {0, 1, 2, -1, 3, -1, -1, -1, 4, -1, -1, -1, 5, -1, -1, -1},   // 3111
  {0, 1, 2, 3, 4, -1, -1, -1, 5, -1, -1, -1, 6, -1, -1, -1},    // 4111
  {0, -1, -1, -1, 1, 2, -1, -1, 3, -1, -1, -1, 4, -1, -1, -1},  // 1211
  {0, 1, -1, -1, 2, 3, -1, -1, 4, -1, -1, -1, 5, -1, -1, -1},   // 2211
  {0, 1, 2, -1, 3, 4, -1, -1, 5, -1, -1, -1, 6, -1, -1, -1},    // 3211
  {0, 1, 2, 3, 4, 5, -1, -1, 6, -1, -1, -1, 7, -1, -1, -1},     // 4211
  {0, -1, -1, -1, 1, 2, 3, -1, 4, -1, -1, -1, 5, -1, -1, -1},   // 1311
  {0, 1, -1, -1, 2, 3, 4, -1, 5, -1, -1, -1, 6, -1, -1, -1},    // 2311
  {0, 1, 2, -1, 3, 4, 5, -1, 6, -1, -1, -1, 7, -1, -1, -1},     // 3311
  {0, 1, 2, 3, 4, 5, 6, -1, 7, -1, -1, -1, 8, -1, -1, -1},      // 4311
  {0, -1, -1, -1, 1, 2, 3, 4, 5, -1, -1, -1, 6, -1, -1, -1},    // 1411
  {0, 1, -1, -1, 2, 3, 4, 5, 6, -1, -1, -1, 7, -1, -1, -1},     // 2411
  {0, 1, 2, -1, 3, 4, 5, 6, 7, -1, -1, -1, 8, -1, -1, -1},      // 3411
  {0, 1, 2, 3, 4, 5, 6, 7, 8, -1, -1, -1, 9, -1, -1, -1},       // 4411
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, -1, -1, 4, -1, -1, -1},  // 1121
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, -1, -1, 5, -1, -1, -1},   // 2121
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, -1, -1, 6, -1, -1, -1},    // 3121
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, -1, -1, 7, -1, -1, -1},     // 4121
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, -1, -1, 5, -1, -1, -1},   // 1221
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, -1, -1, 6, -1, -1, -1},    // 2221
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, -1, -1, 7, -1, -1, -1},     // 3221
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, -1, -1, 8, -1, -1, -1},      // 4221
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, -1, -1, 6, -1, -1, -1},    // 1321
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, -1, -1, 7, -1, -1, -1},     // 2321
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, -1, -1, 8, -1, -1, -1},      // 3321
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, -1, -1, 9, -1, -1, -1},       // 4321
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, -1, -1, 7, -1, -1, -1},     // 1421
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, -1, -1, 8, -1, -1, -1},      // 2421
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, -1, -1, 9, -1, -1, -1},       // 3421
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, 10, -1, -1, -1},       // 4421
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, -1, 5, -1, -1, -1},   // 1131
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, -1, 6, -1, -1, -1},    // 2131
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, -1, 7, -1, -1, -1},     // 3131
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, -1, 8, -1, -1, -1},      // 4131
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, -1, 6, -1, -1, -1},    // 1231
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, -1, 7, -1, -1, -1},     // 2231
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, -1, 8, -1, -1, -1},      // 3231
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, -1, 9, -1, -1, -1},       // 4231
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, -1, 7, -1, -1, -1},     // 1331
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, -1, 8, -1, -1, -1},      // 2331
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, -1, 9, -1, -1, -1},       // 3331
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, -1, 10, -1, -1, -1},       // 4331
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, -1, 8, -1, -1, -1},      // 1431
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, -1, 9, -1, -1, -1},       // 2431
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, -1, 10, -1, -1, -1},       // 3431
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, 11, -1, -1, -1},       // 4431
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, 5, 6, -1, -1, -1},    // 1141
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, 6, 7, -1, -1, -1},     // 2141
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, 7, 8, -1, -1, -1},      // 3141
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, 8, 9, -1, -1, -1},       // 4141
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, 6, 7, -1, -1, -1},     // 1241
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, 7, 8, -1, -1, -1},      // 2241
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, 8, 9, -1, -1, -1},       // 3241
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, 9, 10, -1, -1, -1},       // 4241
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, 7, 8, -1, -1, -1},      // 1341
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, 8, 9, -1, -1, -1},       // 2341
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, 9, 10, -1, -1, -1},       // 3341
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, 10, 11, -1, -1, -1},       // 4341
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1},       // 1441
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, -1, -1},       // 2441
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, 10, 11, -1, -1, -1},       // 3441
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, -1, -1, -1},       // 4441
  {0, -1, -1, -1, 1, -1, -1, -1, 2, -1, -1, -1, 3, 4, -1, -1},  // 1112
  {0, 1, -1, -1, 2, -1, -1, -1, 3, -1, -1, -1, 4, 5, -1, -1},   // 2112
  {0, 1, 2, -1, 3, -1, -1, -1, 4, -1, -1, -1, 5, 6, -1, -1},    // 3112
  {0, 1, 2, 3, 4, -1, -1, -1, 5, -1, -1, -1, 6, 7, -1, -1},     // 4112
  {0, -1, -1, -1, 1, 2, -1, -1, 3, -1, -1, -1, 4, 5, -1, -1},   // 1212
  {0, 1, -1, -1, 2, 3, -1, -1, 4, -1, -1, -1, 5, 6, -1, -1},    // 2212
  {0, 1, 2, -1, 3, 4, -1, -1, 5, -1, -1, -1, 6, 7, -1, -1},     // 3212
  {0, 1, 2, 3, 4, 5, -1, -1, 6, -1, -1, -1, 7, 8, -1, -1},      // 4212
  {0, -1, -1, -1, 1, 2, 3, -1, 4, -1, -1, -1, 5, 6, -1, -1},    // 1312
  {0, 1, -1, -1, 2, 3, 4, -1, 5, -1, -1, -1, 6, 7, -1, -1},     // 2312
  {0, 1, 2, -1, 3, 4, 5, -1, 6, -1, -1, -1, 7, 8, -1, -1},      // 3312
  {0, 1, 2, 3, 4, 5, 6, -1, 7, -1, -1, -1, 8, 9, -1, -1},       // 4312
  {0, -1, -1, -1, 1, 2, 3, 4, 5, -1, -1, -1, 6, 7, -1, -1},     // 1412
  {0, 1, -1, -1, 2, 3, 4, 5, 6, -1, -1, -1, 7, 8, -1, -1},      // 2412
  {0, 1, 2, -1, 3, 4, 5, 6, 7, -1, -1, -1, 8, 9, -1, -1},       // 3412
  {0, 1, 2, 3, 4, 5, 6, 7, 8, -1, -1, -1, 9, 10, -1, -1},       // 4412
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, -1, -1, 4, 5, -1, -1},   // 1122
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, -1, -1, 5, 6, -1, -1},    // 2122
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, -1, -1, 6, 7, -1, -1},     // 3122
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, -1, -1, 7, 8, -1, -1},      // 4122
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, -1, -1, 5, 6, -1, -1},    // 1222
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, -1, -1, 6, 7, -1, -1},     // 2222
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, -1, -1, 7, 8, -1, -1},      // 3222
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, -1, -1, 8, 9, -1, -1},       // 4222
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, -1, -1, 6, 7, -1, -1},     // 1322
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, -1, -1, 7, 8, -1, -1},      // 2322
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, -1, -1, 8, 9, -1, -1},       // 3322
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, -1, -1, 9, 10, -1, -1},       // 4322
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, -1, -1, 7, 8, -1, -1},      // 1422
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, -1, -1, 8, 9, -1, -1},       // 2422
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, -1, -1, 9, 10, -1, -1},       // 3422
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, 10, 11, -1, -1},       // 4422
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, -1, 5, 6, -1, -1},    // 1132
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, -1, 6, 7, -1, -1},     // 2132
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, -1, 7, 8, -1, -1},      // 3132
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, -1, 8, 9, -1, -1},       // 4132
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, -1, 6, 7, -1, -1},     // 1232
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, -1, 7, 8, -1, -1},      // 2232
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, -1, 8, 9, -1, -1},       // 3232
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, -1, 9, 10, -1, -1},       // 4232
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, -1, 7, 8, -1, -1},      // 1332
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, -1, 8, 9, -1, -1},       // 2332
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, -1, 9, 10, -1, -1},       // 3332
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, -1, 10, 11, -1, -1},       // 4332
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, -1, 8, 9, -1, -1},       // 1432
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, -1, 9, 10, -1, -1},       // 2432
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, -1, 10, 11, -1, -1},       // 3432
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, 11, 12, -1, -1},       // 4432
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, 5, 6, 7, -1, -1},     // 1142
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, 6, 7, 8, -1, -1},      // 2142
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, 7, 8, 9, -1, -1},       // 3142
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, 8, 9, 10, -1, -1},       // 4142
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, 6, 7, 8, -1, -1},      // 1242
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, 7, 8, 9, -1, -1},       // 2242
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, 8, 9, 10, -1, -1},       // 3242
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, 9, 10, 11, -1, -1},       // 4242
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, 7, 8, 9, -1, -1},       // 1342
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, 8, 9, 10, -1, -1},       // 2342
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, 9, 10, 11, -1, -1},       // 3342
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, 10, 11, 12, -1, -1},       // 4342
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, -1},       // 1442
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, -1, -1},       // 2442
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, -1, -1},       // 3442
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, -1, -1},       // 4442
  {0, -1, -1, -1, 1, -1, -1, -1, 2, -1, -1, -1, 3, 4, 5, -1},   // 1113
  {0, 1, -1, -1, 2, -1, -1, -1, 3, -1, -1, -1, 4, 5, 6, -1},    // 2113
  {0, 1, 2, -1, 3, -1, -1, -1, 4, -1, -1, -1, 5, 6, 7, -1},     // 3113
  {0, 1, 2, 3, 4, -1, -1, -1, 5, -1, -1, -1, 6, 7, 8, -1},      // 4113
  {0, -1, -1, -1, 1, 2, -1, -1, 3, -1, -1, -1, 4, 5, 6, -1},    // 1213
  {0, 1, -1, -1, 2, 3, -1, -1, 4, -1, -1, -1, 5, 6, 7, -1},     // 2213
  {0, 1, 2, -1, 3, 4, -1, -1, 5, -1, -1, -1, 6, 7, 8, -1},      // 3213
  {0, 1, 2, 3, 4, 5, -1, -1, 6, -1, -1, -1, 7, 8, 9, -1},       // 4213
  {0, -1, -1, -1, 1, 2, 3, -1, 4, -1, -1, -1, 5, 6, 7, -1},     // 1313
  {0, 1, -1, -1, 2, 3, 4, -1, 5, -1, -1, -1, 6, 7, 8, -1},      // 2313
  {0, 1, 2, -1, 3, 4, 5, -1, 6, -1, -1, -1, 7, 8, 9, -1},       // 3313
  {0, 1, 2, 3, 4, 5, 6, -1, 7, -1, -1, -1, 8, 9, 10, -1},       // 4313
  {0, -1, -1, -1, 1, 2, 3, 4, 5, -1, -1, -1, 6, 7, 8, -1},      // 1413
  {0, 1, -1, -1, 2, 3, 4, 5, 6, -1, -1, -1, 7, 8, 9, -1},       // 2413
  {0, 1, 2, -1, 3, 4, 5, 6, 7, -1, -1, -1, 8, 9, 10, -1},       // 3413
  {0, 1, 2, 3, 4, 5, 6, 7, 8, -1, -1, -1, 9, 10, 11, -1},       // 4413
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, -1, -1, 4, 5, 6, -1},    // 1123
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, -1, -1, 5, 6, 7, -1},     // 2123
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, -1, -1, 6, 7, 8, -1},      // 3123
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, -1, -1, 7, 8, 9, -1},       // 4123
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, -1, -1, 5, 6, 7, -1},     // 1223
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, -1, -1, 6, 7, 8, -1},      // 2223
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, -1, -1, 7, 8, 9, -1},       // 3223
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, -1, -1, 8, 9, 10, -1},       // 4223
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, -1, -1, 6, 7, 8, -1},      // 1323
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, -1, -1, 7, 8, 9, -1},       // 2323
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, -1, -1, 8, 9, 10, -1},       // 3323
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, -1, -1, 9, 10, 11, -1},       // 4323
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, -1, -1, 7, 8, 9, -1},       // 1423
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, -1, -1, 8, 9, 10, -1},       // 2423
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, -1, -1, 9, 10, 11, -1},       // 3423
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, 10, 11, 12, -1},       // 4423
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, -1, 5, 6, 7, -1},     // 1133
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, -1, 6, 7, 8, -1},      // 2133
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, -1, 7, 8, 9, -1},       // 3133
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, -1, 8, 9, 10, -1},       // 4133
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, -1, 6, 7, 8, -1},      // 1233
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, -1, 7, 8, 9, -1},       // 2233
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, -1, 8, 9, 10, -1},       // 3233
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, -1, 9, 10, 11, -1},       // 4233
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, -1, 7, 8, 9, -1},       // 1333
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, -1, 8, 9, 10, -1},       // 2333
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, -1, 9, 10, 11, -1},       // 3333
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, -1, 10, 11, 12, -1},       // 4333
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, -1, 8, 9, 10, -1},       // 1433
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, -1, 9, 10, 11, -1},       // 2433
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, -1, 10, 11, 12, -1},       // 3433
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, 11, 12, 13, -1},       // 4433
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, 5, 6, 7, 8, -1},      // 1143
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, 6, 7, 8, 9, -1},       // 2143
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, 7, 8, 9, 10, -1},       // 3143
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, 8, 9, 10, 11, -1},       // 4143
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, 6, 7, 8, 9, -1},       // 1243
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, 7, 8, 9, 10, -1},       // 2243
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, 8, 9, 10, 11, -1},       // 3243
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, 9, 10, 11, 12, -1},       // 4243
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, 7, 8, 9, 10, -1},       // 1343
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, 8, 9, 10, 11, -1},       // 2343
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, 9, 10, 11, 12, -1},       // 3343
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, 10, 11, 12, 13, -1},       // 4343
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, -1},       // 1443
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, -1},       // 2443
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, -1},       // 3443
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, -1},       // 4443
  {0, -1, -1, -1, 1, -1, -1, -1, 2, -1, -1, -1, 3, 4, 5, 6},    // 1114
  {0, 1, -1, -1, 2, -1, -1, -1, 3, -1, -1, -1, 4, 5, 6, 7},     // 2114
  {0, 1, 2, -1, 3, -1, -1, -1, 4, -1, -1, -1, 5, 6, 7, 8},      // 3114
  {0, 1, 2, 3, 4, -1, -1, -1, 5, -1, -1, -1, 6, 7, 8, 9},       // 4114
  {0, -1, -1, -1, 1, 2, -1, -1, 3, -1, -1, -1, 4, 5, 6, 7},     // 1214
  {0, 1, -1, -1, 2, 3, -1, -1, 4, -1, -1, -1, 5, 6, 7, 8},      // 2214
  {0, 1, 2, -1, 3, 4, -1, -1, 5, -1, -1, -1, 6, 7, 8, 9},       // 3214
  {0, 1, 2, 3, 4, 5, -1, -1, 6, -1, -1, -1, 7, 8, 9, 10},       // 4214
  {0, -1, -1, -1, 1, 2, 3, -1, 4, -1, -1, -1, 5, 6, 7, 8},      // 1314
  {0, 1, -1, -1, 2, 3, 4, -1, 5, -1, -1, -1, 6, 7, 8, 9},       // 2314
  {0, 1, 2, -1, 3, 4, 5, -1, 6, -1, -1, -1, 7, 8, 9, 10},       // 3314
  {0, 1, 2, 3, 4, 5, 6, -1, 7, -1, -1, -1, 8, 9, 10, 11},       // 4314
  {0, -1, -1, -1, 1, 2, 3, 4, 5, -1, -1, -1, 6, 7, 8, 9},       // 1414
  {0, 1, -1, -1, 2, 3, 4, 5, 6, -1, -1, -1, 7, 8, 9, 10},       // 2414
  {0, 1, 2, -1, 3, 4, 5, 6, 7, -1, -1, -1, 8, 9, 10, 11},       // 3414
  {0, 1, 2, 3, 4, 5, 6, 7, 8, -1, -1, -1, 9, 10, 11, 12},       // 4414
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, -1, -1, 4, 5, 6, 7},     // 1124
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, -1, -1, 5, 6, 7, 8},      // 2124
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, -1, -1, 6, 7, 8, 9},       // 3124
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, -1, -1, 7, 8, 9, 10},       // 4124
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, -1, -1, 5, 6, 7, 8},      // 1224
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, -1, -1, 6, 7, 8, 9},       // 2224
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, -1, -1, 7, 8, 9, 10},       // 3224
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, -1, -1, 8, 9, 10, 11},       // 4224
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, -1, -1, 6, 7, 8, 9},       // 1324
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, -1, -1, 7, 8, 9, 10},       // 2324
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, -1, -1, 8, 9, 10, 11},       // 3324
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, -1, -1, 9, 10, 11, 12},       // 4324
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, -1, -1, 7, 8, 9, 10},       // 1424
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, -1, -1, 8, 9, 10, 11},       // 2424
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, -1, -1, 9, 10, 11, 12},       // 3424
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, 10, 11, 12, 13},       // 4424
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, -1, 5, 6, 7, 8},      // 1134
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, -1, 6, 7, 8, 9},       // 2134
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, -1, 7, 8, 9, 10},       // 3134
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, -1, 8, 9, 10, 11},       // 4134
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, -1, 6, 7, 8, 9},       // 1234
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, -1, 7, 8, 9, 10},       // 2234
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, -1, 8, 9, 10, 11},       // 3234
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, -1, 9, 10, 11, 12},       // 4234
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, -1, 7, 8, 9, 10},       // 1334
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, -1, 8, 9, 10, 11},       // 2334
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, -1, 9, 10, 11, 12},       // 3334
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, -1, 10, 11, 12, 13},       // 4334
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, -1, 8, 9, 10, 11},       // 1434
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, -1, 9, 10, 11, 12},       // 2434
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, -1, 10, 11, 12, 13},       // 3434
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, 11, 12, 13, 14},       // 4434
  {0, -1, -1, -1, 1, -1, -1, -1, 2, 3, 4, 5, 6, 7, 8, 9},       // 1144
  {0, 1, -1, -1, 2, -1, -1, -1, 3, 4, 5, 6, 7, 8, 9, 10},       // 2144
  {0, 1, 2, -1, 3, -1, -1, -1, 4, 5, 6, 7, 8, 9, 10, 11},       // 3144
  {0, 1, 2, 3, 4, -1, -1, -1, 5, 6, 7, 8, 9, 10, 11, 12},       // 4144
  {0, -1, -1, -1, 1, 2, -1, -1, 3, 4, 5, 6, 7, 8, 9, 10},       // 1244
  {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, 6, 7, 8, 9, 10, 11},       // 2244
  {0, 1, 2, -1, 3, 4, -1, -1, 5, 6, 7, 8, 9, 10, 11, 12},       // 3244
  {0, 1, 2, 3, 4, 5, -1, -1, 6, 7, 8, 9, 10, 11, 12, 13},       // 4244
  {0, -1, -1, -1, 1, 2, 3, -1, 4, 5, 6, 7, 8, 9, 10, 11},       // 1344
  {0, 1, -1, -1, 2, 3, 4, -1, 5, 6, 7, 8, 9, 10, 11, 12},       // 2344
  {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, 9, 10, 11, 12, 13},       // 3344
  {0, 1, 2, 3, 4, 5, 6, -1, 7, 8, 9, 10, 11, 12, 13, 14},       // 4344
  {0, -1, -1, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},       // 1444
  {0, 1, -1, -1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},       // 2444
  {0, 1, 2, -1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},       // 3444
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}        // 4444
};
#endif

class allflic48 {
  private:
    __fq1 __fq2 static size_t copy_fvint64(uint8_t *ptr, uint64_t u64) {
      size_t len = 0;
      do {
        uint8_t b = u64 & 0x7F;
        u64 >>= 7;
        if (u64 > 0)
          b |= 0x80;
        *ptr++ = b;
        len++;
      } while (u64 > 0);
      return len;
    }
    __fq1 __fq2 static uint64_t read_fvint64(const uint8_t *ptr, size_t& len) {
      len = 0;
      uint64_t ret = 0;
      do {
        size_t bval = *ptr & 0x7F;
        ret += (bval << (7 * len));
        len++;
      } while (*ptr++ & 0x80);
      return ret;
    }
    __fq1 __fq2 static uint32_t read_uint32(const uint8_t *ptr) {
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

    __fq1 __fq2 static uint32_t read_uint32x(const uint8_t *ptr, uint8_t len) {
      #ifndef __CUDA_ARCH__
      uint32_t tmp = 0;
      memcpy(&tmp, ptr, sizeof(tmp));
      return tmp & mask32()[len];
      #else
      uint32_t ret = *ptr;
      ptr += len;
      while (len)
        ret |= ((uint32_t) *ptr-- << (len-- * 8));
      return ret;
      #endif
    }

    __fq1 __fq2 static int32_t read_int32x(const uint8_t *ptr, uint8_t len) {
      #ifndef __CUDA_ARCH__
      int32_t tmp = 0;
      memcpy(&tmp, ptr, sizeof(tmp));
      return tmp & mask32()[len];
      #else
      int32_t ret = *ptr;
      ptr += len;
      while (len)
        ret |= ((int32_t) *ptr-- << (len-- * 8));
      return ret;
      #endif
    }

    __fq1 __fq2 static uint64_t read_uint64x(const uint8_t *ptr, uint8_t len) {
      #ifndef __CUDA_ARCH__
      uint64_t tmp = 0;
      memcpy(&tmp, ptr, sizeof(tmp));
      return tmp & mask64()[len];
      #else
      uint64_t ret = *ptr;
      ptr += len;
      while (len)
        ret |= ((uint64_t) *ptr-- << (len-- * 8));
      return ret;
      #endif
    }

    __fq1 __fq2 static int64_t read_int64x(const uint8_t *ptr, uint8_t len) {
      #ifndef __CUDA_ARCH__
      int64_t tmp = 0;
      memcpy(&tmp, ptr, sizeof(tmp));
      return tmp & mask64()[len];
      #else
      int64_t ret = *ptr;
      ptr += len;
      while (len)
        ret |= ((int64_t) *ptr-- << (len-- * 8));
      return ret;
      #endif
    }

    __fq1 __fq2 static uint64_t read_uint64(const uint8_t *ptr) {
      #ifndef __CUDA_ARCH__
      uint64_t tmp;
      memcpy(&tmp, ptr, sizeof(tmp));
      return tmp;
      #else
      uint64_t ret = *ptr++;
      ret |= (*ptr++ << 8);
      ret |= (*ptr++ << 16);
      ret |= (*ptr++ << 24);
      ret |= ((uint64_t) *ptr++ << 32);
      ret |= ((uint64_t) *ptr++ << 40);
      ret |= ((uint64_t) *ptr++ << 48);
      ret |= ((uint64_t) *ptr << 56);
      return ret;
      #endif
    }

    __fq1 __fq2 static void set_uint32(uint8_t *ptr, uint32_t u32) {
      #ifndef __CUDA_ARCH__
      memcpy(ptr, &u32, sizeof(u32));
      #else
      *ptr++ = u32;
      *ptr++ = u32 >> 8;
      *ptr++ = u32 >> 16;
      *ptr++ = u32 >> 24;
      #endif
    }

    __fq1 __fq2 static void set_uint64(uint8_t *ptr, uint64_t u64) {
      #ifndef __CUDA_ARCH__
      memcpy(ptr, &u64, sizeof(u64));
      #else
      *ptr++ = u64;
      *ptr++ = u64 >> 8;
      *ptr++ = u64 >> 16;
      *ptr++ = u64 >> 24;
      *ptr++ = u64 >> 32;
      *ptr++ = u64 >> 40;
      *ptr++ = u64 >> 48;
      *ptr = u64 >> 56;
      #endif
    }

  public:
    //constexpr static double tens[] = {1.000000000000001, 10.000000000000001, 100.000000000000001, 1000.000000000000001, 10000.000000000000001, 100000.000000000000001, 1000000.000000000000001, 10000000.000000000000001, 100000000.000000000000001, 1000000000.000000000000001, 10000000000.00000000000001, 100000000000.0000000000001, 1000000000000.0000000000001, 10000000000000.000000000001, 100000000000000.00000000001, 1000000000000000.0000000001, 10000000000000000.000000001};
    __fq1 __fq2 inline const static double *tens() {
      static const double values[] = {1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
      return values;
    }
    __fq1 __fq2 inline const static uint32_t *mask32() {
      static const uint32_t values[] = { 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF };
      return values;
    }
    __fq1 __fq2 inline const static uint64_t *mask64() {
      static const uint64_t values[] = { 0xFFUL, 0xFFFFUL, 0xFFFFFFUL, 0xFFFFFFFFUL , 0xFFFFFFFFFFUL , 0xFFFFFFFFFFFFUL , 0xFFFFFFFFFFFFFFUL , 0xFFFFFFFFFFFFFFFFUL };
      return values;
    }
    __fq1 __fq2 allflic48() {
    }
    __fq1 __fq2 static inline size_t first_bit_pos64(uint64_t num) {
    #ifdef _MSC_VER
        unsigned long index;
        if (num == 0 || !_BitScanReverse64(&index, num))
            return 0;
        return index;
    #else
        return num == 0 ? 0 : 63 - __builtin_clzll(num);
    #endif
    }
    __fq1 __fq2 static inline size_t first_bit_pos32(uint64_t num) {
    #ifdef _MSC_VER
        unsigned long index;
        if (num == 0 || !_BitScanReverse(&index, (unsigned long)num))
            return 0;
        return index;
    #else
        return num == 0 ? 0 : 31 - __builtin_clz((unsigned int)num);
    #endif
    }
    __fq1 __fq2 static uint8_t cvt_dbl2_i64(double dbl, int64_t& i64) {
      const double epsilon = 1e-20;
      const size_t zero_nine_thres = 5;
      i64 = static_cast<int64_t>(dbl);
      int64_t ifrac = 0;
      size_t frac_width = 0;
      double frac = fabs(dbl - i64);
      int digit, prev_digit = 8;
      size_t rpt_count = 0;
      while (frac > epsilon && frac_width < 20) {
        frac *= 10;
        digit = frac;
        frac -= digit;
        // printf("d: %d, pd: %d, fw: %lu\n", digit, prev_digit, frac_width);
        if ((prev_digit == 0 && digit == 0) ||
            (prev_digit == 9 && digit == 9)) {
          rpt_count++;
          if (rpt_count > zero_nine_thres) {
            frac_width--;
            ifrac = (ifrac - prev_digit) / 10;
            if (digit == 9)
              ifrac++;
            i64 = static_cast<int64_t>(dbl * tens()[frac_width]);
            double dbl_back = static_cast<double>(i64);
            dbl_back /= tens()[frac_width];
            if (dbl != dbl_back) {
              memcpy(&i64, &dbl, 8);
              return UINT8_MAX;
            }
            return frac_width;
          }
          continue;
        }
        while (rpt_count > 0) {
          ifrac = (ifrac * 10) + prev_digit;
          frac_width++;
          rpt_count--;
        }
        ifrac = (ifrac * 10) + digit;
        frac_width++;
        prev_digit = digit;
      }
      if (frac_width == 20) {
        memcpy(&i64, &dbl, 8);
        return UINT8_MAX;
      }
      i64 = static_cast<int64_t>(dbl * tens()[frac_width]);
      double dbl_back = static_cast<double>(i64);
      dbl_back /= tens()[frac_width];
      if (dbl != dbl_back) {
        memcpy(&i64, &dbl, 8);
        return UINT8_MAX;
      }
      return frac_width;
    }
    static void print_bits(uint64_t u64) {
      uint64_t mask = (1ULL << 63);
      for (size_t i = 0; i < 64; i++) {
        printf("%d", u64 & mask ? 1 : 0);
        mask >>= 1;
      }
      printf("\n");
    }
    static int64_t reduce_neg(int64_t i64) {
      uint64_t u64 = i64 < 0 ? -i64 : i64;
      u64 <<= 1;
      if (i64 < 0)
        u64 |= 1ULL;
      i64 = static_cast<int64_t>(u64);
      return i64;
    }
    static int64_t expand_neg(int64_t i64) {
      uint64_t u64;
      u64 = static_cast<uint64_t>(i64);
      i64 = u64 >> 1;
      if (u64 & 1) {
        i64 = -i64;
        if (i64 == 0)
          i64 = INT64_MIN;
      }
      return i64;
    }
    __fq1 __fq2 static uint64_t zigzag_encode(int64_t x) {
      return (static_cast<uint64_t>(x) << 1) ^ static_cast<uint64_t>(x >> 63);
    }
    __fq1 __fq2 static int64_t zigzag_decode(uint64_t ux) {
        return (ux >> 1) ^ -static_cast<int64_t>(ux & 1);
    }
    __fq1 __fq2 static uint8_t *simple_encode_single(int32_t i32, uint8_t *out, uint8_t or_to_len = 0) {
      uint32_t u32;
      memcpy(&u32, &i32, 4);
      size_t byt_len = first_bit_pos32(u32) / 8;
      uint8_t *o = out;
      *o++ = (byt_len | or_to_len);
      memcpy(o, &u32, 4);
      o += byt_len;
      return o + 1;
    }
    __fq1 __fq2 static uint8_t *simple_encode_single(int64_t i64, uint8_t *out, uint8_t or_to_len = 0) {
      uint64_t u64;
      memcpy(&u64, &i64, 8);
      size_t byt_len = first_bit_pos64(u64) / 8;
      uint8_t *o = out;
      *o++ = (byt_len | or_to_len);
      memcpy(o, &u64, 8);
      o += byt_len;

      // uint8_t *o = out;
      // do {
      //   o++;
      //   *o = u64;
      //   u64 >>= 8;
      // } while (u64 > 0);
      // *out = (o - out - 1);
      return o + 1;
    }
    __fq1 __fq2 static size_t simple_encode(const int64_t *in, size_t count, uint8_t *out) {
      uint8_t *o = out;
      for (size_t i = 0; i < count; i++) {
        o = simple_encode_single(in[i], o);
      }
      return o - out;
    }
    __fq1 __fq2 static uint8_t simple_decode_single(const uint8_t *in, int64_t *out) {
      uint8_t frac_width = *in & 0xF8;
      size_t ilen = *in++ & 0x07;
      *out = read_uint64x(in, ilen);
      return frac_width;
    }
    __fq1 __fq2 static size_t simple_decode(const uint8_t *in, size_t count, int64_t *out) {
      size_t ilen;
      int64_t *o = out;
      while (count-- > 0) {
        ilen = *in++ & 0x07;
        *o++ = read_uint64x(in, ilen);
        ilen++;
        in += ilen;
      }
      return o - out;
    }
    __fq1 __fq2 static size_t encode(const int32_t *in, size_t count, uint8_t *out, int32_t for_val) {
      size_t for_len = copy_fvint64(out, for_val);
      size_t hdr_len = count / 4;
      if (count % 4)
        hdr_len++;
      uint8_t *hout = out + for_len;
      memset(hout, '\0', hdr_len);
      uint8_t *dout = out + hdr_len + for_len;
      uint32_t in32;
      size_t bit_len;
      size_t shl = 0;
      for (size_t i = 0; i < count; i++) {
        memcpy(&in32, in + i, 4);
        #ifndef __CUDA_ARCH__
        bit_len = first_bit_pos32(in32) / 8;
        memcpy(dout, &in32, 4);
        dout += bit_len;
        dout++;
        *hout |= (bit_len << shl);
        #else
        if (in32 < (1U << 8)) {
          *dout++ = in32;
        } else if (in32 < (1U << 16)) {
          *dout++ = in32;
          *dout++ = in32 >> 8;
          *hout |= (1 << shl);
        } else if (in32 < (1U << 24)) {
          *dout++ = in32;
          *dout++ = in32 >> 8;
          *dout++ = in32 >> 16;
          *hout |= (2 << shl);
        } else {
          set_uint32(dout, in32);
          dout += 4;
          *hout |= (3 << shl);
        }
        #endif
        shl += 2;
        if (shl == 8) {
          shl = 0;
          hout++;
        }
      }
      return dout - out;
    }

#if defined(__aarch64__)
    static size_t decode(const uint8_t *in, size_t count, int32_t *out) {
      size_t sz = 0;
      size_t for_len;
      int64_t for_val = read_fvint64(in, for_len);
      sz += for_len;
      size_t hdr_len = count / 4;
      if (count % 4)
        hdr_len++;

      // printf("decoded for: %Llu\n", for_val);

      const uint8_t *hin = in + sz;
      sz += hdr_len;

      int32_t *fout = out;
      for (size_t i = 0; i < count; i += 4) {
        uint8x16_t Data = vld1q_u8(in + sz); // load 16 bytes
        uint8x16_t Shuf = vld1q_u8((const uint8_t *) &shuffleTable[*hin]); // load shuffle mask

        // emulate _mm_shuffle_epi8 using NEON's vqtbl1q_u8
        uint8x16_t Shuffled = vqtbl1q_u8(Data, Shuf);

        vst1q_u8((uint8_t *) out, Shuffled); // store 16 bytes
        sz += lengthTable[*hin];
        out += 4;
        hin++;
      }
      for (size_t i = 0; i < count; i++)
        fout[i] += for_val;

      return sz;
    }
#else
#ifdef __SSSE3__
    static xmm_t get_shuf_mask(uint8_t key) {
      return *(xmm_t *) &shuffleTable[key];
      // size_t len;
      // size_t off = 0;
      // int8_t shuf_mask[16];
      // int8_t *ptr;
      // memset(shuf_mask, 0xFF, 16);
      // for (size_t i = 0; i < 4; i++) {
      //   ptr = shuf_mask + i * 4;
      //   len = (key >> i * 2) & 3;
      //   len++;
      //   do {
      //     *ptr++ = off++;
      //   } while (--len);
      // }
      // return *(xmm_t *) shuf_mask;
    }
    static size_t decode(const uint8_t *in, size_t count, int32_t *out) {
      size_t sz = 0;
      size_t for_len;
      int64_t for_val = read_fvint64(in, for_len);
      sz += for_len;
      size_t hdr_len = count / 4;
      if (count % 4)
        hdr_len++;

      // printf("decoded for: %Llu\n", for_val);

      const uint8_t *hin = in + sz;
      sz += hdr_len;
      int32_t *fout = out;
      xmm_t Data, Shuf;
      for (size_t i = 0; i < count; i += 4) {
        Data = _mm_loadu_si128((xmm_t *) (in + sz));
        Shuf = *(xmm_t *) &shuffleTable[*hin];
        Data = _mm_shuffle_epi8(Data, Shuf);
        _mm_storeu_si128((xmm_t *) out, Data);
        sz += lengthTable[*hin];
        out += 4;
        hin++;
      }
      for (size_t i = 0; i < count; i++)
        fout[i] += for_val;
      return sz;
    }
#else
__fq1 __fq2 static size_t decode(const uint8_t *in, size_t count, int32_t *out) {
      size_t sz = 0;
      size_t for_len;
      int64_t for_val = read_fvint64(in, for_len);
      sz += for_len;
      size_t hdr_len = count / 4;
      if (count % 4)
        hdr_len++;
      const uint8_t *hin = in + sz;
      sz += hdr_len;
      int32_t *fout = out;
      size_t shr = 0;
      uint8_t vlen;
      uint32_t keys = read_uint32(hin);
      for (size_t i = 0; i < count; i++) {
        vlen = (keys >> shr) & 3;
        out[i] = read_int32x(in + sz, vlen);
        sz += vlen;
        sz++;
        shr += 2;
        if (shr == 32) {
          shr = 0;
          hin+=4;
          keys = read_uint32(hin);
        }
      }
      for (size_t i = 0; i < count; i++)
        fout[i] += for_val;
      return sz;
    }
#endif
#endif

    __fq1 __fq2 static size_t encode(const int64_t *in, size_t count, uint8_t *out, int64_t for_val, uint8_t *exceptions = nullptr) {
      size_t for_len = copy_fvint64(out, for_val);
      size_t hdr_len = count * 3 / 8;
      if (count % 8)
        hdr_len += 3; // wasting space?
      uint8_t *hout = out + for_len;
      memset(hout, '\0', hdr_len);
      uint8_t *dout = out + hdr_len + for_len;
      size_t pos = 1;
      size_t bc;
      uint8_t shl_arr[] = {0, 0, 3, 6, 1, 4, 7, 2, 5};
      uint64_t in64;
      for (size_t i = 0; i < count; i++) {
        memcpy(&in64, in + i, 8);
        #ifndef __CUDA_ARCH__
        if (exceptions != nullptr && exceptions[i] != 0)
          bc = 7;
        else
          bc = first_bit_pos64(in64) / 8;
        memcpy(dout, &in64, 8);
        dout += bc;
        dout++;
        #else
        if (exceptions != nullptr && exceptions[i] != 0)
          in64 = UINT64_MAX;
        if (in64 < (1UL << 8)) {
          *dout++ = in64;
          bc = 0;
        } else if (in64 < (1UL << 16)) {
          *dout++ = in64;
          *dout++ = in64 >> 8;
          bc = 1;
        } else if (in64 < (1UL << 24)) {
          *dout++ = in64;
          *dout++ = in64 >> 8;
          *dout++ = in64 >> 16;
          bc = 2;
        } else if (in64 < (1UL << 32)) {
          *dout++ = in64;
          *dout++ = in64 >> 8;
          *dout++ = in64 >> 16;
          *dout++ = in64 >> 24;
          bc = 3;
        } else if (in64 < (1UL << 40)) {
          *dout++ = in64;
          *dout++ = in64 >> 8;
          *dout++ = in64 >> 16;
          *dout++ = in64 >> 24;
          *dout++ = in64 >> 32;
          bc = 4;
        } else if (in64 < (1UL << 48)) {
          *dout++ = in64;
          *dout++ = in64 >> 8;
          *dout++ = in64 >> 16;
          *dout++ = in64 >> 24;
          *dout++ = in64 >> 32;
          *dout++ = in64 >> 40;
          bc = 5;
        } else if (in64 < (1UL << 56)) {
          *dout++ = in64;
          *dout++ = in64 >> 8;
          *dout++ = in64 >> 16;
          *dout++ = in64 >> 24;
          *dout++ = in64 >> 32;
          *dout++ = in64 >> 40;
          *dout++ = in64 >> 48;
          bc = 6;
        } else {
          in64 = in[i];
          set_uint64(dout, in64);
          dout += 8;
          bc = 7;
        }
        #endif
        *hout |= (bc << shl_arr[pos]);
        if ((pos % 3) == 0) {
          hout++;
          *hout |= (bc >> (8 - shl_arr[pos]));
        }
        pos++;
        if (pos == 9) {
          pos = 1;
          hout++;
        }
      }
      return dout - out;
    }
    __fq1 __fq2 static size_t decode(const uint8_t *in, size_t count, int64_t *out, uint8_t *lens = nullptr) {
      size_t sz = 0;
      size_t for_len;
      int64_t for_val = read_fvint64(in, for_len);
      sz += for_len;
      size_t hdr_len = count * 3 / 8;
      if (count % 8)
        hdr_len += 3;
      const uint8_t *hin = in + sz;
      sz += hdr_len;
      size_t shr = 0;
      uint8_t vlen;
      uint32_t keys = read_uint32(hin);
      for (size_t i = 0; i < count; i++) {
        vlen = (keys >> shr) & 7;
        if (lens != nullptr)
          lens[i] = vlen;
        out[i] = read_int64x(in + sz, vlen);
        //printf("%llu\n", *out);
        sz += vlen;
        sz++;
        shr += 3;
        if (shr == 24) {
          shr = 0;
          hin += 3;
          keys = read_uint32(hin);
        }
      }
      for (size_t i = 0; i < count; i++)
        out[i] += for_val;
      return sz;
    }
    __fq1 __fq2 static size_t decode_len(const uint8_t *in, size_t pos) {
      uint32_t u32 = read_uint32(in + pos / 8 * 3);
      return (u32 >> ((pos % 8) * 3)) & 7;
    }

};

}

#endif
