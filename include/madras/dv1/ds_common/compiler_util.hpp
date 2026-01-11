#ifndef COMPILER_UTIL_HPP
#define COMPILER_UTIL_HPP

// Begin suppression of unused-function warnings
#if defined(__clang__)
  #define BEGIN_IGNORE_UNUSED_FUNCTION \
    _Pragma("clang diagnostic push") \
    _Pragma("clang diagnostic ignored \"-Wunused-function\"")

#elif defined(__GNUC__) || defined(__GNUG__)
  #define BEGIN_IGNORE_UNUSED_FUNCTION \
    _Pragma("GCC diagnostic push") \
    _Pragma("GCC diagnostic ignored \"-Wunused-function\"")

#elif defined(_MSC_VER)
  #define BEGIN_IGNORE_UNUSED_FUNCTION \
    __pragma(warning(push)) \
    __pragma(warning(disable: 4505))  // MSVC: unreferenced local function has been removed

#else
  #define BEGIN_IGNORE_UNUSED_FUNCTION
#endif

// End suppression
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
  #define END_IGNORE_UNUSED_FUNCTION _Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
  #define END_IGNORE_UNUSED_FUNCTION __pragma(warning(pop))
#else
  #define END_IGNORE_UNUSED_FUNCTION
#endif

#endif // COMPILER_UTIL_HPP
