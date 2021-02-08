/* mdbx_load.c - memory-mapped database load tool */

/*
 * Copyright 2015-2021 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#define MDBX_TOOLS /* Avoid using internal mdbx_assert() */
/*
 * Copyright 2015-2021 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

#define MDBX_BUILD_SOURCERY c28f4f8639430c26ee6745bff5a95c11b991330980f283efba4afe6c3d07f335_v0_9_3_11_g34dcb410
#ifdef MDBX_CONFIG_H
#include MDBX_CONFIG_H
#endif

#define LIBMDBX_INTERNALS
#ifdef MDBX_TOOLS
#define MDBX_DEPRECATED
#endif /* MDBX_TOOLS */

/* *INDENT-OFF* */
/* clang-format off */

/* In case the MDBX_DEBUG is undefined set it corresponding to NDEBUG */
#ifndef MDBX_DEBUG
#   ifdef NDEBUG
#       define MDBX_DEBUG 0
#   else
#       define MDBX_DEBUG 1
#   endif
#endif

/* Undefine the NDEBUG if debugging is enforced by MDBX_DEBUG */
#if MDBX_DEBUG
#   undef NDEBUG
#endif

#ifdef MDBX_ALLOY
/* Amalgamated build */
#   define MDBX_INTERNAL_FUNC static
#   define MDBX_INTERNAL_VAR static
#else
/* Non-amalgamated build */
#   define MDBX_INTERNAL_FUNC
#   define MDBX_INTERNAL_VAR extern
#endif /* MDBX_ALLOY */

#ifndef MDBX_DISABLE_GNU_SOURCE
#define MDBX_DISABLE_GNU_SOURCE 0
#endif
#if MDBX_DISABLE_GNU_SOURCE
#undef _GNU_SOURCE
#elif (defined(__linux__) || defined(__gnu_linux__)) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

/*----------------------------------------------------------------------------*/

/* Should be defined before any includes */
#ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
#endif

#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

#ifdef _MSC_VER
#   if _MSC_FULL_VER < 190024234
        /* Actually libmdbx was not tested with compilers older than 19.00.24234 (Visual Studio 2015 Update 3).
         * But you could remove this #error and try to continue at your own risk.
         * In such case please don't rise up an issues related ONLY to old compilers.
         */
#       error "At least \"Microsoft C/C++ Compiler\" version 19.00.24234 (Visual Studio 2015 Update 3) is required."
#   endif
#   ifndef _CRT_SECURE_NO_WARNINGS
#       define _CRT_SECURE_NO_WARNINGS
#   endif
#if _MSC_VER > 1800
#   pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#if _MSC_VER > 1913
#   pragma warning(disable : 5045) /* Compiler will insert Spectre mitigation... */
#endif
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for automatic inline expansion */
#pragma warning(disable : 4201) /* nonstandard extension used : nameless struct / union */
#pragma warning(disable : 4702) /* unreachable code */
#pragma warning(disable : 4706) /* assignment within conditional expression */
#pragma warning(disable : 4127) /* conditional expression is constant */
#pragma warning(disable : 4324) /* 'xyz': structure was padded due to alignment specifier */
#pragma warning(disable : 4310) /* cast truncates constant value */
#pragma warning(disable : 4820) /* bytes padding added after data member for alignment */
#pragma warning(disable : 4548) /* expression before comma has no effect; expected expression with side - effect */
#pragma warning(disable : 4366) /* the result of the unary '&' operator may be unaligned */
#pragma warning(disable : 4200) /* nonstandard extension used: zero-sized array in struct/union */
#pragma warning(disable : 4204) /* nonstandard extension used: non-constant aggregate initializer */
#pragma warning(disable : 4505) /* unreferenced local function has been removed */
#endif                          /* _MSC_VER (warnings) */

#include "mdbx.h"
/*
 * Copyright 2015-2021 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/* *INDENT-OFF* */
/* clang-format off */

#ifndef __GNUC_PREREQ
#   if defined(__GNUC__) && defined(__GNUC_MINOR__)
#       define __GNUC_PREREQ(maj, min) \
          ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#   else
#       define __GNUC_PREREQ(maj, min) (0)
#   endif
#endif /* __GNUC_PREREQ */

#ifndef __CLANG_PREREQ
#   ifdef __clang__
#       define __CLANG_PREREQ(maj,min) \
          ((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#   else
#       define __CLANG_PREREQ(maj,min) (0)
#   endif
#endif /* __CLANG_PREREQ */

#ifndef __GLIBC_PREREQ
#   if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#       define __GLIBC_PREREQ(maj, min) \
          ((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#   else
#       define __GLIBC_PREREQ(maj, min) (0)
#   endif
#endif /* __GLIBC_PREREQ */

#ifndef __has_warning
#   define __has_warning(x) (0)
#endif

#ifndef __has_include
#   define __has_include(x) (0)
#endif

#if __has_feature(thread_sanitizer)
#   define __SANITIZE_THREAD__ 1
#endif

#if __has_feature(address_sanitizer)
#   define __SANITIZE_ADDRESS__ 1
#endif

/*----------------------------------------------------------------------------*/

#ifndef __extern_C
#   ifdef __cplusplus
#       define __extern_C extern "C"
#   else
#       define __extern_C
#   endif
#endif /* __extern_C */

#if !defined(nullptr) && !defined(__cplusplus) || (__cplusplus < 201103L && !defined(_MSC_VER))
#   define nullptr NULL
#endif

/*----------------------------------------------------------------------------*/

#ifndef __always_inline
#   if defined(__GNUC__) || __has_attribute(__always_inline__)
#       define __always_inline __inline __attribute__((__always_inline__))
#   elif defined(_MSC_VER)
#       define __always_inline __forceinline
#   else
#       define __always_inline
#   endif
#endif /* __always_inline */

#ifndef __noinline
#   if defined(__GNUC__) || __has_attribute(__noinline__)
#       define __noinline __attribute__((__noinline__))
#   elif defined(_MSC_VER)
#       define __noinline __declspec(noinline)
#   else
#       define __noinline
#   endif
#endif /* __noinline */

#ifndef __must_check_result
#   if defined(__GNUC__) || __has_attribute(__warn_unused_result__)
#       define __must_check_result __attribute__((__warn_unused_result__))
#   else
#       define __must_check_result
#   endif
#endif /* __must_check_result */

#ifndef __maybe_unused
#   if defined(__GNUC__) || __has_attribute(__unused__)
#       define __maybe_unused __attribute__((__unused__))
#   else
#       define __maybe_unused
#   endif
#endif /* __maybe_unused */

#if !defined(__noop) && !defined(_MSC_VER)
#   define __noop(...) do {} while(0)
#endif /* __noop */

#ifndef __fallthrough
#  if defined(__cplusplus) && (__has_cpp_attribute(fallthrough) &&             \
     (!defined(__clang__) || __clang__ > 4)) || __cplusplus >= 201703L
#    define __fallthrough [[fallthrough]]
#  elif __GNUC_PREREQ(8, 0) && defined(__cplusplus) && __cplusplus >= 201103L
#    define __fallthrough [[fallthrough]]
#  elif __GNUC_PREREQ(7, 0) &&                                                 \
    (!defined(__LCC__) || (__LCC__ == 124 && __LCC_MINOR__ >= 12) ||           \
     (__LCC__ == 125 && __LCC_MINOR__ >= 5) || (__LCC__ >= 126))
#    define __fallthrough __attribute__((__fallthrough__))
#  elif defined(__clang__) && defined(__cplusplus) && __cplusplus >= 201103L &&\
    __has_feature(cxx_attributes) && __has_warning("-Wimplicit-fallthrough")
#    define __fallthrough [[clang::fallthrough]]
#  else
#    define __fallthrough
#  endif
#endif /* __fallthrough */

#ifndef __unreachable
#   if __GNUC_PREREQ(4,5) || __has_builtin(__builtin_unreachable)
#       define __unreachable() __builtin_unreachable()
#   elif defined(_MSC_VER)
#       define __unreachable() __assume(0)
#   else
#       define __unreachable() __noop()
#   endif
#endif /* __unreachable */

#ifndef __prefetch
#   if defined(__GNUC__) || defined(__clang__) || __has_builtin(__builtin_prefetch)
#       define __prefetch(ptr) __builtin_prefetch(ptr)
#   else
#       define __prefetch(ptr) __noop(ptr)
#   endif
#endif /* __prefetch */

#ifndef __nothrow
#   if defined(__cplusplus)
#       if __cplusplus < 201703L
#           define __nothrow throw()
#       else
#           define __nothrow noexcept(true)
#       endif /* __cplusplus */
#   elif defined(__GNUC__) || __has_attribute(__nothrow__)
#       define __nothrow __attribute__((__nothrow__))
#   elif defined(_MSC_VER) && defined(__cplusplus)
#       define __nothrow __declspec(nothrow)
#   else
#       define __nothrow
#   endif
#endif /* __nothrow */

#ifndef __hidden
#   if defined(__GNUC__) || __has_attribute(__visibility__)
#       define __hidden __attribute__((__visibility__("hidden")))
#   else
#       define __hidden
#   endif
#endif /* __hidden */

#ifndef __optimize
#   if defined(__OPTIMIZE__)
#       if (defined(__GNUC__) && !defined(__clang__)) || __has_attribute(__optimize__)
#           define __optimize(ops) __attribute__((__optimize__(ops)))
#       else
#           define __optimize(ops)
#       endif
#   else
#       define __optimize(ops)
#   endif
#endif /* __optimize */

#ifndef __hot
#   if defined(__OPTIMIZE__)
#       if defined(__e2k__)
#           define __hot __attribute__((__hot__)) __optimize(3)
#       elif defined(__clang__) && !__has_attribute(__hot_) \
        && __has_attribute(__section__) && (defined(__linux__) || defined(__gnu_linux__))
            /* just put frequently used functions in separate section */
#           define __hot __attribute__((__section__("text.hot"))) __optimize("O3")
#       elif defined(__GNUC__) || __has_attribute(__hot__)
#           define __hot __attribute__((__hot__)) __optimize("O3")
#       else
#           define __hot  __optimize("O3")
#       endif
#   else
#       define __hot
#   endif
#endif /* __hot */

#ifndef __cold
#   if defined(__OPTIMIZE__)
#       if defined(__e2k__)
#           define __cold __attribute__((__cold__)) __optimize(1)
#       elif defined(__clang__) && !__has_attribute(cold) \
        && __has_attribute(__section__) && (defined(__linux__) || defined(__gnu_linux__))
            /* just put infrequently used functions in separate section */
#           define __cold __attribute__((__section__("text.unlikely"))) __optimize("Os")
#       elif defined(__GNUC__) || __has_attribute(cold)
#           define __cold __attribute__((__cold__)) __optimize("Os")
#       else
#           define __cold __optimize("Os")
#       endif
#   else
#       define __cold
#   endif
#endif /* __cold */

#ifndef __flatten
#   if defined(__OPTIMIZE__) && (defined(__GNUC__) || __has_attribute(__flatten__))
#       define __flatten __attribute__((__flatten__))
#   else
#       define __flatten
#   endif
#endif /* __flatten */

#ifndef likely
#   if (defined(__GNUC__) || __has_builtin(__builtin_expect)) && !defined(__COVERITY__)
#       define likely(cond) __builtin_expect(!!(cond), 1)
#   else
#       define likely(x) (!!(x))
#   endif
#endif /* likely */

#ifndef unlikely
#   if (defined(__GNUC__) || __has_builtin(__builtin_expect)) && !defined(__COVERITY__)
#       define unlikely(cond) __builtin_expect(!!(cond), 0)
#   else
#       define unlikely(x) (!!(x))
#   endif
#endif /* unlikely */

#ifndef __anonymous_struct_extension__
#   if defined(__GNUC__)
#       define __anonymous_struct_extension__ __extension__
#   else
#       define __anonymous_struct_extension__
#   endif
#endif /* __anonymous_struct_extension__ */

#ifndef __Wpedantic_format_voidptr
    static __inline __maybe_unused const void* MDBX_PURE_FUNCTION
        __Wpedantic_format_voidptr(const void* ptr) {return ptr;}
#   define __Wpedantic_format_voidptr(ARG) __Wpedantic_format_voidptr(ARG)
#endif /* __Wpedantic_format_voidptr */

/*----------------------------------------------------------------------------*/

#if defined(MDBX_USE_VALGRIND)
#   include <valgrind/memcheck.h>
#   ifndef VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE
        /* LY: available since Valgrind 3.10 */
#       define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#       define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   endif
#elif !defined(RUNNING_ON_VALGRIND)
#   define VALGRIND_CREATE_MEMPOOL(h,r,z)
#   define VALGRIND_DESTROY_MEMPOOL(h)
#   define VALGRIND_MEMPOOL_TRIM(h,a,s)
#   define VALGRIND_MEMPOOL_ALLOC(h,a,s)
#   define VALGRIND_MEMPOOL_FREE(h,a)
#   define VALGRIND_MEMPOOL_CHANGE(h,a,b,s)
#   define VALGRIND_MAKE_MEM_NOACCESS(a,s)
#   define VALGRIND_MAKE_MEM_DEFINED(a,s)
#   define VALGRIND_MAKE_MEM_UNDEFINED(a,s)
#   define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   define VALGRIND_CHECK_MEM_IS_ADDRESSABLE(a,s) (0)
#   define VALGRIND_CHECK_MEM_IS_DEFINED(a,s) (0)
#   define RUNNING_ON_VALGRIND (0)
#endif /* MDBX_USE_VALGRIND */

#ifdef __SANITIZE_ADDRESS__
#   include <sanitizer/asan_interface.h>
#elif !defined(ASAN_POISON_MEMORY_REGION)
#   define ASAN_POISON_MEMORY_REGION(addr, size) \
        ((void)(addr), (void)(size))
#   define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
        ((void)(addr), (void)(size))
#endif /* __SANITIZE_ADDRESS__ */

/*----------------------------------------------------------------------------*/

#ifndef ARRAY_LENGTH
#   ifdef __cplusplus
        template <typename T, size_t N>
        char (&__ArraySizeHelper(T (&array)[N]))[N];
#       define ARRAY_LENGTH(array) (sizeof(::__ArraySizeHelper(array)))
#   else
#       define ARRAY_LENGTH(array) (sizeof(array) / sizeof(array[0]))
#   endif
#endif /* ARRAY_LENGTH */

#ifndef ARRAY_END
#   define ARRAY_END(array) (&array[ARRAY_LENGTH(array)])
#endif /* ARRAY_END */

#ifndef STRINGIFY
#   define STRINGIFY_HELPER(x) #x
#   define STRINGIFY(x) STRINGIFY_HELPER(x)
#endif /* STRINGIFY */

#define CONCAT(a,b) a##b
#define XCONCAT(a,b) CONCAT(a,b)

#ifndef offsetof
#   define offsetof(type, member)  __builtin_offsetof(type, member)
#endif /* offsetof */

#ifndef container_of
#   define container_of(ptr, type, member) \
        ((type *)((char *)(ptr) - offsetof(type, member)))
#endif /* container_of */

#define MDBX_TETRAD(a, b, c, d)                                                \
  ((uint32_t)(a) << 24 | (uint32_t)(b) << 16 | (uint32_t)(c) << 8 | (d))

#define MDBX_STRING_TETRAD(str) MDBX_TETRAD(str[0], str[1], str[2], str[3])

#define FIXME "FIXME: " __FILE__ ", " STRINGIFY(__LINE__)

#ifndef STATIC_ASSERT_MSG
#   if defined(static_assert)
#       define STATIC_ASSERT_MSG(expr, msg) static_assert(expr, msg)
#   elif defined(_STATIC_ASSERT)
#       define STATIC_ASSERT_MSG(expr, msg) _STATIC_ASSERT(expr)
#   elif defined(_MSC_VER)
#       include <crtdbg.h>
#       define STATIC_ASSERT_MSG(expr, msg) _STATIC_ASSERT(expr)
#   elif (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L) \
          || __has_feature(c_static_assert)
#       define STATIC_ASSERT_MSG(expr, msg) _Static_assert(expr, msg)
#   else
#       define STATIC_ASSERT_MSG(expr, msg) switch (0) {case 0:case (expr):;}
#   endif
#endif /* STATIC_ASSERT */

#ifndef STATIC_ASSERT
#   define STATIC_ASSERT(expr) STATIC_ASSERT_MSG(expr, #expr)
#endif

/* *INDENT-ON* */
/* clang-format on */

#if defined(__GNUC__) && !__GNUC_PREREQ(4,2)
    /* Actually libmdbx was not tested with compilers older than GCC 4.2.
     * But you could ignore this warning at your own risk.
     * In such case please don't rise up an issues related ONLY to old compilers.
     */
#   warning "libmdbx required GCC >= 4.2"
#endif

#if defined(__clang__) && !__CLANG_PREREQ(3,8)
    /* Actually libmdbx was not tested with CLANG older than 3.8.
     * But you could ignore this warning at your own risk.
     * In such case please don't rise up an issues related ONLY to old compilers.
     */
#   warning "libmdbx required CLANG >= 3.8"
#endif

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2,12)
    /* Actually libmdbx was not tested with something older than glibc 2.12.
     * But you could ignore this warning at your own risk.
     * In such case please don't rise up an issues related ONLY to old systems.
     */
#   warning "libmdbx was only tested with GLIBC >= 2.12."
#endif

#ifdef __SANITIZE_THREAD__
#   warning "libmdbx don't compatible with ThreadSanitizer, you will get a lot of false-positive issues."
#endif /* __SANITIZE_THREAD__ */

#if __has_warning("-Wnested-anon-types")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Wnested-anon-types"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Wnested-anon-types"
#   else
#      pragma warning disable "nested-anon-types"
#   endif
#endif /* -Wnested-anon-types */

#if __has_warning("-Wconstant-logical-operand")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Wconstant-logical-operand"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Wconstant-logical-operand"
#   else
#      pragma warning disable "constant-logical-operand"
#   endif
#endif /* -Wconstant-logical-operand */

#if defined(__LCC__) && (__LCC__ <= 121)
    /* bug #2798 */
#   pragma diag_suppress alignment_reduction_ignored
#elif defined(__ICC)
#   pragma warning(disable: 3453 1366)
#elif __has_warning("-Walignment-reduction-ignored")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Walignment-reduction-ignored"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Walignment-reduction-ignored"
#   else
#       pragma warning disable "alignment-reduction-ignored"
#   endif
#endif /* -Walignment-reduction-ignored */

/* *INDENT-ON* */
/* clang-format on */

#ifdef __cplusplus
extern "C" {
#endif

/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

/*
 * Copyright 2015-2021 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */


/*----------------------------------------------------------------------------*/
/* Microsoft compiler generates a lot of warning for self includes... */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                 * mode specified; termination on exception is \
                                 * not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#if defined(_WIN32) || defined(_WIN64)
#if !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif
#if !defined(_NO_CRT_STDIO_INLINE) && MDBX_BUILD_SHARED_LIBRARY &&             \
    !defined(MDBX_TOOLS) && MDBX_AVOID_CRT
#define _NO_CRT_STDIO_INLINE
#endif
#elif !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif /* Windows */

/*----------------------------------------------------------------------------*/
/* C99 includes */
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* C11 stdalign.h */
#if __has_include(<stdalign.h>)
#include <stdalign.h>
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define alignas(N) _Alignas(N)
#elif defined(_MSC_VER)
#define alignas(N) __declspec(align(N))
#elif __has_attribute(__aligned__) || defined(__GNUC__)
#define alignas(N) __attribute__((__aligned__(N)))
#else
#error "FIXME: Required _alignas() or equivalent."
#endif

/*----------------------------------------------------------------------------*/
/* Systems includes */

#ifdef __APPLE__
#include <TargetConditionals.h>
#endif /* Apple OSX & iOS */

#if defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||     \
    defined(__BSD__) || defined(__bsdi__) || defined(__DragonFly__) ||         \
    defined(__APPLE__) || defined(__MACH__)
#include <sys/cdefs.h>
#include <sys/mount.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#if defined(__FreeBSD__) || defined(__DragonFly__)
#include <vm/vm_param.h>
#elif defined(__OpenBSD__) || defined(__NetBSD__)
#include <uvm/uvm_param.h>
#else
#define SYSCTL_LEGACY_NONCONST_MIB
#endif
#ifndef __MACH__
#include <sys/vmmeter.h>
#endif
#else
#include <malloc.h>
#if !(defined(__sun) || defined(__SVR4) || defined(__svr4__) ||                \
      defined(_WIN32) || defined(_WIN64))
#include <mntent.h>
#endif /* !Solaris */
#endif /* !xBSD */

#if defined(__FreeBSD__) || __has_include(<malloc_np.h>)
#include <malloc_np.h>
#endif

#if defined(__APPLE__) || defined(__MACH__) || __has_include(<malloc/malloc.h>)
#include <malloc/malloc.h>
#endif /* MacOS */

#if defined(__MACH__)
#include <mach/host_info.h>
#include <mach/mach_host.h>
#include <mach/mach_port.h>
#include <uuid/uuid.h>
#undef P_DIRTY
#endif

#if defined(__linux__) || defined(__gnu_linux__)
#include <linux/sysctl.h>
#include <sched.h>
#include <sys/sendfile.h>
#include <sys/statfs.h>
#endif /* Linux */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 0
#endif

#ifndef _XOPEN_SOURCE_EXTENDED
#define _XOPEN_SOURCE_EXTENDED 0
#else
#include <utmpx.h>
#endif /* _XOPEN_SOURCE_EXTENDED */

#if defined(__sun) || defined(__SVR4) || defined(__svr4__)
#include <kstat.h>
#include <sys/mnttab.h>
/* On Solaris, it's easier to add a missing prototype rather than find a
 * combination of #defines that break nothing. */
__extern_C key_t ftok(const char *, int);
#endif /* SunOS/Solaris */

#if defined(_WIN32) || defined(_WIN64)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <excpt.h>
#include <tlhelp32.h>
#include <windows.h>
#include <winnt.h>
#include <winternl.h>
#define HAVE_SYS_STAT_H
#define HAVE_SYS_TYPES_H
typedef HANDLE mdbx_thread_t;
typedef unsigned mdbx_thread_key_t;
#define MDBX_OSAL_SECTION HANDLE
#define MAP_FAILED NULL
#define HIGH_DWORD(v) ((DWORD)((sizeof(v) > 4) ? ((uint64_t)(v) >> 32) : 0))
#define THREAD_CALL WINAPI
#define THREAD_RESULT DWORD
typedef struct {
  HANDLE mutex;
  HANDLE event[2];
} mdbx_condpair_t;
typedef CRITICAL_SECTION mdbx_fastmutex_t;

#if MDBX_AVOID_CRT
#ifndef mdbx_malloc
static inline void *mdbx_malloc(size_t bytes) {
  return LocalAlloc(LMEM_FIXED, bytes);
}
#endif /* mdbx_malloc */

#ifndef mdbx_calloc
static inline void *mdbx_calloc(size_t nelem, size_t size) {
  return LocalAlloc(LMEM_FIXED | LMEM_ZEROINIT, nelem * size);
}
#endif /* mdbx_calloc */

#ifndef mdbx_realloc
static inline void *mdbx_realloc(void *ptr, size_t bytes) {
  return ptr ? LocalReAlloc(ptr, bytes, LMEM_MOVEABLE)
             : LocalAlloc(LMEM_FIXED, bytes);
}
#endif /* mdbx_realloc */

#ifndef mdbx_free
#define mdbx_free LocalFree
#endif /* mdbx_free */
#else
#define mdbx_malloc malloc
#define mdbx_calloc calloc
#define mdbx_realloc realloc
#define mdbx_free free
#define mdbx_strdup _strdup
#endif /* MDBX_AVOID_CRT */

#ifndef snprintf
#define snprintf _snprintf /* ntdll */
#endif

#ifndef vsnprintf
#define vsnprintf _vsnprintf /* ntdll */
#endif

#else /*----------------------------------------------------------------------*/

#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <unistd.h>
typedef pthread_t mdbx_thread_t;
typedef pthread_key_t mdbx_thread_key_t;
#define INVALID_HANDLE_VALUE (-1)
#define THREAD_CALL
#define THREAD_RESULT void *
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond[2];
} mdbx_condpair_t;
typedef pthread_mutex_t mdbx_fastmutex_t;
#define mdbx_malloc malloc
#define mdbx_calloc calloc
#define mdbx_realloc realloc
#define mdbx_free free
#define mdbx_strdup strdup
#endif /* Platform */

#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
/* malloc_usable_size() already provided */
#elif defined(__APPLE__)
#define malloc_usable_size(ptr) malloc_size(ptr)
#elif defined(_MSC_VER) && !MDBX_AVOID_CRT
#define malloc_usable_size(ptr) _msize(ptr)
#endif /* malloc_usable_size */

#ifdef __ANDROID_API__
#include <android/log.h>
#if __ANDROID_API__ >= 21
#include <sys/sendfile.h>
#endif
#endif /* Android */

/* *INDENT-OFF* */
/* clang-format off */
#if defined(HAVE_SYS_STAT_H) || __has_include(<sys/stat.h>)
#include <sys/stat.h>
#endif
#if defined(HAVE_SYS_TYPES_H) || __has_include(<sys/types.h>)
#include <sys/types.h>
#endif
#if defined(HAVE_SYS_FILE_H) || __has_include(<sys/file.h>)
#include <sys/file.h>
#endif
/* *INDENT-ON* */
/* clang-format on */

#ifndef SSIZE_MAX
#define SSIZE_MAX INTPTR_MAX
#endif

#if !defined(MADV_DODUMP) && defined(MADV_CORE)
#define MADV_DODUMP MADV_CORE
#endif /* MADV_CORE -> MADV_DODUMP */

#if !defined(MADV_DONTDUMP) && defined(MADV_NOCORE)
#define MADV_DONTDUMP MADV_NOCORE
#endif /* MADV_NOCORE -> MADV_DONTDUMP */

#if defined(i386) || defined(__386) || defined(__i386) || defined(__i386__) || \
    defined(i486) || defined(__i486) || defined(__i486__) ||                   \
    defined(i586) | defined(__i586) || defined(__i586__) || defined(i686) ||   \
    defined(__i686) || defined(__i686__) || defined(_M_IX86) ||                \
    defined(_X86_) || defined(__THW_INTEL__) || defined(__I86__) ||            \
    defined(__INTEL__) || defined(__x86_64) || defined(__x86_64__) ||          \
    defined(__amd64__) || defined(__amd64) || defined(_M_X64) ||               \
    defined(_M_AMD64) || defined(__IA32__) || defined(__INTEL__)
#ifndef __ia32__
/* LY: define neutral __ia32__ for x86 and x86-64 */
#define __ia32__ 1
#endif /* __ia32__ */
#if !defined(__amd64__) && (defined(__x86_64) || defined(__x86_64__) ||        \
                            defined(__amd64) || defined(_M_X64))
/* LY: define trusty __amd64__ for all AMD64/x86-64 arch */
#define __amd64__ 1
#endif /* __amd64__ */
#endif /* all x86 */

#if (-6 & 5) || CHAR_BIT != 8 || UINT_MAX < 0xffffffff || ULONG_MAX % 0xFFFF
#error                                                                         \
    "Sanity checking failed: Two's complement, reasonably sized integer types"
#endif

#if UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul
#define MDBX_WORDBITS 64
#else
#define MDBX_WORDBITS 32
#endif /* MDBX_WORDBITS */

/*----------------------------------------------------------------------------*/
/* Compiler's includes for builtins/intrinsics */

#if defined(_MSC_VER) || defined(__INTEL_COMPILER)
#include <intrin.h>
#elif __GNUC_PREREQ(4, 4) || defined(__clang__)
#if defined(__ia32__) || defined(__e2k__)
#include <x86intrin.h>
#endif /* __ia32__ */
#if defined(__ia32__)
#include <cpuid.h>
#endif /* __ia32__ */
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#include <mbarrier.h>
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
#include <machine/sys/inline.h>
#elif defined(__IBMC__) && defined(__powerpc)
#include <atomic.h>
#elif defined(_AIX)
#include <builtins.h>
#include <sys/atomic_op.h>
#elif (defined(__osf__) && defined(__DECC)) || defined(__alpha)
#include <c_asm.h>
#include <machine/builtins.h>
#elif defined(__MWERKS__)
/* CodeWarrior - troubles ? */
#pragma gcc_extensions
#elif defined(__SNC__)
/* Sony PS3 - troubles ? */
#elif defined(__hppa__) || defined(__hppa)
#include <machine/inline.h>
#else
#error Unsupported C compiler, please use GNU C 4.4 or newer
#endif /* Compiler */

/*----------------------------------------------------------------------------*/
/* Byteorder */

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)

/* *INDENT-OFF* */
/* clang-format off */
#if defined(__GLIBC__) || defined(__GNU_LIBRARY__) || defined(__ANDROID_API__) ||  \
    defined(HAVE_ENDIAN_H) || __has_include(<endian.h>)
#include <endian.h>
#elif defined(__APPLE__) || defined(__MACH__) || defined(__OpenBSD__) ||       \
    defined(HAVE_MACHINE_ENDIAN_H) || __has_include(<machine/endian.h>)
#include <machine/endian.h>
#elif defined(HAVE_SYS_ISA_DEFS_H) || __has_include(<sys/isa_defs.h>)
#include <sys/isa_defs.h>
#elif (defined(HAVE_SYS_TYPES_H) && defined(HAVE_SYS_ENDIAN_H)) ||             \
    (__has_include(<sys/types.h>) && __has_include(<sys/endian.h>))
#include <sys/endian.h>
#include <sys/types.h>
#elif defined(__bsdi__) || defined(__DragonFly__) || defined(__FreeBSD__) ||   \
    defined(__NetBSD__) ||                              \
    defined(HAVE_SYS_PARAM_H) || __has_include(<sys/param.h>)
#include <sys/param.h>
#endif /* OS */
/* *INDENT-ON* */
/* clang-format on */

#if defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ __LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ __BIG_ENDIAN
#define __BYTE_ORDER__ __BYTE_ORDER
#elif defined(_BYTE_ORDER) && defined(_LITTLE_ENDIAN) && defined(_BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ _LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ _BIG_ENDIAN
#define __BYTE_ORDER__ _BYTE_ORDER
#else
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321

#if defined(__LITTLE_ENDIAN__) ||                                              \
    (defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN)) ||                      \
    defined(__ARMEL__) || defined(__THUMBEL__) || defined(__AARCH64EL__) ||    \
    defined(__MIPSEL__) || defined(_MIPSEL) || defined(__MIPSEL) ||            \
    defined(_M_ARM) || defined(_M_ARM64) || defined(__e2k__) ||                \
    defined(__elbrus_4c__) || defined(__elbrus_8c__) || defined(__bfin__) ||   \
    defined(__BFIN__) || defined(__ia64__) || defined(_IA64) ||                \
    defined(__IA64__) || defined(__ia64) || defined(_M_IA64) ||                \
    defined(__itanium__) || defined(__ia32__) || defined(__CYGWIN__) ||        \
    defined(_WIN64) || defined(_WIN32) || defined(__TOS_WIN__) ||              \
    defined(__WINDOWS__)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__

#elif defined(__BIG_ENDIAN__) ||                                               \
    (defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN)) ||                      \
    defined(__ARMEB__) || defined(__THUMBEB__) || defined(__AARCH64EB__) ||    \
    defined(__MIPSEB__) || defined(_MIPSEB) || defined(__MIPSEB) ||            \
    defined(__m68k__) || defined(M68000) || defined(__hppa__) ||               \
    defined(__hppa) || defined(__HPPA__) || defined(__sparc__) ||              \
    defined(__sparc) || defined(__370__) || defined(__THW_370__) ||            \
    defined(__s390__) || defined(__s390x__) || defined(__SYSC_ZARCH__)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__

#else
#error __BYTE_ORDER__ should be defined.
#endif /* Arch */

#endif
#endif /* __BYTE_ORDER__ || __ORDER_LITTLE_ENDIAN__ || __ORDER_BIG_ENDIAN__ */

/* Get the size of a memory page for the system.
 * This is the basic size that the platform's memory manager uses, and is
 * fundamental to the use of memory-mapped files. */
MDBX_NOTHROW_CONST_FUNCTION static __maybe_unused __inline size_t
mdbx_syspagesize(void) {
#if defined(_WIN32) || defined(_WIN64)
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  return si.dwPageSize;
#else
  return sysconf(_SC_PAGE_SIZE);
#endif
}

typedef struct mdbx_mmap_param {
  union {
    void *address;
    uint8_t *dxb;
    struct MDBX_lockinfo *lck;
  };
  mdbx_filehandle_t fd;
  size_t limit;   /* mapping length, but NOT a size of file nor DB */
  size_t current; /* mapped region size, i.e. the size of file and DB */
#if defined(_WIN32) || defined(_WIN64)
  uint64_t filesize /* in-process cache of a file size. */;
#endif
#ifdef MDBX_OSAL_SECTION
  MDBX_OSAL_SECTION section;
#endif
} mdbx_mmap_t;

typedef union bin128 {
  __anonymous_struct_extension__ struct { uint64_t x, y; };
  __anonymous_struct_extension__ struct { uint32_t a, b, c, d; };
} bin128_t;

#if defined(_WIN32) || defined(_WIN64)
typedef union MDBX_srwlock {
  struct {
    long volatile readerCount;
    long volatile writerCount;
  };
  RTL_SRWLOCK native;
} MDBX_srwlock;
#endif /* Windows */

#ifdef __cplusplus
extern void mdbx_osal_jitter(bool tiny);
#else

/*----------------------------------------------------------------------------*/
/* Atomics */

#if defined(__cplusplus) && !defined(__STDC_NO_ATOMICS__) && (__has_include(<cstdatomic>) || __has_extension(cxx_atomic))
#include <cstdatomic>
#define MDBX_HAVE_C11ATOMICS
#elif !defined(__cplusplus) &&                                                 \
    (__STDC_VERSION__ >= 201112L || __has_extension(c_atomic)) &&              \
    !defined(__STDC_NO_ATOMICS__) &&                                           \
    (__GNUC_PREREQ(4, 9) || __CLANG_PREREQ(3, 8) ||                            \
     !(defined(__GNUC__) || defined(__clang__)))
#include <stdatomic.h>
#define MDBX_HAVE_C11ATOMICS
#elif defined(__GNUC__) || defined(__clang__)
#elif defined(_MSC_VER)
#pragma warning(disable : 4163) /* 'xyz': not available as an intrinsic */
#pragma warning(disable : 4133) /* 'function': incompatible types - from       \
                                   'size_t' to 'LONGLONG' */
#pragma warning(disable : 4244) /* 'return': conversion from 'LONGLONG' to     \
                                   'std::size_t', possible loss of data */
#pragma warning(disable : 4267) /* 'function': conversion from 'size_t' to     \
                                   'long', possible loss of data */
#pragma intrinsic(_InterlockedExchangeAdd, _InterlockedCompareExchange)
#pragma intrinsic(_InterlockedExchangeAdd64, _InterlockedCompareExchange64)
#elif defined(__APPLE__)
#include <libkern/OSAtomic.h>
#else
#error FIXME atomic-ops
#endif

/*----------------------------------------------------------------------------*/
/* Memory/Compiler barriers, cache coherence */

#if __has_include(<sys/cachectl.h>)
#include <sys/cachectl.h>
#elif defined(__mips) || defined(__mips__) || defined(__mips64) ||             \
    defined(__mips64__) || defined(_M_MRX000) || defined(_MIPS_) ||            \
    defined(__MWERKS__) || defined(__sgi)
/* MIPS should have explicit cache control */
#include <sys/cachectl.h>
#endif

static __maybe_unused __inline void mdbx_compiler_barrier(void) {
#if defined(__clang__) || defined(__GNUC__)
  __asm__ __volatile__("" ::: "memory");
#elif defined(_MSC_VER)
  _ReadWriteBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
  __memory_barrier();
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __compiler_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
  _Asm_sched_fence(/* LY: no-arg meaning 'all expect ALU', e.g. 0x3D3D */);
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) ||             \
    defined(__ppc64__) || defined(__powerpc64__)
  __fence();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

static __maybe_unused __inline void mdbx_memory_barrier(void) {
#ifdef MDBX_HAVE_C11ATOMICS
  atomic_thread_fence(memory_order_seq_cst);
#elif defined(__ATOMIC_SEQ_CST)
#ifdef __clang__
  __c11_atomic_thread_fence(__ATOMIC_SEQ_CST);
#else
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
#endif
#elif defined(__clang__) || defined(__GNUC__)
  __sync_synchronize();
#elif defined(_WIN32) || defined(_WIN64)
  MemoryBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
#if defined(__ia32__)
  _mm_mfence();
#else
  __mf();
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __machine_rw_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
  _Asm_mf();
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) ||             \
    defined(__ppc64__) || defined(__powerpc64__)
  __lwsync();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

/*----------------------------------------------------------------------------*/
/* libc compatibility stuff */

#if (!defined(__GLIBC__) && __GLIBC_PREREQ(2, 1)) &&                           \
    (defined(_GNU_SOURCE) || defined(_BSD_SOURCE))
#define mdbx_asprintf asprintf
#define mdbx_vasprintf vasprintf
#else
MDBX_INTERNAL_FUNC MDBX_PRINTF_ARGS(2, 3) int __maybe_unused
    mdbx_asprintf(char **strp, const char *fmt, ...);
MDBX_INTERNAL_FUNC int mdbx_vasprintf(char **strp, const char *fmt, va_list ap);
#endif

/*----------------------------------------------------------------------------*/
/* OS abstraction layer stuff */

/* max bytes to write in one call */
#if defined(_WIN32) || defined(_WIN64)
#define MAX_WRITE UINT32_C(0x01000000)
#else
#define MAX_WRITE UINT32_C(0x3fff0000)
#endif

#if defined(__linux__) || defined(__gnu_linux__)
MDBX_INTERNAL_VAR uint32_t mdbx_linux_kernel_version;
MDBX_INTERNAL_VAR bool mdbx_RunningOnWSL1 /* Windows Subsystem 1 for Linux */;
#endif /* Linux */

#ifndef mdbx_strdup
LIBMDBX_API char *mdbx_strdup(const char *str);
#endif

static __maybe_unused __inline int mdbx_get_errno(void) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD rc = GetLastError();
#else
  int rc = errno;
#endif
  return rc;
}

#ifndef mdbx_memalign_alloc
MDBX_INTERNAL_FUNC int mdbx_memalign_alloc(size_t alignment, size_t bytes,
                                           void **result);
#endif
#ifndef mdbx_memalign_free
MDBX_INTERNAL_FUNC void mdbx_memalign_free(void *ptr);
#endif

MDBX_INTERNAL_FUNC int mdbx_condpair_init(mdbx_condpair_t *condpair);
MDBX_INTERNAL_FUNC int mdbx_condpair_lock(mdbx_condpair_t *condpair);
MDBX_INTERNAL_FUNC int mdbx_condpair_unlock(mdbx_condpair_t *condpair);
MDBX_INTERNAL_FUNC int mdbx_condpair_signal(mdbx_condpair_t *condpair,
                                            bool part);
MDBX_INTERNAL_FUNC int mdbx_condpair_wait(mdbx_condpair_t *condpair, bool part);
MDBX_INTERNAL_FUNC int mdbx_condpair_destroy(mdbx_condpair_t *condpair);

MDBX_INTERNAL_FUNC int mdbx_fastmutex_init(mdbx_fastmutex_t *fastmutex);
MDBX_INTERNAL_FUNC int mdbx_fastmutex_acquire(mdbx_fastmutex_t *fastmutex);
MDBX_INTERNAL_FUNC int mdbx_fastmutex_release(mdbx_fastmutex_t *fastmutex);
MDBX_INTERNAL_FUNC int mdbx_fastmutex_destroy(mdbx_fastmutex_t *fastmutex);

MDBX_INTERNAL_FUNC int mdbx_pwritev(mdbx_filehandle_t fd, struct iovec *iov,
                                    int iovcnt, uint64_t offset,
                                    size_t expected_written);
MDBX_INTERNAL_FUNC int mdbx_pread(mdbx_filehandle_t fd, void *buf, size_t count,
                                  uint64_t offset);
MDBX_INTERNAL_FUNC int mdbx_pwrite(mdbx_filehandle_t fd, const void *buf,
                                   size_t count, uint64_t offset);
MDBX_INTERNAL_FUNC int mdbx_write(mdbx_filehandle_t fd, const void *buf,
                                  size_t count);

MDBX_INTERNAL_FUNC int
mdbx_thread_create(mdbx_thread_t *thread,
                   THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                   void *arg);
MDBX_INTERNAL_FUNC int mdbx_thread_join(mdbx_thread_t thread);

enum mdbx_syncmode_bits {
  MDBX_SYNC_NONE = 0,
  MDBX_SYNC_DATA = 1,
  MDBX_SYNC_SIZE = 2,
  MDBX_SYNC_IODQ = 4
};

MDBX_INTERNAL_FUNC int mdbx_fsync(mdbx_filehandle_t fd,
                                  const enum mdbx_syncmode_bits mode_bits);
MDBX_INTERNAL_FUNC int mdbx_ftruncate(mdbx_filehandle_t fd, uint64_t length);
MDBX_INTERNAL_FUNC int mdbx_fseek(mdbx_filehandle_t fd, uint64_t pos);
MDBX_INTERNAL_FUNC int mdbx_filesize(mdbx_filehandle_t fd, uint64_t *length);

enum mdbx_openfile_purpose {
  MDBX_OPEN_DXB_READ = 0,
  MDBX_OPEN_DXB_LAZY = 1,
  MDBX_OPEN_DXB_DSYNC = 2,
  MDBX_OPEN_LCK = 3,
  MDBX_OPEN_COPY = 4,
  MDBX_OPEN_DELETE = 5
};

MDBX_INTERNAL_FUNC int mdbx_openfile(const enum mdbx_openfile_purpose purpose,
                                     const MDBX_env *env, const char *pathname,
                                     mdbx_filehandle_t *fd,
                                     mdbx_mode_t unix_mode_bits);
MDBX_INTERNAL_FUNC int mdbx_closefile(mdbx_filehandle_t fd);
MDBX_INTERNAL_FUNC int mdbx_removefile(const char *pathname);
MDBX_INTERNAL_FUNC int mdbx_removedirectory(const char *pathname);
MDBX_INTERNAL_FUNC int mdbx_is_pipe(mdbx_filehandle_t fd);
MDBX_INTERNAL_FUNC int mdbx_lockfile(mdbx_filehandle_t fd, bool wait);

#define MMAP_OPTION_TRUNCATE 1
#define MMAP_OPTION_SEMAPHORE 2
MDBX_INTERNAL_FUNC int mdbx_mmap(const int flags, mdbx_mmap_t *map,
                                 const size_t must, const size_t limit,
                                 const unsigned options);
MDBX_INTERNAL_FUNC int mdbx_munmap(mdbx_mmap_t *map);
MDBX_INTERNAL_FUNC int mdbx_mresize(int flags, mdbx_mmap_t *map, size_t current,
                                    size_t wanna, const bool may_move);
#if defined(_WIN32) || defined(_WIN64)
typedef struct {
  unsigned limit, count;
  HANDLE handles[31];
} mdbx_handle_array_t;
MDBX_INTERNAL_FUNC int
mdbx_suspend_threads_before_remap(MDBX_env *env, mdbx_handle_array_t **array);
MDBX_INTERNAL_FUNC int
mdbx_resume_threads_after_remap(mdbx_handle_array_t *array);
#endif /* Windows */
MDBX_INTERNAL_FUNC int mdbx_msync(mdbx_mmap_t *map, size_t offset,
                                  size_t length,
                                  enum mdbx_syncmode_bits mode_bits);
MDBX_INTERNAL_FUNC int mdbx_check_fs_rdonly(mdbx_filehandle_t handle,
                                            const char *pathname, int err);

static __maybe_unused __inline uint32_t mdbx_getpid(void) {
  STATIC_ASSERT(sizeof(mdbx_pid_t) <= sizeof(uint32_t));
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

static __maybe_unused __inline uintptr_t mdbx_thread_self(void) {
  mdbx_tid_t thunk;
  STATIC_ASSERT(sizeof(uintptr_t) >= sizeof(thunk));
#if defined(_WIN32) || defined(_WIN64)
  thunk = GetCurrentThreadId();
#else
  thunk = pthread_self();
#endif
  return (uintptr_t)thunk;
}

MDBX_INTERNAL_FUNC void __maybe_unused mdbx_osal_jitter(bool tiny);
MDBX_INTERNAL_FUNC uint64_t mdbx_osal_monotime(void);
MDBX_INTERNAL_FUNC uint64_t
mdbx_osal_16dot16_to_monotime(uint32_t seconds_16dot16);
MDBX_INTERNAL_FUNC uint32_t mdbx_osal_monotime_to_16dot16(uint64_t monotime);

MDBX_INTERNAL_FUNC bin128_t mdbx_osal_bootid(void);
/*----------------------------------------------------------------------------*/
/* lck stuff */

/// \brief Initialization of synchronization primitives linked with MDBX_env
///   instance both in LCK-file and within the current process.
/// \param
///   global_uniqueness_flag = true - denotes that there are no other processes
///     working with DB and LCK-file. Thus the function MUST initialize
///     shared synchronization objects in memory-mapped LCK-file.
///   global_uniqueness_flag = false - denotes that at least one process is
///     already working with DB and LCK-file, including the case when DB
///     has already been opened in the current process. Thus the function
///     MUST NOT initialize shared synchronization objects in memory-mapped
///     LCK-file that are already in use.
/// \return Error code or zero on success.
MDBX_INTERNAL_FUNC int mdbx_lck_init(MDBX_env *env,
                                     MDBX_env *inprocess_neighbor,
                                     int global_uniqueness_flag);

/// \brief Disconnects from shared interprocess objects and destructs
///   synchronization objects linked with MDBX_env instance
///   within the current process.
/// \param
///   inprocess_neighbor = NULL - if the current process does not have other
///     instances of MDBX_env linked with the DB being closed.
///     Thus the function MUST check for other processes working with DB or
///     LCK-file, and keep or destroy shared synchronization objects in
///     memory-mapped LCK-file depending on the result.
///   inprocess_neighbor = not-NULL - pointer to another instance of MDBX_env
///     (anyone of there is several) working with DB or LCK-file within the
///     current process. Thus the function MUST NOT try to acquire exclusive
///     lock and/or try to destruct shared synchronization objects linked with
///     DB or LCK-file. Moreover, the implementation MUST ensure correct work
///     of other instances of MDBX_env within the current process, e.g.
///     restore POSIX-fcntl locks after the closing of file descriptors.
/// \return Error code (MDBX_PANIC) or zero on success.
MDBX_INTERNAL_FUNC int mdbx_lck_destroy(MDBX_env *env,
                                        MDBX_env *inprocess_neighbor);

/// \brief Connects to shared interprocess locking objects and tries to acquire
///   the maximum lock level (shared if exclusive is not available)
///   Depending on implementation or/and platform (Windows) this function may
///   acquire the non-OS super-level lock (e.g. for shared synchronization
///   objects initialization), which will be downgraded to OS-exclusive or
///   shared via explicit calling of mdbx_lck_downgrade().
/// \return
///   MDBX_RESULT_TRUE (-1) - if an exclusive lock was acquired and thus
///     the current process is the first and only after the last use of DB.
///   MDBX_RESULT_FALSE (0) - if a shared lock was acquired and thus
///     DB has already been opened and now is used by other processes.
///   Otherwise (not 0 and not -1) - error code.
MDBX_INTERNAL_FUNC int mdbx_lck_seize(MDBX_env *env);

/// \brief Downgrades the level of initially acquired lock to
///   operational level specified by argument. The reson for such downgrade:
///    - unblocking of other processes that are waiting for access, i.e.
///      if (env->me_flags & MDBX_EXCLUSIVE) != 0, then other processes
///      should be made aware that access is unavailable rather than
///      wait for it.
///    - freeing locks that interfere file operation (especially for Windows)
///   (env->me_flags & MDBX_EXCLUSIVE) == 0 - downgrade to shared lock.
///   (env->me_flags & MDBX_EXCLUSIVE) != 0 - downgrade to exclusive
///   operational lock.
/// \return Error code or zero on success
MDBX_INTERNAL_FUNC int mdbx_lck_downgrade(MDBX_env *env);

/// \brief Locks LCK-file or/and table of readers for (de)registering.
/// \return Error code or zero on success
MDBX_INTERNAL_FUNC int mdbx_rdt_lock(MDBX_env *env);

/// \brief Unlocks LCK-file or/and table of readers after (de)registering.
MDBX_INTERNAL_FUNC void mdbx_rdt_unlock(MDBX_env *env);

/// \brief Acquires lock for DB change (on writing transaction start)
///   Reading transactions will not be blocked.
///   Declared as LIBMDBX_API because it is used in mdbx_chk.
/// \return Error code or zero on success
LIBMDBX_API int mdbx_txn_lock(MDBX_env *env, bool dont_wait);

/// \brief Releases lock once DB changes is made (after writing transaction
///   has finished).
///   Declared as LIBMDBX_API because it is used in mdbx_chk.
LIBMDBX_API void mdbx_txn_unlock(MDBX_env *env);

/// \brief Sets alive-flag of reader presence (indicative lock) for PID of
///   the current process. The function does no more than needed for
///   the correct working of mdbx_rpid_check() in other processes.
/// \return Error code or zero on success
MDBX_INTERNAL_FUNC int mdbx_rpid_set(MDBX_env *env);

/// \brief Resets alive-flag of reader presence (indicative lock)
///   for PID of the current process. The function does no more than needed
///   for the correct working of mdbx_rpid_check() in other processes.
/// \return Error code or zero on success
MDBX_INTERNAL_FUNC int mdbx_rpid_clear(MDBX_env *env);

/// \brief Checks for reading process status with the given pid with help of
///   alive-flag of presence (indicative lock) or using another way.
/// \return
///   MDBX_RESULT_TRUE (-1) - if the reader process with the given PID is alive
///     and working with DB (indicative lock is present).
///   MDBX_RESULT_FALSE (0) - if the reader process with the given PID is absent
///     or not working with DB (indicative lock is not present).
///   Otherwise (not 0 and not -1) - error code.
MDBX_INTERNAL_FUNC int mdbx_rpid_check(MDBX_env *env, uint32_t pid);

#if defined(_WIN32) || defined(_WIN64)

typedef void(WINAPI *MDBX_srwlock_function)(MDBX_srwlock *);
MDBX_INTERNAL_VAR MDBX_srwlock_function mdbx_srwlock_Init,
    mdbx_srwlock_AcquireShared, mdbx_srwlock_ReleaseShared,
    mdbx_srwlock_AcquireExclusive, mdbx_srwlock_ReleaseExclusive;

typedef BOOL(WINAPI *MDBX_GetFileInformationByHandleEx)(
    _In_ HANDLE hFile, _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
    _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);
MDBX_INTERNAL_VAR MDBX_GetFileInformationByHandleEx
    mdbx_GetFileInformationByHandleEx;

typedef BOOL(WINAPI *MDBX_GetVolumeInformationByHandleW)(
    _In_ HANDLE hFile, _Out_opt_ LPWSTR lpVolumeNameBuffer,
    _In_ DWORD nVolumeNameSize, _Out_opt_ LPDWORD lpVolumeSerialNumber,
    _Out_opt_ LPDWORD lpMaximumComponentLength,
    _Out_opt_ LPDWORD lpFileSystemFlags,
    _Out_opt_ LPWSTR lpFileSystemNameBuffer, _In_ DWORD nFileSystemNameSize);
MDBX_INTERNAL_VAR MDBX_GetVolumeInformationByHandleW
    mdbx_GetVolumeInformationByHandleW;

typedef DWORD(WINAPI *MDBX_GetFinalPathNameByHandleW)(_In_ HANDLE hFile,
                                                      _Out_ LPWSTR lpszFilePath,
                                                      _In_ DWORD cchFilePath,
                                                      _In_ DWORD dwFlags);
MDBX_INTERNAL_VAR MDBX_GetFinalPathNameByHandleW mdbx_GetFinalPathNameByHandleW;

typedef BOOL(WINAPI *MDBX_SetFileInformationByHandle)(
    _In_ HANDLE hFile, _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
    _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);
MDBX_INTERNAL_VAR MDBX_SetFileInformationByHandle
    mdbx_SetFileInformationByHandle;

typedef NTSTATUS(NTAPI *MDBX_NtFsControlFile)(
    IN HANDLE FileHandle, IN OUT HANDLE Event,
    IN OUT PVOID /* PIO_APC_ROUTINE */ ApcRoutine, IN OUT PVOID ApcContext,
    OUT PIO_STATUS_BLOCK IoStatusBlock, IN ULONG FsControlCode,
    IN OUT PVOID InputBuffer, IN ULONG InputBufferLength,
    OUT OPTIONAL PVOID OutputBuffer, IN ULONG OutputBufferLength);
MDBX_INTERNAL_VAR MDBX_NtFsControlFile mdbx_NtFsControlFile;

typedef uint64_t(WINAPI *MDBX_GetTickCount64)(void);
MDBX_INTERNAL_VAR MDBX_GetTickCount64 mdbx_GetTickCount64;

#if !defined(_WIN32_WINNT_WIN8) || _WIN32_WINNT < _WIN32_WINNT_WIN8
typedef struct _WIN32_MEMORY_RANGE_ENTRY {
  PVOID VirtualAddress;
  SIZE_T NumberOfBytes;
} WIN32_MEMORY_RANGE_ENTRY, *PWIN32_MEMORY_RANGE_ENTRY;
#endif /* Windows 8.x */

typedef BOOL(WINAPI *MDBX_PrefetchVirtualMemory)(
    HANDLE hProcess, ULONG_PTR NumberOfEntries,
    PWIN32_MEMORY_RANGE_ENTRY VirtualAddresses, ULONG Flags);
MDBX_INTERNAL_VAR MDBX_PrefetchVirtualMemory mdbx_PrefetchVirtualMemory;

#if 0 /* LY: unused for now */
#if !defined(_WIN32_WINNT_WIN81) || _WIN32_WINNT < _WIN32_WINNT_WIN81
typedef enum OFFER_PRIORITY {
  VmOfferPriorityVeryLow = 1,
  VmOfferPriorityLow,
  VmOfferPriorityBelowNormal,
  VmOfferPriorityNormal
} OFFER_PRIORITY;
#endif /* Windows 8.1 */

typedef DWORD(WINAPI *MDBX_DiscardVirtualMemory)(PVOID VirtualAddress,
                                                 SIZE_T Size);
MDBX_INTERNAL_VAR MDBX_DiscardVirtualMemory mdbx_DiscardVirtualMemory;

typedef DWORD(WINAPI *MDBX_ReclaimVirtualMemory)(PVOID VirtualAddress,
                                                 SIZE_T Size);
MDBX_INTERNAL_VAR MDBX_ReclaimVirtualMemory mdbx_ReclaimVirtualMemory;

typedef DWORD(WINAPI *MDBX_OfferVirtualMemory(
  PVOID          VirtualAddress,
  SIZE_T         Size,
  OFFER_PRIORITY Priority
);
MDBX_INTERNAL_VAR MDBX_OfferVirtualMemory mdbx_OfferVirtualMemory;
#endif /* unused for now */

typedef enum _SECTION_INHERIT { ViewShare = 1, ViewUnmap = 2 } SECTION_INHERIT;

typedef NTSTATUS(NTAPI *MDBX_NtExtendSection)(IN HANDLE SectionHandle,
                                              IN PLARGE_INTEGER NewSectionSize);
MDBX_INTERNAL_VAR MDBX_NtExtendSection mdbx_NtExtendSection;

static __inline bool mdbx_RunningUnderWine(void) {
  return !mdbx_NtExtendSection;
}

typedef LSTATUS(WINAPI *MDBX_RegGetValueA)(HKEY hkey, LPCSTR lpSubKey,
                                           LPCSTR lpValue, DWORD dwFlags,
                                           LPDWORD pdwType, PVOID pvData,
                                           LPDWORD pcbData);
MDBX_INTERNAL_VAR MDBX_RegGetValueA mdbx_RegGetValueA;

#endif /* Windows */

#endif /* !__cplusplus */

/*----------------------------------------------------------------------------*/

#if defined(_MSC_VER) && _MSC_VER >= 1900
/* LY: MSVC 2015/2017/2019 has buggy/inconsistent PRIuPTR/PRIxPTR macros
 * for internal format-args checker. */
#undef PRIuPTR
#undef PRIiPTR
#undef PRIdPTR
#undef PRIxPTR
#define PRIuPTR "Iu"
#define PRIiPTR "Ii"
#define PRIdPTR "Id"
#define PRIxPTR "Ix"
#define PRIuSIZE "zu"
#define PRIiSIZE "zi"
#define PRIdSIZE "zd"
#define PRIxSIZE "zx"
#endif /* fix PRI*PTR for _MSC_VER */

#ifndef PRIuSIZE
#define PRIuSIZE PRIuPTR
#define PRIiSIZE PRIiPTR
#define PRIdSIZE PRIdPTR
#define PRIxSIZE PRIxPTR
#endif /* PRI*SIZE macros for MSVC */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#define mdbx_sourcery_anchor XCONCAT(mdbx_sourcery_, MDBX_BUILD_SOURCERY)
#if defined(MDBX_TOOLS)
extern LIBMDBX_API const char *const mdbx_sourcery_anchor;
#endif

/*******************************************************************************
 *******************************************************************************
 *******************************************************************************
 *
 *
 *         ####   #####    #####     #     ####   #    #   ####
 *        #    #  #    #     #       #    #    #  ##   #  #
 *        #    #  #    #     #       #    #    #  # #  #   ####
 *        #    #  #####      #       #    #    #  #  # #       #
 *        #    #  #          #       #    #    #  #   ##  #    #
 *         ####   #          #       #     ####   #    #   ####
 *
 *
 */

/** \defgroup build_option Build options
 * The libmdbx build options.
 @{ */

#ifdef DOXYGEN
/* !!! Actually this is a fake definitions     !!!
 * !!! for documentation generation by Doxygen !!! */

/** Controls enabling of debugging features.
 *
 *  - `MDBX_DEBUG = 0` (by default) Disables any debugging features at all,
 *                     including logging and assertion controls.
 *                     Logging level and corresponding debug flags changing
 *                     by \ref mdbx_setup_debug() will not have effect.
 *  - `MDBX_DEBUG > 0` Enables code for the debugging features (logging,
 *                     assertions checking and internal audit).
 *                     Simultaneously sets the default logging level
 *                     to the `MDBX_DEBUG` value.
 *                     Also enables \ref MDBX_DBG_AUDIT if `MDBX_DEBUG >= 2`.
 *
 * \ingroup build_option */
#define MDBX_DEBUG 0...7

/** Disables using of GNU libc extensions. */
#define MDBX_DISABLE_GNU_SOURCE 0 or 1

#endif /* DOXYGEN */

/** Using fcntl(F_FULLFSYNC) with 5-10 times slowdown */
#define MDBX_OSX_WANNA_DURABILITY 0
/** Using fsync() with chance of data lost on power failure */
#define MDBX_OSX_WANNA_SPEED 1

#ifndef MDBX_OSX_SPEED_INSTEADOF_DURABILITY
/** Choices \ref MDBX_OSX_WANNA_DURABILITY or \ref MDBX_OSX_WANNA_SPEED
 * for OSX & iOS */
#define MDBX_OSX_SPEED_INSTEADOF_DURABILITY MDBX_OSX_WANNA_DURABILITY
#endif /* MDBX_OSX_SPEED_INSTEADOF_DURABILITY */

/** Controls checking PID against reuse DB environment after the fork() */
#ifndef MDBX_ENV_CHECKPID
#if defined(MADV_DONTFORK) || defined(_WIN32) || defined(_WIN64)
/* PID check could be omitted:
 *  - on Linux when madvise(MADV_DONTFORK) is available, i.e. after the fork()
 *    mapped pages will not be available for child process.
 *  - in Windows where fork() not available. */
#define MDBX_ENV_CHECKPID 0
#else
#define MDBX_ENV_CHECKPID 1
#endif
#define MDBX_ENV_CHECKPID_CONFIG "AUTO=" STRINGIFY(MDBX_ENV_CHECKPID)
#else
#define MDBX_ENV_CHECKPID_CONFIG STRINGIFY(MDBX_ENV_CHECKPID)
#endif /* MDBX_ENV_CHECKPID */

/** Controls checking transaction owner thread against misuse transactions from
 * other threads. */
#ifndef MDBX_TXN_CHECKOWNER
#define MDBX_TXN_CHECKOWNER 1
#define MDBX_TXN_CHECKOWNER_CONFIG "AUTO=" STRINGIFY(MDBX_TXN_CHECKOWNER)
#else
#define MDBX_TXN_CHECKOWNER_CONFIG STRINGIFY(MDBX_TXN_CHECKOWNER)
#endif /* MDBX_TXN_CHECKOWNER */

/** Does a system have battery-backed Real-Time Clock or just a fake. */
#ifndef MDBX_TRUST_RTC
#if defined(__linux__) || defined(__gnu_linux__) || defined(__NetBSD__) ||     \
    defined(__OpenBSD__)
#define MDBX_TRUST_RTC 0 /* a lot of embedded systems have a fake RTC */
#else
#define MDBX_TRUST_RTC 1
#endif
#define MDBX_TRUST_RTC_CONFIG "AUTO=" STRINGIFY(MDBX_TRUST_RTC)
#else
#define MDBX_TRUST_RTC_CONFIG STRINGIFY(MDBX_TRUST_RTC)
#endif /* MDBX_TRUST_RTC */

/** Controls online database auto-compactification during write-transactions. */
#ifndef MDBX_ENABLE_REFUND
#define MDBX_ENABLE_REFUND 1
#endif
#if !(MDBX_ENABLE_REFUND == 0 || MDBX_ENABLE_REFUND == 1)
#error MDBX_ENABLE_REFUND must be defined as 0 or 1
#endif /* MDBX_ENABLE_REFUND */

/** Disable some checks to reduce an overhead and detection probability of
 * database corruption to a values closer to the LMDB. */
#ifndef MDBX_DISABLE_PAGECHECKS
#define MDBX_DISABLE_PAGECHECKS 0
#endif
#if !(MDBX_DISABLE_PAGECHECKS == 0 || MDBX_DISABLE_PAGECHECKS == 1)
#error MDBX_DISABLE_PAGECHECKS must be defined as 0 or 1
#endif /* MDBX_DISABLE_PAGECHECKS */

/** Controls sort order of internal page number lists.
 * The database format depend on this option and libmdbx builded with different
 * option value are incompatible. */
#ifndef MDBX_PNL_ASCENDING
#define MDBX_PNL_ASCENDING 0
#endif
#if !(MDBX_PNL_ASCENDING == 0 || MDBX_PNL_ASCENDING == 1)
#error MDBX_PNL_ASCENDING must be defined as 0 or 1
#endif /* MDBX_PNL_ASCENDING */

//------------------------------------------------------------------------------

/** Win32 File Locking API for \ref MDBX_LOCKING */
#define MDBX_LOCKING_WIN32FILES -1

/** SystemV IPC semaphores for \ref MDBX_LOCKING */
#define MDBX_LOCKING_SYSV 5

/** POSIX-1 Shared anonymous semaphores for \ref MDBX_LOCKING */
#define MDBX_LOCKING_POSIX1988 1988

/** POSIX-2001 Shared Mutexes for \ref MDBX_LOCKING */
#define MDBX_LOCKING_POSIX2001 2001

/** POSIX-2008 Robust Mutexes for \ref MDBX_LOCKING */
#define MDBX_LOCKING_POSIX2008 2008

/** BeOS Benaphores, aka Futexes for \ref MDBX_LOCKING */
#define MDBX_LOCKING_BENAPHORE 1995

/** Advanced: Choices the locking implementation (autodetection by default). */
#if defined(_WIN32) || defined(_WIN64)
#define MDBX_LOCKING MDBX_LOCKING_WIN32FILES
#else
#ifndef MDBX_LOCKING
#if defined(_POSIX_THREAD_PROCESS_SHARED) &&                                   \
    _POSIX_THREAD_PROCESS_SHARED >= 200112L && !defined(__FreeBSD__)

/* Some platforms define the EOWNERDEAD error code even though they
 * don't support Robust Mutexes. If doubt compile with -MDBX_LOCKING=2001. */
#if defined(EOWNERDEAD) && _POSIX_THREAD_PROCESS_SHARED >= 200809L &&          \
    ((defined(_POSIX_THREAD_ROBUST_PRIO_INHERIT) &&                            \
      _POSIX_THREAD_ROBUST_PRIO_INHERIT > 0) ||                                \
     (defined(_POSIX_THREAD_ROBUST_PRIO_PROTECT) &&                            \
      _POSIX_THREAD_ROBUST_PRIO_PROTECT > 0) ||                                \
     defined(PTHREAD_MUTEX_ROBUST) || defined(PTHREAD_MUTEX_ROBUST_NP)) &&     \
    (!defined(__GLIBC__) ||                                                    \
     __GLIBC_PREREQ(2, 10) /* troubles with Robust mutexes before 2.10 */)
#define MDBX_LOCKING MDBX_LOCKING_POSIX2008
#else
#define MDBX_LOCKING MDBX_LOCKING_POSIX2001
#endif
#elif defined(__sun) || defined(__SVR4) || defined(__svr4__)
#define MDBX_LOCKING MDBX_LOCKING_POSIX1988
#else
#define MDBX_LOCKING MDBX_LOCKING_SYSV
#endif
#define MDBX_LOCKING_CONFIG "AUTO=" STRINGIFY(MDBX_LOCKING)
#else
#define MDBX_LOCKING_CONFIG STRINGIFY(MDBX_LOCKING)
#endif /* MDBX_LOCKING */
#endif /* !Windows */

/** Advanced: Using POSIX OFD-locks (autodetection by default). */
#ifndef MDBX_USE_OFDLOCKS
#if defined(F_OFD_SETLK) && defined(F_OFD_SETLKW) && defined(F_OFD_GETLK) &&   \
    !defined(MDBX_SAFE4QEMU) &&                                                \
    !defined(__sun) /* OFD-lock are broken on Solaris */
#define MDBX_USE_OFDLOCKS 1
#else
#define MDBX_USE_OFDLOCKS 0
#endif
#define MDBX_USE_OFDLOCKS_CONFIG "AUTO=" STRINGIFY(MDBX_USE_OFDLOCKS)
#else
#define MDBX_USE_OFDLOCKS_CONFIG STRINGIFY(MDBX_USE_OFDLOCKS)
#endif /* MDBX_USE_OFDLOCKS */

/** Advanced: Using sendfile() syscall (autodetection by default). */
#ifndef MDBX_USE_SENDFILE
#if ((defined(__linux__) || defined(__gnu_linux__)) &&                         \
     !defined(__ANDROID_API__)) ||                                             \
    (defined(__ANDROID_API__) && __ANDROID_API__ >= 21)
#define MDBX_USE_SENDFILE 1
#else
#define MDBX_USE_SENDFILE 0
#endif
#endif /* MDBX_USE_SENDFILE */

/** Advanced: Using copy_file_range() syscall (autodetection by default). */
#ifndef MDBX_USE_COPYFILERANGE
#if __GLIBC_PREREQ(2, 27) && defined(_GNU_SOURCE)
#define MDBX_USE_COPYFILERANGE 1
#else
#define MDBX_USE_COPYFILERANGE 0
#endif
#endif /* MDBX_USE_COPYFILERANGE */

/** Advanced: Using sync_file_range() syscall (autodetection by default). */
#ifndef MDBX_USE_SYNCFILERANGE
#if ((defined(__linux__) || defined(__gnu_linux__)) &&                         \
     defined(SYNC_FILE_RANGE_WRITE) && !defined(__ANDROID_API__)) ||           \
    (defined(__ANDROID_API__) && __ANDROID_API__ >= 26)
#define MDBX_USE_SYNCFILERANGE 1
#else
#define MDBX_USE_SYNCFILERANGE 0
#endif
#endif /* MDBX_USE_SYNCFILERANGE */

//------------------------------------------------------------------------------

#ifndef MDBX_CPU_WRITEBACK_INCOHERENT
#if defined(__ia32__) || defined(__e2k__) || defined(__hppa) ||                \
    defined(__hppa__) || defined(DOXYGEN)
#define MDBX_CPU_WRITEBACK_INCOHERENT 0
#else
#define MDBX_CPU_WRITEBACK_INCOHERENT 1
#endif
#endif /* MDBX_CPU_WRITEBACK_INCOHERENT */

#ifndef MDBX_MMAP_INCOHERENT_FILE_WRITE
#ifdef __OpenBSD__
#define MDBX_MMAP_INCOHERENT_FILE_WRITE 1
#else
#define MDBX_MMAP_INCOHERENT_FILE_WRITE 0
#endif
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */

#ifndef MDBX_MMAP_INCOHERENT_CPU_CACHE
#if defined(__mips) || defined(__mips__) || defined(__mips64) ||               \
    defined(__mips64__) || defined(_M_MRX000) || defined(_MIPS_) ||            \
    defined(__MWERKS__) || defined(__sgi)
/* MIPS has cache coherency issues. */
#define MDBX_MMAP_INCOHERENT_CPU_CACHE 1
#else
/* LY: assume no relevant mmap/dcache issues. */
#define MDBX_MMAP_INCOHERENT_CPU_CACHE 0
#endif
#endif /* MDBX_MMAP_INCOHERENT_CPU_CACHE */

#ifndef MDBX_64BIT_ATOMIC
#if MDBX_WORDBITS >= 64 || defined(DOXYGEN)
#define MDBX_64BIT_ATOMIC 1
#else
#define MDBX_64BIT_ATOMIC 0
#endif
#define MDBX_64BIT_ATOMIC_CONFIG "AUTO=" STRINGIFY(MDBX_64BIT_ATOMIC)
#else
#define MDBX_64BIT_ATOMIC_CONFIG STRINGIFY(MDBX_64BIT_ATOMIC)
#endif /* MDBX_64BIT_ATOMIC */

#ifndef MDBX_64BIT_CAS
#if defined(ATOMIC_LLONG_LOCK_FREE)
#if ATOMIC_LLONG_LOCK_FREE > 1
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS 0
#endif
#elif defined(__GCC_ATOMIC_LLONG_LOCK_FREE)
#if __GCC_ATOMIC_LLONG_LOCK_FREE > 1
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS 0
#endif
#elif defined(__CLANG_ATOMIC_LLONG_LOCK_FREE)
#if __CLANG_ATOMIC_LLONG_LOCK_FREE > 1
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS 0
#endif
#elif defined(_MSC_VER) || defined(__APPLE__) || defined(DOXYGEN)
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS MDBX_64BIT_ATOMIC
#endif
#define MDBX_64BIT_CAS_CONFIG "AUTO=" STRINGIFY(MDBX_64BIT_CAS)
#else
#define MDBX_64BIT_CAS_CONFIG STRINGIFY(MDBX_64BIT_CAS)
#endif /* MDBX_64BIT_CAS */

#if !defined(MDBX_UNALIGNED_OK)
#if defined(_MSC_VER)
#define MDBX_UNALIGNED_OK 1 /* avoid MSVC misoptimization */
#elif __CLANG_PREREQ(5, 0) || __GNUC_PREREQ(5, 0)
#define MDBX_UNALIGNED_OK 0 /* expecting optimization is well done */
#elif (defined(__ia32__) || defined(__ARM_FEATURE_UNALIGNED)) &&               \
    !defined(__ALIGNED__)
#define MDBX_UNALIGNED_OK 1
#else
#define MDBX_UNALIGNED_OK 0
#endif
#endif /* MDBX_UNALIGNED_OK */

#ifndef MDBX_CACHELINE_SIZE
#if defined(SYSTEM_CACHE_ALIGNMENT_SIZE)
#define MDBX_CACHELINE_SIZE SYSTEM_CACHE_ALIGNMENT_SIZE
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#define MDBX_CACHELINE_SIZE 128
#else
#define MDBX_CACHELINE_SIZE 64
#endif
#endif /* MDBX_CACHELINE_SIZE */

/** @} end of build options */
/*******************************************************************************
 *******************************************************************************
 ******************************************************************************/

/*----------------------------------------------------------------------------*/
/* Basic constants and types */

typedef union {
  volatile uint32_t weak;
#ifdef MDBX_HAVE_C11ATOMICS
  volatile _Atomic uint32_t c11a;
#endif /* MDBX_HAVE_C11ATOMICS */
} MDBX_atomic_uint32_t;

typedef union {
  volatile uint64_t weak;
#if defined(MDBX_HAVE_C11ATOMICS) && (MDBX_64BIT_CAS || MDBX_64BIT_ATOMIC)
  volatile _Atomic uint64_t c11a;
#endif
#if !defined(MDBX_HAVE_C11ATOMICS) || !MDBX_64BIT_CAS || !MDBX_64BIT_ATOMIC
  __anonymous_struct_extension__ struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    MDBX_atomic_uint32_t low, high;
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    MDBX_atomic_uint32_t high, low;
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
  };
#endif
} MDBX_atomic_uint64_t;

/* The minimum number of keys required in a database page.
 * Setting this to a larger value will place a smaller bound on the
 * maximum size of a data item. Data items larger than this size will
 * be pushed into overflow pages instead of being stored directly in
 * the B-tree node. This value used to default to 4. With a page size
 * of 4096 bytes that meant that any item larger than 1024 bytes would
 * go into an overflow page. That also meant that on average 2-3KB of
 * each overflow page was wasted space. The value cannot be lower than
 * 2 because then there would no longer be a tree structure. With this
 * value, items larger than 2KB will go into overflow pages, and on
 * average only 1KB will be wasted. */
#define MDBX_MINKEYS 2

/* A stamp that identifies a file as an MDBX file.
 * There's nothing special about this value other than that it is easily
 * recognizable, and it will reflect any byte order mismatches. */
#define MDBX_MAGIC UINT64_C(/* 56-bit prime */ 0x59659DBDEF4C11)

/* The version number for a database's datafile format. */
#define MDBX_DATA_VERSION 2
/* The version number for a database's lockfile format. */
#define MDBX_LOCK_VERSION 3

/* handle for the DB used to track free pages. */
#define FREE_DBI 0
/* handle for the default DB. */
#define MAIN_DBI 1
/* Number of DBs in metapage (free and main) - also hardcoded elsewhere */
#define CORE_DBS 2

/* Number of meta pages - also hardcoded elsewhere */
#define NUM_METAS 3

/* A page number in the database.
 *
 * MDBX uses 32 bit for page numbers. This limits database
 * size up to 2^44 bytes, in case of 4K pages. */
typedef uint32_t pgno_t;
typedef MDBX_atomic_uint32_t atomic_pgno_t;
#define PRIaPGNO PRIu32
#define MAX_PAGENO UINT32_C(0x7FFFffff)
#define MIN_PAGENO NUM_METAS

#define SAFE64_INVALID_THRESHOLD UINT64_C(0xffffFFFF00000000)

/* A transaction ID. */
typedef uint64_t txnid_t;
typedef MDBX_atomic_uint64_t atomic_txnid_t;
#define PRIaTXN PRIi64
#define MIN_TXNID UINT64_C(1)
#define MAX_TXNID (SAFE64_INVALID_THRESHOLD - 1)
#define INITIAL_TXNID (MIN_TXNID + NUM_METAS - 1)
#define INVALID_TXNID UINT64_MAX
/* LY: for testing non-atomic 64-bit txnid on 32-bit arches.
 * #define MDBX_TXNID_STEP (UINT32_MAX / 3) */
#ifndef MDBX_TXNID_STEP
#if MDBX_64BIT_CAS
#define MDBX_TXNID_STEP 1u
#else
#define MDBX_TXNID_STEP 2u
#endif
#endif /* MDBX_TXNID_STEP */

/* Used for offsets within a single page.
 * Since memory pages are typically 4 or 8KB in size, 12-13 bits,
 * this is plenty. */
typedef uint16_t indx_t;

#define MEGABYTE ((size_t)1 << 20)

/*----------------------------------------------------------------------------*/
/* Core structures for database and shared memory (i.e. format definition) */
#pragma pack(push, 1)

/* Information about a single database in the environment. */
typedef struct MDBX_db {
  uint16_t md_flags;        /* see mdbx_dbi_open */
  uint16_t md_depth;        /* depth of this tree */
  uint32_t md_xsize;        /* key-size for MDBX_DUPFIXED (LEAF2 pages) */
  pgno_t md_root;           /* the root page of this tree */
  pgno_t md_branch_pages;   /* number of internal pages */
  pgno_t md_leaf_pages;     /* number of leaf pages */
  pgno_t md_overflow_pages; /* number of overflow pages */
  uint64_t md_seq;          /* table sequence counter */
  uint64_t md_entries;      /* number of data items */
  uint64_t md_mod_txnid;    /* txnid of last committed modification */
} MDBX_db;

/* database size-related parameters */
typedef struct mdbx_geo_t {
  uint16_t grow;   /* datafile growth step in pages */
  uint16_t shrink; /* datafile shrink threshold in pages */
  pgno_t lower;    /* minimal size of datafile in pages */
  pgno_t upper;    /* maximal size of datafile in pages */
  pgno_t now;      /* current size of datafile in pages */
  pgno_t next;     /* first unused page in the datafile,
                    * but actually the file may be shorter. */
} mdbx_geo_t;

/* Meta page content.
 * A meta page is the start point for accessing a database snapshot.
 * Pages 0-1 are meta pages. Transaction N writes meta page (N % 2). */
typedef struct MDBX_meta {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_DATA_VERSION. */
  uint32_t mm_magic_and_version[2];

  /* txnid that committed this page, the first of a two-phase-update pair */
  uint32_t mm_txnid_a[2];

  uint16_t mm_extra_flags;  /* extra DB flags, zero (nothing) for now */
  uint8_t mm_validator_id;  /* ID of checksum and page validation method,
                             * zero (nothing) for now */
  uint8_t mm_extra_pagehdr; /* extra bytes in the page header,
                             * zero (nothing) for now */

  mdbx_geo_t mm_geo; /* database size-related parameters */

  MDBX_db mm_dbs[CORE_DBS]; /* first is free space, 2nd is main db */
                            /* The size of pages used in this DB */
#define mm_psize mm_dbs[FREE_DBI].md_xsize
/* Any persistent environment flags, see mdbx_env */
#define mm_flags mm_dbs[FREE_DBI].md_flags
  MDBX_canary mm_canary;

#define MDBX_DATASIGN_NONE 0u
#define MDBX_DATASIGN_WEAK 1u
#define SIGN_IS_STEADY(sign) ((sign) > MDBX_DATASIGN_WEAK)
#define META_IS_STEADY(meta)                                                   \
  SIGN_IS_STEADY(unaligned_peek_u64(4, (meta)->mm_datasync_sign))
  uint32_t mm_datasync_sign[2];

  /* txnid that committed this page, the second of a two-phase-update pair */
  uint32_t mm_txnid_b[2];

  /* Number of non-meta pages which were put in GC after COW. May be 0 in case
   * DB was previously handled by libmdbx without corresponding feature.
   * This value in couple with mr_snapshot_pages_retired allows fast estimation
   * of "how much reader is restraining GC recycling". */
  uint32_t mm_pages_retired[2];

  /* The analogue /proc/sys/kernel/random/boot_id or similar to determine
   * whether the system was rebooted after the last use of the database files.
   * If there was no reboot, but there is no need to rollback to the last
   * steady sync point. Zeros mean that no relevant information is available
   * from the system. */
  bin128_t mm_bootid;

} MDBX_meta;

/* Common header for all page types. The page type depends on mp_flags.
 *
 * P_BRANCH and P_LEAF pages have unsorted 'MDBX_node's at the end, with
 * sorted mp_ptrs[] entries referring to them. Exception: P_LEAF2 pages
 * omit mp_ptrs and pack sorted MDBX_DUPFIXED values after the page header.
 *
 * P_OVERFLOW records occupy one or more contiguous pages where only the
 * first has a page header. They hold the real data of F_BIGDATA nodes.
 *
 * P_SUBP sub-pages are small leaf "pages" with duplicate data.
 * A node with flag F_DUPDATA but not F_SUBDATA contains a sub-page.
 * (Duplicate data can also go in sub-databases, which use normal pages.)
 *
 * P_META pages contain MDBX_meta, the start point of an MDBX snapshot.
 *
 * Each non-metapage up to MDBX_meta.mm_last_pg is reachable exactly once
 * in the snapshot: Either used by a database or listed in a GC record. */
typedef struct MDBX_page {
  union {
    uint64_t mp_txnid;         /* txnid that committed this page */
    struct MDBX_page *mp_next; /* for in-memory list of freed pages */
  };
  uint16_t mp_leaf2_ksize; /* key size if this is a LEAF2 page */
#define P_BRANCH 0x01      /* branch page */
#define P_LEAF 0x02        /* leaf page */
#define P_OVERFLOW 0x04    /* overflow page */
#define P_META 0x08        /* meta page */
#define P_DIRTY 0x10       /* dirty page, also set for P_SUBP pages */
#define P_LEAF2 0x20       /* for MDBX_DUPFIXED records */
#define P_SUBP 0x40        /* for MDBX_DUPSORT sub-pages */
#define P_BAD 0x80         /* explicit flag for invalid/bad page */
#define P_LOOSE 0x4000     /* page was dirtied then freed, can be reused */
#define P_KEEP 0x8000      /* leave this page alone during spill */
  uint16_t mp_flags;
  union {
    uint32_t mp_pages; /* number of overflow pages */
    __anonymous_struct_extension__ struct {
      indx_t mp_lower; /* lower bound of free space */
      indx_t mp_upper; /* upper bound of free space */
    };
  };
  pgno_t mp_pgno; /* page number */

#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  indx_t mp_ptrs[] /* dynamic size */;
#endif /* C99 */
} MDBX_page;

/* Size of the page header, excluding dynamic data at the end */
#define PAGEHDRSZ ((unsigned)offsetof(MDBX_page, mp_ptrs))

#pragma pack(pop)

#if MDBX_LOCKING == MDBX_LOCKING_WIN32FILES
#define MDBX_CLOCK_SIGN UINT32_C(0xF10C)
typedef void mdbx_ipclock_t;
#elif MDBX_LOCKING == MDBX_LOCKING_SYSV

#define MDBX_CLOCK_SIGN UINT32_C(0xF18D)
typedef mdbx_pid_t mdbx_ipclock_t;
#ifndef EOWNERDEAD
#define EOWNERDEAD MDBX_RESULT_TRUE
#endif

#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
#define MDBX_CLOCK_SIGN UINT32_C(0x8017)
typedef pthread_mutex_t mdbx_ipclock_t;
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
#define MDBX_CLOCK_SIGN UINT32_C(0xFC29)
typedef sem_t mdbx_ipclock_t;
#else
#error "FIXME"
#endif /* MDBX_LOCKING */

#if MDBX_LOCKING > MDBX_LOCKING_SYSV && !defined(__cplusplus)
MDBX_INTERNAL_FUNC int mdbx_ipclock_stub(mdbx_ipclock_t *ipc);
MDBX_INTERNAL_FUNC int mdbx_ipclock_destroy(mdbx_ipclock_t *ipc);
#endif /* MDBX_LOCKING */

/* Reader Lock Table
 *
 * Readers don't acquire any locks for their data access. Instead, they
 * simply record their transaction ID in the reader table. The reader
 * mutex is needed just to find an empty slot in the reader table. The
 * slot's address is saved in thread-specific data so that subsequent
 * read transactions started by the same thread need no further locking to
 * proceed.
 *
 * If MDBX_NOTLS is set, the slot address is not saved in thread-specific data.
 * No reader table is used if the database is on a read-only filesystem.
 *
 * Since the database uses multi-version concurrency control, readers don't
 * actually need any locking. This table is used to keep track of which
 * readers are using data from which old transactions, so that we'll know
 * when a particular old transaction is no longer in use. Old transactions
 * that have discarded any data pages can then have those pages reclaimed
 * for use by a later write transaction.
 *
 * The lock table is constructed such that reader slots are aligned with the
 * processor's cache line size. Any slot is only ever used by one thread.
 * This alignment guarantees that there will be no contention or cache
 * thrashing as threads update their own slot info, and also eliminates
 * any need for locking when accessing a slot.
 *
 * A writer thread will scan every slot in the table to determine the oldest
 * outstanding reader transaction. Any freed pages older than this will be
 * reclaimed by the writer. The writer doesn't use any locks when scanning
 * this table. This means that there's no guarantee that the writer will
 * see the most up-to-date reader info, but that's not required for correct
 * operation - all we need is to know the upper bound on the oldest reader,
 * we don't care at all about the newest reader. So the only consequence of
 * reading stale information here is that old pages might hang around a
 * while longer before being reclaimed. That's actually good anyway, because
 * the longer we delay reclaiming old pages, the more likely it is that a
 * string of contiguous pages can be found after coalescing old pages from
 * many old transactions together. */

/* The actual reader record, with cacheline padding. */
typedef struct MDBX_reader {
  /* Current Transaction ID when this transaction began, or (txnid_t)-1.
   * Multiple readers that start at the same time will probably have the
   * same ID here. Again, it's not important to exclude them from
   * anything; all we need to know is which version of the DB they
   * started from so we can avoid overwriting any data used in that
   * particular version. */
  MDBX_atomic_uint64_t /* txnid_t */ mr_txnid;

  /* The information we store in a single slot of the reader table.
   * In addition to a transaction ID, we also record the process and
   * thread ID that owns a slot, so that we can detect stale information,
   * e.g. threads or processes that went away without cleaning up.
   *
   * NOTE: We currently don't check for stale records.
   * We simply re-init the table when we know that we're the only process
   * opening the lock file. */

  /* The thread ID of the thread owning this txn. */
  MDBX_atomic_uint64_t mr_tid;

  /* The process ID of the process owning this reader txn. */
  MDBX_atomic_uint32_t mr_pid;

  /* The number of pages used in the reader's MVCC snapshot,
   * i.e. the value of meta->mm_geo.next and txn->mt_next_pgno */
  atomic_pgno_t mr_snapshot_pages_used;
  /* Number of retired pages at the time this reader starts transaction. So,
   * at any time the difference mm_pages_retired - mr_snapshot_pages_retired
   * will give the number of pages which this reader restraining from reuse. */
  MDBX_atomic_uint64_t mr_snapshot_pages_retired;
} MDBX_reader;

/* The header for the reader table (a memory-mapped lock file). */
typedef struct MDBX_lockinfo {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with with MDBX_LOCK_VERSION. */
  uint64_t mti_magic_and_version;

  /* Format of this lock file. Must be set to MDBX_LOCK_FORMAT. */
  uint32_t mti_os_and_format;

  /* Flags which environment was opened. */
  MDBX_atomic_uint32_t mti_envmode;

  /* Threshold of un-synced-with-disk pages for auto-sync feature,
   * zero means no-threshold, i.e. auto-sync is disabled. */
  atomic_pgno_t mti_autosync_threshold;

  /* Low 32-bit of txnid with which meta-pages was synced,
   * i.e. for sync-polling in the MDBX_NOMETASYNC mode. */
  MDBX_atomic_uint32_t mti_meta_sync_txnid;

  /* Period for timed auto-sync feature, i.e. at the every steady checkpoint
   * the mti_unsynced_timeout sets to the current_time + mti_autosync_period.
   * The time value is represented in a suitable system-dependent form, for
   * example clock_gettime(CLOCK_BOOTTIME) or clock_gettime(CLOCK_MONOTONIC).
   * Zero means timed auto-sync is disabled. */
  MDBX_atomic_uint64_t mti_autosync_period;

  /* Marker to distinguish uniqueness of DB/CLK.*/
  MDBX_atomic_uint64_t mti_bait_uniqueness;

  alignas(MDBX_CACHELINE_SIZE) /* cacheline ---------------------------------*/

  /* Write transaction lock. */
#if MDBX_LOCKING > 0
      mdbx_ipclock_t mti_wlock;
#endif /* MDBX_LOCKING > 0 */

  atomic_txnid_t mti_oldest_reader;

  /* Timestamp of the last steady sync. Value is represented in a suitable
   * system-dependent form, for example clock_gettime(CLOCK_BOOTTIME) or
   * clock_gettime(CLOCK_MONOTONIC). */
  MDBX_atomic_uint64_t mti_sync_timestamp;

  /* Number un-synced-with-disk pages for auto-sync feature. */
  atomic_pgno_t mti_unsynced_pages;

  /* Number of page which was discarded last time by madvise(MADV_FREE). */
  atomic_pgno_t mti_discarded_tail;

  /* Timestamp of the last readers check. */
  MDBX_atomic_uint64_t mti_reader_check_timestamp;

  alignas(MDBX_CACHELINE_SIZE) /* cacheline ---------------------------------*/

  /* Readeaders registration lock. */
#if MDBX_LOCKING > 0
      mdbx_ipclock_t mti_rlock;
#endif /* MDBX_LOCKING > 0 */

  /* The number of slots that have been used in the reader table.
   * This always records the maximum count, it is not decremented
   * when readers release their slots. */
  MDBX_atomic_uint32_t mti_numreaders;
  MDBX_atomic_uint32_t mti_readers_refresh_flag;

#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  alignas(MDBX_CACHELINE_SIZE) /* cacheline ---------------------------------*/
      MDBX_reader mti_readers[] /* dynamic size */;
#endif /* C99 */
} MDBX_lockinfo;

/* Lockfile format signature: version, features and field layout */
#define MDBX_LOCK_FORMAT                                                       \
  (MDBX_CLOCK_SIGN * 27733 + (unsigned)sizeof(MDBX_reader) * 13 +              \
   (unsigned)offsetof(MDBX_reader, mr_snapshot_pages_used) * 251 +             \
   (unsigned)offsetof(MDBX_lockinfo, mti_oldest_reader) * 83 +                 \
   (unsigned)offsetof(MDBX_lockinfo, mti_numreaders) * 37 +                    \
   (unsigned)offsetof(MDBX_lockinfo, mti_readers) * 29)

#define MDBX_DATA_MAGIC                                                        \
  ((MDBX_MAGIC << 8) + MDBX_PNL_ASCENDING * 64 + MDBX_DATA_VERSION)
#define MDBX_DATA_MAGIC_DEVEL ((MDBX_MAGIC << 8) + 255)

#define MDBX_LOCK_MAGIC ((MDBX_MAGIC << 8) + MDBX_LOCK_VERSION)

#ifndef MDBX_ASSUME_MALLOC_OVERHEAD
#define MDBX_ASSUME_MALLOC_OVERHEAD (sizeof(void *) * 2u)
#endif /* MDBX_ASSUME_MALLOC_OVERHEAD */

/* The maximum size of a database page.
 *
 * It is 64K, but value-PAGEHDRSZ must fit in MDBX_page.mp_upper.
 *
 * MDBX will use database pages < OS pages if needed.
 * That causes more I/O in write transactions: The OS must
 * know (read) the whole page before writing a partial page.
 *
 * Note that we don't currently support Huge pages. On Linux,
 * regular data files cannot use Huge pages, and in general
 * Huge pages aren't actually pageable. We rely on the OS
 * demand-pager to read our data and page it out when memory
 * pressure from other processes is high. So until OSs have
 * actual paging support for Huge pages, they're not viable. */
#define MAX_PAGESIZE MDBX_MAX_PAGESIZE
#define MIN_PAGESIZE MDBX_MIN_PAGESIZE

#define MIN_MAPSIZE (MIN_PAGESIZE * MIN_PAGENO)
#if defined(_WIN32) || defined(_WIN64)
#define MAX_MAPSIZE32 UINT32_C(0x38000000)
#else
#define MAX_MAPSIZE32 UINT32_C(0x7f000000)
#endif
#define MAX_MAPSIZE64 (MAX_PAGENO * (uint64_t)MAX_PAGESIZE)

#if MDBX_WORDBITS >= 64
#define MAX_MAPSIZE MAX_MAPSIZE64
#define MDBX_READERS_LIMIT                                                     \
  ((MAX_PAGESIZE - sizeof(MDBX_lockinfo)) / sizeof(MDBX_reader))
#define MDBX_PGL_LIMIT MAX_PAGENO
#else
#define MDBX_READERS_LIMIT 1024
#define MAX_MAPSIZE MAX_MAPSIZE32
#define MDBX_PGL_LIMIT (MAX_MAPSIZE32 / MIN_PAGESIZE)
#endif /* MDBX_WORDBITS */

/*----------------------------------------------------------------------------*/

/* An PNL is an Page Number List, a sorted array of IDs.
 * The first element of the array is a counter for how many actual page-numbers
 * are in the list. By default PNLs are sorted in descending order, this allow
 * cut off a page with lowest pgno (at the tail) just truncating the list. The
 * sort order of PNLs is controlled by the MDBX_PNL_ASCENDING build option. */
typedef pgno_t *MDBX_PNL;

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_ORDERED(first, last) ((first) < (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) >= (last))
#else
#define MDBX_PNL_ORDERED(first, last) ((first) > (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) <= (last))
#endif

/* List of txnid, only for MDBX_txn.tw.lifo_reclaimed */
typedef txnid_t *MDBX_TXL;

/* An Dirty-Page list item is an pgno/pointer pair. */
typedef struct MDBX_dp {
  pgno_t pgno;
  MDBX_page *ptr;
} MDBX_dp;

/* An DPL (dirty-page list) is a sorted array of MDBX_DPs. */
typedef struct MDBX_dpl {
  unsigned sorted;
  unsigned length;
  unsigned detent; /* allocated size excluding the MDBX_DPL_RESERVE_GAP */
#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  MDBX_dp items[] /* dynamic size with holes at zero and after the last */;
#endif
} MDBX_dpl;

/* PNL sizes */
#define MDBX_PNL_GRANULATE 1024
#define MDBX_PNL_RADIXSORT_THRESHOLD 1024
#define MDBX_PNL_INITIAL                                                       \
  (MDBX_PNL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(pgno_t))

#define MDBX_TXL_GRANULATE 32
#define MDBX_TXL_INITIAL                                                       \
  (MDBX_TXL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t))
#define MDBX_TXL_MAX                                                           \
  ((1u << 17) - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t))

#define MDBX_PNL_ALLOCLEN(pl) ((pl)[-1])
#define MDBX_PNL_SIZE(pl) ((pl)[0])
#define MDBX_PNL_FIRST(pl) ((pl)[1])
#define MDBX_PNL_LAST(pl) ((pl)[MDBX_PNL_SIZE(pl)])
#define MDBX_PNL_BEGIN(pl) (&(pl)[1])
#define MDBX_PNL_END(pl) (&(pl)[MDBX_PNL_SIZE(pl) + 1])

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_LEAST(pl) MDBX_PNL_FIRST(pl)
#define MDBX_PNL_MOST(pl) MDBX_PNL_LAST(pl)
#else
#define MDBX_PNL_LEAST(pl) MDBX_PNL_LAST(pl)
#define MDBX_PNL_MOST(pl) MDBX_PNL_FIRST(pl)
#endif

#define MDBX_PNL_SIZEOF(pl) ((MDBX_PNL_SIZE(pl) + 1) * sizeof(pgno_t))
#define MDBX_PNL_IS_EMPTY(pl) (MDBX_PNL_SIZE(pl) == 0)

/*----------------------------------------------------------------------------*/
/* Internal structures */

/* Auxiliary DB info.
 * The information here is mostly static/read-only. There is
 * only a single copy of this record in the environment. */
typedef struct MDBX_dbx {
  MDBX_val md_name;                /* name of the database */
  MDBX_cmp_func *md_cmp;           /* function for comparing keys */
  MDBX_cmp_func *md_dcmp;          /* function for comparing data items */
  size_t md_klen_min, md_klen_max; /* min/max key length for the database */
  size_t md_vlen_min,
      md_vlen_max; /* min/max value/data length for the database */
} MDBX_dbx;

/* A database transaction.
 * Every operation requires a transaction handle. */
struct MDBX_txn {
#define MDBX_MT_SIGNATURE UINT32_C(0x93D53A31)
  size_t mt_signature;
  MDBX_txn *mt_parent; /* parent of a nested txn */
  /* Nested txn under this txn, set together with flag MDBX_TXN_HAS_CHILD */
  MDBX_txn *mt_child;
  mdbx_geo_t mt_geo;
  /* next unallocated page */
#define mt_next_pgno mt_geo.next
  /* corresponding to the current size of datafile */
#define mt_end_pgno mt_geo.now

  /* Transaction Flags */
  /* mdbx_txn_begin() flags */
#define MDBX_TXN_RO_BEGIN_FLAGS (MDBX_TXN_RDONLY | MDBX_TXN_RDONLY_PREPARE)
#define MDBX_TXN_RW_BEGIN_FLAGS                                                \
  (MDBX_TXN_NOMETASYNC | MDBX_TXN_NOSYNC | MDBX_TXN_TRY)
  /* Additional flag for mdbx_sync_locked() */
#define MDBX_SHRINK_ALLOWED UINT32_C(0x40000000)

  /* internal txn flags */
#define MDBX_TXN_FINISHED 0x01  /* txn is finished or never began */
#define MDBX_TXN_ERROR 0x02     /* txn is unusable after an error */
#define MDBX_TXN_DIRTY 0x04     /* must write, even if dirty list is empty */
#define MDBX_TXN_SPILLS 0x08    /* txn or a parent has spilled pages */
#define MDBX_TXN_HAS_CHILD 0x10 /* txn has an MDBX_txn.mt_child */
  /* most operations on the txn are currently illegal */
#define MDBX_TXN_BLOCKED                                                       \
  (MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_HAS_CHILD)

#define TXN_FLAGS                                                              \
  (MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_DIRTY | MDBX_TXN_SPILLS |     \
   MDBX_TXN_HAS_CHILD)

#if (TXN_FLAGS & (MDBX_TXN_RW_BEGIN_FLAGS | MDBX_TXN_RO_BEGIN_FLAGS)) ||       \
    ((MDBX_TXN_RW_BEGIN_FLAGS | MDBX_TXN_RO_BEGIN_FLAGS | TXN_FLAGS) &         \
     MDBX_SHRINK_ALLOWED)
#error "Oops, some flags overlapped or wrong"
#endif

  unsigned mt_flags;
  /* The ID of this transaction. IDs are integers incrementing from 1.
   * Only committed write transactions increment the ID. If a transaction
   * aborts, the ID may be re-used by the next writer. */
  txnid_t mt_txnid;
  MDBX_env *mt_env; /* the DB environment */
  /* Array of records for each DB known in the environment. */
  MDBX_dbx *mt_dbxs;
  /* Array of MDBX_db records for each known DB */
  MDBX_db *mt_dbs;
  /* Array of sequence numbers for each DB handle */
  unsigned *mt_dbiseqs;

  /* Transaction DBI Flags */
#define DBI_DIRTY MDBX_DBI_DIRTY /* DB was written in this txn */
#define DBI_STALE MDBX_DBI_STALE /* Named-DB record is older than txnID */
#define DBI_FRESH MDBX_DBI_FRESH /* Named-DB handle opened in this txn */
#define DBI_CREAT MDBX_DBI_CREAT /* Named-DB handle created in this txn */
#define DBI_VALID 0x10           /* DB handle is valid, see also DB_VALID */
#define DBI_USRVALID 0x20        /* As DB_VALID, but not set for FREE_DBI */
#define DBI_DUPDATA 0x40         /* DB is MDBX_DUPSORT data */
#define DBI_AUDITED 0x80         /* Internal flag for accounting during audit */
  /* Array of flags for each DB */
  uint8_t *mt_dbistate;
  /* Number of DB records in use, or 0 when the txn is finished.
   * This number only ever increments until the txn finishes; we
   * don't decrement it when individual DB handles are closed. */
  MDBX_dbi mt_numdbs;
  size_t mt_owner; /* thread ID that owns this transaction */
  MDBX_canary mt_canary;
  void *mt_userctx; /* User-settable context */

  union {
    struct {
      /* For read txns: This thread/txn's reader table slot, or NULL. */
      MDBX_reader *reader;
    } to;
    struct {
      /* In write txns, array of cursors for each DB */
      MDBX_cursor **cursors;
      pgno_t *reclaimed_pglist; /* Reclaimed GC pages */
      txnid_t last_reclaimed;   /* ID of last used record */
#if MDBX_ENABLE_REFUND
      pgno_t loose_refund_wl /* FIXME: describe */;
#endif /* MDBX_ENABLE_REFUND */
      /* dirtylist room: Dirty array size - dirty pages visible to this txn.
       * Includes ancestor txns' dirty pages not hidden by other txns'
       * dirty/spilled pages. Thus commit(nested txn) has room to merge
       * dirtylist into mt_parent after freeing hidden mt_parent pages. */
      unsigned dirtyroom;
      /* For write txns: Modified pages. Sorted when not MDBX_WRITEMAP. */
      MDBX_dpl *dirtylist;
      /* The list of reclaimed txns from GC */
      MDBX_TXL lifo_reclaimed;
      /* The list of pages that became unused during this transaction. */
      MDBX_PNL retired_pages;
      /* The list of loose pages that became unused and may be reused
       * in this transaction, linked through `mp_next`. */
      MDBX_page *loose_pages;
      /* Number of loose pages (tw.loose_pages) */
      unsigned loose_count;
      /* The sorted list of dirty pages we temporarily wrote to disk
       * because the dirty list was full. page numbers in here are
       * shifted left by 1, deleted slots have the LSB set. */
      MDBX_PNL spill_pages;
      unsigned spill_least_removed;
    } tw;
  };
};

#if MDBX_WORDBITS >= 64
#define CURSOR_STACK 32
#else
#define CURSOR_STACK 24
#endif

struct MDBX_xcursor;

/* Cursors are used for all DB operations.
 * A cursor holds a path of (page pointer, key index) from the DB
 * root to a position in the DB, plus other state. MDBX_DUPSORT
 * cursors include an xcursor to the current data item. Write txns
 * track their cursors and keep them up to date when data moves.
 * Exception: An xcursor's pointer to a P_SUBP page can be stale.
 * (A node with F_DUPDATA but no F_SUBDATA contains a subpage). */
struct MDBX_cursor {
#define MDBX_MC_LIVE UINT32_C(0xFE05D5B1)
#define MDBX_MC_READY4CLOSE UINT32_C(0x2817A047)
#define MDBX_MC_WAIT4EOT UINT32_C(0x90E297A7)
  uint32_t mc_signature;
  /* The database handle this cursor operates on */
  MDBX_dbi mc_dbi;
  /* Next cursor on this DB in this txn */
  MDBX_cursor *mc_next;
  /* Backup of the original cursor if this cursor is a shadow */
  MDBX_cursor *mc_backup;
  /* Context used for databases with MDBX_DUPSORT, otherwise NULL */
  struct MDBX_xcursor *mc_xcursor;
  /* The transaction that owns this cursor */
  MDBX_txn *mc_txn;
  /* The database record for this cursor */
  MDBX_db *mc_db;
  /* The database auxiliary record for this cursor */
  MDBX_dbx *mc_dbx;
  /* The mt_dbistate for this database */
  uint8_t *mc_dbistate;
  unsigned mc_snum; /* number of pushed pages */
  unsigned mc_top;  /* index of top page, normally mc_snum-1 */

  /* Cursor state flags. */
#define C_INITIALIZED 0x01 /* cursor has been initialized and is valid */
#define C_EOF 0x02         /* No more data */
#define C_SUB 0x04         /* Cursor is a sub-cursor */
#define C_DEL 0x08         /* last op was a cursor_del */
#define C_UNTRACK 0x10     /* Un-track cursor when closing */
#define C_RECLAIMING 0x20  /* GC lookup is prohibited */
#define C_GCFREEZE 0x40    /* reclaimed_pglist must not be updated */

  /* Cursor checking flags. */
#define C_COPYING 0x100  /* skip key-value length check (copying simplify) */
#define C_UPDATING 0x200 /* update/rebalance pending */
#define C_RETIRING 0x400 /* refs to child pages may be invalid */
#define C_SKIPORD 0x800  /* don't check keys ordering */

  unsigned mc_flags;              /* see mdbx_cursor */
  MDBX_page *mc_pg[CURSOR_STACK]; /* stack of pushed pages */
  indx_t mc_ki[CURSOR_STACK];     /* stack of page indices */
};

/* Context for sorted-dup records.
 * We could have gone to a fully recursive design, with arbitrarily
 * deep nesting of sub-databases. But for now we only handle these
 * levels - main DB, optional sub-DB, sorted-duplicate DB. */
typedef struct MDBX_xcursor {
  /* A sub-cursor for traversing the Dup DB */
  MDBX_cursor mx_cursor;
  /* The database record for this Dup DB */
  MDBX_db mx_db;
  /* The auxiliary DB record for this Dup DB */
  MDBX_dbx mx_dbx;
  /* The mt_dbistate for this Dup DB */
  uint8_t mx_dbistate;
} MDBX_xcursor;

typedef struct MDBX_cursor_couple {
  MDBX_cursor outer;
  void *mc_userctx; /* User-settable context */
  MDBX_xcursor inner;
} MDBX_cursor_couple;

/* The database environment. */
struct MDBX_env {
#define MDBX_ME_SIGNATURE UINT32_C(0x9A899641)
  MDBX_atomic_uint32_t me_signature;
  /* Failed to update the meta page. Probably an I/O error. */
#define MDBX_FATAL_ERROR UINT32_C(0x80000000)
  /* Some fields are initialized. */
#define MDBX_ENV_ACTIVE UINT32_C(0x20000000)
  /* me_txkey is set */
#define MDBX_ENV_TXKEY UINT32_C(0x10000000)
  /* Legacy MDBX_MAPASYNC (prior v0.9) */
#define MDBX_DEPRECATED_MAPASYNC UINT32_C(0x100000)
#define ENV_INTERNAL_FLAGS (MDBX_FATAL_ERROR | MDBX_ENV_ACTIVE | MDBX_ENV_TXKEY)
  uint32_t me_flags;
  mdbx_mmap_t me_dxb_mmap; /* The main data file */
#define me_map me_dxb_mmap.dxb
#define me_lazy_fd me_dxb_mmap.fd
  mdbx_filehandle_t me_dsync_fd;
  mdbx_mmap_t me_lck_mmap; /* The lock file */
#define me_lfd me_lck_mmap.fd
#define me_lck me_lck_mmap.lck

  unsigned me_psize;    /* DB page size, initialized from me_os_psize */
  uint8_t me_psize2log; /* log2 of DB page size */
  int8_t me_stuck_meta; /* recovery-only: target meta page or less that zero */
  unsigned me_os_psize; /* OS page size, from mdbx_syspagesize() */
  unsigned me_maxreaders; /* size of the reader table */
  mdbx_fastmutex_t me_dbi_lock;
  MDBX_dbi me_numdbs;         /* number of DBs opened */
  MDBX_dbi me_maxdbs;         /* size of the DB table */
  uint32_t me_pid;            /* process ID of this env */
  mdbx_thread_key_t me_txkey; /* thread-key for readers */
  char *me_pathname;          /* path to the DB files */
  void *me_pbuf;              /* scratch area for DUPSORT put() */
  MDBX_txn *me_txn;           /* current write transaction */
  MDBX_txn *me_txn0;          /* prealloc'd write transaction */

  /* write-txn lock */
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  union {
    key_t key;
    int semid;
  } me_sysv_ipc;
#endif /* MDBX_LOCKING == MDBX_LOCKING_SYSV */

#if MDBX_LOCKING > 0
  mdbx_ipclock_t *me_wlock;
#endif /* MDBX_LOCKING > 0 */

  MDBX_dbx *me_dbxs;         /* array of static DB info */
  uint16_t *me_dbflags;      /* array of flags from MDBX_db.md_flags */
  unsigned *me_dbiseqs;      /* array of dbi sequence numbers */
  atomic_txnid_t *me_oldest; /* ID of oldest reader last time we looked */
  MDBX_page *me_dp_reserve;  /* list of malloc'd blocks for re-use */
  /* PNL of pages that became unused in a write txn */
  MDBX_PNL me_retired_pages;
  /* Number of freelist items that can fit in a single overflow page */
  unsigned me_maxgc_ov1page;
  unsigned me_branch_nodemax; /* max size of a branch-node */
  uint32_t me_live_reader;    /* have liveness lock in reader table */
  void *me_userctx;           /* User-settable context */
  MDBX_atomic_uint64_t *me_sync_timestamp;
  MDBX_atomic_uint64_t *me_autosync_period;
  atomic_pgno_t *me_unsynced_pages;
  atomic_pgno_t *me_autosync_threshold;
  atomic_pgno_t *me_discarded_tail;
  MDBX_atomic_uint32_t *me_meta_sync_txnid;
  MDBX_hsr_func *me_hsr_callback; /* Callback for kicking laggard readers */
  unsigned me_dp_reserve_len;
  struct {
    unsigned dp_reserve_limit;
    unsigned rp_augment_limit;
    unsigned dp_limit;
    unsigned dp_initial;
    uint8_t dp_loose_limit;
    uint8_t spill_max_denominator;
    uint8_t spill_min_denominator;
    uint8_t spill_parent4child_denominator;
  } me_options;
  struct {
#if MDBX_LOCKING > 0
    mdbx_ipclock_t wlock;
#endif /* MDBX_LOCKING > 0 */
    atomic_txnid_t oldest;
    MDBX_atomic_uint64_t sync_timestamp;
    MDBX_atomic_uint64_t autosync_period;
    atomic_pgno_t autosync_pending;
    atomic_pgno_t autosync_threshold;
    atomic_pgno_t discarded_tail;
    MDBX_atomic_uint32_t meta_sync_txnid;
  } me_lckless_stub;
#if MDBX_DEBUG
  MDBX_assert_func *me_assert_func; /*  Callback for assertion failures */
#endif
#ifdef MDBX_USE_VALGRIND
  int me_valgrind_handle;
#endif
#if defined(MDBX_USE_VALGRIND) || defined(__SANITIZE_ADDRESS__)
  pgno_t me_poison_edge;
#endif /* MDBX_USE_VALGRIND || __SANITIZE_ADDRESS__ */
  MDBX_env *me_lcklist_next;

  /* struct me_dbgeo used for accepting db-geo params from user for the new
   * database creation, i.e. when mdbx_env_set_geometry() was called before
   * mdbx_env_open(). */
  struct {
    size_t lower;  /* minimal size of datafile */
    size_t upper;  /* maximal size of datafile */
    size_t now;    /* current size of datafile */
    size_t grow;   /* step to grow datafile */
    size_t shrink; /* threshold to shrink datafile */
  } me_dbgeo;

#if defined(_WIN32) || defined(_WIN64)
  MDBX_srwlock me_remap_guard;
  /* Workaround for LockFileEx and WriteFile multithread bug */
  CRITICAL_SECTION me_windowsbug_lock;
#else
  mdbx_fastmutex_t me_remap_guard;
#endif
};

#ifndef __cplusplus
/*----------------------------------------------------------------------------*/
/* Debug and Logging stuff */

#define MDBX_RUNTIME_FLAGS_INIT                                                \
  ((MDBX_DEBUG) > 0) * MDBX_DBG_ASSERT + ((MDBX_DEBUG) > 1) * MDBX_DBG_AUDIT

extern uint8_t mdbx_runtime_flags;
extern uint8_t mdbx_loglevel;
extern MDBX_debug_func *mdbx_debug_logger;

MDBX_INTERNAL_FUNC void MDBX_PRINTF_ARGS(4, 5)
    mdbx_debug_log(int level, const char *function, int line, const char *fmt,
                   ...) MDBX_PRINTF_ARGS(4, 5);
MDBX_INTERNAL_FUNC void mdbx_debug_log_va(int level, const char *function,
                                          int line, const char *fmt,
                                          va_list args);

#define mdbx_log_enabled(msg) unlikely(msg <= mdbx_loglevel)

#if MDBX_DEBUG

#define mdbx_assert_enabled() unlikely(mdbx_runtime_flags &MDBX_DBG_ASSERT)

#define mdbx_audit_enabled() unlikely(mdbx_runtime_flags &MDBX_DBG_AUDIT)

#else /* MDBX_DEBUG */

#define mdbx_audit_enabled() (0)

#if !defined(NDEBUG) || defined(MDBX_FORCE_ASSERTIONS)
#define mdbx_assert_enabled() (1)
#else
#define mdbx_assert_enabled() (0)
#endif /* NDEBUG */

#endif /* MDBX_DEBUG */

#if !MDBX_DEBUG && defined(__ANDROID_API__)
#define mdbx_assert_fail(env, msg, func, line)                                 \
  __android_log_assert(msg, "mdbx", "%s:%u", func, line)
#else
void mdbx_assert_fail(const MDBX_env *env, const char *msg, const char *func,
                      int line);
#endif

#define mdbx_debug_extra(fmt, ...)                                             \
  do {                                                                         \
    if (MDBX_DEBUG && mdbx_log_enabled(MDBX_LOG_EXTRA))                        \
      mdbx_debug_log(MDBX_LOG_EXTRA, __func__, __LINE__, fmt, __VA_ARGS__);    \
  } while (0)

#define mdbx_debug_extra_print(fmt, ...)                                       \
  do {                                                                         \
    if (MDBX_DEBUG && mdbx_log_enabled(MDBX_LOG_EXTRA))                        \
      mdbx_debug_log(MDBX_LOG_EXTRA, NULL, 0, fmt, __VA_ARGS__);               \
  } while (0)

#define mdbx_trace(fmt, ...)                                                   \
  do {                                                                         \
    if (MDBX_DEBUG && mdbx_log_enabled(MDBX_LOG_TRACE))                        \
      mdbx_debug_log(MDBX_LOG_TRACE, __func__, __LINE__, fmt "\n",             \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_debug(fmt, ...)                                                   \
  do {                                                                         \
    if (MDBX_DEBUG && mdbx_log_enabled(MDBX_LOG_DEBUG))                        \
      mdbx_debug_log(MDBX_LOG_DEBUG, __func__, __LINE__, fmt "\n",             \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_verbose(fmt, ...)                                                 \
  do {                                                                         \
    if (MDBX_DEBUG && mdbx_log_enabled(MDBX_LOG_VERBOSE))                      \
      mdbx_debug_log(MDBX_LOG_VERBOSE, __func__, __LINE__, fmt "\n",           \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_notice(fmt, ...)                                                  \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_NOTICE))                                     \
      mdbx_debug_log(MDBX_LOG_NOTICE, __func__, __LINE__, fmt "\n",            \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_warning(fmt, ...)                                                 \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_WARN))                                       \
      mdbx_debug_log(MDBX_LOG_WARN, __func__, __LINE__, fmt "\n",              \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_error(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_log_enabled(MDBX_LOG_ERROR))                                      \
      mdbx_debug_log(MDBX_LOG_ERROR, __func__, __LINE__, fmt "\n",             \
                     __VA_ARGS__);                                             \
  } while (0)

#define mdbx_fatal(fmt, ...)                                                   \
  mdbx_debug_log(MDBX_LOG_FATAL, __func__, __LINE__, fmt "\n", __VA_ARGS__);

#define mdbx_ensure_msg(env, expr, msg)                                        \
  do {                                                                         \
    if (unlikely(!(expr)))                                                     \
      mdbx_assert_fail(env, msg, __func__, __LINE__);                          \
  } while (0)

#define mdbx_ensure(env, expr) mdbx_ensure_msg(env, expr, #expr)

/* assert(3) variant in environment context */
#define mdbx_assert(env, expr)                                                 \
  do {                                                                         \
    if (mdbx_assert_enabled())                                                 \
      mdbx_ensure(env, expr);                                                  \
  } while (0)

/* assert(3) variant in cursor context */
#define mdbx_cassert(mc, expr) mdbx_assert((mc)->mc_txn->mt_env, expr)

/* assert(3) variant in transaction context */
#define mdbx_tassert(txn, expr) mdbx_assert((txn)->mt_env, expr)

#ifndef MDBX_TOOLS /* Avoid using internal mdbx_assert() */
#undef assert
#define assert(expr) mdbx_assert(NULL, expr)
#endif

/*----------------------------------------------------------------------------*/
/* Cache coherence and mmap invalidation */

#if MDBX_CPU_WRITEBACK_INCOHERENT
#define mdbx_flush_incoherent_cpu_writeback() mdbx_memory_barrier()
#else
#define mdbx_flush_incoherent_cpu_writeback() mdbx_compiler_barrier()
#endif /* MDBX_CPU_WRITEBACK_INCOHERENT */

static __maybe_unused __inline void
mdbx_flush_incoherent_mmap(void *addr, size_t nbytes, const intptr_t pagesize) {
#if MDBX_MMAP_INCOHERENT_FILE_WRITE
  char *const begin = (char *)(-pagesize & (intptr_t)addr);
  char *const end =
      (char *)(-pagesize & (intptr_t)((char *)addr + nbytes + pagesize - 1));
  int err = msync(begin, end - begin, MS_SYNC | MS_INVALIDATE) ? errno : 0;
  mdbx_assert(nullptr, err == 0);
  (void)err;
#else
  (void)pagesize;
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */

#if MDBX_MMAP_INCOHERENT_CPU_CACHE
#ifdef DCACHE
  /* MIPS has cache coherency issues.
   * Note: for any nbytes >= on-chip cache size, entire is flushed. */
  cacheflush(addr, nbytes, DCACHE);
#else
#error "Oops, cacheflush() not available"
#endif /* DCACHE */
#endif /* MDBX_MMAP_INCOHERENT_CPU_CACHE */

#if !MDBX_MMAP_INCOHERENT_FILE_WRITE && !MDBX_MMAP_INCOHERENT_CPU_CACHE
  (void)addr;
  (void)nbytes;
#endif
}

/*----------------------------------------------------------------------------*/
/* Internal prototypes */

MDBX_INTERNAL_FUNC int mdbx_cleanup_dead_readers(MDBX_env *env, int rlocked,
                                                 int *dead);
MDBX_INTERNAL_FUNC int mdbx_rthc_alloc(mdbx_thread_key_t *key,
                                       MDBX_reader *begin, MDBX_reader *end);
MDBX_INTERNAL_FUNC void mdbx_rthc_remove(const mdbx_thread_key_t key);

MDBX_INTERNAL_FUNC void mdbx_rthc_global_init(void);
MDBX_INTERNAL_FUNC void mdbx_rthc_global_dtor(void);
MDBX_INTERNAL_FUNC void mdbx_rthc_thread_dtor(void *ptr);

static __maybe_unused __inline void mdbx_jitter4testing(bool tiny) {
#if MDBX_DEBUG
  if (MDBX_DBG_JITTER & mdbx_runtime_flags)
    mdbx_osal_jitter(tiny);
#else
  (void)tiny;
#endif
}

#endif /* !__cplusplus */

#define MDBX_IS_ERROR(rc)                                                      \
  ((rc) != MDBX_RESULT_TRUE && (rc) != MDBX_RESULT_FALSE)

/* Internal error codes, not exposed outside libmdbx */
#define MDBX_NO_ROOT (MDBX_LAST_ADDED_ERRCODE + 10)

/* Debugging output value of a cursor DBI: Negative in a sub-cursor. */
#define DDBI(mc)                                                               \
  (((mc)->mc_flags & C_SUB) ? -(int)(mc)->mc_dbi : (int)(mc)->mc_dbi)

/* Key size which fits in a DKBUF. */
#define DKBUF_MAXKEYSIZE 511 /* FIXME */

#if MDBX_DEBUG
#define DKBUF char _kbuf[DKBUF_MAXKEYSIZE * 4 + 2]
#define DKEY(x) mdbx_dump_val(x, _kbuf, DKBUF_MAXKEYSIZE * 2 + 1)
#define DVAL(x)                                                                \
  mdbx_dump_val(x, _kbuf + DKBUF_MAXKEYSIZE * 2 + 1, DKBUF_MAXKEYSIZE * 2 + 1)
#else
#define DKBUF ((void)(0))
#define DKEY(x) ("-")
#define DVAL(x) ("-")
#endif

/* An invalid page number.
 * Mainly used to denote an empty tree. */
#define P_INVALID (~(pgno_t)0)

/* Test if the flags f are set in a flag word w. */
#define F_ISSET(w, f) (((w) & (f)) == (f))

/* Round n up to an even number. */
#define EVEN(n) (((n) + 1U) & -2) /* sign-extending -2 to match n+1U */

/* Default size of memory map.
 * This is certainly too small for any actual applications. Apps should
 * always set  the size explicitly using mdbx_env_set_mapsize(). */
#define DEFAULT_MAPSIZE MEGABYTE

/* Number of slots in the reader table.
 * This value was chosen somewhat arbitrarily. The 61 is a prime number,
 * and such readers plus a couple mutexes fit into single 4KB page.
 * Applications should set the table size using mdbx_env_set_maxreaders(). */
#define DEFAULT_READERS 61

/* Test if a page is a leaf page */
#define IS_LEAF(p) (((p)->mp_flags & P_LEAF) != 0)
/* Test if a page is a LEAF2 page */
#define IS_LEAF2(p) unlikely(((p)->mp_flags & P_LEAF2) != 0)
/* Test if a page is a branch page */
#define IS_BRANCH(p) (((p)->mp_flags & P_BRANCH) != 0)
/* Test if a page is an overflow page */
#define IS_OVERFLOW(p) unlikely(((p)->mp_flags & P_OVERFLOW) != 0)
/* Test if a page is a sub page */
#define IS_SUBP(p) (((p)->mp_flags & P_SUBP) != 0)
/* Test if a page is dirty */
#define IS_DIRTY(p) (((p)->mp_flags & P_DIRTY) != 0)

#define PAGETYPE(p) ((p)->mp_flags & (P_BRANCH | P_LEAF | P_LEAF2 | P_OVERFLOW))

/* Header for a single key/data pair within a page.
 * Used in pages of type P_BRANCH and P_LEAF without P_LEAF2.
 * We guarantee 2-byte alignment for 'MDBX_node's.
 *
 * Leaf node flags describe node contents.  F_BIGDATA says the node's
 * data part is the page number of an overflow page with actual data.
 * F_DUPDATA and F_SUBDATA can be combined giving duplicate data in
 * a sub-page/sub-database, and named databases (just F_SUBDATA). */
typedef struct MDBX_node {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  union {
    uint32_t mn_dsize;
    uint32_t mn_pgno32;
  };
  uint8_t mn_flags; /* see mdbx_node flags */
  uint8_t mn_extra;
  uint16_t mn_ksize; /* key size */
#else
  uint16_t mn_ksize; /* key size */
  uint8_t mn_extra;
  uint8_t mn_flags; /* see mdbx_node flags */
  union {
    uint32_t mn_pgno32;
    uint32_t mn_dsize;
  };
#endif /* __BYTE_ORDER__ */

  /* mdbx_node Flags */
#define F_BIGDATA 0x01 /* data put on overflow page */
#define F_SUBDATA 0x02 /* data is a sub-database */
#define F_DUPDATA 0x04 /* data has duplicates */

  /* valid flags for mdbx_node_add() */
#define NODE_ADD_FLAGS (F_DUPDATA | F_SUBDATA | MDBX_RESERVE | MDBX_APPEND)

#if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L) ||              \
    (!defined(__cplusplus) && defined(_MSC_VER))
  uint8_t mn_data[] /* key and data are appended here */;
#endif /* C99 */
} MDBX_node;

#define DB_PERSISTENT_FLAGS                                                    \
  (MDBX_REVERSEKEY | MDBX_DUPSORT | MDBX_INTEGERKEY | MDBX_DUPFIXED |          \
   MDBX_INTEGERDUP | MDBX_REVERSEDUP)

/* mdbx_dbi_open() flags */
#define DB_USABLE_FLAGS (DB_PERSISTENT_FLAGS | MDBX_CREATE | MDBX_DB_ACCEDE)

#define DB_VALID 0x8000 /* DB handle is valid, for me_dbflags */
#define DB_INTERNAL_FLAGS DB_VALID

#if DB_INTERNAL_FLAGS & DB_USABLE_FLAGS
#error "Oops, some flags overlapped or wrong"
#endif
#if DB_PERSISTENT_FLAGS & ~DB_USABLE_FLAGS
#error "Oops, some flags overlapped or wrong"
#endif

/* max number of pages to commit in one writev() call */
#define MDBX_COMMIT_PAGES 64
#if defined(IOV_MAX) && IOV_MAX < MDBX_COMMIT_PAGES /* sysconf(_SC_IOV_MAX) */
#undef MDBX_COMMIT_PAGES
#define MDBX_COMMIT_PAGES IOV_MAX
#endif

/*
 *                /
 *                | -1, a < b
 * CMP2INT(a,b) = <  0, a == b
 *                |  1, a > b
 *                \
 */
#if 1
/* LY: fast enough on most systems */
#define CMP2INT(a, b) (((b) > (a)) ? -1 : (a) > (b))
#else
#define CMP2INT(a, b) (((a) > (b)) - ((b) > (a)))
#endif

/* Do not spill pages to disk if txn is getting full, may fail instead */
#define MDBX_NOSPILL 0x8000

MDBX_NOTHROW_CONST_FUNCTION static __maybe_unused __inline pgno_t
pgno_add(pgno_t base, pgno_t augend) {
  assert(base <= MAX_PAGENO);
  return (augend < MAX_PAGENO - base) ? base + augend : MAX_PAGENO;
}

MDBX_NOTHROW_CONST_FUNCTION static __maybe_unused __inline pgno_t
pgno_sub(pgno_t base, pgno_t subtrahend) {
  assert(base >= MIN_PAGENO);
  return (subtrahend < base - MIN_PAGENO) ? base - subtrahend : MIN_PAGENO;
}

MDBX_NOTHROW_CONST_FUNCTION static __always_inline __maybe_unused bool
is_powerof2(size_t x) {
  return (x & (x - 1)) == 0;
}

MDBX_NOTHROW_CONST_FUNCTION static __always_inline __maybe_unused size_t
floor_powerof2(size_t value, size_t granularity) {
  assert(is_powerof2(granularity));
  return value & ~(granularity - 1);
}

MDBX_NOTHROW_CONST_FUNCTION static __always_inline __maybe_unused size_t
ceil_powerof2(size_t value, size_t granularity) {
  return floor_powerof2(value + granularity - 1, granularity);
}

/* Only a subset of the mdbx_env flags can be changed
 * at runtime. Changing other flags requires closing the
 * environment and re-opening it with the new flags. */
#define ENV_CHANGEABLE_FLAGS                                                   \
  (MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | MDBX_DEPRECATED_MAPASYNC |             \
   MDBX_NOMEMINIT | MDBX_COALESCE | MDBX_PAGEPERTURB | MDBX_ACCEDE)
#define ENV_CHANGELESS_FLAGS                                                   \
  (MDBX_NOSUBDIR | MDBX_RDONLY | MDBX_WRITEMAP | MDBX_NOTLS | MDBX_NORDAHEAD | \
   MDBX_LIFORECLAIM | MDBX_EXCLUSIVE)
#define ENV_USABLE_FLAGS (ENV_CHANGEABLE_FLAGS | ENV_CHANGELESS_FLAGS)

#if !defined(__cplusplus) || defined(__cpp_constexpr)
static __maybe_unused void static_checks(void) {
  STATIC_ASSERT_MSG(INT16_MAX - CORE_DBS == MDBX_MAX_DBI,
                    "Oops, MDBX_MAX_DBI or CORE_DBS?");
  STATIC_ASSERT_MSG((unsigned)(MDBX_DB_ACCEDE | MDBX_CREATE) ==
                        ((DB_USABLE_FLAGS | DB_INTERNAL_FLAGS) &
                         (ENV_USABLE_FLAGS | ENV_INTERNAL_FLAGS)),
                    "Oops, some flags overlapped or wrong");
  STATIC_ASSERT_MSG((ENV_INTERNAL_FLAGS & ENV_USABLE_FLAGS) == 0,
                    "Oops, some flags overlapped or wrong");
}
#endif /* Disabled for MSVC 19.0 (VisualStudio 2015) */

#ifdef __cplusplus
}
#endif

#include <ctype.h>

#if defined(_WIN32) || defined(_WIN64)
/*
 * POSIX getopt for Windows
 *
 * AT&T Public License
 *
 * Code given out at the 1985 UNIFORUM conference in Dallas.
 */

/*----------------------------------------------------------------------------*/
/* Microsoft compiler generates a lot of warning for self includes... */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                 * mode specified; termination on exception is \
                                 * not guaranteed. Specify /EHsc */
#if !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif
#endif /* _MSC_VER (warnings) */

#include <stdio.h>
#include <string.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif
/*----------------------------------------------------------------------------*/

#ifndef NULL
#define NULL 0
#endif

#ifndef EOF
#define EOF (-1)
#endif

int optind = 1;
int optopt;
char *optarg;

int getopt(int argc, char *const argv[], const char *opts) {
  static int sp = 1;
  int c;
  const char *cp;

  if (sp == 1) {
    if (optind >= argc || argv[optind][0] != '-' || argv[optind][1] == '\0')
      return EOF;
    else if (strcmp(argv[optind], "--") == 0) {
      optind++;
      return EOF;
    }
  }
  optopt = c = argv[optind][sp];
  if (c == ':' || (cp = strchr(opts, c)) == NULL) {
    fprintf(stderr, "%s: %s -- %c\n", argv[0], "illegal option", c);
    if (argv[optind][++sp] == '\0') {
      optind++;
      sp = 1;
    }
    return '?';
  }
  if (*++cp == ':') {
    if (argv[optind][sp + 1] != '\0')
      optarg = &argv[optind++][sp + 1];
    else if (++optind >= argc) {
      fprintf(stderr, "%s: %s -- %c\n", argv[0], "option requires an argument",
              c);
      sp = 1;
      return '?';
    } else
      optarg = argv[optind++];
    sp = 1;
  } else {
    if (argv[optind][++sp] == '\0') {
      sp = 1;
      optind++;
    }
    optarg = NULL;
  }
  return c;
}

static volatile BOOL user_break;
static BOOL WINAPI ConsoleBreakHandlerRoutine(DWORD dwCtrlType) {
  (void)dwCtrlType;
  user_break = true;
  return true;
}

#else /* WINDOWS */

static volatile sig_atomic_t user_break;
static void signal_handler(int sig) {
  (void)sig;
  user_break = 1;
}

#endif /* !WINDOWS */

static char *prog;
static size_t lineno;
static void error(const char *func, int rc) {
  if (lineno)
    fprintf(stderr, "%s: at input line %" PRIiSIZE ": %s() error %d, %s\n",
            prog, lineno, func, rc, mdbx_strerror(rc));
  else
    fprintf(stderr, "%s: %s() error %d %s\n", prog, func, rc,
            mdbx_strerror(rc));
}

static char *valstr(char *line, const char *item) {
  const size_t len = strlen(item);
  if (strncmp(line, item, len) != 0)
    return nullptr;
  if (line[len] != '=') {
    if (line[len] > ' ')
      return nullptr;
    fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected line format for '%s'\n",
            prog, lineno, item);
    exit(EXIT_FAILURE);
  }
  char *ptr = strchr(line, '\n');
  if (ptr)
    *ptr = '\0';
  return line + len + 1;
}

static bool valnum(char *line, const char *item, uint64_t *value) {
  char *str = valstr(line, item);
  if (!str)
    return false;

  char *end = nullptr;
  *value = strtoull(str, &end, 0);
  if (end && *end) {
    fprintf(stderr,
            "%s: line %" PRIiSIZE ": unexpected number format for '%s'\n", prog,
            lineno, item);
    exit(EXIT_FAILURE);
  }
  return true;
}

static bool valbool(char *line, const char *item, bool *value) {
  uint64_t u64;
  if (!valnum(line, item, &u64))
    return false;

  if (u64 > 1) {
    fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected value for '%s'\n", prog,
            lineno, item);
    exit(EXIT_FAILURE);
  }
  *value = u64 != 0;
  return true;
}

/*----------------------------------------------------------------------------*/

static char *subname = nullptr;
static int dbi_flags;
static txnid_t txnid;
static uint64_t sequence;
static MDBX_canary canary;
static MDBX_envinfo envinfo;

#define PRINT 1
#define NOHDR 2
#define GLOBAL 4
static int mode = GLOBAL;

static MDBX_val kbuf, dbuf;

#define STRLENOF(s) (sizeof(s) - 1)

typedef struct flagbit {
  unsigned bit;
  char *name;
  unsigned len;
} flagbit;

#define S(s) s, STRLENOF(s)

flagbit dbflags[] = {
    {MDBX_REVERSEKEY, S("reversekey")}, {MDBX_DUPSORT, S("duplicates")},
    {MDBX_DUPSORT, S("dupsort")},       {MDBX_INTEGERKEY, S("integerkey")},
    {MDBX_DUPFIXED, S("dupfixed")},     {MDBX_INTEGERDUP, S("integerdup")},
    {MDBX_REVERSEDUP, S("reversedup")}, {0, nullptr, 0}};

static int readhdr(void) {
  /* reset parameters */
  if (subname) {
    free(subname);
    subname = nullptr;
  }
  dbi_flags = 0;
  txnid = 0;
  sequence = 0;

  while (true) {
    errno = 0;
    if (fgets(dbuf.iov_base, (int)dbuf.iov_len, stdin) == nullptr)
      return errno ? errno : EOF;
    if (user_break)
      return MDBX_EINTR;

    lineno++;
    uint64_t u64;

    if (valnum(dbuf.iov_base, "VERSION", &u64)) {
      if (u64 != 3) {
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": unsupported value %" PRIu64
                " for %s\n",
                prog, lineno, u64, "VERSION");
        exit(EXIT_FAILURE);
      }
      continue;
    }

    if (valnum(dbuf.iov_base, "db_pagesize", &u64)) {
      if (!(mode & GLOBAL) && envinfo.mi_dxb_pagesize != u64)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore value %" PRIu64
                " for '%s' in non-global context\n",
                prog, lineno, u64, "db_pagesize");
      else if (u64 < MDBX_MIN_PAGESIZE || u64 > MDBX_MAX_PAGESIZE)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore unsupported value %" PRIu64
                " for %s\n",
                prog, lineno, u64, "db_pagesize");
      else
        envinfo.mi_dxb_pagesize = (uint32_t)u64;
      continue;
    }

    char *str = valstr(dbuf.iov_base, "format");
    if (str) {
      if (strcmp(str, "print") == 0) {
        mode |= PRINT;
        continue;
      }
      if (strcmp(str, "bytevalue") == 0) {
        mode &= ~PRINT;
        continue;
      }
      fprintf(stderr, "%s: line %" PRIiSIZE ": unsupported value '%s' for %s\n",
              prog, lineno, str, "format");
      exit(EXIT_FAILURE);
    }

    str = valstr(dbuf.iov_base, "database");
    if (str) {
      if (*str) {
        free(subname);
        subname = mdbx_strdup(str);
        if (!subname) {
          perror("strdup()");
          exit(EXIT_FAILURE);
        }
      }
      continue;
    }

    str = valstr(dbuf.iov_base, "type");
    if (str) {
      if (strcmp(str, "btree") != 0) {
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": unsupported value '%s' for %s\n", prog,
                lineno, str, "type");
        free(subname);
        exit(EXIT_FAILURE);
      }
      continue;
    }

    if (valnum(dbuf.iov_base, "mapaddr", &u64)) {
      if (u64)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                " for %s\n",
                prog, lineno, u64, "mapaddr");
      continue;
    }

    if (valnum(dbuf.iov_base, "mapsize", &u64)) {
      if (!(mode & GLOBAL))
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore value %" PRIu64
                " for '%s' in non-global context\n",
                prog, lineno, u64, "mapsize");
      else if (u64 < MIN_MAPSIZE || u64 > MAX_MAPSIZE64)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                " for %s\n",
                prog, lineno, u64, "mapsize");
      else
        envinfo.mi_mapsize = (size_t)u64;
      continue;
    }

    if (valnum(dbuf.iov_base, "maxreaders", &u64)) {
      if (!(mode & GLOBAL))
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore value %" PRIu64
                " for '%s' in non-global context\n",
                prog, lineno, u64, "maxreaders");
      else if (u64 < 1 || u64 > MDBX_READERS_LIMIT)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                " for %s\n",
                prog, lineno, u64, "maxreaders");
      else
        envinfo.mi_maxreaders = (int)u64;
      continue;
    }

    if (valnum(dbuf.iov_base, "txnid", &u64)) {
      if (u64 < MIN_TXNID || u64 > MAX_TXNID)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                " for %s\n",
                prog, lineno, u64, "txnid");
      txnid = u64;
      continue;
    }

    if (valnum(dbuf.iov_base, "sequence", &u64)) {
      sequence = u64;
      continue;
    }

    str = valstr(dbuf.iov_base, "geometry");
    if (str) {
      if (!(mode & GLOBAL))
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore values %s"
                " for '%s' in non-global context\n",
                prog, lineno, str, "geometry");
      else if (sscanf(str,
                      "l%" PRIu64 ",c%" PRIu64 ",u%" PRIu64 ",s%" PRIu64
                      ",g%" PRIu64,
                      &envinfo.mi_geo.lower, &envinfo.mi_geo.current,
                      &envinfo.mi_geo.upper, &envinfo.mi_geo.shrink,
                      &envinfo.mi_geo.grow) != 5) {
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": unexpected line format for '%s'\n",
                prog, lineno, "geometry");
        exit(EXIT_FAILURE);
      }
      continue;
    }

    str = valstr(dbuf.iov_base, "canary");
    if (str) {
      if (!(mode & GLOBAL))
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": ignore values %s"
                " for '%s' in non-global context\n",
                prog, lineno, str, "canary");
      else if (sscanf(str, "v%" PRIu64 ",x%" PRIu64 ",y%" PRIu64 ",z%" PRIu64,
                      &canary.v, &canary.x, &canary.y, &canary.z) != 4) {
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": unexpected line format for '%s'\n",
                prog, lineno, "canary");
        exit(EXIT_FAILURE);
      }
      continue;
    }

    for (int i = 0; dbflags[i].bit; i++) {
      bool value = false;
      if (valbool(dbuf.iov_base, dbflags[i].name, &value)) {
        if (value)
          dbi_flags |= dbflags[i].bit;
        else
          dbi_flags &= ~dbflags[i].bit;
        goto next;
      }
    }

    str = valstr(dbuf.iov_base, "HEADER");
    if (str) {
      if (strcmp(str, "END") == 0)
        return MDBX_SUCCESS;
    }

    fprintf(stderr,
            "%s: line %" PRIiSIZE ": unrecognized keyword ignored: %s\n", prog,
            lineno, (char *)dbuf.iov_base);
  next:;
  }
  return EOF;
}

static int badend(void) {
  fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected end of input\n", prog,
          lineno);
  return errno ? errno : MDBX_ENODATA;
}

static int unhex(unsigned char *c2) {
  int x, c;
  x = *c2++ & 0x4f;
  if (x & 0x40)
    x -= 55;
  c = x << 4;
  x = *c2 & 0x4f;
  if (x & 0x40)
    x -= 55;
  c |= x;
  return c;
}

static int readline(MDBX_val *out, MDBX_val *buf) {
  unsigned char *c1, *c2, *end;
  size_t len, l2;
  int c;

  if (user_break)
    return MDBX_EINTR;

  errno = 0;
  if (!(mode & NOHDR)) {
    c = fgetc(stdin);
    if (c == EOF)
      return errno ? errno : EOF;
    if (c != ' ') {
      lineno++;
      errno = 0;
      if (fgets(buf->iov_base, (int)buf->iov_len, stdin)) {
        if (c == 'D' && !strncmp(buf->iov_base, "ATA=END", STRLENOF("ATA=END")))
          return EOF;
      }
      return badend();
    }
  }
  if (fgets(buf->iov_base, (int)buf->iov_len, stdin) == nullptr)
    return errno ? errno : EOF;
  lineno++;

  c1 = buf->iov_base;
  len = strlen((char *)c1);
  l2 = len;

  /* Is buffer too short? */
  while (c1[len - 1] != '\n') {
    buf->iov_base = mdbx_realloc(buf->iov_base, buf->iov_len * 2);
    if (!buf->iov_base) {
      fprintf(stderr, "%s: line %" PRIiSIZE ": out of memory, line too long\n",
              prog, lineno);
      return MDBX_ENOMEM;
    }
    c1 = buf->iov_base;
    c1 += l2;
    errno = 0;
    if (fgets((char *)c1, (int)buf->iov_len + 1, stdin) == nullptr)
      return errno ? errno : EOF;
    buf->iov_len *= 2;
    len = strlen((char *)c1);
    l2 += len;
  }
  c1 = c2 = buf->iov_base;
  len = l2;
  c1[--len] = '\0';
  end = c1 + len;

  if (mode & PRINT) {
    while (c2 < end) {
      if (unlikely(*c2 == '\\')) {
        if (c2[1] == '\\') {
          *c1++ = '\\';
        } else {
          if (c2 + 3 > end || !isxdigit(c2[1]) || !isxdigit(c2[2]))
            return badend();
          *c1++ = (char)unhex(++c2);
        }
        c2 += 2;
      } else {
        /* copies are redundant when no escapes were used */
        *c1++ = *c2++;
      }
    }
  } else {
    /* odd length not allowed */
    if (len & 1)
      return badend();
    while (c2 < end) {
      if (!isxdigit(*c2) || !isxdigit(c2[1]))
        return badend();
      *c1++ = (char)unhex(c2);
      c2 += 2;
    }
  }
  c2 = out->iov_base = buf->iov_base;
  out->iov_len = c1 - c2;

  return MDBX_SUCCESS;
}

static void usage(void) {
  fprintf(stderr,
          "usage: %s "
          "[-V] [-q] [-a] [-f file] [-s name] [-N] [-p] [-T] [-r] [-n] dbpath\n"
          "  -V\t\tprint version and exit\n"
          "  -q\t\tbe quiet\n"
          "  -a\t\tappend records in input order (required for custom "
          "comparators)\n"
          "  -f file\tread from file instead of stdin\n"
          "  -s name\tload into named subDB\n"
          "  -N\t\tdon't overwrite existing records when loading, just skip "
          "ones\n"
          "  -p\t\tpurge subDB before loading\n"
          "  -T\t\tread plaintext\n"
          "  -r\t\trescue mode (ignore errors to load corrupted DB dump)\n"
          "  -n\t\tdon't use subdirectory for newly created database "
          "(MDBX_NOSUBDIR)\n",
          prog);
  exit(EXIT_FAILURE);
}

static int equal_or_greater(const MDBX_val *a, const MDBX_val *b) {
  return (a->iov_len == b->iov_len &&
          memcmp(a->iov_base, b->iov_base, a->iov_len) == 0)
             ? 0
             : 1;
}

int main(int argc, char *argv[]) {
  int i, rc;
  MDBX_env *env = nullptr;
  MDBX_txn *txn = nullptr;
  MDBX_cursor *mc = nullptr;
  MDBX_dbi dbi;
  char *envname = nullptr;
  int envflags = MDBX_SAFE_NOSYNC | MDBX_ACCEDE, putflags = MDBX_UPSERT;
  bool quiet = false;
  bool rescue = false;
  bool purge = false;

  prog = argv[0];
  if (argc < 2)
    usage();

  while ((i = getopt(argc, argv,
                     "a"
                     "f:"
                     "n"
                     "s:"
                     "N"
                     "p"
                     "T"
                     "V"
                     "r"
                     "q")) != EOF) {
    switch (i) {
    case 'V':
      printf("mdbx_load version %d.%d.%d.%d\n"
             " - source: %s %s, commit %s, tree %s\n"
             " - anchor: %s\n"
             " - build: %s for %s by %s\n"
             " - flags: %s\n"
             " - options: %s\n",
             mdbx_version.major, mdbx_version.minor, mdbx_version.release,
             mdbx_version.revision, mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_version.git.commit,
             mdbx_version.git.tree, mdbx_sourcery_anchor, mdbx_build.datetime,
             mdbx_build.target, mdbx_build.compiler, mdbx_build.flags,
             mdbx_build.options);
      return EXIT_SUCCESS;
    case 'a':
      putflags |= MDBX_APPEND;
      break;
    case 'f':
      if (freopen(optarg, "r", stdin) == nullptr) {
        fprintf(stderr, "%s: %s: open: %s\n", prog, optarg,
                mdbx_strerror(errno));
        exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 's':
      subname = mdbx_strdup(optarg);
      break;
    case 'N':
      putflags |= MDBX_NOOVERWRITE | MDBX_NODUPDATA;
      break;
    case 'p':
      purge = true;
      break;
    case 'T':
      mode |= NOHDR | PRINT;
      break;
    case 'q':
      quiet = true;
      break;
    case 'r':
      rescue = true;
      break;
    default:
      usage();
    }
  }

  if (optind != argc - 1)
    usage();

#if defined(_WIN32) || defined(_WIN64)
  SetConsoleCtrlHandler(ConsoleBreakHandlerRoutine, true);
#else
#ifdef SIGPIPE
  signal(SIGPIPE, signal_handler);
#endif
#ifdef SIGHUP
  signal(SIGHUP, signal_handler);
#endif
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
#endif /* !WINDOWS */

  envname = argv[optind];
  if (!quiet)
    printf("mdbx_load %s (%s, T-%s)\nRunning for %s...\n",
           mdbx_version.git.describe, mdbx_version.git.datetime,
           mdbx_version.git.tree, envname);
  fflush(nullptr);

  dbuf.iov_len = 4096;
  dbuf.iov_base = mdbx_malloc(dbuf.iov_len);
  if (!dbuf.iov_base) {
    rc = MDBX_ENOMEM;
    error("value-buffer", rc);
    goto env_close;
  }

  /* read first header for mapsize= */
  if (!(mode & NOHDR)) {
    rc = readhdr();
    if (unlikely(rc != MDBX_SUCCESS)) {
      if (rc == EOF)
        rc = MDBX_ENODATA;
      error("readheader", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_create(&env);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_create", rc);
    return EXIT_FAILURE;
  }

  mdbx_env_set_maxdbs(env, 2);
  if (envinfo.mi_maxreaders) {
    rc = mdbx_env_set_maxreaders(env, envinfo.mi_maxreaders);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_maxreaders", rc);
      goto env_close;
    }
  }

  if (envinfo.mi_geo.current | envinfo.mi_mapsize) {
    if (envinfo.mi_geo.current) {
      rc = mdbx_env_set_geometry(
          env, (intptr_t)envinfo.mi_geo.lower, (intptr_t)envinfo.mi_geo.current,
          (intptr_t)envinfo.mi_geo.upper, (intptr_t)envinfo.mi_geo.shrink,
          (intptr_t)envinfo.mi_geo.grow,
          envinfo.mi_dxb_pagesize ? (intptr_t)envinfo.mi_dxb_pagesize : -1);
    } else {
      if (envinfo.mi_mapsize > MAX_MAPSIZE) {
        fprintf(
            stderr,
            "Database size is too large for current system (mapsize=%" PRIu64
            " is great than system-limit %zu)\n",
            envinfo.mi_mapsize, (size_t)MAX_MAPSIZE);
        goto env_close;
      }
      rc = mdbx_env_set_geometry(
          env, (intptr_t)envinfo.mi_mapsize, (intptr_t)envinfo.mi_mapsize,
          (intptr_t)envinfo.mi_mapsize, 0, 0,
          envinfo.mi_dxb_pagesize ? (intptr_t)envinfo.mi_dxb_pagesize : -1);
    }
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_geometry", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_open(env, envname, envflags, 0664);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_open", rc);
    goto env_close;
  }

  kbuf.iov_len = mdbx_env_get_maxvalsize_ex(env, 0) + 1;
  if (kbuf.iov_len >= INTPTR_MAX / 2) {
    fprintf(stderr, "mdbx_env_get_maxkeysize() failed, returns %zu\n",
            kbuf.iov_len);
    goto env_close;
  }

  kbuf.iov_base = malloc(kbuf.iov_len);
  if (!kbuf.iov_base) {
    rc = MDBX_ENOMEM;
    error("key-buffer", rc);
    goto env_close;
  }

  while (rc == MDBX_SUCCESS) {
    if (user_break) {
      rc = MDBX_EINTR;
      break;
    }

    rc = mdbx_txn_begin(env, nullptr, 0, &txn);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_txn_begin", rc);
      goto env_close;
    }

    if (mode & GLOBAL) {
      mode -= GLOBAL;
      if (canary.v | canary.x | canary.y | canary.z) {
        rc = mdbx_canary_put(txn, &canary);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_canary_put", rc);
          goto txn_abort;
        }
      }
    }

    const char *const dbi_name = subname ? subname : "@MAIN";
    rc =
        mdbx_dbi_open_ex(txn, subname, dbi_flags | MDBX_CREATE, &dbi,
                         (putflags & MDBX_APPEND) ? equal_or_greater : nullptr,
                         (putflags & MDBX_APPEND) ? equal_or_greater : nullptr);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_dbi_open_ex", rc);
      goto txn_abort;
    }

    uint64_t present_sequence;
    rc = mdbx_dbi_sequence(txn, dbi, &present_sequence, 0);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_dbi_sequence", rc);
      goto txn_abort;
    }
    if (present_sequence > sequence) {
      fprintf(stderr,
              "present sequence for '%s' value (%" PRIu64
              ") is greater than loaded (%" PRIu64 ")\n",
              dbi_name, present_sequence, sequence);
      rc = MDBX_RESULT_TRUE;
      goto txn_abort;
    }
    if (present_sequence < sequence) {
      rc = mdbx_dbi_sequence(txn, dbi, nullptr, sequence - present_sequence);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_dbi_sequence", rc);
        goto txn_abort;
      }
    }

    if (purge) {
      rc = mdbx_drop(txn, dbi, false);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_drop", rc);
        goto txn_abort;
      }
    }

    if (putflags & MDBX_APPEND)
      putflags = (dbi_flags & MDBX_DUPSORT) ? putflags | MDBX_APPENDDUP
                                            : putflags & ~MDBX_APPENDDUP;

    rc = mdbx_cursor_open(txn, dbi, &mc);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_cursor_open", rc);
      goto txn_abort;
    }

    int batch = 0;
    while (rc == MDBX_SUCCESS) {
      MDBX_val key, data;
      rc = readline(&key, &kbuf);
      if (rc == EOF)
        break;

      if (rc == MDBX_SUCCESS)
        rc = readline(&data, &dbuf);
      if (rc) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": failed to read key value\n",
                prog, lineno);
        goto txn_abort;
      }

      rc = mdbx_cursor_put(mc, &key, &data, putflags);
      if (rc == MDBX_KEYEXIST && putflags)
        continue;
      if (rc == MDBX_BAD_VALSIZE && rescue) {
        fprintf(stderr, "%s: skip line %" PRIiSIZE ": due %s\n", prog, lineno,
                mdbx_strerror(rc));
        continue;
      }
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_cursor_put", rc);
        goto txn_abort;
      }
      batch++;

      MDBX_txn_info txn_info;
      rc = mdbx_txn_info(txn, &txn_info, false);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_txn_info", rc);
        goto txn_abort;
      }

      if (batch == 10000 || txn_info.txn_space_dirty > MEGABYTE * 256) {
        rc = mdbx_txn_commit(txn);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_txn_commit", rc);
          goto env_close;
        }
        batch = 0;

        rc = mdbx_txn_begin(env, nullptr, 0, &txn);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_txn_begin", rc);
          goto env_close;
        }
        rc = mdbx_cursor_bind(txn, mc, dbi);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_cursor_bind", rc);
          goto txn_abort;
        }
      }
    }

    mdbx_cursor_close(mc);
    mc = nullptr;
    rc = mdbx_txn_commit(txn);
    txn = nullptr;
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_txn_commit", rc);
      goto env_close;
    }
    if (subname) {
      assert(dbi != MAIN_DBI);
      rc = mdbx_dbi_close(env, dbi);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_dbi_close", rc);
        goto env_close;
      }
    } else {
      assert(dbi == MAIN_DBI);
    }

    /* try read next header */
    if (!(mode & NOHDR))
      rc = readhdr();
    else if (ferror(stdin) || feof(stdin))
      break;
  }

  switch (rc) {
  case EOF:
    rc = MDBX_SUCCESS;
  case MDBX_SUCCESS:
    break;
  case MDBX_EINTR:
    fprintf(stderr, "Interrupted by signal/user\n");
    break;
  default:
    if (unlikely(rc != MDBX_SUCCESS))
      error("readline", rc);
  }

txn_abort:
  mdbx_cursor_close(mc);
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);
  free(kbuf.iov_base);
  free(dbuf.iov_base);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
