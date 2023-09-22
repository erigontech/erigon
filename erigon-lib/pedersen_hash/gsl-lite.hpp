//
// gsl-lite is based on GSL: Guideline Support Library.
// For more information see https://github.com/martinmoene/gsl-lite
//
// Copyright (c) 2015-2018 Martin Moene
// Copyright (c) 2015-2018 Microsoft Corporation. All rights reserved.
//
// This code is licensed under the MIT License (MIT).
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#pragma once

#ifndef GSL_GSL_LITE_HPP_INCLUDED
#define GSL_GSL_LITE_HPP_INCLUDED

#include <algorithm>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#define  gsl_lite_MAJOR  0
#define  gsl_lite_MINOR  32
#define  gsl_lite_PATCH  0
#define  gsl_lite_VERSION  gsl_STRINGIFY(gsl_lite_MAJOR) "." gsl_STRINGIFY(gsl_lite_MINOR) "." gsl_STRINGIFY(gsl_lite_PATCH)

// gsl-lite backward compatibility:

#ifdef gsl_CONFIG_ALLOWS_SPAN_CONTAINER_CTOR
# define gsl_CONFIG_ALLOWS_UNCONSTRAINED_SPAN_CONTAINER_CTOR  gsl_CONFIG_ALLOWS_SPAN_CONTAINER_CTOR
# pragma message ("gsl_CONFIG_ALLOWS_SPAN_CONTAINER_CTOR is deprecated since gsl-lite 0.7.0; replace with gsl_CONFIG_ALLOWS_UNCONSTRAINED_SPAN_CONTAINER_CTOR, or consider span(with_container, cont).")
#endif

// M-GSL compatibility:

#if defined( GSL_THROW_ON_CONTRACT_VIOLATION )
# define gsl_CONFIG_CONTRACT_VIOLATION_THROWS  1
#endif

#if defined( GSL_TERMINATE_ON_CONTRACT_VIOLATION )
# define gsl_CONFIG_CONTRACT_VIOLATION_THROWS  0
#endif

#if defined( GSL_UNENFORCED_ON_CONTRACT_VIOLATION )
# define gsl_CONFIG_CONTRACT_LEVEL_OFF  1
#endif

// Configuration: Features

#ifndef  gsl_FEATURE_WITH_CONTAINER_TO_STD
# define gsl_FEATURE_WITH_CONTAINER_TO_STD  99
#endif

#ifndef  gsl_FEATURE_MAKE_SPAN_TO_STD
# define gsl_FEATURE_MAKE_SPAN_TO_STD  99
#endif

#ifndef  gsl_FEATURE_BYTE_SPAN_TO_STD
# define gsl_FEATURE_BYTE_SPAN_TO_STD  99
#endif

#ifndef  gsl_FEATURE_HAVE_IMPLICIT_MACRO
# define gsl_FEATURE_HAVE_IMPLICIT_MACRO  1
#endif

#ifndef  gsl_FEATURE_HAVE_OWNER_MACRO
# define gsl_FEATURE_HAVE_OWNER_MACRO  1
#endif

#ifndef  gsl_FEATURE_EXPERIMENTAL_RETURN_GUARD
# define gsl_FEATURE_EXPERIMENTAL_RETURN_GUARD  0
#endif

// Configuration: Other

#ifndef  gsl_CONFIG_DEPRECATE_TO_LEVEL
# define gsl_CONFIG_DEPRECATE_TO_LEVEL  0
#endif

#ifndef  gsl_CONFIG_SPAN_INDEX_TYPE
# define gsl_CONFIG_SPAN_INDEX_TYPE  size_t
#endif

#ifndef  gsl_CONFIG_NOT_NULL_EXPLICIT_CTOR
# define gsl_CONFIG_NOT_NULL_EXPLICIT_CTOR  0
#endif

#ifndef  gsl_CONFIG_NOT_NULL_GET_BY_CONST_REF
# define gsl_CONFIG_NOT_NULL_GET_BY_CONST_REF  0
#endif

#ifndef  gsl_CONFIG_CONFIRMS_COMPILATION_ERRORS
# define gsl_CONFIG_CONFIRMS_COMPILATION_ERRORS  0
#endif

#ifndef  gsl_CONFIG_ALLOWS_NONSTRICT_SPAN_COMPARISON
# define gsl_CONFIG_ALLOWS_NONSTRICT_SPAN_COMPARISON  1
#endif

#ifndef  gsl_CONFIG_ALLOWS_UNCONSTRAINED_SPAN_CONTAINER_CTOR
# define gsl_CONFIG_ALLOWS_UNCONSTRAINED_SPAN_CONTAINER_CTOR  0
#endif

#if    defined( gsl_CONFIG_CONTRACT_LEVEL_ON )
# define        gsl_CONFIG_CONTRACT_LEVEL_MASK  0x11
#elif  defined( gsl_CONFIG_CONTRACT_LEVEL_OFF )
# define        gsl_CONFIG_CONTRACT_LEVEL_MASK  0x00
#elif  defined( gsl_CONFIG_CONTRACT_LEVEL_EXPECTS_ONLY )
# define        gsl_CONFIG_CONTRACT_LEVEL_MASK  0x01
#elif  defined( gsl_CONFIG_CONTRACT_LEVEL_ENSURES_ONLY )
# define        gsl_CONFIG_CONTRACT_LEVEL_MASK  0x10
#else
# define        gsl_CONFIG_CONTRACT_LEVEL_MASK  0x11
#endif

#if   !defined( gsl_CONFIG_CONTRACT_VIOLATION_THROWS     ) && \
      !defined( gsl_CONFIG_CONTRACT_VIOLATION_TERMINATES )
# define        gsl_CONFIG_CONTRACT_VIOLATION_THROWS_V 0
#elif  defined( gsl_CONFIG_CONTRACT_VIOLATION_THROWS     ) && \
      !defined( gsl_CONFIG_CONTRACT_VIOLATION_TERMINATES )
# define        gsl_CONFIG_CONTRACT_VIOLATION_THROWS_V 1
#elif !defined( gsl_CONFIG_CONTRACT_VIOLATION_THROWS     ) && \
       defined( gsl_CONFIG_CONTRACT_VIOLATION_TERMINATES )
# define        gsl_CONFIG_CONTRACT_VIOLATION_THROWS_V 0
#else
# error only one of gsl_CONFIG_CONTRACT_VIOLATION_THROWS and gsl_CONFIG_CONTRACT_VIOLATION_TERMINATES may be defined.
#endif

// C++ language version detection (C++20 is speculative):
// Note: VC14.0/1900 (VS2015) lacks too much from C++14.

#ifndef   gsl_CPLUSPLUS
# ifdef  _MSVC_LANG
#  define gsl_CPLUSPLUS  (_MSC_VER == 1900 ? 201103L : _MSVC_LANG )
# else
#  define gsl_CPLUSPLUS  __cplusplus
# endif
#endif

#define gsl_CPP98_OR_GREATER  ( gsl_CPLUSPLUS >= 199711L )
#define gsl_CPP11_OR_GREATER  ( gsl_CPLUSPLUS >= 201103L )
#define gsl_CPP14_OR_GREATER  ( gsl_CPLUSPLUS >= 201402L )
#define gsl_CPP17_OR_GREATER  ( gsl_CPLUSPLUS >= 201703L )
#define gsl_CPP20_OR_GREATER  ( gsl_CPLUSPLUS >= 202000L )

// C++ language version (represent 98 as 3):

#define gsl_CPLUSPLUS_V  ( gsl_CPLUSPLUS / 100 - (gsl_CPLUSPLUS > 200000 ? 2000 : 1994) )

// half-open range [lo..hi):
#define gsl_BETWEEN( v, lo, hi ) ( (lo) <= (v) && (v) < (hi) )

#if defined( _MSC_VER ) && !defined( __clang__ )
# define gsl_COMPILER_MSVC_VERSION ( _MSC_VER / 10 - 10 * ( 5 + ( _MSC_VER < 1900 ) ) )
#else
# define gsl_COMPILER_MSVC_VERSION 0
#endif

#define gsl_COMPILER_VERSION( major, minor, patch ) ( 10 * ( 10 * (major) + (minor) ) + (patch) )

#if defined __clang__
# define gsl_COMPILER_CLANG_VERSION gsl_COMPILER_VERSION( __clang_major__, __clang_minor__, __clang_patchlevel__ )
#else
# define gsl_COMPILER_CLANG_VERSION 0
#endif

#if defined __GNUC__
# define gsl_COMPILER_GNUC_VERSION gsl_COMPILER_VERSION( __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__ )
#else
# define gsl_COMPILER_GNUC_VERSION 0
#endif

// Compiler non-strict aliasing:

#if defined __clang__ || defined __GNUC__
# define gsl_may_alias  __attribute__((__may_alias__))
#else
# define gsl_may_alias
#endif

// Presence of gsl, language and library features:

#define gsl_IN_STD( v )  ( (v) == 98 || (v) >= gsl_CPLUSPLUS_V )

#define gsl_DEPRECATE_TO_LEVEL( level )  ( level <= gsl_CONFIG_DEPRECATE_TO_LEVEL )
#define gsl_FEATURE_TO_STD(   feature )  ( gsl_IN_STD( gsl_FEATURE( feature##_TO_STD ) ) )
#define gsl_FEATURE(          feature )  ( gsl_FEATURE_##feature )
#define gsl_CONFIG(           feature )  ( gsl_CONFIG_##feature )
#define gsl_HAVE(             feature )  ( gsl_HAVE_##feature )

// Presence of wide character support:

#ifdef __DJGPP__
# define gsl_HAVE_WCHAR 0
#else
# define gsl_HAVE_WCHAR 1
#endif

// Presence of language & library features:

#ifdef _HAS_CPP0X
# define gsl_HAS_CPP0X  _HAS_CPP0X
#else
# define gsl_HAS_CPP0X  0
#endif

#define gsl_CPP11_100  (gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 100)
#define gsl_CPP11_110  (gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 110)
#define gsl_CPP11_120  (gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 120)
#define gsl_CPP11_140  (gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 140)

#define gsl_CPP14_000  (gsl_CPP14_OR_GREATER)
#define gsl_CPP14_120  (gsl_CPP14_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 120)
#define gsl_CPP14_140  (gsl_CPP14_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 140)

#define gsl_CPP17_000  (gsl_CPP17_OR_GREATER)
#define gsl_CPP17_140  (gsl_CPP17_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 140)

#define gsl_CPP11_140_CPP0X_90   (gsl_CPP11_140 || (gsl_COMPILER_MSVC_VERSION >=  90 && gsl_HAS_CPP0X))
#define gsl_CPP11_140_CPP0X_100  (gsl_CPP11_140 || (gsl_COMPILER_MSVC_VERSION >= 100 && gsl_HAS_CPP0X))

// Presence of C++11 language features:

#define gsl_HAVE_AUTO                   gsl_CPP11_100
#define gsl_HAVE_NULLPTR                gsl_CPP11_100
#define gsl_HAVE_RVALUE_REFERENCE       gsl_CPP11_100

#define gsl_HAVE_ENUM_CLASS             gsl_CPP11_110

#define gsl_HAVE_ALIAS_TEMPLATE         gsl_CPP11_120
#define gsl_HAVE_DEFAULT_FUNCTION_TEMPLATE_ARG  gsl_CPP11_120
#define gsl_HAVE_EXPLICIT               gsl_CPP11_120
#define gsl_HAVE_INITIALIZER_LIST       gsl_CPP11_120

#define gsl_HAVE_CONSTEXPR_11           gsl_CPP11_140
#define gsl_HAVE_IS_DEFAULT             gsl_CPP11_140
#define gsl_HAVE_IS_DELETE              gsl_CPP11_140
#define gsl_HAVE_NOEXCEPT               gsl_CPP11_140

#if gsl_CPP11_OR_GREATER
// see above
#endif

// Presence of C++14 language features:

#define gsl_HAVE_CONSTEXPR_14           gsl_CPP14_000
#define gsl_HAVE_DECLTYPE_AUTO          gsl_CPP14_140

// Presence of C++17 language features:
// MSVC: template parameter deduction guides since Visual Studio 2017 v15.7

#define gsl_HAVE_ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE  gsl_CPP17_000
#define gsl_HAVE_DEDUCTION_GUIDES      (gsl_CPP17_000 && ! gsl_BETWEEN( gsl_COMPILER_MSVC_VERSION, 1, 999 ) )

// Presence of C++ library features:

#define gsl_HAVE_ADDRESSOF              gsl_CPP17_000
#define gsl_HAVE_ARRAY                  gsl_CPP11_110
#define gsl_HAVE_TYPE_TRAITS            gsl_CPP11_110
#define gsl_HAVE_TR1_TYPE_TRAITS        gsl_CPP11_110

#define gsl_HAVE_CONTAINER_DATA_METHOD  gsl_CPP11_140_CPP0X_90
#define gsl_HAVE_STD_DATA               gsl_CPP17_000

#define gsl_HAVE_SIZED_TYPES            gsl_CPP11_140

#define gsl_HAVE_MAKE_SHARED            gsl_CPP11_140_CPP0X_100
#define gsl_HAVE_SHARED_PTR             gsl_CPP11_140_CPP0X_100
#define gsl_HAVE_UNIQUE_PTR             gsl_CPP11_140_CPP0X_100

#define gsl_HAVE_MAKE_UNIQUE            gsl_CPP14_120

#define gsl_HAVE_UNCAUGHT_EXCEPTIONS    gsl_CPP17_140

#define gsl_HAVE_ADD_CONST              gsl_HAVE_TYPE_TRAITS
#define gsl_HAVE_INTEGRAL_CONSTANT      gsl_HAVE_TYPE_TRAITS
#define gsl_HAVE_REMOVE_CONST           gsl_HAVE_TYPE_TRAITS
#define gsl_HAVE_REMOVE_REFERENCE       gsl_HAVE_TYPE_TRAITS

#define gsl_HAVE_TR1_ADD_CONST          gsl_HAVE_TR1_TYPE_TRAITS
#define gsl_HAVE_TR1_INTEGRAL_CONSTANT  gsl_HAVE_TR1_TYPE_TRAITS
#define gsl_HAVE_TR1_REMOVE_CONST       gsl_HAVE_TR1_TYPE_TRAITS
#define gsl_HAVE_TR1_REMOVE_REFERENCE   gsl_HAVE_TR1_TYPE_TRAITS

// C++ feature usage:

#if gsl_HAVE( ADDRESSOF )
# define gsl_ADDRESSOF(x)  std::addressof(x)
#else
# define gsl_ADDRESSOF(x)  (&x)
#endif

#if gsl_HAVE( CONSTEXPR_11 )
# define gsl_constexpr constexpr
#else
# define gsl_constexpr /*constexpr*/
#endif

#if gsl_HAVE( CONSTEXPR_14 )
# define gsl_constexpr14 constexpr
#else
# define gsl_constexpr14 /*constexpr*/
#endif

#if gsl_HAVE( EXPLICIT )
# define gsl_explicit explicit
#else
# define gsl_explicit /*explicit*/
#endif

#if gsl_FEATURE( HAVE_IMPLICIT_MACRO )
# define implicit /*implicit*/
#endif

#if gsl_HAVE( IS_DELETE )
# define gsl_is_delete = delete
#else
# define gsl_is_delete
#endif

#if gsl_HAVE( IS_DELETE )
# define gsl_is_delete_access public
#else
# define gsl_is_delete_access private
#endif

#if !gsl_HAVE( NOEXCEPT ) || gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )
# define gsl_noexcept /*noexcept*/
#else
# define gsl_noexcept noexcept
#endif

#if gsl_HAVE( NULLPTR )
# define gsl_nullptr  nullptr
#else
# define gsl_nullptr  NULL
#endif

#define gsl_DIMENSION_OF( a ) ( sizeof(a) / sizeof(0[a]) )

// Other features:

#define gsl_HAVE_CONSTRAINED_SPAN_CONTAINER_CTOR  \
    ( gsl_HAVE_DEFAULT_FUNCTION_TEMPLATE_ARG && gsl_HAVE_CONTAINER_DATA_METHOD )

// Note: !defined(__NVCC__) doesn't work with nvcc here:
#define gsl_HAVE_UNCONSTRAINED_SPAN_CONTAINER_CTOR  \
    ( gsl_CONFIG_ALLOWS_UNCONSTRAINED_SPAN_CONTAINER_CTOR && (__NVCC__== 0) )

// GSL API (e.g. for CUDA platform):

#ifndef   gsl_api
# ifdef   __CUDACC__
#  define gsl_api __host__ __device__
# else
#  define gsl_api /*gsl_api*/
# endif
#endif

// Additional includes:

#if gsl_HAVE( ARRAY )
# include <array>
#endif

#if gsl_HAVE( TYPE_TRAITS )
# include <type_traits>
#elif gsl_HAVE( TR1_TYPE_TRAITS )
# include <tr1/type_traits>
#endif

#if gsl_HAVE( SIZED_TYPES )
# include <cstdint>
#endif

// MSVC warning suppression macros:

#if gsl_COMPILER_MSVC_VERSION >= 140
# define gsl_SUPPRESS_MSGSL_WARNING(expr)        [[gsl::suppress(expr)]]
# define gsl_SUPPRESS_MSVC_WARNING(code, descr)  __pragma(warning(suppress: code) )
# define gsl_DISABLE_MSVC_WARNINGS(codes)        __pragma(warning(push))  __pragma(warning(disable: codes))
# define gsl_RESTORE_MSVC_WARNINGS()             __pragma(warning(pop ))
#else
# define gsl_SUPPRESS_MSGSL_WARNING(expr)
# define gsl_SUPPRESS_MSVC_WARNING(code, descr)
# define gsl_DISABLE_MSVC_WARNINGS(codes)
# define gsl_RESTORE_MSVC_WARNINGS()
#endif

// Suppress the following MSVC GSL warnings:
// - C26410: gsl::r.32: the parameter 'ptr' is a reference to const unique pointer, use const T* or const T& instead
// - C26415: gsl::r.30: smart pointer parameter 'ptr' is used only to access contained pointer. Use T* or T& instead
// - C26418: gsl::r.36: shared pointer parameter 'ptr' is not copied or moved. Use T* or T& instead
// - C26472, gsl::t.1 : don't use a static_cast for arithmetic conversions;
//                      use brace initialization, gsl::narrow_cast or gsl::narow
// - C26439, gsl::f.6 : special function 'function' can be declared 'noexcept'
// - C26440, gsl::f.6 : function 'function' can be declared 'noexcept'
// - C26473: gsl::t.1 : don't cast between pointer types where the source type and the target type are the same
// - C26481: gsl::b.1 : don't use pointer arithmetic. Use span instead
// - C26482, gsl::b.2 : only index into arrays using constant expressions
// - C26490: gsl::t.1 : don't use reinterpret_cast

gsl_DISABLE_MSVC_WARNINGS( 26410 26415 26418 26472 26439 26440 26473 26481 26482 26490 )

namespace gsl {

// forward declare span<>:

template< class T >
class span;

namespace details {

// C++11 emulation:

#if gsl_HAVE( ADD_CONST )

using std::add_const;

#elif gsl_HAVE( TR1_ADD_CONST )

using std::tr1::add_const;

#else

template< class T > struct add_const { typedef const T type; };

#endif // gsl_HAVE( ADD_CONST )

#if gsl_HAVE( REMOVE_CONST )

using std::remove_cv;
using std::remove_const;
using std::remove_volatile;

#elif gsl_HAVE( TR1_REMOVE_CONST )

using std::tr1::remove_cv;
using std::tr1::remove_const;
using std::tr1::remove_volatile;

#else

template< class T > struct remove_const          { typedef T type; };
template< class T > struct remove_const<T const> { typedef T type; };

template< class T > struct remove_volatile             { typedef T type; };
template< class T > struct remove_volatile<T volatile> { typedef T type; };

template< class T >
struct remove_cv
{
    typedef typename details::remove_volatile<typename details::remove_const<T>::type>::type type;
};

#endif // gsl_HAVE( REMOVE_CONST )

#if gsl_HAVE( INTEGRAL_CONSTANT )

using std::integral_constant;
using std::true_type;
using std::false_type;

#elif gsl_HAVE( TR1_INTEGRAL_CONSTANT )

using std::tr1::integral_constant;
using std::tr1::true_type;
using std::tr1::false_type;

#else

template< int v > struct integral_constant { enum { value = v }; };
typedef integral_constant< true  > true_type;
typedef integral_constant< false > false_type;

#endif

#if gsl_HAVE( TYPE_TRAITS )

template< class Q >
struct is_span_oracle : std::false_type{};

template< class T>
struct is_span_oracle< span<T> > : std::true_type{};

template< class Q >
struct is_span : is_span_oracle< typename std::remove_cv<Q>::type >{};

template< class Q >
struct is_std_array_oracle : std::false_type{};

#if gsl_HAVE( ARRAY )

template< class T, std::size_t Extent >
struct is_std_array_oracle< std::array<T, Extent> > : std::true_type{};

#endif

template< class Q >
struct is_std_array : is_std_array_oracle< typename std::remove_cv<Q>::type >{};

template< class Q >
struct is_array : std::false_type {};

template< class T >
struct is_array<T[]> : std::true_type {};

template< class T, std::size_t N >
struct is_array<T[N]> : std::true_type {};

#endif // gsl_HAVE( TYPE_TRAITS )

} // namespace details

//
// GSL.util: utilities
//

// index type for all container indexes/subscripts/sizes
typedef gsl_CONFIG_SPAN_INDEX_TYPE index;   // p0122r3 uses std::ptrdiff_t

//
// GSL.owner: ownership pointers
//
#if gsl_HAVE( SHARED_PTR )
  using std::unique_ptr;
  using std::shared_ptr;
  using std::make_shared;
# if gsl_HAVE( MAKE_UNIQUE )
  using std::make_unique;
# endif
#endif

#if  gsl_HAVE( ALIAS_TEMPLATE )
# if gsl_HAVE( TYPE_TRAITS )
  template< class T, class = typename std::enable_if< std::is_pointer<T>::value >::type >
  using owner = T;
# else
  template< class T > using owner = T;
# endif
#else
  template< class T > struct owner { typedef T type; };
#endif

#define gsl_HAVE_OWNER_TEMPLATE  gsl_HAVE_ALIAS_TEMPLATE

#if gsl_FEATURE( HAVE_OWNER_MACRO )
# if gsl_HAVE( OWNER_TEMPLATE )
#  define Owner(t)  ::gsl::owner<t>
# else
#  define Owner(t)  ::gsl::owner<t>::type
# endif
#endif

//
// GSL.assert: assertions
//

#define gsl_ELIDE_CONTRACT_EXPECTS  ( 0 == ( gsl_CONFIG_CONTRACT_LEVEL_MASK & 0x01 ) )
#define gsl_ELIDE_CONTRACT_ENSURES  ( 0 == ( gsl_CONFIG_CONTRACT_LEVEL_MASK & 0x10 ) )

#if gsl_ELIDE_CONTRACT_EXPECTS
# define Expects( x )  /* Expects elided */
#elif gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )
# define Expects( x )  ::gsl::fail_fast_assert( (x), "GSL: Precondition failure at " __FILE__ ":" gsl_STRINGIFY(__LINE__) );
#else
# define Expects( x )  ::gsl::fail_fast_assert( (x) )
#endif

#if gsl_ELIDE_CONTRACT_EXPECTS
# define gsl_EXPECTS_UNUSED_PARAM( x )  /* Make param unnamed if Expects elided */
#else
# define gsl_EXPECTS_UNUSED_PARAM( x )  x
#endif

#if gsl_ELIDE_CONTRACT_ENSURES
# define Ensures( x )  /* Ensures elided */
#elif gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )
# define Ensures( x )  ::gsl::fail_fast_assert( (x), "GSL: Postcondition failure at " __FILE__ ":" gsl_STRINGIFY(__LINE__) );
#else
# define Ensures( x )  ::gsl::fail_fast_assert( (x) )
#endif

#define gsl_STRINGIFY(  x )  gsl_STRINGIFY_( x )
#define gsl_STRINGIFY_( x )  #x

struct fail_fast : public std::logic_error
{
    gsl_api explicit fail_fast( char const * const message )
    : std::logic_error( message ) {}
};

// workaround for gcc 5 throw/terminate constexpr bug:

#if gsl_BETWEEN( gsl_COMPILER_GNUC_VERSION, 430, 600 ) && gsl_HAVE( CONSTEXPR_14 )

# if gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )

gsl_api inline gsl_constexpr14 auto fail_fast_assert( bool cond, char const * const message ) -> void
{
    !cond ? throw fail_fast( message ) : 0;
}

# else

gsl_api inline gsl_constexpr14 auto fail_fast_assert( bool cond ) -> void
{
    struct F { static gsl_constexpr14 void f(){}; };

    !cond ? std::terminate() : F::f();
}

# endif

#else // workaround

# if gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )

gsl_api inline gsl_constexpr14 void fail_fast_assert( bool cond, char const * const message )
{
    if ( !cond )
        throw fail_fast( message );
}

# else

gsl_api inline gsl_constexpr14 void fail_fast_assert( bool cond ) gsl_noexcept
{
    if ( !cond )
        std::terminate();
}

# endif
#endif // workaround

//
// GSL.util: utilities
//

#if gsl_FEATURE( EXPERIMENTAL_RETURN_GUARD )

// Add uncaught_exceptions for pre-2017 MSVC, GCC and Clang
// Return unsigned char to save stack space, uncaught_exceptions can only increase by 1 in a scope

namespace details {

inline unsigned char to_uchar( unsigned x ) gsl_noexcept
{
    return static_cast<unsigned char>( x );
}

#if gsl_HAVE( UNCAUGHT_EXCEPTIONS )

inline unsigned char uncaught_exceptions() gsl_noexcept
{
    return to_uchar( std::uncaught_exceptions() );
}

#elif gsl_COMPILER_MSVC_VERSION

extern "C" char * __cdecl _getptd();
inline unsigned char uncaught_exceptions() gsl_noexcept
{
    return to_uchar( *reinterpret_cast<unsigned*>(_getptd() + (sizeof(void*) == 8 ? 0x100 : 0x90) ) );
}

#elif gsl_COMPILER_CLANG_VERSION || gsl_COMPILER_GNUC_VERSION

extern "C" char * __cxa_get_globals();
inline unsigned char uncaught_exceptions() gsl_noexcept
{
    return to_uchar( *reinterpret_cast<unsigned*>(__cxa_get_globals() + sizeof(void*) ) );
}
#endif
}
#endif

#if gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 110

template< class F >
class final_action
{
public:
    gsl_api explicit final_action( F action ) gsl_noexcept
        : action_( std::move( action ) )
        , invoke_( true )
    {}

    gsl_api final_action( final_action && other ) gsl_noexcept
        : action_( std::move( other.action_ ) )
        , invoke_( other.invoke_ )
    {
        other.invoke_ = false;
    }

    gsl_api virtual ~final_action() gsl_noexcept
    {
        if ( invoke_ )
            action_();
    }

gsl_is_delete_access:
    gsl_api final_action( final_action const  & ) gsl_is_delete;
    gsl_api final_action & operator=( final_action const & ) gsl_is_delete;
    gsl_api final_action & operator=( final_action && ) gsl_is_delete;

protected:
    gsl_api void dismiss() gsl_noexcept
    {
        invoke_ = false;
    }

private:
    F action_;
    bool invoke_;
};

template< class F >
gsl_api inline final_action<F> finally( F const & action ) gsl_noexcept
{
    return final_action<F>( action );
}

template< class F >
gsl_api inline final_action<F> finally( F && action ) gsl_noexcept
{
    return final_action<F>( std::forward<F>( action ) );
}

#if gsl_FEATURE( EXPERIMENTAL_RETURN_GUARD )

template< class F >
class final_action_return : public final_action<F>
{
public:
    gsl_api explicit final_action_return( F && action ) gsl_noexcept
        : final_action<F>( std::move( action ) )
        , exception_count( details::uncaught_exceptions() )
    {}

    gsl_api final_action_return( final_action_return && other ) gsl_noexcept
        : final_action<F>( std::move( other ) )
        , exception_count( details::uncaught_exceptions() )
    {}

    gsl_api ~final_action_return() override
    {
        if ( details::uncaught_exceptions() != exception_count )
            this->dismiss();
    }

gsl_is_delete_access:
    gsl_api final_action_return( final_action_return const & ) gsl_is_delete;
    gsl_api final_action_return & operator=( final_action_return const & ) gsl_is_delete;

private:
    unsigned char exception_count;
};

template< class F >
gsl_api inline final_action_return<F> on_return( F const & action ) gsl_noexcept
{
    return final_action_return<F>( action );
}

template< class F >
gsl_api inline final_action_return<F> on_return( F && action ) gsl_noexcept
{
    return final_action_return<F>( std::forward<F>( action ) );
}

template< class F >
class final_action_error : public final_action<F>
{
public:
    gsl_api explicit final_action_error( F && action ) gsl_noexcept
        : final_action<F>( std::move( action ) )
        , exception_count( details::uncaught_exceptions() )
    {}

    gsl_api final_action_error( final_action_error && other ) gsl_noexcept
        : final_action<F>( std::move( other ) )
        , exception_count( details::uncaught_exceptions() )
    {}

    gsl_api ~final_action_error() override
    {
        if ( details::uncaught_exceptions() == exception_count )
            this->dismiss();
    }

gsl_is_delete_access:
    gsl_api final_action_error( final_action_error const & ) gsl_is_delete;
    gsl_api final_action_error & operator=( final_action_error const & ) gsl_is_delete;

private:
    unsigned char exception_count;
};

template< class F >
gsl_api inline final_action_error<F> on_error( F const & action ) gsl_noexcept
{
    return final_action_error<F>( action );
}

template< class F >
gsl_api inline final_action_error<F> on_error( F && action ) gsl_noexcept
{
    return final_action_error<F>( std::forward<F>( action ) );
}

#endif // gsl_FEATURE( EXPERIMENTAL_RETURN_GUARD )

#else // gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 110

class final_action
{
public:
    typedef void (*Action)();

    gsl_api final_action( Action action )
    : action_( action )
    , invoke_( true )
    {}

    gsl_api final_action( final_action const & other )
        : action_( other.action_ )
        , invoke_( other.invoke_ )
    {
        other.invoke_ = false;
    }

    gsl_api virtual ~final_action()
    {
        if ( invoke_ )
            action_();
    }

protected:
    gsl_api void dismiss()
    {
        invoke_ = false;
    }

private:
    gsl_api final_action & operator=( final_action const & );

private:
    Action action_;
    mutable bool invoke_;
};

template< class F >
gsl_api inline final_action finally( F const & f )
{
    return final_action(( f ));
}

#if gsl_FEATURE( EXPERIMENTAL_RETURN_GUARD )

class final_action_return : public final_action
{
public:
    gsl_api explicit final_action_return( Action action )
        : final_action( action )
        , exception_count( details::uncaught_exceptions() )
    {}

    gsl_api ~final_action_return()
    {
        if ( details::uncaught_exceptions() != exception_count )
            this->dismiss();
    }

private:
    gsl_api final_action_return & operator=( final_action_return const & );

private:
    unsigned char exception_count;
};

template< class F >
gsl_api inline final_action_return on_return( F const & action )
{
    return final_action_return( action );
}

class final_action_error : public final_action
{
public:
    gsl_api explicit final_action_error( Action action )
        : final_action( action )
        , exception_count( details::uncaught_exceptions() )
    {}

    gsl_api ~final_action_error()
    {
        if ( details::uncaught_exceptions() == exception_count )
            this->dismiss();
    }

private:
    gsl_api final_action_error & operator=( final_action_error const & );

private:
    unsigned char exception_count;
};

template< class F >
gsl_api inline final_action_error on_error( F const & action )
{
    return final_action_error( action );
}

#endif // gsl_FEATURE( EXPERIMENTAL_RETURN_GUARD )

#endif // gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION == 110

#if gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 120

template< class T, class U >
gsl_api inline gsl_constexpr T narrow_cast( U && u ) gsl_noexcept
{
    return static_cast<T>( std::forward<U>( u ) );
}

#else

template< class T, class U >
gsl_api inline T narrow_cast( U u ) gsl_noexcept
{
    return static_cast<T>( u );
}

#endif // gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 120

struct narrowing_error : public std::exception {};

#if gsl_HAVE( TYPE_TRAITS )

namespace details
{
    template< class T, class U >
    struct is_same_signedness : public std::integral_constant<bool, std::is_signed<T>::value == std::is_signed<U>::value>
    {};
}
#endif

template< class T, class U >
gsl_api inline T narrow( U u )
{
    T t = narrow_cast<T>( u );

    if ( static_cast<U>( t ) != u )
    {
#if gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )
        throw narrowing_error();
#else
        std::terminate();
#endif
    }

#if gsl_HAVE( TYPE_TRAITS )
# if gsl_COMPILER_MSVC_VERSION
    // Suppress MSVC level 4 warning C4127 (conditional expression is constant)
    if ( 0, ! details::is_same_signedness<T, U>::value && ( ( t < T() ) != ( u < U() ) ) )
# else
    if (    ! details::is_same_signedness<T, U>::value && ( ( t < T() ) != ( u < U() ) ) )
# endif
#else
    // Don't assume T() works:
    if ( ( t < 0 ) != ( u < 0 ) )
#endif
    {
#if gsl_CONFIG( CONTRACT_VIOLATION_THROWS_V )
        throw narrowing_error();
#else
        std::terminate();
#endif
    }
    return t;
}

//
// at() - Bounds-checked way of accessing static arrays, std::array, std::vector.
//

template< class T, size_t N >
gsl_api inline gsl_constexpr14 T & at( T(&arr)[N], size_t index )
{
    Expects( index < N );
    return arr[index];
}

#if gsl_HAVE( ARRAY )

template< class T, size_t N >
gsl_api inline gsl_constexpr14 T & at( std::array<T, N> & arr, size_t index )
{
    Expects( index < N );
    return arr[index];
}
#endif

template< class Container >
gsl_api inline gsl_constexpr14 auto at(Container & cont, size_t index)->decltype(cont[0])
{
    Expects( index < cont.size() );
    return cont[index];
}

#if gsl_HAVE( INITIALIZER_LIST )

template< class T >
gsl_api inline const gsl_constexpr14 T & at( std::initializer_list<T> cont, size_t index )
{
    Expects( index < cont.size() );
    return *( cont.begin() + index );
}
#endif

template< class T >
gsl_api inline gsl_constexpr T & at( span<T> s, size_t index )
{
    return s.at( index );
}

//
// GSL.views: views
//

//
// not_null<> - Wrap any indirection and enforce non-null.
//
template< class T >
class not_null
{
#if gsl_CONFIG( NOT_NULL_EXPLICIT_CTOR )
# define gsl_not_null_explicit   explicit
#else
# define gsl_not_null_explicit /*explicit*/
#endif

#if gsl_CONFIG( NOT_NULL_GET_BY_CONST_REF )
    typedef T const & get_result_t;
#else
    typedef T get_result_t;
#endif

public:
#if gsl_HAVE( TYPE_TRAITS )
    static_assert( std::is_assignable<T&, std::nullptr_t>::value, "T cannot be assigned nullptr." );
#endif

    template< class U
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class Dummy = typename std::enable_if<std::is_constructible<T, U>::value>::type
#endif
    >
    gsl_api gsl_constexpr14 gsl_not_null_explicit 
#if gsl_HAVE( RVALUE_REFERENCE )
    not_null( U && u )
    : ptr_( std::forward<U>( u ) )
#else
    not_null( U const & u )
    : ptr_( u )
#endif
    {
        Expects( ptr_ != gsl_nullptr );
    }
#undef gsl_not_null_explicit
    
#if gsl_HAVE( IS_DEFAULT )
    gsl_api                ~not_null() = default;
    gsl_api gsl_constexpr   not_null( not_null &&      other ) = default;
    gsl_api gsl_constexpr   not_null( not_null const & other ) = default;
    gsl_api                 not_null & operator=( not_null &&      other ) = default;
    gsl_api                 not_null & operator=( not_null const & other ) = default;
#else
    gsl_api                ~not_null() {};
    gsl_api gsl_constexpr   not_null( not_null const & other ) : ptr_ ( other.ptr_  ) {}
    gsl_api                 not_null & operator=( not_null const & other ) { ptr_ = other.ptr_; return *this; }
# if gsl_HAVE( RVALUE_REFERENCE )
    gsl_api gsl_constexpr   not_null( not_null && other ) : ptr_( std::move( other.get() ) ) {}
    gsl_api                 not_null & operator=( not_null && other ) { ptr_ = std::move( other.get() ); return *this; }
# endif
#endif

    template< class U
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class Dummy = typename std::enable_if<std::is_convertible<U, T>::value>::type
#endif
    >
    gsl_api gsl_constexpr not_null( not_null<U> const & other )
    : ptr_( other.get() )
    {}

    gsl_api gsl_constexpr14 get_result_t get() const
    {
        // Without cheating and changing ptr_ from the outside, this check is superfluous:
        Ensures( ptr_ != gsl_nullptr );
        return ptr_;
    }

    gsl_api gsl_constexpr operator get_result_t  () const { return get(); }
    gsl_api gsl_constexpr get_result_t operator->() const { return get(); }

#if gsl_HAVE( DECLTYPE_AUTO )
    gsl_api gsl_constexpr decltype(auto) operator*() const { return *get(); }
#endif

gsl_is_delete_access:
    // prevent compilation when initialized with a nullptr or literal 0:
#if gsl_HAVE( NULLPTR )
    gsl_api not_null(             std::nullptr_t ) gsl_is_delete;
    gsl_api not_null & operator=( std::nullptr_t ) gsl_is_delete;
#else
    gsl_api not_null(             int ) gsl_is_delete;
    gsl_api not_null & operator=( int ) gsl_is_delete;
#endif

    // unwanted operators...pointers only point to single objects!
    gsl_api not_null & operator++() gsl_is_delete;
    gsl_api not_null & operator--() gsl_is_delete;
    gsl_api not_null   operator++( int ) gsl_is_delete;
    gsl_api not_null   operator--( int ) gsl_is_delete;
    gsl_api not_null & operator+ ( size_t ) gsl_is_delete;
    gsl_api not_null & operator+=( size_t ) gsl_is_delete;
    gsl_api not_null & operator- ( size_t ) gsl_is_delete;
    gsl_api not_null & operator-=( size_t ) gsl_is_delete;
    gsl_api not_null & operator+=( std::ptrdiff_t ) gsl_is_delete;
    gsl_api not_null & operator-=( std::ptrdiff_t ) gsl_is_delete;
    gsl_api void       operator[]( std::ptrdiff_t ) const gsl_is_delete;

private:
    T ptr_;
};

// not_null with implicit constructor, allowing copy-initialization:

template< class T >
class not_null_ic : public not_null<T>
{
public:
    template< class U
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class Dummy = typename std::enable_if<std::is_constructible<T, U>::value>::type
#endif
    >
    gsl_api gsl_constexpr14
#if gsl_HAVE( RVALUE_REFERENCE )
    not_null_ic( U && u )
    : not_null<T>( std::forward<U>( u ) )
#else
    not_null_ic( U const & u )
    : not_null<T>( u )
#endif
    {}
};

// more not_null unwanted operators

template< class T, class U >
std::ptrdiff_t operator-( not_null<T> const &, not_null<U> const & ) gsl_is_delete;

template< class T >
not_null<T> operator-( not_null<T> const &, std::ptrdiff_t ) gsl_is_delete;

template< class T >
not_null<T> operator+( not_null<T> const &, std::ptrdiff_t ) gsl_is_delete;

template< class T >
not_null<T> operator+( std::ptrdiff_t, not_null<T> const & ) gsl_is_delete;

// not_null comparisons

template< class T, class U >
gsl_api inline gsl_constexpr bool operator==( not_null<T> const & l, not_null<U> const & r )
{
    return  l.get() == r.get();
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator< ( not_null<U> const & l, not_null<U> const & r )
{
    return l.get() < r.get();
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator!=( not_null<U> const & l, not_null<U> const & r )
{
    return !( l == r );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator<=( not_null<U> const & l, not_null<U> const & r )
{
    return !( r < l );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator> ( not_null<U> const & l, not_null<U> const & r )
{
    return ( r < l );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator>=( not_null<U> const & l, not_null<U> const & r )
{
    return !( l < r );
}

//
// Byte-specific type.
//
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
  enum class gsl_may_alias byte : unsigned char {};
#else
  struct gsl_may_alias byte { typedef unsigned char type; type v; };
#endif

#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
# define gsl_ENABLE_IF_INTEGRAL_T(T)  \
    , class = typename std::enable_if<std::is_integral<T>::value>::type
#else
# define gsl_ENABLE_IF_INTEGRAL_T(T)
#endif

template< class T >
gsl_api inline gsl_constexpr byte to_byte( T v ) gsl_noexcept
{
#if    gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return static_cast<byte>( v );
#elif  gsl_HAVE( CONSTEXPR_11 )
    return { static_cast<typename byte::type>( v ) };
#else
    byte b = { static_cast<typename byte::type>( v ) }; return b;
#endif
}

template< class IntegerType  gsl_ENABLE_IF_INTEGRAL_T( IntegerType ) >
gsl_api inline gsl_constexpr IntegerType to_integer( byte b ) gsl_noexcept
{
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return static_cast<typename std::underlying_type<byte>::type>( b );
#else
    return b.v;
#endif
}

gsl_api inline gsl_constexpr unsigned char to_uchar( byte b ) gsl_noexcept
{
    return to_integer<unsigned char>( b );
}

gsl_api inline gsl_constexpr unsigned char to_uchar( int i ) gsl_noexcept
{
    return static_cast<unsigned char>( i );
}

#if ! gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )

gsl_api inline gsl_constexpr bool operator==( byte l, byte r ) gsl_noexcept
{
    return l.v == r.v;
}

gsl_api inline gsl_constexpr bool operator!=( byte l, byte r ) gsl_noexcept
{
    return !( l == r );
}

gsl_api inline gsl_constexpr bool operator< ( byte l, byte r ) gsl_noexcept
{
    return l.v < r.v;
}

gsl_api inline gsl_constexpr bool operator<=( byte l, byte r ) gsl_noexcept
{
    return !( r < l );
}

gsl_api inline gsl_constexpr bool operator> ( byte l, byte r ) gsl_noexcept
{
    return ( r < l );
}

gsl_api inline gsl_constexpr bool operator>=( byte l, byte r ) gsl_noexcept
{
    return !( l < r );
}
#endif

template< class IntegerType  gsl_ENABLE_IF_INTEGRAL_T( IntegerType ) >
gsl_api inline gsl_constexpr14 byte & operator<<=( byte & b, IntegerType shift ) gsl_noexcept
{
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return b = to_byte( to_uchar( b ) << shift );
#else
    b.v = to_uchar( b.v << shift ); return b;
#endif
}

template< class IntegerType  gsl_ENABLE_IF_INTEGRAL_T( IntegerType ) >
gsl_api inline gsl_constexpr byte operator<<( byte b, IntegerType shift ) gsl_noexcept
{
    return to_byte( to_uchar( b ) << shift );
}

template< class IntegerType  gsl_ENABLE_IF_INTEGRAL_T( IntegerType ) >
gsl_api inline gsl_constexpr14 byte & operator>>=( byte & b, IntegerType shift ) gsl_noexcept
{
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return b = to_byte( to_uchar( b ) >> shift );
#else
    b.v = to_uchar( b.v >> shift ); return b;
#endif
}

template< class IntegerType  gsl_ENABLE_IF_INTEGRAL_T( IntegerType ) >
gsl_api inline gsl_constexpr byte operator>>( byte b, IntegerType shift ) gsl_noexcept
{
    return to_byte( to_uchar( b ) >> shift );
}

gsl_api inline gsl_constexpr14 byte & operator|=( byte & l, byte r ) gsl_noexcept
{
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return l = to_byte( to_uchar( l ) | to_uchar( r ) );
#else
    l.v = to_uchar( l ) | to_uchar( r ); return l;
#endif
}

gsl_api inline gsl_constexpr byte operator|( byte l, byte r ) gsl_noexcept
{
    return to_byte( to_uchar( l ) | to_uchar( r ) );
}

gsl_api inline gsl_constexpr14 byte & operator&=( byte & l, byte r ) gsl_noexcept
{
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return l = to_byte( to_uchar( l ) & to_uchar( r ) );
#else
    l.v = to_uchar( l ) & to_uchar( r ); return l;
#endif
}

gsl_api inline gsl_constexpr byte operator&( byte l, byte r ) gsl_noexcept
{
    return to_byte( to_uchar( l ) & to_uchar( r ) );
}

gsl_api inline gsl_constexpr14 byte & operator^=( byte & l, byte r ) gsl_noexcept
{
#if gsl_HAVE( ENUM_CLASS_CONSTRUCTION_FROM_UNDERLYING_TYPE )
    return l = to_byte( to_uchar( l ) ^ to_uchar (r ) );
#else
    l.v = to_uchar( l ) ^ to_uchar (r ); return l;
#endif
}

gsl_api inline gsl_constexpr byte operator^( byte l, byte r ) gsl_noexcept
{
    return to_byte( to_uchar( l ) ^ to_uchar( r ) );
}

gsl_api inline gsl_constexpr byte operator~( byte b ) gsl_noexcept
{
    return to_byte( ~to_uchar( b ) );
}

#if gsl_FEATURE_TO_STD( WITH_CONTAINER )

// Tag to select span constructor taking a container (prevent ms-gsl warning C26426):

struct with_container_t { gsl_constexpr with_container_t() gsl_noexcept {} };
const  gsl_constexpr   with_container_t with_container;

#endif

#if gsl_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR )

namespace details {

// Can construct from containers that:

template<
    class Container, class ElementType
    , class = typename std::enable_if<
        ! details::is_span< Container >::value &&
        ! details::is_array< Container >::value &&
        ! details::is_std_array< Container >::value &&
          std::is_convertible<typename std::remove_pointer<decltype(std::declval<Container>().data())>::type(*)[], ElementType(*)[] >::value
    >::type
#if gsl_HAVE( STD_DATA )
      // data(cont) and size(cont) well-formed:
    , class = decltype( std::data( std::declval<Container>() ) )
    , class = decltype( std::size( std::declval<Container>() ) )
#endif
>
struct can_construct_span_from : details::true_type{};

} // namespace details
#endif

//
// span<> - A 1D view of contiguous T's, replace (*,len).
//
template< class T >
class span
{
    template< class U > friend class span;

public:
    typedef index index_type;

    typedef T element_type;
    typedef typename details::remove_cv< T >::type value_type;

    typedef T & reference;
    typedef T * pointer;
    typedef T const * const_pointer;
    typedef T const & const_reference;

    typedef pointer       iterator;
    typedef const_pointer const_iterator;

    typedef std::reverse_iterator< iterator >       reverse_iterator;
    typedef std::reverse_iterator< const_iterator > const_reverse_iterator;

    typedef typename std::iterator_traits< iterator >::difference_type difference_type;

    // 26.7.3.2 Constructors, copy, and assignment [span.cons]

    gsl_api gsl_constexpr14 span() gsl_noexcept
        : first_( gsl_nullptr )
        , last_ ( gsl_nullptr )
    {
        Expects( size() == 0 );
    }

#if ! gsl_DEPRECATE_TO_LEVEL( 5 )

#if gsl_HAVE( NULLPTR )
    gsl_api gsl_constexpr14 span( std::nullptr_t, index_type gsl_EXPECTS_UNUSED_PARAM( size_in ) )
        : first_( nullptr )
        , last_ ( nullptr )
    {
        Expects( size_in == 0 );
    }
#endif

#if gsl_HAVE( IS_DELETE )
    gsl_api gsl_constexpr span( reference data_in )
        : span( &data_in, 1 )
    {}

    gsl_api gsl_constexpr span( element_type && ) = delete;
#endif

#endif // deprecate

    gsl_api gsl_constexpr14 span( pointer data_in, index_type size_in )
        : first_( data_in )
        , last_ ( data_in + size_in )
    {
        Expects( size_in == 0 || ( size_in > 0 && data_in != gsl_nullptr ) );
    }

    gsl_api gsl_constexpr14 span( pointer first_in, pointer last_in )
        : first_( first_in )
        , last_ ( last_in )
    {
        Expects( first_in <= last_in );
    }

#if ! gsl_DEPRECATE_TO_LEVEL( 5 )

    template< class U >
    gsl_api gsl_constexpr14 span( U * & data_in, index_type size_in )
        : first_( data_in )
        , last_ ( data_in + size_in )
    {
        Expects( size_in == 0 || ( size_in > 0 && data_in != gsl_nullptr ) );
    }

    template< class U >
    gsl_api gsl_constexpr14 span( U * const & data_in, index_type size_in )
        : first_( data_in )
        , last_ ( data_in + size_in )
    {
        Expects( size_in == 0 || ( size_in > 0 && data_in != gsl_nullptr ) );
    }

#endif // deprecate

#if ! gsl_DEPRECATE_TO_LEVEL( 5 )
    template< class U, size_t N >
    gsl_api gsl_constexpr span( U (&arr)[N] ) gsl_noexcept
        : first_( gsl_ADDRESSOF( arr[0] ) )
        , last_ ( gsl_ADDRESSOF( arr[0] ) + N )
    {}
#else
    template< size_t N
# if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class = typename std::enable_if<
            std::is_convertible<value_type(*)[], element_type(*)[] >::value
        >::type
# endif
    >
    gsl_api gsl_constexpr span( element_type (&arr)[N] ) gsl_noexcept
        : first_( gsl_ADDRESSOF( arr[0] ) )
        , last_ ( gsl_ADDRESSOF( arr[0] ) + N )
    {}
#endif // deprecate

#if gsl_HAVE( ARRAY )
#if ! gsl_DEPRECATE_TO_LEVEL( 5 )

    template< class U, size_t N >
    gsl_api gsl_constexpr span( std::array< U, N > & arr )
        : first_( arr.data() )
        , last_ ( arr.data() + N )
    {}

    template< class U, size_t N >
    gsl_api gsl_constexpr span( std::array< U, N > const & arr )
        : first_( arr.data() )
        , last_ ( arr.data() + N )
    {}

#else

    template< size_t N
# if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class = typename std::enable_if<
            std::is_convertible<value_type(*)[], element_type(*)[] >::value
        >::type
# endif
    >
    gsl_api gsl_constexpr span( std::array< value_type, N > & arr )
        : first_( arr.data() )
        , last_ ( arr.data() + N )
    {}

    template< size_t N
# if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class = typename std::enable_if<
            std::is_convertible<value_type(*)[], element_type(*)[] >::value
        >::type
# endif
    >
    gsl_api gsl_constexpr span( std::array< value_type, N > const & arr )
        : first_( arr.data() )
        , last_ ( arr.data() + N )
    {}

#endif // deprecate
#endif // gsl_HAVE( ARRAY )

#if gsl_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR )
    template< class Container
        , class = typename std::enable_if<
            details::can_construct_span_from< Container, element_type >::value
        >::type
    >
    gsl_api gsl_constexpr span( Container & cont )
        : first_( cont.data() )
        , last_ ( cont.data() + cont.size() )
    {}

    template< class Container
        , class = typename std::enable_if<
            std::is_const< element_type >::value &&
            details::can_construct_span_from< Container, element_type >::value
        >::type
    >
    gsl_api gsl_constexpr span( Container const & cont )
        : first_( cont.data() )
        , last_ ( cont.data() + cont.size() )
    {}

#elif gsl_HAVE( UNCONSTRAINED_SPAN_CONTAINER_CTOR )

    template< class Container >
    gsl_api gsl_constexpr span( Container & cont )
        : first_( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) )
        , last_ ( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) + cont.size() )
    {}

    template< class Container >
    gsl_api gsl_constexpr span( Container const & cont )
        : first_( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) )
        , last_ ( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) + cont.size() )
    {}

#endif

#if gsl_FEATURE_TO_STD( WITH_CONTAINER )

    template< class Container >
    gsl_api gsl_constexpr span( with_container_t, Container & cont )
        : first_( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) )
        , last_ ( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) + cont.size() )
    {}

    template< class Container >
    gsl_api gsl_constexpr span( with_container_t, Container const & cont )
        : first_( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) )
        , last_ ( cont.size() == 0 ? gsl_nullptr : gsl_ADDRESSOF( cont[0] ) + cont.size() )
    {}

#endif

#if ! gsl_DEPRECATE_TO_LEVEL( 4 )
    // constructor taking shared_ptr deprecated since 0.29.0

#if gsl_HAVE( SHARED_PTR )
    gsl_api gsl_constexpr span( shared_ptr<element_type> const & ptr )
        : first_( ptr.get() )
        , last_ ( ptr.get() ? ptr.get() + 1 : 0 )
    {}
#endif

    // constructors taking unique_ptr deprecated since 0.29.0

#if gsl_HAVE( UNIQUE_PTR )
# if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
    template< class ArrayElementType = typename std::add_pointer<element_type>::type >
# else
    template< class ArrayElementType >
# endif
    gsl_api gsl_constexpr span( unique_ptr<ArrayElementType> const & ptr, index_type count )
        : first_( ptr.get() )
        , last_ ( ptr.get() + count )
    {}

    gsl_api gsl_constexpr span( unique_ptr<element_type> const & ptr )
        : first_( ptr.get() )
        , last_ ( ptr.get() ? ptr.get() + 1 : 0 )
    {}
#endif

#endif // deprecate shared_ptr, unique_ptr

#if gsl_HAVE( IS_DEFAULT ) && ! gsl_BETWEEN( gsl_COMPILER_GNUC_VERSION, 430, 600)
    gsl_api gsl_constexpr span( span && ) gsl_noexcept = default;
    gsl_api gsl_constexpr span( span const & ) = default;
#else
    gsl_api gsl_constexpr span( span const & other )
        : first_( other.begin() )
        , last_ ( other.end() )
    {}
#endif

#if gsl_HAVE( IS_DEFAULT )
    ~span() = default;
#else
    ~span() {}
#endif

#if gsl_HAVE( IS_DEFAULT )
    gsl_api gsl_constexpr14 span & operator=( span && ) gsl_noexcept = default;
    gsl_api gsl_constexpr14 span & operator=( span const & ) gsl_noexcept = default;
#else
    gsl_api span & operator=( span other ) gsl_noexcept
    {
        other.swap( *this );
        return *this;
    }
#endif

    template< class U
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class = typename std::enable_if<
            std::is_convertible<U(*)[], element_type(*)[]>::value
        >::type
#endif
    >
    gsl_api gsl_constexpr span( span<U> const & other )
        : first_( other.begin() )
        , last_ ( other.end() )
    {}

#if 0
    // Converting from other span ?
    template< class U > operator=();
#endif

    // 26.7.3.3 Subviews [span.sub]

    gsl_api gsl_constexpr14 span first( index_type count ) const gsl_noexcept
    {
        Expects( 0 <= count && count <= this->size() );
        return span( this->data(), count );
    }

    gsl_api gsl_constexpr14 span last( index_type count ) const gsl_noexcept
    {
        Expects( 0 <= count && count <= this->size() );
        return span( this->data() + this->size() - count, count );
    }

    gsl_api gsl_constexpr14 span subspan( index_type offset ) const gsl_noexcept
    {
        Expects( 0 <= offset && offset <= this->size() );
        return span( this->data() + offset, this->size() - offset );
    }

    gsl_api gsl_constexpr14 span subspan( index_type offset, index_type count ) const gsl_noexcept
    {
        Expects(
            0 <= offset && offset <= this->size() &&
            0 <= count  && count + offset <= this->size() );
        return span( this->data() + offset, count );
    }

    // 26.7.3.4 Observers [span.obs]

    gsl_api gsl_constexpr index_type size() const gsl_noexcept
    {
        return narrow_cast<index_type>( last_ - first_ );
    }

    gsl_api gsl_constexpr index_type size_bytes() const gsl_noexcept
    {
        return size() * narrow_cast<index_type>( sizeof( element_type ) );
    }

    gsl_api gsl_constexpr bool empty() const gsl_noexcept
    {
        return size() == 0;
    }

    // 26.7.3.5 Element access [span.elem]

    gsl_api gsl_constexpr reference operator[]( index_type index ) const
    {
       return at( index );
    }

    gsl_api gsl_constexpr reference operator()( index_type index ) const
    {
       return at( index );
    }

    gsl_api gsl_constexpr14 reference at( index_type index ) const
    {
       Expects( index < size() );
       return first_[ index ];
    }

    gsl_api gsl_constexpr pointer data() const gsl_noexcept
    {
        return first_;
    }

    // 26.7.3.6 Iterator support [span.iterators]

    gsl_api gsl_constexpr iterator begin() const gsl_noexcept
    {
        return iterator( first_ );
    }

    gsl_api gsl_constexpr iterator end() const gsl_noexcept
    {
        return iterator( last_ );
    }

    gsl_api gsl_constexpr const_iterator cbegin() const gsl_noexcept
    {
#if gsl_CPP11_OR_GREATER
        return { begin() };
#else
        return const_iterator( begin() );
#endif
    }

    gsl_api gsl_constexpr const_iterator cend() const gsl_noexcept
    {
#if gsl_CPP11_OR_GREATER
        return { end() };
#else
        return const_iterator( end() );
#endif
    }

    gsl_api gsl_constexpr reverse_iterator rbegin() const gsl_noexcept
    {
        return reverse_iterator( end() );
    }

    gsl_api gsl_constexpr reverse_iterator rend() const gsl_noexcept
    {
        return reverse_iterator( begin() );
    }

    gsl_api gsl_constexpr const_reverse_iterator crbegin() const gsl_noexcept
    {
        return const_reverse_iterator( cend() );
    }

    gsl_api gsl_constexpr const_reverse_iterator crend() const gsl_noexcept
    {
        return const_reverse_iterator( cbegin() );
    }

    gsl_api void swap( span & other ) gsl_noexcept
    {
        using std::swap;
        swap( first_, other.first_ );
        swap( last_ , other.last_  );
    }

#if ! gsl_DEPRECATE_TO_LEVEL( 3 )
    // member length() deprecated since 0.29.0

    gsl_api gsl_constexpr index_type length() const gsl_noexcept
    {
        return size();
    }

    // member length_bytes() deprecated since 0.29.0

    gsl_api gsl_constexpr index_type length_bytes() const gsl_noexcept
    {
        return size_bytes();
    }
#endif

#if ! gsl_DEPRECATE_TO_LEVEL( 2 )
    // member as_bytes(), as_writeable_bytes deprecated since 0.17.0

    gsl_api span< const byte > as_bytes() const gsl_noexcept
    {
        return span< const byte >( reinterpret_cast<const byte *>( data() ), size_bytes() ); // NOLINT
    }

    gsl_api span< byte > as_writeable_bytes() const gsl_noexcept
    {
        return span< byte >( reinterpret_cast<byte *>( data() ), size_bytes() ); // NOLINT
    }

#endif

    template< class U >
    gsl_api span< U > as_span() const gsl_noexcept
    {
        Expects( ( this->size_bytes() % sizeof(U) ) == 0 );
        return span< U >( reinterpret_cast<U *>( this->data() ), this->size_bytes() / sizeof( U ) ); // NOLINT
    }

private:
    pointer first_;
    pointer last_;
};

// class template argument deduction guides:

#if gsl_HAVE( DEDUCTION_GUIDES )   // gsl_CPP17_OR_GREATER

template< class T, size_t N >
span( T (&)[N] ) -> span<T /*, N*/>;

template< class T, size_t N >
span( std::array<T, N> & ) -> span<T /*, N*/>;

template< class T, size_t N >
span( std::array<T, N> const & ) -> span<const T /*, N*/>;

template< class Container >
span( Container& ) -> span<typename Container::value_type>;

template< class Container >
span( Container const & ) -> span<const typename Container::value_type>;

#endif // gsl_HAVE( DEDUCTION_GUIDES )

// 26.7.3.7 Comparison operators [span.comparison]

#if gsl_CONFIG( ALLOWS_NONSTRICT_SPAN_COMPARISON )

template< class T, class U >
gsl_api inline gsl_constexpr bool operator==( span<T> const & l, span<U> const & r )
{
    return  l.size()  == r.size()
        && (l.begin() == r.begin() || std::equal( l.begin(), l.end(), r.begin() ) );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator< ( span<T> const & l, span<U> const & r )
{
    return std::lexicographical_compare( l.begin(), l.end(), r.begin(), r.end() );
}

#else

template< class T >
gsl_api inline gsl_constexpr bool operator==( span<T> const & l, span<T> const & r )
{
    return  l.size()  == r.size()
        && (l.begin() == r.begin() || std::equal( l.begin(), l.end(), r.begin() ) );
}

template< class T >
gsl_api inline gsl_constexpr bool operator< ( span<T> const & l, span<T> const & r )
{
    return std::lexicographical_compare( l.begin(), l.end(), r.begin(), r.end() );
}
#endif

template< class T, class U >
gsl_api inline gsl_constexpr bool operator!=( span<T> const & l, span<U> const & r )
{
    return !( l == r );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator<=( span<T> const & l, span<U> const & r )
{
    return !( r < l );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator> ( span<T> const & l, span<U> const & r )
{
    return ( r < l );
}

template< class T, class U >
gsl_api inline gsl_constexpr bool operator>=( span<T> const & l, span<U> const & r )
{
    return !( l < r );
}

// span algorithms

namespace details {

template< class II, class N, class OI >
gsl_api inline OI copy_n( II first, N count, OI result )
{
    if ( count > 0 )
    {
        *result++ = *first;
        for ( N i = 1; i < count; ++i )
        {
            *result++ = *++first;
        }
    }
    return result;
}
}

template< class T, class U >
gsl_api inline void copy( span<T> src, span<U> dest )
{
#if gsl_CPP14_OR_GREATER // gsl_HAVE( TYPE_TRAITS ) (circumvent Travis clang 3.4)
    static_assert( std::is_assignable<U &, T const &>::value, "Cannot assign elements of source span to elements of destination span" );
#endif
    Expects( dest.size() >= src.size() );
    details::copy_n( src.data(), src.size(), dest.data() );
}

// span creator functions (see ctors)

template< class T >
gsl_api inline span< const byte > as_bytes( span<T> spn ) gsl_noexcept
{
    return span< const byte >( reinterpret_cast<const byte *>( spn.data() ), spn.size_bytes() ); // NOLINT
}

template< class T>
gsl_api inline span< byte > as_writeable_bytes( span<T> spn ) gsl_noexcept
{
    return span< byte >( reinterpret_cast<byte *>( spn.data() ), spn.size_bytes() ); // NOLINT
}

#if gsl_FEATURE_TO_STD( MAKE_SPAN )

template< class T >
gsl_api inline gsl_constexpr span<T>
make_span( T * ptr, typename span<T>::index_type count )
{
    return span<T>( ptr, count );
}

template< class T >
gsl_api inline gsl_constexpr span<T>
make_span( T * first, T * last )
{
    return span<T>( first, last );
}

template< class T, size_t N >
gsl_api inline gsl_constexpr span<T>
make_span( T (&arr)[N] )
{
    return span<T>( gsl_ADDRESSOF( arr[0] ), N );
}

#if gsl_HAVE( ARRAY )

template< class T, size_t N >
gsl_api inline gsl_constexpr span<T>
make_span( std::array<T,N> & arr )
{
    return span<T>( arr );
}

template< class T, size_t N >
gsl_api inline gsl_constexpr span<const T>
make_span( std::array<T,N> const & arr )
{
    return span<const T>( arr );
}
#endif

#if gsl_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR ) && gsl_HAVE( AUTO )

template< class Container, class = decltype(std::declval<Container>().data()) >
gsl_api inline gsl_constexpr auto
make_span( Container & cont ) -> span< typename Container::value_type >
{
    return span< typename Container::value_type >( cont );
}

template< class Container, class = decltype(std::declval<Container>().data()) >
gsl_api inline gsl_constexpr auto
make_span( Container const & cont ) -> span< const typename Container::value_type >
{
    return span< const typename Container::value_type >( cont );
}

#else

template< class T >
gsl_api inline span<T>
make_span( std::vector<T> & cont )
{
    return span<T>( with_container, cont );
}

template< class T >
gsl_api inline span<const T>
make_span( std::vector<T> const & cont )
{
    return span<const T>( with_container, cont );
}
#endif

#if gsl_FEATURE_TO_STD( WITH_CONTAINER )

template< class Container >
gsl_api inline gsl_constexpr span<typename Container::value_type>
make_span( with_container_t, Container & cont ) gsl_noexcept
{
    return span< typename Container::value_type >( with_container, cont );
}

template< class Container >
gsl_api inline gsl_constexpr span<const typename Container::value_type>
make_span( with_container_t, Container const & cont ) gsl_noexcept
{
    return span< const typename Container::value_type >( with_container, cont );
}

#endif // gsl_FEATURE_TO_STD( WITH_CONTAINER )

template< class Ptr >
gsl_api inline span<typename Ptr::element_type>
make_span( Ptr & ptr )
{
    return span<typename Ptr::element_type>( ptr );
}

template< class Ptr >
gsl_api inline span<typename Ptr::element_type>
make_span( Ptr & ptr, typename span<typename Ptr::element_type>::index_type count )
{
    return span<typename Ptr::element_type>( ptr, count);
}

#endif // gsl_FEATURE_TO_STD( MAKE_SPAN )

#if gsl_FEATURE_TO_STD( BYTE_SPAN )

template< class T >
gsl_api inline gsl_constexpr span<byte>
byte_span( T & t ) gsl_noexcept
{
    return span<byte>( reinterpret_cast<byte *>( &t ), sizeof(T) );
}

template< class T >
gsl_api inline gsl_constexpr span<const byte>
byte_span( T const & t ) gsl_noexcept
{
    return span<const byte>( reinterpret_cast<byte const *>( &t ), sizeof(T) );
}

#endif // gsl_FEATURE_TO_STD( BYTE_SPAN )

//
// basic_string_span:
//

template< class T >
class basic_string_span;

namespace details {

template< class T >
struct is_basic_string_span_oracle : false_type {};

template< class T >
struct is_basic_string_span_oracle< basic_string_span<T> > : true_type {};

template< class T >
struct is_basic_string_span : is_basic_string_span_oracle< typename remove_cv<T>::type > {};

template< class T >
gsl_api inline gsl_constexpr14 std::size_t string_length( T * ptr, std::size_t max )
{
    if ( ptr == gsl_nullptr || max <= 0 )
        return 0;

    std::size_t len = 0;
    while ( len < max && ptr[len] ) // NOLINT
        ++len;

    return len;
}

} // namespace details

//
// basic_string_span<> - A view of contiguous characters, replace (*,len).
//
template< class T >
class basic_string_span
{
public:
    typedef T element_type;
    typedef span<T> span_type;

    typedef typename span_type::index_type index_type;
    typedef typename span_type::difference_type difference_type;

    typedef typename span_type::pointer pointer ;
    typedef typename span_type::reference reference ;

    typedef typename span_type::iterator iterator ;
    typedef typename span_type::const_iterator const_iterator ;
    typedef typename span_type::reverse_iterator reverse_iterator;
    typedef typename span_type::const_reverse_iterator const_reverse_iterator;

    // construction:

#if gsl_HAVE( IS_DEFAULT )
    gsl_api gsl_constexpr basic_string_span() gsl_noexcept = default;
#else
    gsl_api gsl_constexpr basic_string_span() gsl_noexcept {}
#endif

#if gsl_HAVE( NULLPTR )
    gsl_api gsl_constexpr basic_string_span( std::nullptr_t ptr ) gsl_noexcept
    : span_( ptr, index_type( 0 ) )
    {}
#endif

    gsl_api gsl_constexpr basic_string_span( pointer ptr )
    : span_( remove_z( ptr, std::numeric_limits<index_type>::max() ) )
    {}

    gsl_api gsl_constexpr basic_string_span( pointer ptr, index_type count )
    : span_( ptr, count )
    {}

    gsl_api gsl_constexpr basic_string_span( pointer firstElem, pointer lastElem )
    : span_( firstElem, lastElem )
    {}

    template< std::size_t N >
    gsl_api gsl_constexpr basic_string_span( element_type (&arr)[N] )
    : span_( remove_z( gsl_ADDRESSOF( arr[0] ), N ) )
    {}

#if gsl_HAVE( ARRAY )

    template< std::size_t N >
    gsl_api gsl_constexpr basic_string_span( std::array< typename details::remove_const<element_type>::type, N> & arr )
    : span_( remove_z( arr ) )
    {}

    template< std::size_t N >
    gsl_api gsl_constexpr basic_string_span( std::array< typename details::remove_const<element_type>::type, N> const & arr )
    : span_( remove_z( arr ) )
    {}

#endif

#if gsl_HAVE( CONSTRAINED_SPAN_CONTAINER_CTOR )

    // Exclude: array, [basic_string,] basic_string_span

    template<
        class Container,
        class = typename std::enable_if<
            ! details::is_std_array< Container >::value
            && ! details::is_basic_string_span< Container >::value
            && std::is_convertible< typename Container::pointer, pointer >::value
            && std::is_convertible< typename Container::pointer, decltype(std::declval<Container>().data()) >::value
        >::type
    >
    gsl_api gsl_constexpr basic_string_span( Container & cont )
    : span_( ( cont ) )
    {}

    // Exclude: array, [basic_string,] basic_string_span

    template<
        class Container,
        class = typename std::enable_if<
            ! details::is_std_array< Container >::value
            && ! details::is_basic_string_span< Container >::value
            && std::is_convertible< typename Container::pointer, pointer >::value
            && std::is_convertible< typename Container::pointer, decltype(std::declval<Container const &>().data()) >::value
        >::type
    >
    gsl_api gsl_constexpr basic_string_span( Container const & cont )
    : span_( ( cont ) )
    {}

#elif gsl_HAVE( UNCONSTRAINED_SPAN_CONTAINER_CTOR )

    template< class Container >
    gsl_api gsl_constexpr basic_string_span( Container & cont )
    : span_( cont )
    {}

    template< class Container >
    gsl_api gsl_constexpr basic_string_span( Container const & cont )
    : span_( cont )
    {}

#else

    template< class U >
    gsl_api gsl_constexpr basic_string_span( span<U> const & rhs )
    : span_( rhs )
    {}

#endif

#if gsl_FEATURE_TO_STD( WITH_CONTAINER )

    template< class Container >
    gsl_api gsl_constexpr basic_string_span( with_container_t, Container & cont )
    : span_( with_container, cont )
    {}
#endif

#if gsl_HAVE( IS_DEFAULT )
# if gsl_BETWEEN( gsl_COMPILER_GNUC_VERSION, 440, 600 )
    gsl_api gsl_constexpr basic_string_span( basic_string_span const & rhs ) = default;

    gsl_api gsl_constexpr basic_string_span( basic_string_span && rhs ) = default;
# else
    gsl_api gsl_constexpr basic_string_span( basic_string_span const & rhs ) gsl_noexcept = default;

    gsl_api gsl_constexpr basic_string_span( basic_string_span && rhs ) gsl_noexcept = default;
# endif
#endif

    template< class U
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )
        , class = typename std::enable_if< std::is_convertible<typename basic_string_span<U>::pointer, pointer>::value >::type
#endif
    >
    gsl_api gsl_constexpr basic_string_span( basic_string_span<U> const & rhs )
    : span_( reinterpret_cast<pointer>( rhs.data() ), rhs.length() ) // NOLINT
    {}

#if gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 120
    template< class U
        , class = typename std::enable_if< std::is_convertible<typename basic_string_span<U>::pointer, pointer>::value >::type
    >
    gsl_api gsl_constexpr basic_string_span( basic_string_span<U> && rhs )
    : span_( reinterpret_cast<pointer>( rhs.data() ), rhs.length() ) // NOLINT
    {}
#endif

    template< class CharTraits, class Allocator >
    gsl_api gsl_constexpr basic_string_span(
        std::basic_string< typename details::remove_const<element_type>::type, CharTraits, Allocator > & str )
    : span_( gsl_ADDRESSOF( str[0] ), str.length() )
    {}

    template< class CharTraits, class Allocator >
    gsl_api gsl_constexpr basic_string_span(
        std::basic_string< typename details::remove_const<element_type>::type, CharTraits, Allocator > const & str )
    : span_( gsl_ADDRESSOF( str[0] ), str.length() )
    {}

    // destruction, assignment:

#if gsl_HAVE( IS_DEFAULT )
    gsl_api ~basic_string_span() gsl_noexcept = default;

    gsl_api basic_string_span & operator=( basic_string_span const & rhs ) gsl_noexcept = default;

    gsl_api basic_string_span & operator=( basic_string_span && rhs ) gsl_noexcept = default;
#endif

    // sub span:

    gsl_api gsl_constexpr basic_string_span first( index_type count ) const
    {
        return span_.first( count );
    }

    gsl_api gsl_constexpr basic_string_span last( index_type count ) const
    {
        return span_.last( count );
    }

    gsl_api gsl_constexpr basic_string_span subspan( index_type offset ) const
    {
        return span_.subspan( offset );
    }

    gsl_api gsl_constexpr basic_string_span subspan( index_type offset, index_type count ) const
    {
        return span_.subspan( offset, count );
    }

    // observers:

    gsl_api gsl_constexpr index_type length() const gsl_noexcept
    {
        return span_.size();
    }

    gsl_api gsl_constexpr index_type size() const gsl_noexcept
    {
        return span_.size();
    }

    gsl_api gsl_constexpr index_type length_bytes() const gsl_noexcept
    {
        return span_.size_bytes();
    }

    gsl_api gsl_constexpr index_type size_bytes() const gsl_noexcept
    {
        return span_.size_bytes();
    }

    gsl_api gsl_constexpr bool empty() const gsl_noexcept
    {
        return size() == 0;
    }

    gsl_api gsl_constexpr reference operator[]( index_type idx ) const
    {
        return span_[idx];
    }

    gsl_api gsl_constexpr reference operator()( index_type idx ) const
    {
        return span_[idx];
    }

    gsl_api gsl_constexpr pointer data() const gsl_noexcept
    {
        return span_.data();
    }

    gsl_api iterator begin() const gsl_noexcept
    {
        return span_.begin();
    }

    gsl_api iterator end() const gsl_noexcept
    {
        return span_.end();
    }

    gsl_api reverse_iterator rbegin() const gsl_noexcept
    {
        return span_.rbegin();
    }

    gsl_api reverse_iterator rend() const gsl_noexcept
    {
        return span_.rend();
    }

    // const version not in p0123r2:

    gsl_api const_iterator cbegin() const gsl_noexcept
    {
        return span_.cbegin();
    }

    gsl_api const_iterator cend() const gsl_noexcept
    {
        return span_.cend();
    }

    gsl_api const_reverse_iterator crbegin() const gsl_noexcept
    {
        return span_.crbegin();
    }

    gsl_api const_reverse_iterator crend() const gsl_noexcept
    {
        return span_.crend();
    }

private:
    gsl_api static gsl_constexpr14 span_type remove_z( pointer const & sz, std::size_t max )
    {
        return span_type( sz, details::string_length( sz, max ) );
    }

#if gsl_HAVE( ARRAY )
    template< size_t N >
    gsl_api static gsl_constexpr14 span_type remove_z( std::array<typename details::remove_const<element_type>::type, N> & arr )
    {
        return remove_z( gsl_ADDRESSOF( arr[0] ), narrow_cast< std::size_t >( N ) );
    }

    template< size_t N >
    gsl_api static gsl_constexpr14 span_type remove_z( std::array<typename details::remove_const<element_type>::type, N> const & arr )
    {
        return remove_z( gsl_ADDRESSOF( arr[0] ), narrow_cast< std::size_t >( N ) );
    }
#endif

private:
    span_type span_;
};

// basic_string_span comparison functions:

#if gsl_CONFIG( ALLOWS_NONSTRICT_SPAN_COMPARISON )

template< class T, class U >
gsl_api inline gsl_constexpr14 bool operator==( basic_string_span<T> const & l, U const & u ) gsl_noexcept
{
    const basic_string_span< typename details::add_const<T>::type > r( u );

    return l.size() == r.size()
        && std::equal( l.begin(), l.end(), r.begin() );
}

template< class T, class U >
gsl_api inline gsl_constexpr14 bool operator<( basic_string_span<T> const & l, U const & u ) gsl_noexcept
{
    const basic_string_span< typename details::add_const<T>::type > r( u );

    return std::lexicographical_compare( l.begin(), l.end(), r.begin(), r.end() );
}

#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )

template< class T, class U,
    class = typename std::enable_if<!details::is_basic_string_span<U>::value >::type >
gsl_api inline gsl_constexpr14 bool operator==( U const & u, basic_string_span<T> const & r ) gsl_noexcept
{
    const basic_string_span< typename details::add_const<T>::type > l( u );

    return l.size() == r.size()
        && std::equal( l.begin(), l.end(), r.begin() );
}

template< class T, class U,
    class = typename std::enable_if<!details::is_basic_string_span<U>::value >::type >
gsl_api inline gsl_constexpr14 bool operator<( U const & u, basic_string_span<T> const & r ) gsl_noexcept
{
    const basic_string_span< typename details::add_const<T>::type > l( u );

    return std::lexicographical_compare( l.begin(), l.end(), r.begin(), r.end() );
}
#endif

#else //gsl_CONFIG( ALLOWS_NONSTRICT_SPAN_COMPARISON )

template< class T >
gsl_api inline gsl_constexpr14 bool operator==( basic_string_span<T> const & l, basic_string_span<T> const & r ) gsl_noexcept
{
    return l.size() == r.size()
        && std::equal( l.begin(), l.end(), r.begin() );
}

template< class T >
gsl_api inline gsl_constexpr14 bool operator<( basic_string_span<T> const & l, basic_string_span<T> const & r ) gsl_noexcept
{
    return std::lexicographical_compare( l.begin(), l.end(), r.begin(), r.end() );
}

#endif // gsl_CONFIG( ALLOWS_NONSTRICT_SPAN_COMPARISON )

template< class T, class U >
gsl_api inline gsl_constexpr14 bool operator!=( basic_string_span<T> const & l, U const & r ) gsl_noexcept
{
    return !( l == r );
}

template< class T, class U >
gsl_api inline gsl_constexpr14 bool operator<=( basic_string_span<T> const & l, U const & r ) gsl_noexcept
{
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG ) || ! gsl_CONFIG( ALLOWS_NONSTRICT_SPAN_COMPARISON )
    return !( r < l );
#else
    basic_string_span< typename details::add_const<T>::type > rr( r );
    return !( rr < l );
#endif
}

template< class T, class U >
gsl_api inline gsl_constexpr14 bool operator>( basic_string_span<T> const & l, U const & r ) gsl_noexcept
{
#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG ) || ! gsl_CONFIG( ALLOWS_NONSTRICT_SPAN_COMPARISON )
    return ( r < l );
#else
    basic_string_span< typename details::add_const<T>::type > rr( r );
    return ( rr < l );
#endif
}

template< class T, class U >
gsl_api inline gsl_constexpr14 bool operator>=( basic_string_span<T> const & l, U const & r ) gsl_noexcept
{
    return !( l < r );
}

#if gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )

template< class T, class U,
    class = typename std::enable_if<!details::is_basic_string_span<U>::value >::type >
gsl_api inline gsl_constexpr14 bool operator!=( U const & l, basic_string_span<T> const & r ) gsl_noexcept
{
    return !( l == r );
}

template< class T, class U,
    class = typename std::enable_if<!details::is_basic_string_span<U>::value >::type >
gsl_api inline gsl_constexpr14 bool operator<=( U const & l, basic_string_span<T> const & r ) gsl_noexcept
{
    return !( r < l );
}

template< class T, class U,
    class = typename std::enable_if<!details::is_basic_string_span<U>::value >::type >
gsl_api inline gsl_constexpr14 bool operator>( U const & l, basic_string_span<T> const & r ) gsl_noexcept
{
    return ( r < l );
}

template< class T, class U,
    class = typename std::enable_if<!details::is_basic_string_span<U>::value >::type >
gsl_api inline gsl_constexpr14 bool operator>=( U const & l, basic_string_span<T> const & r ) gsl_noexcept
{
    return !( l < r );
}

#endif // gsl_HAVE( DEFAULT_FUNCTION_TEMPLATE_ARG )

// convert basic_string_span to byte span:

template< class T >
gsl_api inline span< const byte > as_bytes( basic_string_span<T> spn ) gsl_noexcept
{
    return span< const byte >( reinterpret_cast<const byte *>( spn.data() ), spn.size_bytes() ); // NOLINT
}

//
// String types:
//

typedef char * zstring;
typedef const char * czstring;

#if gsl_HAVE( WCHAR )
typedef wchar_t * zwstring;
typedef const wchar_t * cwzstring;
#endif

typedef basic_string_span< char > string_span;
typedef basic_string_span< char const > cstring_span;

#if gsl_HAVE( WCHAR )
typedef basic_string_span< wchar_t > wstring_span;
typedef basic_string_span< wchar_t const > cwstring_span;
#endif

// to_string() allow (explicit) conversions from string_span to string

#if 0

template< class T >
gsl_api inline std::basic_string< typename std::remove_const<T>::type > to_string( basic_string_span<T> spn )
{
     std::string( spn.data(), spn.length() );
}

#else

gsl_api inline std::string to_string( string_span const & spn )
{
    return std::string( spn.data(), spn.length() );
}

gsl_api inline std::string to_string( cstring_span const & spn )
{
    return std::string( spn.data(), spn.length() );
}

#if gsl_HAVE( WCHAR )

gsl_api inline std::wstring to_string( wstring_span const & spn )
{
    return std::wstring( spn.data(), spn.length() );
}

gsl_api inline std::wstring to_string( cwstring_span const & spn )
{
    return std::wstring( spn.data(), spn.length() );
}

#endif // gsl_HAVE( WCHAR )
#endif // to_string()

//
// Stream output for string_span types
//

namespace details {

template< class Stream >
gsl_api void write_padding( Stream & os, std::streamsize n )
{
    for ( std::streamsize i = 0; i < n; ++i )
        os.rdbuf()->sputc( os.fill() );
}

template< class Stream, class Span >
gsl_api Stream & write_to_stream( Stream & os, Span const & spn )
{
    typename Stream::sentry sentry( os );

    if ( !os )
        return os;

    const std::streamsize length = narrow<std::streamsize>( spn.length() );

    // Whether, and how, to pad
    const bool pad = ( length < os.width() );
    const bool left_pad = pad && ( os.flags() & std::ios_base::adjustfield ) == std::ios_base::right;

    if ( left_pad )
        write_padding( os, os.width() - length );

    // Write span characters
    os.rdbuf()->sputn( spn.begin(), length );

    if ( pad && !left_pad )
        write_padding( os, os.width() - length );

    // Reset output stream width
    os.width(0);

    return os;
}

} // namespace details

template< typename Traits >
gsl_api std::basic_ostream< char, Traits > & operator<<( std::basic_ostream< char, Traits > & os, string_span const & spn )
{
    return details::write_to_stream( os, spn );
}

template< typename Traits >
gsl_api std::basic_ostream< char, Traits > & operator<<( std::basic_ostream< char, Traits > & os, cstring_span const & spn )
{
    return details::write_to_stream( os, spn );
}

#if gsl_HAVE( WCHAR )

template< typename Traits >
gsl_api std::basic_ostream< wchar_t, Traits > & operator<<( std::basic_ostream< wchar_t, Traits > & os, wstring_span const & spn )
{
    return details::write_to_stream( os, spn );
}

template< typename Traits >
gsl_api std::basic_ostream< wchar_t, Traits > & operator<<( std::basic_ostream< wchar_t, Traits > & os, cwstring_span const & spn )
{
    return details::write_to_stream( os, spn );
}

#endif // gsl_HAVE( WCHAR )

//
// ensure_sentinel()
//
// Provides a way to obtain a span from a contiguous sequence
// that ends with a (non-inclusive) sentinel value.
//
// Will fail-fast if sentinel cannot be found before max elements are examined.
//
namespace details {

template< class T, class SizeType, const T Sentinel >
gsl_api static span<T> ensure_sentinel( T * seq, SizeType max = std::numeric_limits<SizeType>::max() )
{
    typedef T * pointer;

    gsl_SUPPRESS_MSVC_WARNING( 26429, "f.23: symbol 'cur' is never tested for nullness, it can be marked as not_null" )

    pointer cur = seq;

    while ( static_cast<SizeType>( cur - seq ) < max && *cur != Sentinel )
        ++cur;

    Expects( *cur == Sentinel );

    return span<T>( seq, narrow_cast< typename span<T>::index_type >( cur - seq ) );
}
} // namespace details

//
// ensure_z - creates a string_span for a czstring or cwzstring.
// Will fail fast if a null-terminator cannot be found before
// the limit of size_type.
//

template< class T >
gsl_api inline span<T> ensure_z( T * const & sz, size_t max = std::numeric_limits<size_t>::max() )
{
    return details::ensure_sentinel<T, size_t, 0>( sz, max );
}

template< class T, size_t N >
gsl_api inline span<T> ensure_z( T (&sz)[N] )
{
    return ensure_z( gsl_ADDRESSOF( sz[0] ), N );
}

# if gsl_HAVE( TYPE_TRAITS )

template< class Container >
gsl_api inline span< typename std::remove_pointer<typename Container::pointer>::type >
ensure_z( Container & cont )
{
    return ensure_z( cont.data(), cont.length() );
}
# endif

} // namespace gsl

#if gsl_CPP11_OR_GREATER || gsl_COMPILER_MSVC_VERSION >= 120

namespace std {

template<>
struct hash< gsl::byte >
{
public:
    std::size_t operator()( gsl::byte v ) const gsl_noexcept
    {
        return gsl::to_integer<std::size_t>( v );
    }
};

} // namespace std

#endif

gsl_RESTORE_MSVC_WARNINGS()

#endif // GSL_GSL_LITE_HPP_INCLUDED

// end of file
