#ifndef STARKWARE_ALGEBRA_BIG_INT_H_
#define STARKWARE_ALGEBRA_BIG_INT_H_

#include <cstddef>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "gsl-lite.hpp"

#include "error_handling.h"
#include "prng.h"

namespace starkware {

static constexpr inline __uint128_t Umul128(uint64_t x, uint64_t y) {
  return static_cast<__uint128_t>(x) * static_cast<__uint128_t>(y);
}

template <size_t N>
class BigInt {
 public:
  static constexpr size_t kDigits = N * std::numeric_limits<uint64_t>::digits;

  BigInt() = default;

  template <size_t K>
  constexpr BigInt(const BigInt<K>& v) noexcept;  // NOLINT implicit cast.
  constexpr explicit BigInt(const std::array<uint64_t, N>& v) noexcept : value_(v) {}
  constexpr explicit BigInt(uint64_t v) noexcept : value_(std::array<uint64_t, N>({v})) {}

  static constexpr BigInt One() { return BigInt(std::array<uint64_t, N>({1})); }
  static constexpr BigInt Zero() { return BigInt(std::array<uint64_t, N>({0})); }

  static BigInt RandomBigInt(Prng* prng);

  /*
    Returns pair of the form (result, overflow_occurred).
  */
  static constexpr std::pair<BigInt, bool> Add(const BigInt& a, const BigInt& b);
  constexpr BigInt operator+(const BigInt& other) const { return Add(*this, other).first; }
  constexpr BigInt operator-(const BigInt& other) const { return Sub(*this, other).first; }
  constexpr BigInt operator-() const { return Zero() - *this; }

  /*
    Multiplies two BigInt<N> numbers, this and other. Returns the result as a
    BigInt<2*N>.
  */
  constexpr BigInt<2 * N> operator*(const BigInt& other) const;

  /*
    Multiplies two BigInt<N> numbers modulo a third.
  */
  static BigInt MulMod(const BigInt& a, const BigInt& b, const BigInt& modulus);

  /*
    Computes the inverse of *this in the field GF(prime).
    If prime is not a prime number, the behavior is undefined.
  */
  BigInt InvModPrime(const BigInt& prime) const;

  /*
    Return pair of the form (result, underflow_occurred).
  */
  static constexpr std::pair<BigInt, bool> Sub(const BigInt& a, const BigInt& b);

  constexpr bool operator<(const BigInt& b) const;

  constexpr bool operator>=(const BigInt& b) const { return !(*this < b); }

  constexpr bool operator>(const BigInt& b) const { return b < *this; }

  constexpr bool operator<=(const BigInt& b) const { return !(*this > b); }

  /*
    Returns the pair (q, r) such that this == q*divisor + r and r < divisor.
  */
  std::pair<BigInt, BigInt> Div(const BigInt& divisor) const;

  /*
    Returns the representation of the number as a string of the form "0x...".
  */
  std::string ToString() const;

  std::vector<bool> ToBoolVector() const;

  /*
    Returns (x % target) assuming x is in the range [0, 2*target).

    The function assumes that target.NumLeadingZeros() > 0.

    Typically used after a Montgomery reduction which produces an output that
    satisfies the range requirement above.
  */
  static constexpr BigInt ReduceIfNeeded(const BigInt& x, const BigInt& target);

  /*
    Calculates x*y/2^256 mod modulus, assuming that montgomery_mprime is
    (-(modulus^-1)) mod 2^64. Assumes that modulus.NumLeadingZeros() > 0.
  */
  static constexpr BigInt MontMul(
      const BigInt& x, const BigInt& y, const BigInt& modulus, uint64_t montgomery_mprime);

  constexpr bool operator==(const BigInt& other) const;

  constexpr bool operator!=(const BigInt& other) const { return !(*this == other); }

  constexpr uint64_t& operator[](int i) { return gsl::at(value_, i); }

  constexpr const uint64_t& operator[](int i) const { return gsl::at(value_, i); }

  static constexpr size_t LimbCount() { return N; }

  /*
    Returns the number of leading zero's.
  */
  constexpr size_t NumLeadingZeros() const;

 private:
  std::array<uint64_t, N> value_;
};

template <size_t N>
std::ostream& operator<<(std::ostream& os, const BigInt<N>& bigint);

}  // namespace starkware

/*
  Implements the user defined _Z literal that constructs a BigInt of an
  arbitrary size. For example: BigInt<4> a =
  0x73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001_Z;
*/
template <char... Chars>
static constexpr auto operator"" _Z();

#include "big_int.inl"

#endif  // STARKWARE_ALGEBRA_BIG_INT_H_
