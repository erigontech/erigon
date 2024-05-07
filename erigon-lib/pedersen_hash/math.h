#ifndef STARKWARE_UTILS_MATH_H_
#define STARKWARE_UTILS_MATH_H_

#include <cstdint>
#include <vector>

#include "error_handling.h"

namespace starkware {

using std::size_t;

constexpr uint64_t inline Pow2(uint64_t n) {
  ASSERT(n < 64, "n must be smaller than 64.");
  return UINT64_C(1) << n;
}

/*
  Returns floor(Log_2(n)), n must be > 0.
*/
constexpr size_t inline Log2Floor(const uint64_t n) {
  ASSERT(n != 0, "log2 of 0 is undefined");
  static_assert(sizeof(long long) == 8, "It is assumed that long long is 64bits");  // NOLINT
  return 63 - __builtin_clzll(n);
}

/*
  Computes base to the power of the number given by exponent_bits in a generic group, given the
  element one in the group and a function mult(const GroupElementT& multiplier, GroupElementT* dst)
  that performs:
    *dst *= multiplier
  in the group.
  Note that it is possible that the address of multiplier is the same as dst.
*/
template <typename GroupElementT, typename MultFunc>
GroupElementT GenericPow(
    const GroupElementT& base, const std::vector<bool>& exponent_bits, const GroupElementT& one,
    const MultFunc& mult) {
  GroupElementT power = base;
  GroupElementT res = one;
  for (const auto&& b : exponent_bits) {
    if (b) {
      mult(power, &res);
    }

    mult(power, &power);
  }

  return res;
}

}  // namespace starkware

#endif  // STARKWARE_UTILS_MATH_H_
