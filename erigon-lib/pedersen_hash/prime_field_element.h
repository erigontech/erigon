#ifndef STARKWARE_ALGEBRA_PRIME_FIELD_ELEMENT_H_
#define STARKWARE_ALGEBRA_PRIME_FIELD_ELEMENT_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "big_int.h"
#include "error_handling.h"
#include "prng.h"

namespace starkware {

/*
  Represents an element of GF(p) for p = 2^251 + 17 * 2^192 + 1.
  The value is stored in Montgomery representation.
*/
class PrimeFieldElement {
 public:
  using ValueType = BigInt<4>;
  static constexpr ValueType kModulus =
      0x800000000000011000000000000000000000000000000000000000000000001_Z;
  static constexpr ValueType kMontgomeryR =
      0x7fffffffffffdf0ffffffffffffffffffffffffffffffffffffffffffffffe1_Z;
  static constexpr ValueType kMontgomeryRSquared =
      0x7ffd4ab5e008810ffffffffff6f800000000001330ffffffffffd737e000401_Z;
  static constexpr ValueType kMontgomeryRCubed =
      0x38e5f79873c0a6df47d84f8363000187545706677ffcc06cc7177d1406df18e_Z;
  static constexpr uint64_t kMontgomeryMPrime = ~uint64_t(0);
  static constexpr ValueType kHalfMultiplicativeGroupSize =
      0x400000000000008800000000000000000000000000000000000000000000000_Z;

  PrimeFieldElement() = delete;

  static PrimeFieldElement FromUint(uint64_t val) {
    return PrimeFieldElement(
        // Note that because MontgomeryMul divides by r we need to multiply by r^2 here.
        MontgomeryMul(ValueType(val), kMontgomeryRSquared));
  }

  static constexpr PrimeFieldElement FromBigInt(const ValueType& val) {
    return PrimeFieldElement(
        // Note that because MontgomeryMul divides by r we need to multiply by r^2 here.
        MontgomeryMul(val, kMontgomeryRSquared));
  }

  static PrimeFieldElement RandomElement(Prng* prng);

  static constexpr PrimeFieldElement Zero() { return PrimeFieldElement(ValueType({})); }

  static constexpr PrimeFieldElement One() { return PrimeFieldElement(kMontgomeryR); }

  PrimeFieldElement operator*(const PrimeFieldElement& rhs) const {
    return PrimeFieldElement(MontgomeryMul(value_, rhs.value_));
  }

  PrimeFieldElement operator+(const PrimeFieldElement& rhs) const {
    return PrimeFieldElement{ValueType::ReduceIfNeeded(value_ + rhs.value_, kModulus)};
  }

  PrimeFieldElement operator-(const PrimeFieldElement& rhs) const {
    return PrimeFieldElement{(value_ >= rhs.value_) ? (value_ - rhs.value_)
                                                    : (value_ + kModulus - rhs.value_)};
  }

  PrimeFieldElement operator-() const { return Zero() - *this; }

  PrimeFieldElement operator/(const PrimeFieldElement& rhs) const { return *this * rhs.Inverse(); }

  bool operator==(const PrimeFieldElement& rhs) const { return value_ == rhs.value_; }
  bool operator!=(const PrimeFieldElement& rhs) const { return !(*this == rhs); }

  PrimeFieldElement Inverse() const {
    ASSERT(*this != PrimeFieldElement::Zero(), "Zero does not have an inverse");
    return Pow((kModulus - 0x2_Z).ToBoolVector());
  }

  /*
    Returns the power of a field element, where exponent_bits[0] is the least significant bit of the
    exponent.
    Note that this function doesn't support negative exponents.
  */
  PrimeFieldElement Pow(const std::vector<bool>& exponent_bits) const;

  /*
    Returns the power of a field element for the given exponent.
  */
  PrimeFieldElement Pow(const uint64_t exponent) const;

  /*
    For a field element x, returns true if there exists a field element y such that x = y^2.
  */
  bool IsSquare() const;

  /*
    For a field element x, returns an element y such that y^2 = x. If no such y exists, the function
    throws an exception.
  */
  PrimeFieldElement Sqrt() const;

  /*
    Returns the standard representation.

    A value in the range [0, kBigPrimeConstants::kModulus) in non-Montogomery representation.
  */
  ValueType ToStandardForm() const { return MontgomeryMul(value_, ValueType::One()); }

  std::string ToString() const { return ToStandardForm().ToString(); }

 private:
  explicit constexpr PrimeFieldElement(ValueType val) : value_(val) {}

  static constexpr ValueType MontgomeryMul(const ValueType& x, const ValueType& y) {
    return ValueType::MontMul(x, y, kModulus, kMontgomeryMPrime);
  }

  ValueType value_;
};

inline std::ostream& operator<<(std::ostream& out, const PrimeFieldElement& element) {
  return out << element.ToString();
}

}  // namespace starkware

#endif  // STARKWARE_ALGEBRA_PRIME_FIELD_ELEMENT_H_
