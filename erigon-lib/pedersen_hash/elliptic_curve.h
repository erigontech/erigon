#ifndef STARKWARE_ALGEBRA_ELLIPTIC_CURVE_H_
#define STARKWARE_ALGEBRA_ELLIPTIC_CURVE_H_

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "gsl-lite.hpp"

#include "big_int.h"

namespace starkware {

using std::size_t;

/*
  Represents a point on an elliptic curve of the form: y^2 = x^3 + alpha*x + beta.
*/
template <typename FieldElementT>
class EcPoint {
 public:
  constexpr EcPoint(const FieldElementT& x, const FieldElementT& y) : x(x), y(y) {}

  bool operator==(const EcPoint& rhs) const { return x == rhs.x && y == rhs.y; }
  bool operator!=(const EcPoint& rhs) const { return !(*this == rhs); }

  /*
    Computes the point added to itself.
  */
  EcPoint Double(const FieldElementT& alpha) const;

  /*
    Returns the sum of two points. The added point must be different than both the original point
    and its negation.
  */
  EcPoint operator+(const EcPoint& rhs) const;
  EcPoint operator-() const { return EcPoint(x, -y); }
  EcPoint operator-(const EcPoint& rhs) const { return (*this) + (-rhs); }

  /*
    Returns a random point on the curve: y^2 = x^3 + alpha*x + beta.
  */
  static EcPoint Random(const FieldElementT& alpha, const FieldElementT& beta, Prng* prng);

  /*
    Returns one of the two points with the given x coordinate or nullopt if there is no such point.
  */
  static std::optional<EcPoint> GetPointFromX(
      const FieldElementT& x, const FieldElementT& alpha, const FieldElementT& beta);

  template <typename OtherFieldElementT>
  EcPoint<OtherFieldElementT> ConvertTo() const;

  /*
    Given the bool vector representing a scalar, and the alpha of the elliptic curve
    "y^2 = x^3 + alpha * x + beta" the point is on, returns scalar*point.
  */
  template <size_t N>
  EcPoint<FieldElementT> MultiplyByScalar(
      const BigInt<N>& scalar, const FieldElementT& alpha) const;

  FieldElementT x;
  FieldElementT y;

 private:
  /*
    Returns the sum of this point with a point in the form of std::optional, where std::nullopt
    represents the curve's zero element.
  */
  std::optional<EcPoint<FieldElementT>> AddOptionalPoint(
      const std::optional<EcPoint<FieldElementT>>& point, const FieldElementT& alpha) const;
};

}  // namespace starkware

#include "elliptic_curve.inl"

#endif  // STARKWARE_ALGEBRA_ELLIPTIC_CURVE_H_
