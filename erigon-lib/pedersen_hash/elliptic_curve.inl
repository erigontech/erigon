#include "error_handling.h"

namespace starkware {

template <typename FieldElementT>
auto EcPoint<FieldElementT>::Double(const FieldElementT& alpha) const -> EcPoint {
  // Doubling a point cannot be done by adding the point to itself with the function AddPoints
  // because this function assumes that it gets distinct points. Usually, in order to sum two
  // points, one should draw a straight line containing these points, find the third point in the
  // intersection of the line and the curve, and then negate the y coordinate. In the special case
  // where the two points are the same point, one should draw the line that intersects the elliptic
  // curve "twice" at that point. This means that the slope of the line should be equal to the slope
  // of the curve at this point. That is, the derivative of the function
  // y = sqrt(x^3 + alpha * x + beta), which is slope = dy/dx = (3 * x^2 + alpha)/(2 * y). Note that
  // if y = 0 then the point is a 2-torsion (doubling it gives infinity). The line is then given by
  // y = slope * x + y_intercept. The third intersection point is found using the equation that is
  // true for all cases: slope^2 = x_1 + x_2 + x_3 (where x_1, x_2 and x_3 are the x coordinates of
  // three points in the intersection of the curve with a line).
  ASSERT(y != FieldElementT::Zero(), "Tangent slope of 2 torsion point is infinite.");
  const auto x_squared = x * x;
  const FieldElementT tangent_slope = (x_squared + x_squared + x_squared + alpha) / (y + y);
  const FieldElementT x2 = tangent_slope * tangent_slope - (x + x);
  const FieldElementT y2 = tangent_slope * (x - x2) - y;
  return {x2, y2};
}

template <typename FieldElementT>
auto EcPoint<FieldElementT>::operator+(const EcPoint& rhs) const -> EcPoint {
  ASSERT(this->x != rhs.x, "x values should be different for arbitrary points");
  // To sum two points, one should draw a straight line containing these points, find the
  // third point in the intersection of the line and the curve, and then negate the y coordinate.
  // Notice that if x_1 = x_2 then either they are the same point or their sum is infinity. This
  // function doesn't deal with these cases. The straight line is given by the equation:
  // y = slope * x + y_intercept. The x coordinate of the third point is found by solving the system
  // of equations:

  // y = slope * x + y_intercept
  // y^2 = x^3 + alpha * x + beta

  // These equations yield:
  // (slope * x + y_intercept)^2 = x^3 + alpha * x + beta
  // ==> x^3 - slope^2 * x^2 + (alpha  - 2 * slope * y_intercept) * x + (beta - y_intercept^2) = 0

  // This is a monic polynomial in x whose roots are exactly the x coordinates of the three
  // intersection points of the line with the curve. Thus it is equal to the polynomial:
  // (x - x_1) * (x - x_2) * (x - x_3)
  // where x1, x2, x3 are the x coordinates of those points.
  // Notice that the equality of the coefficient of the x^2 term yields:
  // slope^2 = x_1 + x_2 + x_3.
  const FieldElementT slope = (this->y - rhs.y) / (this->x - rhs.x);
  const FieldElementT x3 = slope * slope - this->x - rhs.x;
  const FieldElementT y3 = slope * (this->x - x3) - this->y;
  return {x3, y3};
}

template <typename FieldElementT>
auto EcPoint<FieldElementT>::GetPointFromX(
    const FieldElementT& x, const FieldElementT& alpha, const FieldElementT& beta)
    -> std::optional<EcPoint> {
  const FieldElementT y_squared = x * x * x + alpha * x + beta;
  if (!y_squared.IsSquare()) {
    return std::nullopt;
  }
  return {{x, y_squared.Sqrt()}};
}

template <typename FieldElementT>
auto EcPoint<FieldElementT>::Random(
    const FieldElementT& alpha, const FieldElementT& beta, Prng* prng) -> EcPoint {
  // Each iteration has probability of ~1/2 to fail. Thus the probability of failing 100 iterations
  // is negligible.
  for (size_t i = 0; i < 100; ++i) {
    const FieldElementT x = FieldElementT::RandomElement(prng);
    const std::optional<EcPoint> pt = GetPointFromX(x, alpha, beta);
    if (pt.has_value()) {
      // Change the sign of the returned y coordinate with probability 1/2.
      if (prng->RandomUint64(0, 1) == 1) {
        return -*pt;
      }
      return *pt;
    }
  }
  ASSERT(false, "No random point found.");
}

template <typename FieldElementT>
template <typename OtherFieldElementT>
EcPoint<OtherFieldElementT> EcPoint<FieldElementT>::ConvertTo() const {
  return EcPoint<OtherFieldElementT>(OtherFieldElementT(x), OtherFieldElementT(y));
}

template <typename FieldElementT>
template <size_t N>
EcPoint<FieldElementT> EcPoint<FieldElementT>::MultiplyByScalar(
    const BigInt<N>& scalar, const FieldElementT& alpha) const {
  std::optional<EcPoint<FieldElementT>> res;
  EcPoint<FieldElementT> power = *this;
  for (const auto& b : scalar.ToBoolVector()) {
    if (b) {
      res = power.AddOptionalPoint(res, alpha);
    }
    // If power == -power, then power + power == zero, and will remain zero (so res will not
    // change) until the end of the for loop. Therefore there is no point to keep looping.
    if (power == -power) {
      break;
    }
    power = power.Double(alpha);
  }
  ASSERT(res.has_value(), "Result of multiplication is the curve's zero element.");
  return *res;
}

template <typename FieldElementT>
std::optional<EcPoint<FieldElementT>> EcPoint<FieldElementT>::AddOptionalPoint(
    const std::optional<EcPoint<FieldElementT>>& point, const FieldElementT& alpha) const {
  if (!point) {
    return *this;
  }
  // If a == -b, then a+b == zero element.
  if (*point == -*this) {
    return std::nullopt;
  }
  if (*point == *this) {
    return point->Double(alpha);
  }
  return *point + *this;
}

}  // namespace starkware
