#include "pedersen_hash.h"

#include <vector>

#include "elliptic_curve.h"
#include "fraction_field_element.h"
#include "elliptic_curve_constants.h"
#include "error_handling.h"

namespace starkware {

namespace {

EcPoint<FractionFieldElement<PrimeFieldElement>> EcSubsetSumHash(
    const EcPoint<FractionFieldElement<PrimeFieldElement>>& shift_point,
    gsl::span<const EcPoint<PrimeFieldElement>> points, const PrimeFieldElement& selector_value) {
  using FractionFieldElementT = FractionFieldElement<PrimeFieldElement>;
  const auto selector_value_as_big_int = selector_value.ToStandardForm();
  const std::vector<bool> selector_bits = selector_value_as_big_int.ToBoolVector();
  ASSERT(points.size() <= selector_bits.size(), "Too many points.");

  auto partial_sum = shift_point;
  for (size_t j = 0; j < points.size(); j++) {
    const auto point = points[j].template ConvertTo<FractionFieldElementT>();
    ASSERT(partial_sum.x != point.x, "Adding a point to itself or to its inverse point.");
    if (selector_bits[j]) {
      partial_sum = partial_sum + point;
    }
  }
  for (size_t j = points.size(); j < selector_bits.size(); j++) {
    ASSERT(selector_bits[j] == 0, "Given selector is too big.");
  }
  return partial_sum;
}

}  // namespace

PrimeFieldElement PedersenHash(const PrimeFieldElement& x, const PrimeFieldElement& y) {
  const size_t n_element_bits = 252;
  const auto& consts = GetEcConstants();
  const auto& shift_point = consts.k_points[0];
  const auto points_span = gsl::make_span(consts.k_points).subspan(2);

  auto cur_sum = shift_point.template ConvertTo<FractionFieldElement<PrimeFieldElement>>();
  cur_sum = EcSubsetSumHash(cur_sum, points_span.subspan(0, n_element_bits), x);
  cur_sum = EcSubsetSumHash(cur_sum, points_span.subspan(n_element_bits, n_element_bits), y);

  const EcPoint<PrimeFieldElement> res = cur_sum.template ConvertTo<PrimeFieldElement>();
  return res.x;
}

}  // namespace starkware
