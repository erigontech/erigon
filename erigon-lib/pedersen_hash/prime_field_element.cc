#include "prime_field_element.h"

namespace starkware {

PrimeFieldElement PrimeFieldElement::RandomElement(Prng* prng) {
  constexpr size_t kMostSignificantLimb = ValueType::LimbCount() - 1;
  static_assert(
      kModulus[kMostSignificantLimb] != 0, "We assume kModulus[kMostSignificantLimb] is not zero");
  constexpr uint64_t kBitsMask = (Pow2(Log2Floor(kModulus[kMostSignificantLimb]) + 1)) - 1;

  PrimeFieldElement random_element = PrimeFieldElement::Zero();
  do {
    random_element.value_ = ValueType::RandomBigInt(prng);
    random_element.value_[kMostSignificantLimb] &= kBitsMask;
  } while (random_element.value_ >= kModulus);  // Required to enforce uniformity.

  return random_element;
}

PrimeFieldElement PrimeFieldElement::Pow(const std::vector<bool>& exponent_bits) const {
  return GenericPow(
      *this, exponent_bits, PrimeFieldElement::One(),
      [](const PrimeFieldElement& multiplier, PrimeFieldElement* dst) {
        *dst = *dst * multiplier;
      });
}

PrimeFieldElement PrimeFieldElement::Pow(const uint64_t exponent) const {
  return Pow(BigInt<1>(exponent).ToBoolVector());
}

bool PrimeFieldElement::IsSquare() const {
  if (*this == PrimeFieldElement::Zero()) {
    return true;
  }

  // value is a square if and only if value^((p-1) / 2) = 1.
  return Pow(kHalfMultiplicativeGroupSize.ToBoolVector()) == PrimeFieldElement::One();
}

PrimeFieldElement PrimeFieldElement::Sqrt() const {
  if (*this == PrimeFieldElement::Zero()) {
    return PrimeFieldElement::Zero();
  }

  // We use the following algorithm to compute the square root of the element:
  // Let v be the input, let +r and -r be the roots of v and consider the ring
  //   R := F[x] / (x^2 - v).
  //
  // This ring is isomorphic to the ring F x F where the isomorphism is given by the map
  //   a*x + b --> (ar + b, -ar + b)  (recall that we don't know r, so we cannot compute this map).
  //
  // Pick a random element x + b in R, and compute (x + b)^((p-1)/2). Let's say that the result is
  // c*x + d.
  // Taking a random element in F to the power of (p-1)/2 gives +1 or -1 with probability
  // 0.5. Since R is isomorphic to F x F (where multiplication is pointwise), the result of the
  // computation will be one of the four pairs:
  //   (+1, +1), (-1, -1), (+1, -1), (-1, +1).
  //
  // If the result is (+1, +1) or (-1, -1) (which are the elements (0*x + 1) and (0*x - 1) in R) -
  // try again with another random element.
  //
  // If the result is (+1, -1) then cr + d = 1 and -cr + d = -1. Therefore r = c^{-1} and d=0. In
  // the second case -r = c^{-1}. In both cases c^{-1} will be the returned root.

  // Store an element in R as a pair: first * x + second.
  using RingElement = std::pair<PrimeFieldElement, PrimeFieldElement>;
  const RingElement one{PrimeFieldElement::Zero(), PrimeFieldElement::One()};
  const RingElement minus_one{PrimeFieldElement::Zero(), -PrimeFieldElement::One()};

  auto mult = [this](const RingElement& multiplier, RingElement* dst) {
    // Compute res * multiplier in the ring.
    auto res_first = multiplier.first * dst->second + multiplier.second * dst->first;
    auto res_second = multiplier.first * dst->first * *this + multiplier.second * dst->second;
    *dst = {res_first, res_second};
  };

  // Compute q = (p - 1) / 2 and get its bits.
  const std::vector<bool> q_bits = kHalfMultiplicativeGroupSize.ToBoolVector();

  Prng prng;
  while (true) {
    // Pick a random element (x + b) in R.
    RingElement random_element{PrimeFieldElement::One(), PrimeFieldElement::RandomElement(&prng)};

    // Compute the exponentiation: random_element ^ ((p-1) / 2).
    RingElement res = GenericPow(random_element, q_bits, one, mult);

    // If res is either 1 or -1, try again.
    if (res == one || res == minus_one) {
      continue;
    }

    const PrimeFieldElement root = res.first.Inverse();

    ASSERT(root * root == *this, "value does not have a square root.");

    return root;
  }
}

}  // namespace starkware
