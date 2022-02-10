#ifndef STARKWARE_CRYPTO_PEDERSEN_HASH_H_
#define STARKWARE_CRYPTO_PEDERSEN_HASH_H_

#include "gsl-lite.hpp"

#include "prime_field_element.h"

namespace starkware {

/*
  Computes the Starkware version of the Pedersen hash of x and y.
  The hash is defined by:
    shift_point + x_low * P_0 + x_high * P1 + y_low * P2  + y_high * P3
  where x_low is the 248 low bits of x, x_high is the 4 high bits of x and similarly for y.
  shift_point, P_0, P_1, P_2, P_3 are constant points generated from the digits of pi.
*/
PrimeFieldElement PedersenHash(const PrimeFieldElement& x, const PrimeFieldElement& y);

}  // namespace starkware

#endif  // STARKWARE_CRYPTO_PEDERSEN_HASH_H_
