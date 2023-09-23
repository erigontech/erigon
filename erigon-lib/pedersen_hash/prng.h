#ifndef STARKWARE_UTILS_PRNG_H_
#define STARKWARE_UTILS_PRNG_H_

#include <limits>
#include <random>

namespace starkware {

class Prng {
 public:
  Prng() : mt_prng_(std::random_device()()) {}

  /*
    Returns a random integer in the range [lower_bound, upper_bound].
  */
  uint64_t RandomUint64(uint64_t lower_bound, uint64_t upper_bound) {
    return std::uniform_int_distribution<uint64_t>(lower_bound, upper_bound)(mt_prng_);
  }

  /*
    Returns a random integer in the range [0, 2^64).
    Note: This random number generator is NOT cryptographically secure.
  */
  uint64_t RandomUint64() { return RandomUint64(0, std::numeric_limits<uint64_t>::max()); }

 private:
  std::mt19937 mt_prng_;
};

}  // namespace starkware

#endif  // STARKWARE_UTILS_PRNG_H_
