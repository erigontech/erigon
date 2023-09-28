#include <iomanip>
#include <ios>
#include <limits>
#include <sstream>
#include <tuple>

#include "math.h"

namespace starkware {

template <size_t N>
BigInt<N> BigInt<N>::RandomBigInt(Prng* prng) {
  std::array<uint64_t, N> value{};
  for (size_t i = 0; i < N; ++i) {
    gsl::at(value, i) = prng->RandomUint64();
  }
  return BigInt(value);
}

template <size_t N>
template <size_t K>
constexpr BigInt<N>::BigInt(const BigInt<K>& v) noexcept : value_{} {
  static_assert(N > K, "trimming is not supported");
  for (size_t i = 0; i < K; ++i) {
    gsl::at(value_, i) = v[i];
  }

  for (size_t i = K; i < N; ++i) {
    gsl::at(value_, i) = 0;
  }
}

template <size_t N>
constexpr std::pair<BigInt<N>, bool> BigInt<N>::Add(const BigInt& a, const BigInt& b) {
  bool carry{};
  BigInt r{0};

  for (size_t i = 0; i < N; ++i) {
    __uint128_t res = static_cast<__uint128_t>(a[i]) + b[i] + carry;
    carry = (res >> 64) != static_cast<__uint128_t>(0);
    r[i] = static_cast<uint64_t>(res);
  }

  return std::make_pair(r, carry);
}

template <size_t N>
constexpr BigInt<2 * N> BigInt<N>::operator*(const BigInt<N>& other) const {
  constexpr auto kResSize = 2 * N;
  BigInt<kResSize> final_res = BigInt<kResSize>::Zero();
  // Multiply this by other using long multiplication algorithm.
  for (size_t i = 0; i < N; ++i) {
    uint64_t carry = static_cast<uint64_t>(0U);
    for (size_t j = 0; j < N; ++j) {
      // For M == UINT64_MAX, we have: a*b+c+d <= M*M + 2M = (M+1)^2 - 1 ==
      // UINT128_MAX. So we can do a multiplication and an addition without an
      // overflow.
      __uint128_t res = Umul128((*this)[j], other[i]) + final_res[i + j] + carry;
      carry = gsl::narrow_cast<uint64_t>(res >> 64);
      final_res[i + j] = gsl::narrow_cast<uint64_t>(res);
    }
    final_res[i + N] = static_cast<uint64_t>(carry);
  }
  return final_res;
}

template <size_t N>
BigInt<N> BigInt<N>::MulMod(const BigInt& a, const BigInt& b, const BigInt& modulus) {
  const BigInt<2 * N> mul_res = a * b;
  const BigInt<2 * N> mul_res_mod = mul_res.Div(BigInt<2 * N>(modulus)).second;

  BigInt<N> res = Zero();

  // Trim mul_res_mod to the N lower limbs (this is possible since it must be smaller than modulus).
  for (size_t i = 0; i < N; ++i) {
    res[i] = mul_res_mod[i];
  }

  return res;
}

template <size_t N>
BigInt<N> BigInt<N>::InvModPrime(const BigInt& prime) const {
  ASSERT(*this != BigInt::Zero(), "Inverse of 0 is not defined.");
  return GenericPow(
      *this, (prime - BigInt(2)).ToBoolVector(), BigInt::One(),
      [&prime](const BigInt& multiplier, BigInt* dst) { *dst = MulMod(*dst, multiplier, prime); });
}

template <size_t N>
constexpr std::pair<BigInt<N>, bool> BigInt<N>::Sub(const BigInt& a, const BigInt& b) {
  bool carry{};
  BigInt r{};

  for (size_t i = 0; i < N; ++i) {
    __uint128_t res = static_cast<__uint128_t>(a[i]) - b[i] - carry;
    carry = (res >> 127) != static_cast<__uint128_t>(0);
    r[i] = static_cast<uint64_t>(res);
  }

  return std::make_pair(r, carry);
}

template <size_t N>
constexpr bool BigInt<N>::operator<(const BigInt& b) const {
  return Sub(*this, b).second;
}

template <size_t N>
std::pair<BigInt<N>, BigInt<N>> BigInt<N>::Div(const BigInt& divisor) const {
  // This is a simple long-division implementation. It is not very efficient and can be improved
  // if this function becomes a bottleneck.
  ASSERT(divisor != BigInt::Zero(), "Divisor must not be zero.");

  bool carry{};
  BigInt res{};
  BigInt shifted_divisor{}, tmp{};
  BigInt a = *this;

  while (a >= divisor) {
    tmp = divisor;
    int shift = -1;
    do {
      shifted_divisor = tmp;
      shift++;
      std::tie(tmp, carry) = Add(shifted_divisor, shifted_divisor);
    } while (!carry && tmp <= a);

    a = Sub(a, shifted_divisor).first;
    res[shift / 64] |= Pow2(shift % 64);
  }

  return {res, a};
}

template <size_t N>
std::string BigInt<N>::ToString() const {
  std::ostringstream res;
  res << "0x";
  for (int i = N - 1; i >= 0; --i) {
    res << std::setfill('0') << std::setw(16) << std::hex << (*this)[i];
  }
  return res.str();
}

template <size_t N>
std::vector<bool> BigInt<N>::ToBoolVector() const {
  std::vector<bool> res;
  for (uint64_t value : value_) {
    for (int i = 0; i < std::numeric_limits<uint64_t>::digits; ++i) {
      res.push_back((value & 1) != 0);
      value >>= 1;
    }
  }
  return res;
}

template <size_t N>
constexpr bool BigInt<N>::operator==(const BigInt<N>& other) const {
  for (size_t i = 0; i < N; ++i) {
    if (gsl::at(value_, i) != gsl::at(other.value_, i)) {
      return false;
    }
  }
  return true;
}

template <size_t N>
constexpr BigInt<N> BigInt<N>::ReduceIfNeeded(const BigInt<N>& x, const BigInt<N>& target) {
  ASSERT(target.NumLeadingZeros() > 0, "target must have at least one leading zero.");
  return (x >= target) ? x - target : x;
}

template <size_t N>
constexpr BigInt<N> BigInt<N>::MontMul(
    const BigInt& x, const BigInt& y, const BigInt& modulus, uint64_t montgomery_mprime) {
  BigInt<N> res{};
  ASSERT(modulus.NumLeadingZeros() > 0, "We require at least one leading zero in the modulus");
  ASSERT(y < modulus, "y is supposed to be smaller then the modulus");
  ASSERT(x < modulus, "x is supposed to be smaller then the modulus.");
  for (size_t i = 0; i < N; ++i) {
    __uint128_t temp = Umul128(x[i], y[0]) + res[0];
    uint64_t u_i = gsl::narrow_cast<uint64_t>(temp) * montgomery_mprime;
    uint64_t carry1 = 0, carry2 = 0;

    for (size_t j = 0; j < N; ++j) {
      if (j != 0) {
        temp = Umul128(x[i], y[j]) + res[j];
      }
      uint64_t low = carry1 + gsl::narrow_cast<uint64_t>(temp);
      carry1 = gsl::narrow_cast<uint64_t>(temp >> 64) + static_cast<uint64_t>(low < carry1);
      temp = Umul128(modulus[j], u_i) + carry2;
      res[j] = low + gsl::narrow_cast<uint64_t>(temp);
      carry2 = gsl::narrow_cast<uint64_t>(temp >> 64) + static_cast<uint64_t>(res[j] < low);
    }
    for (size_t j = 0; j < N - 1; ++j) {
      res[j] = res[j + 1];
    }
    res[N - 1] = carry1 + carry2;
    ASSERT(res[N - 1] >= carry1, "There shouldn't be a carry here.");
  }
  return ReduceIfNeeded(res, modulus);
}

template <size_t N>
constexpr size_t BigInt<N>::NumLeadingZeros() const {
  int i = value_.size() - 1;
  size_t res = 0;

  while (i >= 0 && (gsl::at(value_, i) == 0)) {
    i--;
    res += std::numeric_limits<uint64_t>::digits;
  }

  if (i >= 0) {
    res += __builtin_clzll(gsl::at(value_, i));
  }

  return res;
}

template <size_t N>
std::ostream& operator<<(std::ostream& os, const BigInt<N>& bigint) {
  return os << bigint.ToString();
}

namespace bigint {
namespace details {
/*
  Converts an hex digit ASCII char to the corresponding int.
  Assumes the input is an hex digit.
*/
inline constexpr uint64_t HexCharToUint64(char c) {
  if ('0' <= c && c <= '9') {
    return c - '0';
  }

  if ('A' <= c && c <= 'F') {
    return c - 'A' + 10;
  }

  // The function assumes that the input is an hex digit, so we can assume 'a'
  // <= c && c <= 'f' here.
  return c - 'a' + 10;
}

template <char... Chars>
constexpr auto HexCharArrayToBigInt() {
  constexpr size_t kLen = sizeof...(Chars);
  constexpr std::array<char, kLen> kDigits{Chars...};
  static_assert(kDigits[0] == '0' && kDigits[1] == 'x', "Only hex input is currently supported");

  constexpr size_t kNibblesPerUint64 = 2 * sizeof(uint64_t);
  constexpr size_t kResLen = (kLen - 2 + kNibblesPerUint64 - 1) / (kNibblesPerUint64);
  std::array<uint64_t, kResLen> res{};

  for (size_t i = 0; i < kDigits.size() - 2; ++i) {
    const size_t limb = i / kNibblesPerUint64;
    const size_t nibble_offset = i % kNibblesPerUint64;
    const uint64_t nibble = HexCharToUint64(gsl::at(kDigits, kDigits.size() - i - 1));

    gsl::at(res, limb) |= nibble << (4 * nibble_offset);
  }

  return BigInt<res.size()>(res);
}
}  // namespace details
}  // namespace bigint

template <char... Chars>
static constexpr auto operator"" _Z() {
  // This function is implemented as wrapper that calls the actual
  // implementation and stores it in a constexpr variable as we want to force
  // the evaluation to be done in compile time. We need to have the function
  // call because "constexpr auto kRes = BigInt<res.size()>(res);" won't work
  // unless res is constexpr.

  // Note that the compiler allows HEX and decimal literals but in any case
  // it enforces that Chars... contains only HEX (or decimal) characters.
  constexpr auto kRes = bigint::details::HexCharArrayToBigInt<Chars...>();
  return kRes;
}

}  // namespace starkware
