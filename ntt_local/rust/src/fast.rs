use crate::field::bit_reverse;

// ═══════════════════════════════════════════════════════════════════
//  Public API
// ═══════════════════════════════════════════════════════════════════

/// Pre-computed NTT parameters with automatic Montgomery optimization.
///
/// For q < 2^31, butterflies use u32 Montgomery multiplication
/// (replacing expensive u128 division with cheap multiply-shift).
/// For larger q, falls back to u64 with u128 intermediates.
///
/// Twiddle factor tables are precomputed at construction time.
pub struct FastNttParams {
    /// The prime modulus.
    pub q: u64,
    /// The polynomial dimension (power of 2).
    pub n: usize,
    /// Byte length of one coefficient: ceil(bits(q) / 8).
    pub coeff_bytes: usize,
    /// Internal arithmetic backend.
    inner: NttInner,
}

enum NttInner {
    /// Montgomery arithmetic for q < 2^31 (all practical lattice schemes).
    Mont(MontData),
    /// Plain u64 arithmetic for q in [2^31, 2^63).
    U64(U64Data),
}

impl FastNttParams {
    /// Create fast NTT parameters.
    ///
    /// Validates:
    /// - n must be a non-zero power of 2
    /// - q must be odd, q ≡ 1 (mod 2n), and q < 2^63
    /// - psi must be a primitive 2n-th root of unity mod q
    ///
    /// Automatically selects Montgomery (q < 2^31) or u64 backend.
    pub fn new(q: u64, n: usize, psi: u64) -> Result<Self, &'static str> {
        if q >> 63 != 0 {
            return Err("q must be < 2^63 for fast path");
        }
        if !n.is_power_of_two() || n == 0 {
            return Err("n must be a power of 2");
        }
        let two_n = 2 * n as u64;
        if q % 2 == 0 || q % two_n != 1 {
            return Err("q must satisfy q ≡ 1 (mod 2n)");
        }
        if pow_mod_64(psi, two_n, q) != 1 {
            return Err("psi is not a 2n-th root of unity");
        }
        if pow_mod_64(psi, n as u64, q) != q - 1 {
            return Err("psi is not a primitive 2n-th root of unity");
        }

        let coeff_bytes = ((64 - q.leading_zeros()) as usize + 7) / 8;

        let inner = if q < (1u64 << 31) {
            NttInner::Mont(MontData::new(q as u32, n, psi as u32))
        } else {
            NttInner::U64(U64Data::new(q, n, psi))
        };

        Ok(Self { q, n, coeff_bytes, inner })
    }
}

/// Forward NTT (Cooley-Tukey butterfly).
///
/// Input: coefficients in standard order. Output: NTT in bit-reversed order.
pub fn ntt_fw_fast(a: &[u64], p: &FastNttParams) -> Vec<u64> {
    debug_assert_eq!(a.len(), p.n);
    match &p.inner {
        NttInner::Mont(m) => ntt_fw_mont(a, p.n, m),
        NttInner::U64(u) => ntt_fw_u64(a, p.n, p.q, u),
    }
}

/// Inverse NTT (Gentleman-Sande butterfly).
///
/// Input: NTT in bit-reversed order. Output: coefficients in standard order.
pub fn ntt_inv_fast(a: &[u64], p: &FastNttParams) -> Vec<u64> {
    debug_assert_eq!(a.len(), p.n);
    match &p.inner {
        NttInner::Mont(m) => ntt_inv_mont(a, p.n, m),
        NttInner::U64(u) => ntt_inv_u64(a, p.n, p.q, u),
    }
}

/// Element-wise modular multiplication.
pub fn vec_mul_mod_fast(a: &[u64], b: &[u64], q: u64) -> Vec<u64> {
    debug_assert_eq!(a.len(), b.len());
    if q < (1u64 << 32) {
        // For q < 2^32, products fit in u64 — use hardware divide
        let q32 = q as u64;
        a.iter()
            .zip(b.iter())
            .map(|(&ai, &bi)| (ai * bi) % q32)
            .collect()
    } else {
        a.iter()
            .zip(b.iter())
            .map(|(&ai, &bi)| mul_mod_64(ai, bi, q))
            .collect()
    }
}

/// Element-wise modular addition.
pub fn vec_add_mod_fast(a: &[u64], b: &[u64], q: u64) -> Vec<u64> {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(&ai, &bi)| add_mod_64(ai, bi, q))
        .collect()
}

// ═══════════════════════════════════════════════════════════════════
//  Montgomery backend (q < 2^31)
// ═══════════════════════════════════════════════════════════════════

struct MontData {
    q: u32,
    q_prime: u32, // -q^(-1) mod 2^32
    r2: u32,      // R^2 mod q, where R = 2^32
    psi_rev: Vec<u32>,     // twiddle factors in Montgomery form
    psi_inv_rev: Vec<u32>, // inverse twiddle factors in Montgomery form
    n_inv_mont: u32,       // n^(-1) in Montgomery form
}

impl MontData {
    fn new(q: u32, n: usize, psi: u32) -> Self {
        let q_prime = compute_q_prime(q);
        let r2 = compute_r2(q);
        let log_n = n.trailing_zeros() as usize;
        let psi_inv = pow_mod_32(psi, q - 2, q);

        let mut psi_rev = vec![0u32; n];
        let mut psi_inv_rev = vec![0u32; n];
        for i in 0..n {
            let rev = bit_reverse(i, log_n);
            psi_rev[i] = to_mont(pow_mod_32(psi, rev as u32, q), r2, q, q_prime);
            psi_inv_rev[i] = to_mont(pow_mod_32(psi_inv, rev as u32, q), r2, q, q_prime);
        }

        let n_inv = pow_mod_32(n as u32, q - 2, q);
        let n_inv_mont = to_mont(n_inv, r2, q, q_prime);

        Self { q, q_prime, r2, psi_rev, psi_inv_rev, n_inv_mont }
    }
}

/// Compute -q^(-1) mod 2^32 via Newton's method.
fn compute_q_prime(q: u32) -> u32 {
    debug_assert!(q % 2 == 1);
    let mut x = 1u32;
    // Each iteration doubles correct bits: 1→2→4→8→16→32
    for _ in 0..5 {
        x = x.wrapping_mul(2u32.wrapping_sub(q.wrapping_mul(x)));
    }
    x.wrapping_neg()
}

/// Compute R^2 mod q where R = 2^32.
fn compute_r2(q: u32) -> u32 {
    let r = (1u64 << 32) % q as u64;
    ((r * r) % q as u64) as u32
}

/// Montgomery reduction: T * R^(-1) mod q, without division.
#[inline(always)]
fn mont_redc(t: u64, q: u32, q_prime: u32) -> u32 {
    let m = (t as u32).wrapping_mul(q_prime);
    let u = (t.wrapping_add(m as u64 * q as u64)) >> 32;
    let r = u as u32;
    if r >= q { r - q } else { r }
}

/// Montgomery multiply: a * b * R^(-1) mod q.
#[inline(always)]
fn mont_mul(a: u32, b: u32, q: u32, q_prime: u32) -> u32 {
    mont_redc(a as u64 * b as u64, q, q_prime)
}

/// Convert to Montgomery form: a → a * R mod q.
#[inline(always)]
fn to_mont(a: u32, r2: u32, q: u32, q_prime: u32) -> u32 {
    mont_mul(a, r2, q, q_prime)
}

/// Convert from Montgomery form: ā → a (= ā * R^(-1) mod q).
#[inline(always)]
fn from_mont(a_mont: u32, q: u32, q_prime: u32) -> u32 {
    mont_redc(a_mont as u64, q, q_prime)
}

/// Modular addition for u32 (q < 2^31 so no overflow).
#[inline(always)]
fn add_mod_32(a: u32, b: u32, q: u32) -> u32 {
    let s = a + b;
    if s >= q { s - q } else { s }
}

/// Modular subtraction for u32.
#[inline(always)]
fn sub_mod_32(a: u32, b: u32, q: u32) -> u32 {
    if a >= b { a - b } else { q + a - b }
}

fn pow_mod_32(mut base: u32, mut exp: u32, q: u32) -> u32 {
    if q == 1 { return 0; }
    let mut result = 1u32;
    base %= q;
    while exp > 0 {
        if exp & 1 == 1 {
            result = ((result as u64 * base as u64) % q as u64) as u32;
        }
        exp >>= 1;
        if exp > 0 {
            base = ((base as u64 * base as u64) % q as u64) as u32;
        }
    }
    result
}

/// Forward NTT using Montgomery multiplication.
fn ntt_fw_mont(input: &[u64], n: usize, m: &MontData) -> Vec<u64> {
    let q = m.q;
    let qp = m.q_prime;
    let r2 = m.r2;

    // Convert input to Montgomery form (u64 → u32 mont)
    let mut a: Vec<u32> = input.iter()
        .map(|&x| to_mont(x as u32, r2, q, qp))
        .collect();

    // Cooley-Tukey butterfly
    let mut t = n;
    let mut sz = 1;
    while sz < n {
        t /= 2;
        for i in 0..sz {
            let j1 = 2 * i * t;
            let s = m.psi_rev[sz + i];
            for j in j1..j1 + t {
                let u = a[j];
                let v = mont_mul(a[j + t], s, q, qp);
                a[j] = add_mod_32(u, v, q);
                a[j + t] = sub_mod_32(u, v, q);
            }
        }
        sz *= 2;
    }

    // Convert from Montgomery form (u32 mont → u64)
    a.iter().map(|&x| from_mont(x, q, qp) as u64).collect()
}

/// Inverse NTT using Montgomery multiplication.
fn ntt_inv_mont(input: &[u64], n: usize, m: &MontData) -> Vec<u64> {
    let q = m.q;
    let qp = m.q_prime;
    let r2 = m.r2;

    let mut a: Vec<u32> = input.iter()
        .map(|&x| to_mont(x as u32, r2, q, qp))
        .collect();

    // Gentleman-Sande butterfly
    let mut t = 1;
    let mut sz = n;
    while sz > 1 {
        let h = sz / 2;
        let mut j1 = 0;
        for i in 0..h {
            let s = m.psi_inv_rev[h + i];
            for j in j1..j1 + t {
                let u = a[j];
                let v = a[j + t];
                a[j] = add_mod_32(u, v, q);
                a[j + t] = mont_mul(sub_mod_32(u, v, q), s, q, qp);
            }
            j1 += 2 * t;
        }
        t *= 2;
        sz /= 2;
    }

    // Scale by n^(-1) (in Montgomery form)
    let n_inv = m.n_inv_mont;
    for x in &mut a {
        *x = mont_mul(*x, n_inv, q, qp);
    }

    // Convert from Montgomery form
    a.iter().map(|&x| from_mont(x, q, qp) as u64).collect()
}

// ═══════════════════════════════════════════════════════════════════
//  u64 backend (q in [2^31, 2^63))
// ═══════════════════════════════════════════════════════════════════

struct U64Data {
    psi_rev: Vec<u64>,
    psi_inv_rev: Vec<u64>,
    n_inv: u64,
}

impl U64Data {
    fn new(q: u64, n: usize, psi: u64) -> Self {
        let log_n = n.trailing_zeros() as usize;
        let psi_inv = pow_mod_64(psi, q - 2, q);

        let mut psi_rev = vec![0u64; n];
        let mut psi_inv_rev = vec![0u64; n];
        for i in 0..n {
            let rev = bit_reverse(i, log_n);
            psi_rev[i] = pow_mod_64(psi, rev as u64, q);
            psi_inv_rev[i] = pow_mod_64(psi_inv, rev as u64, q);
        }

        let n_inv = pow_mod_64(n as u64, q - 2, q);
        Self { psi_rev, psi_inv_rev, n_inv }
    }
}

#[inline(always)]
fn mul_mod_64(a: u64, b: u64, q: u64) -> u64 {
    ((a as u128 * b as u128) % q as u128) as u64
}

#[inline(always)]
fn add_mod_64(a: u64, b: u64, q: u64) -> u64 {
    let (s, overflow) = a.overflowing_add(b);
    if overflow || s >= q { s.wrapping_sub(q) } else { s }
}

#[inline(always)]
fn sub_mod_64(a: u64, b: u64, q: u64) -> u64 {
    if a >= b { a - b } else { q + a - b }
}

fn pow_mod_64(mut base: u64, mut exp: u64, q: u64) -> u64 {
    if q == 1 { return 0; }
    let mut result = 1u64;
    base %= q;
    while exp > 0 {
        if exp & 1 == 1 {
            result = mul_mod_64(result, base, q);
        }
        exp >>= 1;
        if exp > 0 {
            base = mul_mod_64(base, base, q);
        }
    }
    result
}

fn ntt_fw_u64(a: &[u64], n: usize, q: u64, u: &U64Data) -> Vec<u64> {
    let mut a = a.to_vec();
    let mut t = n;
    let mut sz = 1;
    while sz < n {
        t /= 2;
        for i in 0..sz {
            let j1 = 2 * i * t;
            let s = u.psi_rev[sz + i];
            for j in j1..j1 + t {
                let u_val = a[j];
                let v = mul_mod_64(a[j + t], s, q);
                a[j] = add_mod_64(u_val, v, q);
                a[j + t] = sub_mod_64(u_val, v, q);
            }
        }
        sz *= 2;
    }
    a
}

fn ntt_inv_u64(a: &[u64], n: usize, q: u64, u: &U64Data) -> Vec<u64> {
    let mut a = a.to_vec();
    let mut t = 1;
    let mut sz = n;
    while sz > 1 {
        let h = sz / 2;
        let mut j1 = 0;
        for i in 0..h {
            let s = u.psi_inv_rev[h + i];
            for j in j1..j1 + t {
                let u_val = a[j];
                let v = a[j + t];
                a[j] = add_mod_64(u_val, v, q);
                a[j + t] = mul_mod_64(sub_mod_64(u_val, v, q), s, q);
            }
            j1 += 2 * t;
        }
        t *= 2;
        sz /= 2;
    }
    let n_inv = u.n_inv;
    for coeff in &mut a {
        *coeff = mul_mod_64(*coeff, n_inv, q);
    }
    a
}

// ═══════════════════════════════════════════════════════════════════
//  Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::{mod_pow, FieldParams};
    use num_bigint::BigUint;
    use num_traits::One;

    fn falcon_fast() -> FastNttParams {
        let q = 12289u64;
        let n = 512usize;
        let psi = pow_mod_64(11, (q - 1) / (2 * n as u64), q);
        FastNttParams::new(q, n, psi).unwrap()
    }

    fn small_fast() -> FastNttParams {
        FastNttParams::new(17, 4, 9).unwrap()
    }

    #[test]
    fn test_montgomery_constants() {
        let q = 12289u32;
        let qp = compute_q_prime(q);
        assert_eq!(q.wrapping_mul(qp.wrapping_neg()), 1);

        let r2 = compute_r2(q);
        let expected = ((1u128 << 64) % q as u128) as u32;
        assert_eq!(r2, expected);
    }

    #[test]
    fn test_montgomery_roundtrip() {
        let q = 12289u32;
        let qp = compute_q_prime(q);
        let r2 = compute_r2(q);

        for a in [0, 1, 42, 12288, 6144] {
            let a_mont = to_mont(a, r2, q, qp);
            let a_back = from_mont(a_mont, q, qp);
            assert_eq!(a, a_back, "Montgomery roundtrip failed for {a}");
        }
    }

    #[test]
    fn test_montgomery_mul() {
        let q = 12289u32;
        let qp = compute_q_prime(q);
        let r2 = compute_r2(q);

        for &(a, b) in &[(5u32, 7), (12288, 12288), (0, 42), (1, 1)] {
            let expected = ((a as u64 * b as u64) % q as u64) as u32;
            let a_mont = to_mont(a, r2, q, qp);
            let b_mont = to_mont(b, r2, q, qp);
            let r_mont = mont_mul(a_mont, b_mont, q, qp);
            let result = from_mont(r_mont, q, qp);
            assert_eq!(result, expected, "Montgomery mul failed for {a} * {b}");
        }
    }

    #[test]
    fn test_roundtrip_small() {
        let p = small_fast();
        let a = vec![1, 2, 3, 4];
        let fwd = ntt_fw_fast(&a, &p);
        let inv = ntt_inv_fast(&fwd, &p);
        assert_eq!(a, inv);
    }

    #[test]
    fn test_roundtrip_falcon() {
        let p = falcon_fast();
        let a: Vec<u64> = (0..512).map(|i| i % 12289).collect();
        let fwd = ntt_fw_fast(&a, &p);
        let inv = ntt_inv_fast(&fwd, &p);
        assert_eq!(a, inv);
    }

    #[test]
    fn test_polymul_small() {
        let p = small_fast();
        let q = p.q;
        let n = p.n;

        let f = vec![1u64, 2, 3, 4];
        let g = vec![5u64, 6, 7, 8];

        let ntt_f = ntt_fw_fast(&f, &p);
        let ntt_g = ntt_fw_fast(&g, &p);
        let ntt_prod = vec_mul_mod_fast(&ntt_f, &ntt_g, q);
        let product = ntt_inv_fast(&ntt_prod, &p);

        let mut expected = vec![0u64; n];
        for i in 0..n {
            for j in 0..n {
                let c = f[i] * g[j] % q;
                if i + j < n {
                    expected[i + j] = (expected[i + j] + c) % q;
                } else {
                    let idx = i + j - n;
                    expected[idx] = (q + expected[idx] - c) % q;
                }
            }
        }

        assert_eq!(product, expected);
    }

    #[test]
    fn test_matches_biguint() {
        let q_big = BigUint::from(12289u64);
        let g_big = BigUint::from(11u64);
        let exp = (&q_big - BigUint::one()) / BigUint::from(1024u64);
        let psi_big = mod_pow(&g_big, &exp, &q_big);
        let params_big = FieldParams::new(q_big, 512, psi_big).unwrap();

        let fast = falcon_fast();

        let a_u64: Vec<u64> = (0..512).map(|i| (i * 7 + 13) % 12289).collect();
        let a_big: Vec<BigUint> = a_u64.iter().map(|&x| BigUint::from(x)).collect();

        let fwd_fast = ntt_fw_fast(&a_u64, &fast);
        let fwd_big = crate::ntt::ntt_fw(&a_big, &params_big);

        for i in 0..512 {
            assert_eq!(
                fwd_fast[i],
                fwd_big[i].to_u64_digits().first().copied().unwrap_or(0),
                "mismatch at index {i}"
            );
        }
    }

    #[test]
    fn test_dilithium_params() {
        let q = 8380417u64;
        let n = 256usize;
        let psi = pow_mod_64(7, (q - 1) / (2 * n as u64), q);
        let p = FastNttParams::new(q, n, psi).unwrap();
        assert!(matches!(p.inner, NttInner::Mont(_)));

        let a: Vec<u64> = (0..n).map(|i| (i as u64 * 31337) % q).collect();
        let inv = ntt_inv_fast(&ntt_fw_fast(&a, &p), &p);
        assert_eq!(a, inv);
    }

    #[test]
    fn test_falcon_uses_montgomery() {
        let p = falcon_fast();
        assert!(matches!(p.inner, NttInner::Mont(_)));
    }
}
