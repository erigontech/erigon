//! Core NTT algorithms: forward, inverse, vectorized mul/add.

use num_bigint::BigUint;

use crate::field::{mod_add, mod_mul, mod_sub, FieldParams};

/// Forward NTT (Cooley-Tukey butterfly, negative wrapped convolution: https://eprint.iacr.org/2016/504.pdf).
pub fn ntt_fw(a: &[BigUint], params: &FieldParams) -> Vec<BigUint> {
    let n = params.n;
    assert_eq!(a.len(), n, "input length must equal n");

    let q = &params.q;
    let psi_rev = params.psi_rev_table();

    let mut a = a.to_vec();

    let mut t = n;
    let mut m = 1;
    while m < n {
        t /= 2;
        for i in 0..m {
            let j1 = 2 * i * t;
            let j2 = j1 + t;
            let s = &psi_rev[m + i];
            for j in j1..j2 {
                let u = a[j].clone();
                let v = mod_mul(&a[j + t], s, q);
                a[j] = mod_add(&u, &v, q);
                a[j + t] = mod_sub(&u, &v, q);
            }
        }
        m *= 2;
    }

    a
}

/// Inverse NTT (Gentleman-Sande butterfly, negative wrapped convolution).
///
/// Input: coefficients `a` in bit-reversed order (length `n`), field params.
/// Output: polynomial in standard order.
///
/// Follows the EIP pseudocode exactly.
pub fn ntt_inv(a: &[BigUint], params: &FieldParams) -> Vec<BigUint> {
    let n = params.n;
    assert_eq!(a.len(), n, "input length must equal n");

    let q = &params.q;
    let psi_inv_rev = params.psi_inv_rev_table();
    let n_inv = params.n_inv();

    let mut a = a.to_vec();

    let mut t = 1;
    let mut m = n;
    while m > 1 {
        let h = m / 2;
        let mut j1 = 0;
        for i in 0..h {
            let j2 = j1 + t;
            let s = &psi_inv_rev[h + i];
            for j in j1..j2 {
                let u = a[j].clone();
                let v = a[j + t].clone();
                a[j] = mod_add(&u, &v, q);
                a[j + t] = mod_mul(&mod_sub(&u, &v, q), s, q);
            }
            j1 += 2 * t;
        }
        t *= 2;
        m /= 2;
    }

    // Multiply by n⁻¹
    for coeff in &mut a {
        *coeff = mod_mul(coeff, &n_inv, q);
    }

    a
}

/// Vectorized element-wise modular multiplication.
///
/// Output[i] = a[i] * b[i] mod q
pub fn vec_mul_mod(a: &[BigUint], b: &[BigUint], q: &BigUint) -> Vec<BigUint> {
    assert_eq!(a.len(), b.len(), "vectors must have equal length");
    a.iter()
        .zip(b.iter())
        .map(|(ai, bi)| mod_mul(ai, bi, q))
        .collect()
}

/// Vectorized element-wise modular addition.
///
/// Output[i] = (a[i] + b[i]) mod q
pub fn vec_add_mod(a: &[BigUint], b: &[BigUint], q: &BigUint) -> Vec<BigUint> {
    assert_eq!(a.len(), b.len(), "vectors must have equal length");
    a.iter()
        .zip(b.iter())
        .map(|(ai, bi)| mod_add(ai, bi, q))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::mod_pow;
    use num_traits::One;

    fn falcon_params() -> FieldParams {
        let q = BigUint::from(12289u64);
        let n: usize = 512;
        let g = BigUint::from(11u64);
        let exp = (&q - BigUint::one()) / BigUint::from(2u64 * n as u64);
        let psi = mod_pow(&g, &exp, &q);
        FieldParams::new(q, n, psi).unwrap()
    }

    fn small_params() -> FieldParams {
        let q = BigUint::from(17u64);
        let n: usize = 4;
        let psi = BigUint::from(9u64);
        FieldParams::new(q, n, psi).unwrap()
    }

    #[test]
    fn test_ntt_roundtrip_small() {
        let params = small_params();
        let a: Vec<BigUint> = vec![1u64, 2, 3, 4]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let ntt_a = ntt_fw(&a, &params);
        let recovered = ntt_inv(&ntt_a, &params);

        assert_eq!(a, recovered, "NTT forward + inverse must recover the original");
    }

    #[test]
    fn test_ntt_roundtrip_falcon() {
        let params = falcon_params();
        let n = params.n;
        let a: Vec<BigUint> = (0..n).map(|i| BigUint::from(i as u64) % &params.q).collect();

        let ntt_a = ntt_fw(&a, &params);
        let recovered = ntt_inv(&ntt_a, &params);

        assert_eq!(a, recovered, "Falcon roundtrip must work");
    }

    #[test]
    fn test_polynomial_multiplication() {
        let params = small_params();
        let q = &params.q;
        let n = params.n;

        let f: Vec<BigUint> = vec![1u64, 2, 3, 4]
            .into_iter()
            .map(BigUint::from)
            .collect();
        let g: Vec<BigUint> = vec![5u64, 6, 7, 8]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let ntt_f = ntt_fw(&f, &params);
        let ntt_g = ntt_fw(&g, &params);
        let ntt_product = vec_mul_mod(&ntt_f, &ntt_g, q);
        let product = ntt_inv(&ntt_product, &params);

        let mut expected = vec![BigUint::from(0u64); n];
        for i in 0..n {
            for j in 0..n {
                let idx = i + j;
                if idx < n {
                    expected[idx] =
                        crate::field::mod_add(&expected[idx], &mod_mul(&f[i], &g[j], q), q);
                } else {
                    let idx = idx - n;
                    expected[idx] =
                        crate::field::mod_sub(&expected[idx], &mod_mul(&f[i], &g[j], q), q);
                }
            }
        }

        assert_eq!(
            product, expected,
            "NTT-based polynomial multiplication must match schoolbook"
        );
    }

    #[test]
    fn test_vec_ops() {
        let q = BigUint::from(17u64);
        let a: Vec<BigUint> = vec![5u64, 10, 15, 3]
            .into_iter()
            .map(BigUint::from)
            .collect();
        let b: Vec<BigUint> = vec![4u64, 8, 3, 16]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let sum = vec_add_mod(&a, &b, &q);
        assert_eq!(sum, vec![
            BigUint::from(9u64),
            BigUint::from(1u64),
            BigUint::from(1u64),
            BigUint::from(2u64),
        ]);

        let prod = vec_mul_mod(&a, &b, &q);
        assert_eq!(prod, vec![
            BigUint::from(3u64),
            BigUint::from(12u64),
            BigUint::from(11u64),
            BigUint::from(14u64),
        ]);
    }
}
